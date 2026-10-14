// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The process assembly of main.cpp — runtime_t → remote_backends_t → engine_t
// → the s3 connector, file manager, S3 orchestrator and Kafka manager → the
// session factory → the servers — built from the real parts, and its shutdown
// order: servers (stopped, their sessions closed) → session factory (sessions
// drain, the sessions pool stops) → Kafka, S3, file, s3 connector → engine
// (final CHECKPOINT, engine pools stop) → remote backends (registry reset) →
// runtime.

#include "session_stack.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/sql_db_connector.hpp"

#include "backends/backends.hpp"
#include "backends/remote_backends.hpp"
#include "connectors/file/manager.hpp"
#include "connectors/s3/manager.hpp"
#include "frontend/common/protocol_config.hpp"
#include "frontend/flight_sql/server.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "frontend/spark_connect_server/spark_connect_server.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/s3/s3_manager.hpp"
#include "otterbrix/federation/name_resolution.hpp"
#include "runtime/engine.hpp"
#include "runtime/runtime.hpp"
#include "session/session_factory.hpp"

#include <arrow/filesystem/s3fs.h>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>
#include <memory>
#include <string>

using otterstax::session::query_session_t;
using otterstax::test::await_session;
using otterstax::test::stmt;

namespace {

    using namespace std::chrono_literals;

    // A remote statement held on its backend until the test lets it go: the
    // data path reports that it started, then waits for release() (bounded, so
    // a broken test cannot hang the run).
    struct backend_gate_t {
        std::mutex mutex;
        std::condition_variable changed;
        bool started{false};
        bool open{false};

        void enter() {
            std::unique_lock lock(mutex);
            started = true;
            changed.notify_all();
            changed.wait_for(lock, 10s, [this] { return open; });
        }
        bool wait_started(std::chrono::milliseconds timeout) {
            std::unique_lock lock(mutex);
            return changed.wait_for(lock, timeout, [this] { return started; });
        }
        void release() {
            std::lock_guard lock(mutex);
            open = true;
            changed.notify_all();
        }
    };

    backend_gate_t& backend_gate() {
        static backend_gate_t gate;
        return gate;
    }

    class gated_connector final : public mysql::MockConnector {
    public:
        using mysql::MockConnector::MockConnector;
        using mysql::MockConnector::runQuery;

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<components::vector::data_chunk_t>(const boost::mysql::results&)>)
            override {
            backend_gate().enter();
            co_return std::make_unique<components::vector::data_chunk_t>(get_chunk());
        }
    };

    std::unique_ptr<mysql::IConnector> gated_mysql_factory(std::pmr::memory_resource* resource,
                                                           boost::asio::io_context&,
                                                           boost::mysql::connect_params,
                                                           std::string alias) {
        return std::make_unique<gated_connector>(mock_config{.resource = resource}, std::move(alias));
    }

    config::ServiceConfig local_ports() {
        config::ServiceConfig config;
        config.mysql.port = 0;
        config.postgres.port = 0;
        config.flight_sql.host = "127.0.0.1";
        config.flight_sql.port = 0;
        config.spark_connect.host = "127.0.0.1";
        config.spark_connect.port = 0;
        // Fixed sizes: a test does not size its servers from the machine.
        config.runtime.mysql_io.threads = 2;
        config.runtime.postgres_io.threads = 2;
        config.runtime.flight_sql_io.threads = 2;
        config.runtime.spark_connect_io.threads = 2;
        return config;
    }

    template<typename T>
    using actor_ptr = std::unique_ptr<T, actor_zeta::pmr::deleter_t>;

    // main.cpp's actors after the engine, in its order (Kafka, declared last,
    // destroyed first); no Kafka poller without a broker.
    struct backend_actors_t {
        backend_actors_t(std::pmr::memory_resource* resource, actor_zeta::address_t dispatcher)
            : s3_connector(actor_zeta::spawn<conn::s3::ConnectorManager>(resource))
            , file(actor_zeta::spawn<conn::file::FileManager>(resource, dispatcher))
            , s3(actor_zeta::spawn<db::S3Manager>(resource, s3_connector->address(), file->address()))
            , kafka(actor_zeta::spawn<otterstax::kafka::KafkaManager>(resource, dispatcher, /*start_pollers=*/false)) {}

        actor_ptr<conn::s3::ConnectorManager> s3_connector;
        actor_ptr<conn::file::FileManager> file;
        actor_ptr<db::S3Manager> s3;
        actor_ptr<otterstax::kafka::KafkaManager> kafka;
    };

    // main.cpp's four servers over `sessions`, in its order (destroyed in the
    // reverse one, each stopping in its destructor); never started here.
    struct servers_t {
        servers_t(otterstax::runtime::runtime_t& runtime,
                  otterstax::session::session_factory_t& sessions,
                  const config::ServiceConfig& config)
            : flight(std::make_unique<flight::server::flight_sql_server>(flight::server::config_t{
                  .host = config.flight_sql.host,
                  .port = config.flight_sql.port,
                  .resource = runtime.resource(),
                  .sessions = &sessions,
                  .threads = config.runtime.flight_sql_io.threads,
              }))
            , mysql(std::make_unique<frontend::mysql::mysql_server>(frontend::frontend_server_config{
                  .resource = runtime.resource(),
                  .port = config.mysql.port,
                  .sessions = &sessions,
                  .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
                  .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
                  .pool_size = config.runtime.mysql_io.threads,
              }))
            , postgres(std::make_unique<frontend::postgres::postgres_server>(frontend::frontend_server_config{
                  .resource = runtime.resource(),
                  .port = config.postgres.port,
                  .sessions = &sessions,
                  .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
                  .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
                  .pool_size = config.runtime.postgres_io.threads,
              }))
            , spark(std::make_unique<frontend::spark::spark_connect_server>(frontend::spark::SparkConnectServerConfig{
                  .host = config.spark_connect.host,
                  .port = config.spark_connect.port,
                  .resource = runtime.resource(),
                  .sessions = &sessions,
                  .session_timeout = std::chrono::seconds(config.spark_connect.session_timeout_s),
                  .io_threads = config.runtime.spark_connect_io.threads,
              })) {}

        std::unique_ptr<flight::server::flight_sql_server> flight;
        std::unique_ptr<frontend::mysql::mysql_server> mysql;
        std::unique_ptr<frontend::postgres::postgres_server> postgres;
        std::unique_ptr<frontend::spark::spark_connect_server> spark;
    };

    // The session factory over the actors, as main.cpp builds it after the
    // federation metadata is published.
    std::unique_ptr<otterstax::session::session_factory_t> open_sessions(otterstax::runtime::runtime_t& runtime,
                                                                         otterstax::runtime::engine_t& engine,
                                                                         otterstax::backends::remote_backends_t& remote,
                                                                         const backend_actors_t& actors) {
        REQUIRE_FALSE(otterstax::backends::publish_federation_metadata(runtime.resource(), engine, remote)
                          .contains_error());
        auto services = otterstax::session::make_session_services(runtime.resource(),
                                                                  engine.dispatcher_address(),
                                                                  actors.s3->address(),
                                                                  actors.file->address(),
                                                                  actors.kafka->address());
        REQUIRE_FALSE(services.has_error());
        return std::make_unique<otterstax::session::session_factory_t>(runtime, &make_parser, std::move(services.value()));
    }

    std::unique_ptr<otterstax::runtime::engine_t> open_engine(otterstax::runtime::runtime_t& runtime,
                                                              const std::string& dir) {
        config::EngineConfig engine_config;
        engine_config.data_dir = dir;
        auto opened = otterstax::runtime::engine_t::open(runtime, engine_config, otterstax::federation::make_primitives());
        INFO((opened.has_error() ? opened.error().what.c_str() : ""));
        REQUIRE_FALSE(opened.has_error());
        return std::move(opened.value());
    }

    // The remote backends, opened before the engine: `connections` over
    // `connectors` (no connection by default).
    std::unique_ptr<otterstax::backends::remote_backends_t>
    open_remote(otterstax::runtime::runtime_t& runtime,
                const config::ConnectionsConfig& connections = {},
                otterstax::backends::connector_factories_t connectors = otterstax::test::mock_connectors()) {
        auto remote = std::make_unique<otterstax::backends::remote_backends_t>(
            runtime.resource(),
            otterstax::backends::remote_backends_options_t{.connectors = connectors,
                                                           .connector_threads = {2, 2, 2},
                                                           .statements = runtime.backends_scheduler()});
        auto err = remote->open(connections, config::ConnectionRetryConfig{});
        INFO(err.what.c_str());
        REQUIRE_FALSE(err.contains_error());
        return remote;
    }

} // namespace

TEST_CASE("process modules: the connector io pools follow service.runtime.connector_io") {
    config::ConnectionsConfig connections;
    connections.mysql.push_back(config::MysqlConnectionDesc{.alias = "m1", .pool_size = 1});
    connections.mysql.push_back(config::MysqlConnectionDesc{.alias = "m2", .pool_size = 3});
    connections.postgresql.push_back(config::PgConnectionDesc{.alias = "p", .pool_size = 2});

    // Auto: one io thread per connection of the backend kind, at least one
    // for a kind with no connection.
    const auto automatic = otterstax::backends::connector_io_threads(0, connections);
    CHECK(automatic.mysql == 4);
    CHECK(automatic.postgres == 2);
    CHECK(automatic.clickhouse == 1);

    const auto none = otterstax::backends::connector_io_threads(0, config::ConnectionsConfig{});
    CHECK(none.mysql == 1);
    CHECK(none.postgres == 1);
    CHECK(none.clickhouse == 1);

    // A configured size is each manager's, whatever the connections.
    const auto fixed = otterstax::backends::connector_io_threads(3, connections);
    CHECK(fixed.mysql == 3);
    CHECK(fixed.postgres == 3);
    CHECK(fixed.clickhouse == 3);
}

// A statement holds a backends thread for its whole remote call: the auto size
// covers every connection, an explicit one is kept.
TEST_CASE("process modules: the backends pool covers the remote connections") {
    config::ConnectionsConfig connections;
    connections.mysql.push_back(config::MysqlConnectionDesc{.alias = "m1", .pool_size = 40});
    connections.postgresql.push_back(config::PgConnectionDesc{.alias = "p", .pool_size = 30});
    connections.clickhouse.push_back(config::ChConnectionDesc{.alias = "c", .pool_size = 30});
    const auto machine = otterstax::runtime::resolve_threads(0);

    CHECK(otterstax::backends::backends_threads(0, connections) == std::max<std::size_t>(machine, 100));
    CHECK(otterstax::backends::backends_threads(0, config::ConnectionsConfig{}) == machine);
    CHECK(otterstax::backends::backends_threads(128, connections) == 128);
}

// A descriptor the connector can never accept is the one configuration error
// of the open, found before any connection is attempted; a failure an attempt
// reports — whatever its code — never aborts.
TEST_CASE("process modules: a malformed descriptor aborts the remote open before any connection") {
    otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
    for (const auto& [port, pool_size] : std::vector<std::pair<std::string, int>>{{"99999", 1}, {"abc", 1}, {"", 0}}) {
        INFO("port '" << port << "', pool_size " << pool_size);
        config::ConnectionsConfig connections;
        connections.mysql.push_back(config::MysqlConnectionDesc{.alias = "fine", .host = "mock", .database = "db"});
        connections.postgresql.push_back(config::PgConnectionDesc{.alias = "bad",
                                                                  .host = "mock",
                                                                  .port = port,
                                                                  .database = "db",
                                                                  .pool_size = pool_size});
        otterstax::backends::remote_backends_t remote(
            runtime.resource(),
            otterstax::backends::remote_backends_options_t{.connectors = otterstax::test::mock_connectors(),
                                                           .connector_threads = {1, 1, 1},
                                                           .statements = runtime.backends_scheduler()});
        auto err = remote.open(connections, config::ConnectionRetryConfig{});
        REQUIRE(err.contains_error());
        CHECK(err.type == core::error_code_t::invalid_parameter);
        CHECK(std::string{err.what.c_str()}.find("'bad'") != std::string::npos);
        // Nothing was opened: not even the valid entry ahead of it.
        CHECK(remote.discovered().empty());
        CHECK(remote.servers().empty());
    }
}

TEST_CASE("process modules: built and destroyed unstarted on a fresh data dir three times over") {
    const auto dir = (std::filesystem::temp_directory_path() / "otterstax_process_assembly").string();

    for (int attempt = 0; attempt < 3; ++attempt) {
        std::filesystem::remove_all(dir);
        REQUIRE_FALSE(std::filesystem::exists(dir));
        {
            otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
            auto remote = open_remote(runtime);
            auto engine = open_engine(runtime, dir);
            auto actors = std::make_unique<backend_actors_t>(runtime.resource(), engine->dispatcher_address());
            // The s3 connector actor initialises Arrow's S3 subsystem while it is
            // being spawned. Arrow requires the process to FinalizeS3 before
            // exit or it may segfault during static teardown; the server does
            // that through conn::s3::subsystem_finalizer_t in main.cpp, this
            // binary through the listener in tests/system/main.cpp.
            REQUIRE(arrow::fs::IsS3Initialized());
            auto sessions = open_sessions(runtime, *engine, *remote, *actors);
            auto servers = std::make_unique<servers_t>(runtime, *sessions, local_ports());
            REQUIRE(sessions->live_sessions() == 0);

            servers.reset();
            sessions.reset();
            actors.reset();
            engine.reset();
            remote.reset();
        }
        REQUIRE(std::filesystem::exists(dir));
    }

    std::filesystem::remove_all(dir);
}

// The shutdown with a connection mid-statement: the connection's session is
// closed, the session finishes the statement it runs (which still
// needs the backends and the engine — both alive) or, if the statement had not
// started, refuses it; it frees itself, the sessions pool stops; then the
// backends and the engine go. The statement is all or nothing — never a part —
// and what it committed survives the engine's final CHECKPOINT.
TEST_CASE("process modules: shutdown runs frontends then sessions then backends then engine") {
    const auto dir = (std::filesystem::temp_directory_path() / "otterstax_process_shutdown").string();
    std::filesystem::remove_all(dir);
    constexpr std::size_t rows = 3000;
    {
        otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
        auto remote = open_remote(runtime);
        auto engine = open_engine(runtime, dir);
        auto actors = std::make_unique<backend_actors_t>(runtime.resource(), engine->dispatcher_address());
        auto sessions = open_sessions(runtime, *engine, *remote, *actors);
        auto servers = std::make_unique<servers_t>(runtime, *sessions, local_ports());

        auto session = sessions->open();
        auto run = [&](session_hash_t id, const std::string& sql) {
            auto [ns, fut] = actor_zeta::send(session->address(), &query_session_t::execute, stmt(id), sql);
            auto r = await_session(std::move(fut), std::chrono::seconds(30), runtime.resource());
            INFO(sql << ": " << (r.has_error() ? r.error().what.c_str() : ""));
            REQUIRE_FALSE(r.has_error());
        };
        run(1, "CREATE DATABASE shut;");
        run(2, "CREATE TABLE shut.t (id bigint);");
        std::string insert = "INSERT INTO shut.t (id) VALUES ";
        for (std::size_t i = 0; i < rows; ++i) {
            insert += (i == 0 ? "(" : ", (") + std::to_string(i) + ")";
        }
        insert += ";";
        // Sent, never awaited: the connection goes away mid-statement.
        auto in_flight =
            actor_zeta::send(session->address(), &query_session_t::execute, stmt(3), std::move(insert)).second;
        session.reset();
        REQUIRE(sessions->live_sessions() <= 1);

        // The servers stop; the session factory drains the sessions and stops
        // their pool.
        servers.reset();
        sessions.reset();
        REQUIRE(in_flight.is_ready());
        // Kafka, S3, file, s3 connector while the engine still runs.
        actors.reset();
        // The engine: its final CHECKPOINT, then its pools stop (~runtime_t
        // would abort on an engine still running on them).
        engine.reset();
        remote.reset();
    }
    {
        // What the statement committed is on disk.
        otterstax::test::test_engine_t reopened(dir, /*fresh=*/false);
        reopened.remove_on_exit();
        auto cursor = reopened.sql("SELECT id FROM shut.t;");
        REQUIRE(cursor);
        INFO((cursor->is_error() ? cursor->get_error().what.c_str() : ""));
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE((cursor->size() == rows || cursor->size() == 0));
    }
}

// The shutdown with a remote statement in flight: a connection's SELECT over a
// remote table waits for its backend (held on the backends pool) when the
// server goes down. The connection's session is closed, the session factory
// waits for it to finish the statement — which still needs the remote backend,
// the backends pool and the engine, all alive — and only then do the backends,
// the engine and the remote backends go. The statement answers its rows.
TEST_CASE("process modules: shutdown waits for a remote statement in flight") {
    const auto dir = (std::filesystem::temp_directory_path() / "otterstax_process_shutdown_remote").string();
    std::filesystem::remove_all(dir);
    {
        auto& gate = backend_gate();
        {
            std::lock_guard lock(gate.mutex);
            gate.started = false;
            gate.open = false;
        }
        otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
        config::ConnectionsConfig connections;
        connections.mysql = otterstax::test::mock_mysql({"slowdb"});
        auto connectors = otterstax::test::mock_connectors();
        connectors.mysql = &gated_mysql_factory;
        auto remote = open_remote(runtime, connections, connectors);
        REQUIRE(remote->discovered().size() == 1);
        auto engine = open_engine(runtime, dir);
        auto actors = std::make_unique<backend_actors_t>(runtime.resource(), engine->dispatcher_address());
        auto sessions = open_sessions(runtime, *engine, *remote, *actors);
        auto servers = std::make_unique<servers_t>(runtime, *sessions, local_ports());

        auto session = sessions->open();
        auto in_flight = actor_zeta::send(session->address(),
                                          &query_session_t::execute,
                                          stmt(1),
                                          std::string{"SELECT * FROM slowdb.db.\"table\";"})
                             .second;
        // The statement is on its backend, held there.
        REQUIRE(gate.wait_started(10000ms));
        REQUIRE_FALSE(in_flight.is_ready());
        // The connection goes away; the backend answers a moment after the
        // shutdown began.
        session.reset();
        std::jthread backend_answers([&gate] {
            std::this_thread::sleep_for(200ms);
            gate.release();
        });

        servers.reset();
        sessions.reset();
        REQUIRE(in_flight.is_ready());
        auto answer = std::move(in_flight).take_ready();
        INFO((answer.has_error() ? answer.error().what.c_str() : ""));
        REQUIRE_FALSE(answer.has_error());
        CHECK(answer.value().size() == 2);

        actors.reset();
        engine.reset();
        remote.reset();
    }
    std::filesystem::remove_all(dir);
}
