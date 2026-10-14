// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The test stack, built from the modules the server is built from, in the
// server's order:
//
//   runtime_t → remote_backends_t (opened: connections of the test's
//   `connections:`, discovered) → engine_t (a real engine in a temp dir, opened
//   with the federation's primitives) → the s3 connector, file manager, S3
//   orchestrator and Kafka manager (s3 aliases registered, federation metadata
//   published) → a session_factory_t on the runtime's sessions pool → query
//   sessions
//
// A "connection" is a query session; a statement is a statement_id_t on it.
// Every helper sends exactly what a frontend sends and awaits the one future
// through the asio bridge, as the frontends do.
//
// Nothing here mocks the engine. The backends are the real ones with the
// connector factories a test chooses (the mock MySQL / PostgreSQL / ClickHouse
// connectors by default); the connections are the ones the test's options
// name, opened at startup as the server opens config.yaml's.

#pragma once

#include "backends/backends.hpp"
#include "backends/remote_backends.hpp"
#include "config/connections/connection_config.hpp"
#include "connectors/file/manager.hpp"
#include "connectors/s3/manager.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/s3/s3_manager.hpp"
#include "otterbrix/federation/name_resolution.hpp"
#include "frontend/common/asio_future_bridge.hpp"
#include "otterbrix/operators/engine_requests.hpp"
#include "otterbrix/parser/parser.hpp"
#include "runtime/engine.hpp"
#include "runtime/runtime.hpp"
#include "session/query_session.hpp"
#include "session/session_factory.hpp"
#include "session/session_payload.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"

#include "../mock/ch_db_connector.hpp"
#include "../mock/pg_db_connector.hpp"
#include "../mock/sql_db_connector.hpp"
#include "drive_future.hpp"
#include "test_helpers.hpp"

#include <actor-zeta.hpp>
#include <components/cursor/cursor.hpp>
#include <components/session/session.hpp>
#include <core/result_wrapper.hpp>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

namespace otterstax::test {

    // A data directory of its own for a test that needs an engine but does not
    // name a directory: /tmp/<prefix>-<pid>-<n>.
    inline std::string unique_test_dir(const std::string& prefix) {
        static std::atomic<unsigned> counter{0};
        return (std::filesystem::temp_directory_path() /
                (prefix + "-" + std::to_string(::getpid()) + "-" + std::to_string(counter.fetch_add(1))))
            .string();
    }

    // Pools small enough for a test process that builds hundreds of stacks.
    inline config::RuntimeConfig test_runtime_config() {
        config::RuntimeConfig config;
        config.general = config::PoolConfig{2};
        config.exec = config::PoolConfig{2};
        config.disk = config::PoolConfig{2};
        config.sessions = config::PoolConfig{4};
        config.backends = config::PoolConfig{1};
        return config;
    }

    // The mock connectors (tests/mock).
    inline backends::connector_factories_t mock_connectors() {
        return backends::connector_factories_t{
            .mysql = &mysql_mock_connector_factory,
            .postgres = &pg_mock_connector_factory,
            .clickhouse = &ch_mock_connector_factory,
        };
    }

    // The remote backends a test engine opens with, before the engine: the
    // connectors and the connections (config.yaml's `connections:` section).
    struct remote_setup_t {
        backends::connector_factories_t connectors = mock_connectors();
        config::ConnectionsConfig connections{};
        config::ConnectionRetryConfig retry{};
    };

    // Mock MySQL connections named `uids`, each over database "db" — the
    // database the default MySQL mock discovers its one table `table` in.
    inline std::vector<config::MysqlConnectionDesc> mock_mysql(std::initializer_list<const char*> uids) {
        std::vector<config::MysqlConnectionDesc> out;
        for (const char* uid : uids) {
            out.push_back(config::MysqlConnectionDesc{.alias = uid, .host = "mock", .database = "db"});
        }
        return out;
    }

    // A runtime, the remote backends and the engine on it, over `data_dir`, in
    // the server's order (the federation registry is installed before the
    // engine opens with the federation's primitives). `fresh` wipes the
    // directory first and removes it once the engine is gone (its final
    // CHECKPOINT writes there); a restart test opens a second one over the same
    // directory after destroying the first, with `fresh` off.
    // What the engine is opened with: the federation's primitives, or a test's
    // own (the federation's plus a rule of its own).
    using engine_primitives_fn = components::planner::primitives_t (*)();

    class test_engine_t {
    public:
        explicit test_engine_t(std::string data_dir,
                               bool fresh = true,
                               remote_setup_t remote = {},
                               engine_primitives_fn primitives = &federation::make_primitives)
            : data_dir_(std::move(data_dir))
            , session_(components::session::session_id_t::generate_uid())
            , remove_on_exit_(fresh) {
            if (fresh) {
                std::filesystem::remove_all(data_dir_);
            }
            std::filesystem::create_directories(data_dir_);
            initialize_all_loggers(data_dir_);
            runtime_ = std::make_unique<runtime::runtime_t>(test_runtime_config());
            remote_ = std::make_unique<backends::remote_backends_t>(runtime_->resource(),
                                                                    backends::remote_backends_options_t{
                                                                        .connectors = remote.connectors,
                                                                        .connector_threads = {2, 2, 2},
                                                                        .statements = runtime_->backends_scheduler(),
                                                                    });
            if (auto err = remote_->open(remote.connections, remote.retry); err.contains_error()) {
                FAIL("test remote backends refused to open: " << err.what.c_str());
            }
            config::EngineConfig engine_config;
            engine_config.data_dir = data_dir_;
            auto opened = runtime::engine_t::open(*runtime_, engine_config, primitives());
            if (opened.has_error()) {
                FAIL("test engine refused to open on " << data_dir_ << ": " << opened.error().what.c_str());
            }
            engine_ = std::move(opened.value());
        }

        test_engine_t(const test_engine_t&) = delete;
        test_engine_t& operator=(const test_engine_t&) = delete;

        ~test_engine_t() {
            // The server's order: the engine (its final CHECKPOINT), the remote
            // backends (registry reset first), the runtime.
            engine_.reset();
            remote_.reset();
            runtime_.reset();
            if (remove_on_exit_) {
                std::filesystem::remove_all(data_dir_);
            }
        }

        // Remove the directory once the engine is gone (its final CHECKPOINT
        // writes there).
        void remove_on_exit() noexcept { remove_on_exit_ = true; }

        const std::string& data_dir() const noexcept { return data_dir_; }
        runtime::runtime_t& runtime() { return *runtime_; }
        runtime::engine_t& engine() { return *engine_; }
        backends::remote_backends_t& remote() { return *remote_; }
        actor_zeta::address_t dispatcher() const { return engine_->dispatcher_address(); }
        std::pmr::memory_resource* resource() const { return engine_->resource(); }
        // The engine session of the test's own direct statements.
        components::session::session_id_t session() const noexcept { return session_; }

        // One plan, sent from the test thread and waited for.
        components::cursor::cursor_t_ptr plan(components::logical_plan::execution_plan_t plan) {
            return engine_->execute_plan_blocking(session_, std::move(plan));
        }

        // One SQL statement straight against the engine (its own parser and
        // transformer — no query session), waited for. An unparsable text is an
        // error cursor.
        components::cursor::cursor_t_ptr sql(const std::string& text) {
            auto planned = otterstax::engine::sql_plan(resource(), text);
            if (planned.has_error()) {
                return components::cursor::make_cursor(resource(), planned.error());
            }
            return plan(std::move(planned.value()));
        }

        // Drives an engine-request coroutine (otterstax::engine::*) from the
        // test thread.
        template<typename T>
        T drive(actor_zeta::unique_future<T> future) {
            return drive_until_ready(std::move(future));
        }

    private:
        std::string data_dir_;
        components::session::session_id_t session_;
        bool remove_on_exit_;
        std::unique_ptr<runtime::runtime_t> runtime_;
        std::unique_ptr<backends::remote_backends_t> remote_;
        std::unique_ptr<runtime::engine_t> engine_;
    };

    struct session_stack {
        // The connection the helpers send to.
        actor_zeta::address_t session;
        test_engine_t* engine;
        std::pmr::memory_resource* resource;
        // The factory a frontend server opens its connections' sessions from.
        otterstax::session::session_factory_t* sessions;

        // The same stack seen through another connection.
        session_stack on(actor_zeta::address_t other) const {
            return session_stack{std::move(other), engine, resource, sessions};
        }
    };

    struct stack_options_t {
        otterstax::session::parser_factory_fn parser_factory = &make_parser;
        backends::connector_factories_t connectors = mock_connectors();
        // The `connections:` section: opened and discovered at startup.
        config::ConnectionsConfig connections{};
        config::ConnectionRetryConfig retry{};
        bool kafka_pollers = false;
        engine_primitives_fn engine_primitives = &federation::make_primitives;
        // Wipe the data dir before and after.
        bool fresh = true;
    };

    // Owns the whole stack and one default connection, its parts declared in
    // main.cpp's order. Teardown runs in the destructor in the order the server
    // shuts down — the connections closed, the session factory (sessions
    // drained, the sessions pool stopped, what it left freed), Kafka, S3, file,
    // the s3 connector, the engine (final CHECKPOINT), the remote backends, the
    // runtime — so it happens whether the test body returns or a failed REQUIRE
    // unwinds through it.
    class session_stack_owner {
    public:
        explicit session_stack_owner(const std::string& data_dir, stack_options_t options = {})
            : options_(options)
            , engine_(std::make_unique<test_engine_t>(
                  data_dir,
                  options.fresh,
                  remote_setup_t{.connectors = options.connectors, .connections = options.connections, .retry = options.retry},
                  options.engine_primitives))
            , s3_connector_(actor_zeta::spawn<conn::s3::ConnectorManager>(engine_->resource()))
            , file_(actor_zeta::spawn<conn::file::FileManager>(engine_->resource(), engine_->dispatcher()))
            , s3_(actor_zeta::spawn<db::S3Manager>(engine_->resource(), s3_connector_->address(), file_->address()))
            , kafka_(actor_zeta::spawn<otterstax::kafka::KafkaManager>(engine_->resource(),
                                                                       engine_->dispatcher(),
                                                                       options.kafka_pollers)) {
            if (auto err = backends::register_s3_aliases(engine_->resource(), s3_connector_->address(), options.connections);
                err.contains_error()) {
                FAIL("s3 aliases refused: " << err.what.c_str());
            }
            if (auto err = backends::publish_federation_metadata(engine_->resource(), engine_->engine(), engine_->remote());
                err.contains_error()) {
                FAIL("federation metadata refused: " << err.what.c_str());
            }
            auto services = otterstax::session::make_session_services(engine_->resource(),
                                                                      engine_->dispatcher(),
                                                                      s3_->address(),
                                                                      file_->address(),
                                                                      kafka_->address());
            REQUIRE_FALSE(services.has_error());
            sessions_ = std::make_unique<otterstax::session::session_factory_t>(engine_->runtime(),
                                                                                options.parser_factory,
                                                                                std::move(services.value()));
            open_connection();
        }

        session_stack_owner(const session_stack_owner&) = delete;
        session_stack_owner& operator=(const session_stack_owner&) = delete;

        ~session_stack_owner() {
            // The reverse of the declarations, spelled out: the data dir goes
            // only once the engine is gone.
            connections_.clear();
            sessions_.reset();
            kafka_.reset();
            s3_.reset();
            file_.reset();
            s3_connector_.reset();
            const std::string data_dir = engine_->data_dir();
            engine_.reset();
            if (options_.fresh) {
                std::filesystem::remove_all(data_dir);
            }
        }

        // Another connection on the same engine, owned by the stack.
        actor_zeta::address_t open_connection() {
            connections_.push_back(sessions_->open());
            return connections_.back()->address();
        }

        // A connection the caller owns (and may close mid-statement).
        otterstax::session::session_ptr open_session() { return sessions_->open(); }

        // The first connection (the stack's default).
        session_stack stack() const {
            return session_stack{connections_.front()->address(), engine_.get(), engine_->resource(), sessions_.get()};
        }

        otterstax::session::query_session_t& default_session() { return *connections_.front(); }
        test_engine_t& engine() { return *engine_; }
        // The actors a session sends to besides the engine.
        actor_zeta::address_t s3_address() const { return s3_->address(); }
        actor_zeta::address_t file_address() const { return file_->address(); }
        actor_zeta::address_t kafka_address() const { return kafka_->address(); }
        backends::remote_backends_t& remote() { return engine_->remote(); }
        otterstax::session::session_factory_t& sessions() { return *sessions_; }

    private:
        stack_options_t options_;
        std::unique_ptr<test_engine_t> engine_;
        template<typename T>
        using actor_ptr = std::unique_ptr<T, actor_zeta::pmr::deleter_t>;
        actor_ptr<conn::s3::ConnectorManager> s3_connector_;
        actor_ptr<conn::file::FileManager> file_;
        actor_ptr<db::S3Manager> s3_;
        actor_ptr<otterstax::kafka::KafkaManager> kafka_;
        std::unique_ptr<otterstax::session::session_factory_t> sessions_;
        std::vector<otterstax::session::session_ptr> connections_;
    };

    template<typename Fn>
    void with_session_stack_parser(const std::string& data_dir,
                                   otterstax::session::parser_factory_fn parser_factory,
                                   Fn&& body) {
        session_stack_owner owner(data_dir, stack_options_t{.parser_factory = parser_factory});
        body(owner.stack());
    }

    template<typename Fn>
    void with_session_stack(const std::string& data_dir, Fn&& body) {
        with_session_stack_parser(data_dir, &make_parser, std::forward<Fn>(body));
    }

    // Drive a session's future from the test thread through the asio bridge —
    // the same poll-based path the frontends use.
    inline core::result_wrapper_t<session_payload>
    await_session(actor_zeta::unique_future<core::result_wrapper_t<session_payload>> fut,
                  std::chrono::milliseconds timeout,
                  std::pmr::memory_resource* resource) {
        return otterstax::await_future_blocking<session_payload>(std::move(fut), resource, timeout);
    }

    inline otterstax::session::statement_id_t stmt(session_hash_t id) { return otterstax::session::statement_id_t{id}; }

    // ─── One call each, the way a frontend makes it: send, then await the one
    // future through the asio bridge. ──────────────────────────────────────────

    inline core::result_wrapper_t<session_payload>
    run_session_sql_payload(const session_stack& s,
                            session_hash_t id,
                            const std::string& sql,
                            std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] = actor_zeta::send(s.session, &otterstax::session::query_session_t::execute, stmt(id), sql);
        return await_session(std::move(fut), timeout, s.resource);
    }

    inline bool run_session_sql(const session_stack& s, session_hash_t id, const std::string& sql, std::string& err) {
        auto r = run_session_sql_payload(s, id, sql);
        if (r.has_error()) {
            err = std::string{r.error().what.c_str()};
            return false;
        }
        err.clear();
        return true;
    }

    inline core::result_wrapper_t<session_payload>
    prepare_session_sql(const session_stack& s,
                        session_hash_t id,
                        const std::string& sql,
                        std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] =
            actor_zeta::send(s.session, &otterstax::session::query_session_t::prepare_schema, stmt(id), sql);
        return await_session(std::move(fut), timeout, s.resource);
    }

    inline core::result_wrapper_t<session_payload>
    execute_session_statement(const session_stack& s,
                              session_hash_t id,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] = actor_zeta::send(s.session, &otterstax::session::query_session_t::execute_statement, stmt(id));
        return await_session(std::move(fut), timeout, s.resource);
    }

    inline core::result_wrapper_t<session_payload>
    execute_session_prepared(const session_stack& s,
                             session_hash_t id,
                             std::pmr::vector<components::types::logical_value_t> parameters,
                             std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] = actor_zeta::send(s.session,
                                          &otterstax::session::query_session_t::execute_prepared_statement,
                                          stmt(id),
                                          std::move(parameters));
        return await_session(std::move(fut), timeout, s.resource);
    }

    inline core::result_wrapper_t<session_payload>
    close_session_statement(const session_stack& s,
                            session_hash_t id,
                            std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] = actor_zeta::send(s.session, &otterstax::session::query_session_t::close_statement, stmt(id));
        return await_session(std::move(fut), timeout, s.resource);
    }

    inline core::result_wrapper_t<session_payload>
    execute_session_plan(const session_stack& s,
                         session_hash_t id,
                         ParsedQueryDataPtr plan,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] =
            actor_zeta::send(s.session, &otterstax::session::query_session_t::execute_plan, stmt(id), std::move(plan));
        return await_session(std::move(fut), timeout, s.resource);
    }

    inline core::result_wrapper_t<session_payload>
    prepare_session_plan(const session_stack& s,
                         session_hash_t id,
                         ParsedQueryDataPtr plan,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto [ns, fut] =
            actor_zeta::send(s.session, &otterstax::session::query_session_t::prepare_plan, stmt(id), std::move(plan));
        return await_session(std::move(fut), timeout, s.resource);
    }

    // prepare_schema, then execute_statement: the two-phase path every
    // extended-protocol client takes — a genuinely different route through the
    // session than execute (classification happens in a different place), so a
    // statement shape that works one way can still be broken the other.
    // `prepare_err` carries a prepare-phase failure (execute is then not
    // attempted and the prepare error returned unchanged, code included).
    inline core::result_wrapper_t<session_payload>
    run_session_prepared(const session_stack& s,
                         session_hash_t id,
                         const std::string& sql,
                         std::string& prepare_err,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(10000)) {
        auto prepared = prepare_session_sql(s, id, sql, timeout);
        if (prepared.has_error()) {
            prepare_err = std::string{prepared.error().what.c_str()};
            return prepared;
        }
        prepare_err.clear();
        return execute_session_statement(s, id, timeout);
    }

    inline void run_session_or_fail(const session_stack& s, session_hash_t id, const std::string& sql) {
        std::string err;
        const bool ok = run_session_sql(s, id, sql, err);
        INFO(sql << ": " << err);
        REQUIRE(ok);
    }

    // Row count of `db.tbl` read straight from the engine. A failed SELECT is a
    // test failure, not a count of zero.
    inline size_t engine_row_count(const session_stack& s, const std::string& db, const std::string& tbl) {
        auto cur = s.engine->sql("SELECT * FROM " + db + "." + tbl + ";");
        if (!cur || cur->is_error()) {
            FAIL("engine_row_count: SELECT * FROM " << db << "." << tbl << " failed: "
                                                    << (cur ? cur->get_error().what.c_str() : "null cursor"));
        }
        return cur->size();
    }

    // Fresh dmldb.people (id bigint, name string) with `rows` rows, inserted in
    // multi-row VALUES batches so a table larger than one chunk is cheap to seed.
    inline void seed_people_bulk(const session_stack& s, session_hash_t& id, size_t rows) {
        run_session_or_fail(s, id++, "CREATE DATABASE dmldb;");
        run_session_or_fail(s, id++, "CREATE TABLE dmldb.people (id bigint, name string);");
        constexpr size_t batch = 500;
        for (size_t first = 0; first < rows; first += batch) {
            const size_t last = std::min(rows, first + batch);
            std::string sql = "INSERT INTO dmldb.people (id, name) VALUES ";
            for (size_t i = first; i < last; ++i) {
                if (i != first) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(i + 1) + ", 'name_" + std::to_string(i) + "')";
            }
            sql += ";";
            run_session_or_fail(s, id++, sql);
        }
    }

} // namespace otterstax::test
