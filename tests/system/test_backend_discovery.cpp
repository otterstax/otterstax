// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Schema discovery runs on a backend agent of the connection
// (remote_backends_t::open at startup, the connection's actor — its discover
// message — for the session's registration of a table a statement missed). What
// is pinned here: the metadata queries that enrich a table's schema —
// PostgreSQL ENUM labels, ClickHouse named types — are part of the discovery,
// so their failure leaves the connection out instead of degrading it silently;
// the ClickHouse named-types query quotes the names it embeds; and a
// table registered again carries the backend's current types, not the ones seen
// first, which is what a read of it converts with.

#include "agent_driver.hpp"
#include "backends/remote_backends.hpp"
#include "session_stack.hpp"
#include "test_helpers.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/pg_db_connector.hpp"

#include <catch2/catch_all.hpp>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <core/result_wrapper.hpp>
#include <libpq-fe.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace {

    // ── PostgreSQL: the ENUM query fails, the rest of the discovery would not ──

    class pg_enum_failing_connector final : public pg::IConnector {
    public:
        pg_enum_failing_connector(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias)
            : resource_(resource)
            , params_(std::move(params))
            , alias_(std::move(alias)) {}

        pg::Status status() const noexcept override { return pg::Status::Connected; }
        pg::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view, otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)>) override {
            co_return std::unique_ptr<data_chunk_t>{};
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            co_return int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                co_return core::error_t(core::error_code_t::io_error,
                                        std::pmr::string{"simulated pg_enum failure", resource_});
            }
            std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                 &PQclear);
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
        }

    private:
        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<pg::IConnector>
    pg_enum_failing_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<pg_enum_failing_connector>(resource, std::move(params), std::move(alias));
    }

    // ── ClickHouse: a connector that records its metadata queries ──────────────

    // connector_factory is a plain function pointer, so the recording and the
    // type the connector reports are shared through these; every access is
    // ordered by the future the io-thread query settles.
    std::mutex g_ch_queries_mutex;
    std::vector<std::string> g_ch_queries;
    // The named type the connector reports for column "t"; switched between
    // discoveries.
    std::atomic<int> g_ch_type_variant{0};
    // Rows of the system.columns answer: 0 rows describes no column.
    std::atomic<bool> g_ch_named_types_malformed{false};

    std::vector<std::string> take_recorded_queries() {
        std::lock_guard guard(g_ch_queries_mutex);
        return std::exchange(g_ch_queries, {});
    }

    clickhouse::Block ch_named_types_block() {
        auto names = std::make_shared<clickhouse::ColumnString>();
        auto types = std::make_shared<clickhouse::ColumnString>();
        names->Append("t");
        types->Append(g_ch_type_variant.load() == 0 ? "String" : "Tuple(a Int32)");
        clickhouse::Block block;
        block.AppendColumn("name", names);
        block.AppendColumn("type", types);
        return block;
    }

    // One column, one row: not the (name, type) pair system.columns answers with.
    clickhouse::Block ch_malformed_named_types_block() {
        auto names = std::make_shared<clickhouse::ColumnString>();
        names->Append("t");
        clickhouse::Block block;
        block.AppendColumn("name", names);
        return block;
    }

    // A 0-row header block describing the column system.columns reports —
    // (t String), or (t Tuple(Int32)) for the named tuple: what the
    // `WHERE 1 = 0` schema probe streams back.
    clickhouse::Block ch_probe_block() {
        clickhouse::Block block;
        if (g_ch_type_variant.load() == 0) {
            block.AppendColumn("t", std::make_shared<clickhouse::ColumnString>());
        } else {
            block.AppendColumn("t",
                               std::make_shared<clickhouse::ColumnTuple>(std::vector<clickhouse::ColumnRef>{
                                   std::make_shared<clickhouse::ColumnInt32>()}));
        }
        return block;
    }

    class ch_recording_connector final : public ch::IConnector {
    public:
        ch_recording_connector(ch::connect_params params, std::string alias)
            : params_(std::move(params))
            , alias_(std::move(alias)) {}

        ch::Status status() const noexcept override { return ch::Status::Connected; }
        ch::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler)
            override {
            ch::select_result_t outcome;
            co_return handler(outcome);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)>) override {
            co_return int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            {
                std::lock_guard guard(g_ch_queries_mutex);
                g_ch_queries.emplace_back(query);
            }
            ch::select_result_t answer;
            if (query.find("system.columns") != std::string_view::npos) {
                answer.blocks.push_back(g_ch_named_types_malformed.load() ? ch_malformed_named_types_block()
                                                                          : ch_named_types_block());
            } else {
                answer.blocks.push_back(ch_probe_block());
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(answer));
        }

    private:
        ch::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<ch::IConnector>
    ch_recording_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        return std::make_unique<ch_recording_connector>(std::move(params), std::move(alias));
    }

    // The remote backends alone, a ClickHouse connection over the recording
    // connector opened by open().
    struct ch_stack {
        std::pmr::memory_resource* resource;
        // The backends pool the statements run on; outlives the backends.
        otterstax::runtime::runtime_t runtime{otterstax::test::test_runtime_config()};
        otterstax::backends::remote_backends_t remote;

        explicit ch_stack(std::pmr::memory_resource* res)
            : resource(res)
            , remote(res,
                     otterstax::backends::remote_backends_options_t{
                         .connectors = otterstax::backends::connector_factories_t{.clickhouse = &ch_recording_factory},
                         .runtime = &runtime}) {
            g_ch_type_variant.store(0);
            g_ch_named_types_malformed.store(false);
            take_recorded_queries();
        }

        core::error_t open(const std::string& alias, const std::string& database, const std::string& table) {
            config::ConnectionsConfig connections;
            connections.clickhouse.push_back(
                config::ChConnectionDesc{.alias = alias, .host = "localhost", .database = database, .table = table});
            return remote.open(connections, config::ConnectionRetryConfig{});
        }

        // A discovery through the connection's actor (its discover message).
        core::result_wrapper_t<otterstax::federation::discovered_tables_t> discover(const qualified_name_t& scope) {
            auto* actor = remote.actor(scope.connection.t);
            REQUIRE(actor != nullptr);
            auto [needs_sched, future] = actor_zeta::send(actor, &otterstax::federation::backend_actor_t::discover, scope);
            static_cast<void>(needs_sched);
            REQUIRE(otterstax::test::poll_until_ready(future, std::chrono::seconds(30)));
            return std::move(future).take_ready();
        }
    };

    // The type discovered for column "t" of the single discovered table.
    components::types::logical_type column_t_type(const otterstax::federation::discovered_tables_t& tables) {
        REQUIRE(tables.size() == 1);
        const auto& fields = tables.front().schema.child_types();
        REQUIRE(fields.size() == 1);
        REQUIRE(fields.front().alias() == "t");
        return fields.front().type();
    }

} // namespace

TEST_CASE("PostgreSQL discovery: a failed ENUM query leaves the connection undiscovered") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
    otterstax::backends::remote_backends_t remote(
        &arena,
        otterstax::backends::remote_backends_options_t{
            .connectors = otterstax::backends::connector_factories_t{.postgres = &pg_enum_failing_factory},
            .runtime = &runtime});
    config::ConnectionsConfig connections;
    connections.postgresql.push_back(config::PgConnectionDesc{.alias = "shop",
                                                              .host = "localhost",
                                                              .database = "pgdb",
                                                              .schema = "public",
                                                              .table = "orders"});
    // Startup goes on without the connection's tables (the failure is logged);
    // nothing of it is recorded.
    auto opened = remote.open(connections, config::ConnectionRetryConfig{});
    INFO(opened.what.c_str());
    REQUIRE_FALSE(opened.contains_error());
    REQUIRE(std::none_of(remote.discovered().begin(), remote.discovered().end(), [](const auto& d) { return d.discovered; }));

    // The discovery itself answers the pg_enum failure, code and text: asked of
    // an agent over the same connector.
    otterstax::test::agent_driver_t agent(&pg_enum_failing_factory, "shop");
    auto reply = agent.discover(qualified_name_t{core::connection_name_t{"shop"},
                                                 core::dbname_t{"pgdb"},
                                                 core::schema_t{"public"},
                                                 core::relname_t{"orders"}});
    REQUIRE(reply.has_error());
    INFO(reply.error().what.c_str());
    CHECK(reply.error().type == core::error_code_t::io_error);
    CHECK(std::string{reply.error().what.c_str()}.find("simulated pg_enum failure") != std::string::npos);
}

TEST_CASE("ClickHouse discovery: the named-types query quotes the database and table") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);

    auto opened = stack.open("ev_alias", "d\\b", "ev'il");
    INFO(opened.what.c_str());
    REQUIRE_FALSE(opened.contains_error());
    REQUIRE(stack.remote.discovered().front().discovered);

    const auto queries = take_recorded_queries();
    const std::string expected = "SELECT name, type FROM system.columns WHERE database = 'd\\\\b' AND table = 'ev''il'";
    REQUIRE(std::find(queries.begin(), queries.end(), expected) != queries.end());
}

TEST_CASE("ClickHouse discovery: a malformed system.columns answer leaves the connection undiscovered") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);
    g_ch_named_types_malformed.store(true);

    auto opened = stack.open("ev_alias", "ev", "events");
    REQUIRE_FALSE(opened.contains_error());
    REQUIRE(std::none_of(stack.remote.discovered().begin(), stack.remote.discovered().end(), [](const auto& d) { return d.discovered; }));
}

TEST_CASE("ClickHouse discovery: a rediscovered table carries the backend's current named types") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);

    auto opened = stack.open("1", "ev", "events");
    INFO(opened.what.c_str());
    REQUIRE_FALSE(opened.contains_error());
    REQUIRE(stack.remote.discovered().size() == 1);
    REQUIRE(column_t_type(stack.remote.discovered().front().tables) == components::types::logical_type::STRING_LITERAL);

    // The backend now reports a named tuple for the same column: the override
    // must shape the discovered schema, and the entry seen first must not win.
    g_ch_type_variant.store(1);
    auto second = stack.discover(qualified_name_t{core::connection_name_t{"1"}, core::dbname_t{"ev"}, core::schema_t{}, core::relname_t{"events"}});
    REQUIRE_FALSE(second.has_error());
    REQUIRE(column_t_type(second.value()) == components::types::logical_type::STRUCT);

    // The refreshed type is what the table's reads hold the column to (the registered column's
    // remote type); the connector answers a data query with no blocks.
    const auto& types = second.value().front().remote_types;
    REQUIRE(types.contains("t"));
    otterstax::test::agent_driver_t agent(&ch_recording_factory);
    auto read = agent.read("SELECT `t` FROM `ev`.`events`", {types.at("t")});
    INFO((read.has_error() ? read.error().what.c_str() : "ok"));
    REQUIRE_FALSE(read.has_error());
}

// ── ClickHouse: one table name in two databases of one connection ──────────

namespace {

    // `events` in two databases: system.columns declares its UInt8 column `flag` as `Bool` in
    // `db1` and as plain `UInt8` in `db2`; the probe and every read answer the wire's UInt8.
    class ch_two_databases_connector final : public ch::IConnector {
    public:
        ch_two_databases_connector(ch::connect_params params, std::string alias)
            : params_(std::move(params))
            , alias_(std::move(alias)) {}

        ch::Status status() const noexcept override { return ch::Status::Connected; }
        ch::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler)
            override {
            auto flag = std::make_shared<clickhouse::ColumnUInt8>();
            flag->Append(1);
            ch::select_result_t outcome;
            outcome.blocks.emplace_back();
            outcome.blocks.back().AppendColumn("flag", flag);
            co_return otterstax::as_query_result<std::unique_ptr<data_chunk_t>>(handler(outcome));
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)>) override {
            co_return int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t answer;
            answer.blocks.emplace_back();
            auto& block = answer.blocks.back();
            if (query.find("system.columns") != std::string_view::npos) {
                auto names = std::make_shared<clickhouse::ColumnString>();
                auto types = std::make_shared<clickhouse::ColumnString>();
                names->Append("flag");
                types->Append(query.find("database = 'db1'") != std::string_view::npos ? "Bool" : "UInt8");
                block.AppendColumn("name", names);
                block.AppendColumn("type", types);
            } else {
                block.AppendColumn("flag", std::make_shared<clickhouse::ColumnUInt8>());
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(answer));
        }

    private:
        ch::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<ch::IConnector>
    ch_two_databases_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        return std::make_unique<ch_two_databases_connector>(std::move(params), std::move(alias));
    }

} // namespace

TEST_CASE("ClickHouse discovery: a table name in two databases of one connection keeps each database's named types") {
    // Each database's table is discovered with its own system.columns types, and a read of it is
    // held to them (the registered columns of each table carry their own).
    otterstax::test::agent_driver_t agent(&ch_two_databases_factory, "ch");
    auto flag_type = [&](const std::string& database) {
        auto reply = agent.discover(
            qualified_name_t{core::connection_name_t{"ch"}, core::dbname_t{database}, core::schema_t{}, core::relname_t{"events"}});
        INFO((reply.has_error() ? reply.error().what.c_str() : "ok"));
        REQUIRE_FALSE(reply.has_error());
        REQUIRE(reply.value().size() == 1);
        const auto& types = reply.value().front().remote_types;
        REQUIRE(types.contains("flag"));
        auto read = agent.read("SELECT `flag` FROM `" + database + "`.`events`", {types.at("flag")});
        INFO((read.has_error() ? read.error().what.c_str() : "ok"));
        REQUIRE_FALSE(read.has_error());
        REQUIRE(read.value().size() == 1);
        REQUIRE(read.value().front().column_count() == 1);
        return read.value().front().types()[0].type();
    };
    CHECK(flag_type("db1") == components::types::logical_type::BOOLEAN);
    CHECK(flag_type("db2") == components::types::logical_type::UTINYINT);
}

// ── MySQL: a configured table scopes the startup discovery, as for PG / CH ──

TEST_CASE("MySQL discovery: a configured table is the only table the startup discovery records") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
    otterstax::backends::remote_backends_t remote(
        &arena,
        otterstax::backends::remote_backends_options_t{
            .connectors = otterstax::backends::connector_factories_t{.mysql = &mysql_mock_connector_factory},
            .runtime = &runtime});

    // The mock lists one table, `table`, for a whole-database discovery and
    // answers the probe of any table: a scoped discovery records the configured
    // table and never lists the database.
    config::ConnectionsConfig connections;
    connections.mysql.push_back(
        config::MysqlConnectionDesc{.alias = "scoped", .host = "localhost", .database = "db", .table = "orders"});
    connections.mysql.push_back(
        config::MysqlConnectionDesc{.alias = "whole", .host = "localhost", .database = "db", .table = ""});
    auto opened = remote.open(connections, config::ConnectionRetryConfig{});
    INFO(opened.what.c_str());
    REQUIRE_FALSE(opened.contains_error());
    REQUIRE(remote.discovered().size() == 2);

    const auto& scoped = remote.discovered()[0];
    CHECK(scoped.connection.name == "scoped");
    REQUIRE(scoped.tables.size() == 1);
    CHECK(scoped.tables.front().name.connection.t == "scoped");
    CHECK(scoped.tables.front().name.database.t == "db");
    CHECK(scoped.tables.front().name.collection.t == "orders");

    const auto& whole = remote.discovered()[1];
    CHECK(whole.connection.name == "whole");
    REQUIRE(whole.tables.size() == 1);
    CHECK(whole.tables.front().name.collection.t == "table");
}


// ── MySQL: a server under NO_BACKSLASH_ESCAPES is refused at startup (its session's check) ──

namespace {

    // The default MySQL mock, except that the connection named `nbe` reports
    // a session sql_mode holding NO_BACKSLASH_ESCAPES (every other one a
    // sql_mode without it).
    class sql_mode_mysql_connector final : public mysql::MockConnector {
    public:
        sql_mode_mysql_connector(std::pmr::memory_resource* resource, std::string alias)
            : mysql::MockConnector(mock_config{.resource = resource}, alias)
            , alias_(std::move(alias)) {}

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            if (query.find("sql_mode") != std::string_view::npos) {
                const auto mode = mysql::mock_results(
                    {{"@@SESSION.sql_mode", boost::mysql::column_type::varchar}},
                    {{std::string{alias_ == "nbe" ? "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES" : "STRICT_TRANS_TABLES"}}});
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(mode));
            }
            co_return co_await mysql::MockConnector::runQuery(query, handler);
        }
        using mysql::MockConnector::runQuery;

    private:
        std::string alias_;
    };

    std::unique_ptr<mysql::IConnector> sql_mode_mysql_factory(std::pmr::memory_resource* resource,
                                                              boost::asio::io_context&,
                                                              boost::mysql::connect_params,
                                                              std::string alias) {
        return std::make_unique<sql_mode_mysql_connector>(resource, std::move(alias));
    }

} // namespace

TEST_CASE("MySQL discovery: a server under NO_BACKSLASH_ESCAPES is left out and the others serve") {
    otterstax::test::stack_options_t options;
    options.connectors.mysql = &sql_mode_mysql_factory;
    options.connections.mysql.push_back(
        config::MysqlConnectionDesc{.alias = "plain", .host = "localhost", .database = "db", .table = "orders"});
    options.connections.mysql.push_back(
        config::MysqlConnectionDesc{.alias = "nbe", .host = "localhost", .database = "db", .table = "orders"});
    otterstax::test::session_stack_owner owner("/tmp/otterstax_backend_discovery_mysql_nbe", options);

    // Startup goes on without the refused connection; nothing of it is recorded.
    auto& remote = owner.remote();
    REQUIRE(remote.discovered().size() == 2);
    CHECK(remote.discovered().front().connection.name == "plain");
    CHECK(remote.discovered().front().discovered);
    CHECK_FALSE(remote.discovered().back().discovered);

    // The other connection serves its table; the refused one is unknown.
    auto served = owner.engine().sql("SELECT id, name FROM plain.db.public.orders;");
    REQUIRE(served);
    INFO((served->is_error() ? served->get_error().what.c_str() : "ok"));
    REQUIRE_FALSE(served->is_error());
    std::uint64_t rows = 0;
    for (const auto& chunk : served->chunks()) {
        rows += chunk.size();
    }
    CHECK(rows == 2);
    auto refused = owner.engine().sql("SELECT id, name FROM nbe.db.public.orders;");
    REQUIRE(refused);
    CHECK(refused->is_error());

    // The connect itself answers the refusal, code and text: the session an
    // agent over the same connector opens is checked before it serves.
    std::pmr::synchronized_pool_resource resource{std::pmr::new_delete_resource()};
    auto agent = actor_zeta::spawn<otterstax::federation::backend_agent_t>(&resource, "nbe");
    agent->attach(sql_mode_mysql_factory(&resource, agent->io(), boost::mysql::connect_params{}, "nbe"));
    auto refusal = agent->connect();
    INFO(refusal.what.c_str());
    CHECK(refusal.type == core::error_code_t::unimplemented_yet);
    CHECK(std::string{refusal.what.c_str()}.find("NO_BACKSLASH_ESCAPES") != std::string::npos);
    CHECK(agent->session_refused());
}
