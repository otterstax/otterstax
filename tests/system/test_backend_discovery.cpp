// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Schema discovery runs on the backend that owns the connector manager
// (remote_backends_t::open at startup, the session's rediscovery later). What
// is pinned here: the metadata queries that enrich a table's schema —
// PostgreSQL ENUM labels, ClickHouse named types — are part of the discovery,
// so their failure leaves the connection out instead of degrading it silently;
// the ClickHouse named-types query quotes the names it embeds; and a
// rediscovered table carries the backend's current types, not the ones seen
// first, which is what the next read converts with.

#include "backends/remote_backends.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "otterbrix/federation/provider.hpp"
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
                         .connector_threads = {2, 2, 2},
                         .statements = runtime.backends_scheduler()}) {
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

        core::result_wrapper_t<otterstax::federation::discovered_tables_t> discover(const qualified_name_t& scope) {
            return remote.clickhouse().discover(scope);
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

TEST_CASE("PostgressManager::discover: a failed ENUM query leaves the connection out") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
    otterstax::backends::remote_backends_t remote(
        &arena,
        otterstax::backends::remote_backends_options_t{
            .connectors = otterstax::backends::connector_factories_t{.postgres = &pg_enum_failing_factory},
            .connector_threads = {2, 2, 2},
            .statements = runtime.backends_scheduler()});
    config::ConnectionsConfig connections;
    connections.postgresql.push_back(config::PgConnectionDesc{.alias = "shop",
                                                              .host = "localhost",
                                                              .database = "pgdb",
                                                              .schema = "public",
                                                              .table = "orders"});
    // Startup goes on without the connection (the failure is logged); nothing
    // of it is recorded.
    auto opened = remote.open(connections, config::ConnectionRetryConfig{});
    INFO(opened.what.c_str());
    REQUIRE_FALSE(opened.contains_error());
    REQUIRE_FALSE(remote.postgres_connectors().hasConnection("shop"));
    REQUIRE(remote.discovered().empty());

    // The discovery itself answers the pg_enum failure, code and text: asked of
    // the same connection opened again by hand (open() removed it).
    auto added = remote.postgres_connectors().addConnection(conn::api_server::PgConnectionParams{.alias = "shop",
                                                                                                 .host = "localhost",
                                                                                                 .database = "pgdb",
                                                                                                 .schema = "public",
                                                                                                 .table = "orders"});
    INFO((added.has_error() ? added.error().what.c_str() : "ok"));
    REQUIRE_FALSE(added.has_error());
    auto reply = remote.postgres().discover(qualified_name_t{core::uid_t{"shop"},
                                                             core::dbname_t{"pgdb"},
                                                             core::schema_t{"public"},
                                                             core::relname_t{"orders"}});
    REQUIRE(reply.has_error());
    INFO(reply.error().what.c_str());
    CHECK(reply.error().type == core::error_code_t::io_error);
    CHECK(std::string{reply.error().what.c_str()}.find("simulated pg_enum failure") != std::string::npos);
}

TEST_CASE("ClickhouseManager::discover: the named-types query quotes the database and table") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);

    auto opened = stack.open("ev_alias", "d\\b", "ev'il");
    INFO(opened.what.c_str());
    REQUIRE_FALSE(opened.contains_error());
    REQUIRE(stack.remote.clickhouse_connectors().hasConnection("ev_alias"));

    const auto queries = take_recorded_queries();
    const std::string expected = "SELECT name, type FROM system.columns WHERE database = 'd\\\\b' AND table = 'ev''il'";
    REQUIRE(std::find(queries.begin(), queries.end(), expected) != queries.end());
}

TEST_CASE("ClickhouseManager::discover: a malformed system.columns answer leaves the connection out") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_stack stack(&arena);
    g_ch_named_types_malformed.store(true);

    auto opened = stack.open("ev_alias", "ev", "events");
    REQUIRE_FALSE(opened.contains_error());
    REQUIRE_FALSE(stack.remote.clickhouse_connectors().hasConnection("ev_alias"));
    REQUIRE(stack.remote.discovered().empty());
}

TEST_CASE("ClickhouseManager::discover: a rediscovered table carries the backend's current named types") {
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
    auto second = stack.discover(qualified_name_t{core::uid_t{"1"}, core::dbname_t{"ev"}, core::schema_t{}, core::relname_t{"events"}});
    REQUIRE_FALSE(second.has_error());
    REQUIRE(column_t_type(second.value()) == components::types::logical_type::STRUCT);

    // The refreshed overrides are what a read converts with; the connector
    // answers a data query with no blocks.
    auto rows = std::make_shared<otterstax::federation::scan_rows_t>();
    auto read = drive_until_ready(stack.remote.clickhouse().scan(
        &arena,
        otterstax::federation::scan_request_t{
            "1",
            "SELECT `t` FROM `ev`.`events`",
            {otterstax::federation::column_origin_t{otterstax::federation::remote_table_name_t{"ev", "", "events"}, "t"}}},
        rows));
    INFO(read.what.c_str());
    REQUIRE_FALSE(read.contains_error());
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

TEST_CASE("ClickhouseManager::scan: a table name in two databases of one connection keeps each database's named types") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
    otterstax::backends::remote_backends_t remote(
        &arena,
        otterstax::backends::remote_backends_options_t{
            .connectors = otterstax::backends::connector_factories_t{.clickhouse = &ch_two_databases_factory},
            .connector_threads = {2, 2, 2},
            .statements = runtime.backends_scheduler()});
    config::ConnectionsConfig connections;
    connections.clickhouse.push_back(
        config::ChConnectionDesc{.alias = "ch", .host = "localhost", .database = "db1", .table = "events"});
    auto opened = remote.open(connections, config::ConnectionRetryConfig{});
    INFO(opened.what.c_str());
    REQUIRE_FALSE(opened.contains_error());
    REQUIRE(remote.discovered().size() == 1);

    // The same table name in another database of the connection (a rediscovery): its named
    // types must not replace db1's.
    auto second = remote.clickhouse().discover(
        qualified_name_t{core::uid_t{"ch"}, core::dbname_t{"db2"}, core::schema_t{}, core::relname_t{"events"}});
    INFO((second.has_error() ? second.error().what.c_str() : "ok"));
    REQUIRE_FALSE(second.has_error());

    auto flag_type = [&](const std::string& database) {
        auto rows = std::make_shared<otterstax::federation::scan_rows_t>();
        auto read = drive_until_ready(remote.clickhouse().scan(
            &arena,
            otterstax::federation::scan_request_t{
                "ch",
                "SELECT `flag` FROM `" + database + "`.`events`",
                {otterstax::federation::column_origin_t{
                    otterstax::federation::remote_table_name_t{database, "", "events"}, "flag"}}},
            rows));
        INFO(read.what.c_str());
        REQUIRE_FALSE(read.contains_error());
        REQUIRE(rows->chunks.size() == 1);
        REQUIRE(rows->chunks.front().column_count() == 1);
        return rows->chunks.front().types()[0].type();
    };
    CHECK(flag_type("db1") == components::types::logical_type::BOOLEAN);
    CHECK(flag_type("db2") == components::types::logical_type::UTINYINT);
}

// ── MySQL: a configured table scopes the startup discovery, as for PG / CH ──

TEST_CASE("MySQLManager::discover: a configured table is the only table the startup discovery records") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    otterstax::runtime::runtime_t runtime(otterstax::test::test_runtime_config());
    otterstax::backends::remote_backends_t remote(
        &arena,
        otterstax::backends::remote_backends_options_t{
            .connectors = otterstax::backends::connector_factories_t{.mysql = &mysql_mock_connector_factory},
            .connector_threads = {2, 2, 2},
            .statements = runtime.backends_scheduler()});

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
    CHECK(scoped.scope.unique_identifier.t == "scoped");
    CHECK(scoped.scope.collection.t == "orders");
    REQUIRE(scoped.tables.size() == 1);
    CHECK(scoped.tables.front().name.unique_identifier.t == "scoped");
    CHECK(scoped.tables.front().name.database.t == "db");
    CHECK(scoped.tables.front().name.collection.t == "orders");

    const auto& whole = remote.discovered()[1];
    CHECK(whole.scope.collection.t.empty());
    REQUIRE(whole.tables.size() == 1);
    CHECK(whole.tables.front().name.collection.t == "table");
}

// ── MySQL: the primary key off the probe's PRI_KEY_FLAG, a binary key dropped ──

namespace {

    // The writes the keyed MySQL backend received, in order.
    std::mutex g_mysql_writes_mutex;
    std::vector<std::string> g_mysql_writes;

    std::vector<std::string> take_mysql_writes() {
        std::lock_guard guard(g_mysql_writes_mutex);
        return std::exchange(g_mysql_writes, {});
    }

    // The OK packet of an UPDATE that matched one row, its info in a language other than English:
    // the matched rows are read by the message's numbers, whatever lc_messages says.
    boost::mysql::results update_ok_results() {
        boost::mysql::results result;
        auto& impl = boost::mysql::detail::access::get_impl(result);
        impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::minimal);
        boost::mysql::diagnostics diag;
        [[maybe_unused]] auto ec = impl.on_head_ok_packet(
            boost::mysql::detail::ok_view{0, 0, 0, 0, "Datensätze gefunden: 1  Geändert: 0  Warnungen: 0"},
            diag);
        REQUIRE_FALSE(ec);
        return result;
    }

    // Database `db` with three tables: `orders` (id INT, name VARCHAR; PRIMARY KEY (id)), `blobs`
    // (k VARBINARY, v INT; PRIMARY KEY (k)) and `wide` (k DECIMAL without a precision the engine can
    // declare — read as its text —, v INT; PRIMARY KEY (k)). The data path answers two rows of the
    // table a statement names; a write is recorded and answers an UPDATE's OK packet.
    class keyed_mysql_connector final : public mysql::IConnector {
    public:
        keyed_mysql_connector(std::pmr::memory_resource* resource, std::string alias)
            : resource_(resource)
            , alias_(std::move(alias)) {}

        mysql::Status status() const noexcept override { return mysql::Status::Connected; }
        boost::mysql::connect_params params() const noexcept override { return {}; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)>) override {
            using components::types::logical_type;
            using components::types::logical_value_t;
            const bool blobs = query.find("`blobs`") != std::string_view::npos;
            std::pmr::vector<components::types::complex_logical_type> fields(resource_);
            fields.emplace_back(blobs ? logical_type::STRING_LITERAL : logical_type::INTEGER, blobs ? "k" : "id");
            fields.emplace_back(blobs ? logical_type::INTEGER : logical_type::STRING_LITERAL, blobs ? "v" : "name");
            auto chunk = std::make_unique<data_chunk_t>(resource_, fields, 2);
            for (std::uint64_t row = 0; row < 2; ++row) {
                const auto number = logical_value_t(resource_, static_cast<int32_t>(row + 1));
                const auto text = logical_value_t(resource_, std::string{row == 0 ? "a" : "b"});
                chunk->set_value(0, row, blobs ? text : number);
                chunk->set_value(1, row, blobs ? number : text);
            }
            chunk->set_cardinality(2);
            co_return std::move(chunk);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view query, otterstax::function_ref_t<int64_t(const boost::mysql::results&)> handler) override {
            {
                std::lock_guard guard(g_mysql_writes_mutex);
                g_mysql_writes.emplace_back(query);
            }
            const auto ok = update_ok_results();
            co_return handler(ok);
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            using boost::mysql::column_type;
            if (query.find("sql_mode") != std::string_view::npos) {
                const auto mode = mysql::mock_results({{"@@SESSION.sql_mode", column_type::varchar}},
                                                      {{std::string{"STRICT_TRANS_TABLES"}}});
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(mode));
            }
            if (query.find("information_schema") != std::string_view::npos) {
                const auto listing = mysql::mock_results(
                    {{"TABLE_NAME", column_type::varchar}},
                    {{std::string{"orders"}}, {std::string{"blobs"}}, {std::string{"wide"}}});
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(listing));
            }
            if (query.find("`orders`") != std::string_view::npos) {
                const auto probe = mysql::mock_results(
                    {{.name = "id", .type = column_type::int_, .primary_key = true}, {"name", column_type::varchar}});
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(probe));
            }
            if (query.find("`blobs`") != std::string_view::npos) {
                const auto probe = mysql::mock_results(
                    {{.name = "k", .type = column_type::varbinary, .primary_key = true}, {"v", column_type::int_}});
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(probe));
            }
            if (query.find("`wide`") != std::string_view::npos) {
                const auto probe = mysql::mock_results(
                    {{.name = "k", .type = column_type::decimal, .primary_key = true}, {"v", column_type::int_}});
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(probe));
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(mysql::mock_ok_results()));
        }

    private:
        std::pmr::memory_resource* resource_;
        std::string alias_;
    };

    std::unique_ptr<mysql::IConnector> keyed_mysql_factory(std::pmr::memory_resource* resource,
                                                           boost::asio::io_context&,
                                                           boost::mysql::connect_params,
                                                           std::string alias) {
        return std::make_unique<keyed_mysql_connector>(resource, std::move(alias));
    }

} // namespace

TEST_CASE("MySQLManager::discover: the PRI_KEY_FLAG key is recorded and names the row a change writes") {
    otterstax::test::stack_options_t options;
    options.connectors.mysql = &keyed_mysql_factory;
    options.connections.mysql.push_back(
        config::MysqlConnectionDesc{.alias = "keyed", .host = "localhost", .database = "db", .table = ""});
    otterstax::test::session_stack_owner owner("/tmp/otterstax_backend_discovery_mysql_key", options);
    take_mysql_writes();

    // The discovery's reply: the key off the probe's column flags; a key over a VARBINARY column
    // names no row the federation can write back, so `blobs` has none; nor does a key over a DECIMAL
    // the engine holds as its text (MySQL would compare `k = '…'` as DOUBLE), so `wide` has none.
    const auto& discovered = owner.remote().discovered();
    REQUIRE(discovered.size() == 1);
    for (const auto& table : discovered.front().tables) {
        CAPTURE(table.name.collection.t);
        CHECK(table.primary_key ==
              (table.name.collection.t == "orders" ? std::vector<std::string>{"id"} : std::vector<std::string>{}));
    }

    auto keys = owner.engine().sql("SELECT table_name, column_name, key_position FROM federation.remote_columns "
                                   "ORDER BY table_name, ordinal;");
    REQUIRE(keys);
    INFO((keys->is_error() ? keys->get_error().what.c_str() : "ok"));
    REQUIRE_FALSE(keys->is_error());
    std::vector<std::tuple<std::string, std::string, std::int64_t>> recorded;
    for (const auto& chunk : keys->chunks()) {
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            recorded.emplace_back(chunk.value(0, row).value<std::string_view>(),
                                  chunk.value(1, row).value<std::string_view>(),
                                  chunk.value(2, row).value<std::int64_t>());
        }
    }
    CHECK(recorded == std::vector<std::tuple<std::string, std::string, std::int64_t>>{{"blobs", "k", 0},
                                                                                      {"blobs", "v", 0},
                                                                                      {"orders", "id", 1},
                                                                                      {"orders", "name", 0},
                                                                                      {"wide", "k", 0},
                                                                                      {"wide", "v", 0}});

    REQUIRE_FALSE(owner.engine().sql("CREATE DATABASE loc;")->is_error());
    REQUIRE_FALSE(owner.engine().sql("CREATE TABLE loc.ids (id INT);")->is_error());
    REQUIRE_FALSE(owner.engine().sql("INSERT INTO loc.ids (id) VALUES (2);")->is_error());
    // A row the engine picks is changed by its key; the count is the rows the backend matched,
    // read off its (here German) OK packet info.
    auto updated = owner.engine().sql("UPDATE keyed.db.orders SET name = 'z' WHERE id IN (SELECT id FROM loc.ids);");
    REQUIRE(updated);
    INFO((updated->is_error() ? updated->get_error().what.c_str() : "ok"));
    REQUIRE_FALSE(updated->is_error());
    CHECK(updated->affected_rows() == 1);
    CHECK(take_mysql_writes() == std::vector<std::string>{"UPDATE `db`.`orders` SET `name` = 'z' WHERE `id` = 2"});

    for (const char* table : {"blobs", "wide"}) {
        CAPTURE(table);
        auto refused = owner.engine().sql(std::string{"UPDATE keyed.db."} + table +
                                          " SET v = 0 WHERE v IN (SELECT id FROM loc.ids);");
        REQUIRE(refused);
        REQUIRE(refused->is_error());
        CHECK(std::string{refused->get_error().what.c_str()}.find("has no primary key") != std::string::npos);
        CHECK(take_mysql_writes().empty());
    }
}

// ── MySQL: a server under NO_BACKSLASH_ESCAPES is refused at startup ──

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

TEST_CASE("MySQLManager::discover: a server under NO_BACKSLASH_ESCAPES is left out and the others serve") {
    otterstax::test::stack_options_t options;
    options.connectors.mysql = &sql_mode_mysql_factory;
    options.connections.mysql.push_back(
        config::MysqlConnectionDesc{.alias = "plain", .host = "localhost", .database = "db", .table = "orders"});
    options.connections.mysql.push_back(
        config::MysqlConnectionDesc{.alias = "nbe", .host = "localhost", .database = "db", .table = "orders"});
    otterstax::test::session_stack_owner owner("/tmp/otterstax_backend_discovery_mysql_nbe", options);

    // Startup goes on without the refused connection; nothing of it is recorded.
    auto& remote = owner.remote();
    CHECK(remote.mysql_connectors().hasConnection("plain"));
    CHECK_FALSE(remote.mysql_connectors().hasConnection("nbe"));
    REQUIRE(remote.discovered().size() == 1);
    CHECK(remote.discovered().front().scope.unique_identifier.t == "plain");

    // The other connection serves its table; the refused one is unknown.
    auto served = owner.engine().sql("SELECT id, name FROM plain.db.orders;");
    REQUIRE(served);
    INFO((served->is_error() ? served->get_error().what.c_str() : "ok"));
    REQUIRE_FALSE(served->is_error());
    std::uint64_t rows = 0;
    for (const auto& chunk : served->chunks()) {
        rows += chunk.size();
    }
    CHECK(rows == 2);
    auto refused = owner.engine().sql("SELECT id, name FROM nbe.db.orders;");
    REQUIRE(refused);
    CHECK(refused->is_error());

    // The discovery itself answers the refusal, code and text: asked of the
    // same connection opened again by hand (open() removed it).
    auto added = remote.mysql_connectors().addConnection(conn::api_server::ConnectionParams{
        .alias = "nbe", .host = "localhost", .database = "db", .table = "orders"});
    INFO((added.has_error() ? added.error().what.c_str() : "ok"));
    REQUIRE_FALSE(added.has_error());
    auto reply = remote.mysql().discover(
        qualified_name_t{core::uid_t{"nbe"}, core::dbname_t{"db"}, core::schema_t{""}, core::relname_t{"orders"}});
    REQUIRE(reply.has_error());
    INFO(reply.error().what.c_str());
    CHECK(reply.error().type == core::error_code_t::unimplemented_yet);
    CHECK(std::string{reply.error().what.c_str()}.find("NO_BACKSLASH_ESCAPES") != std::string::npos);
}
