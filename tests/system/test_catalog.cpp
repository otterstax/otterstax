// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The catalog building blocks over the federation, no query session needed
// except where a statement is the subject: literal escaping for the handwritten
// discovery queries; the Flight SQL GetTables listing (catalog::list_tables over
// the engine's pg_catalog and federation.remote_tables) and its filters; the
// startup discovery error paths, which must not record a half-known table; the
// database names user DDL must leave alone; and a remote DROP TABLE, which runs
// on its backend and takes the table out of the federation metadata.

#include "catalog/discovery.hpp"
#include "catalog/introspection.hpp"
#include "catalog/ownership.hpp"
#include "session_stack.hpp"

#include "../mock/ch_db_connector.hpp"
#include "../mock/pg_db_connector.hpp"

#include <boost/asio.hpp>
#include <clickhouse/columns/numeric.h>
#include <core/result_wrapper.hpp>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <memory_resource>
#include <string>
#include <vector>

namespace {

    namespace cat = otterstax::catalog;

    // ClickHouse connector whose schema-discovery overload replays a fixed set of
    // blocks — an empty vector reproduces a probe that produced no header block.
    class ch_replay_connector final : public ch::IConnector {
    public:
        ch_replay_connector(ch::connect_params params, std::string alias, std::vector<clickhouse::Block> blocks)
            : params_(std::move(params))
            , alias_(std::move(alias))
            , blocks_(std::move(blocks)) {}

        ch::Status status() const noexcept override { return ch::Status::Connected; }
        ch::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>>>
        runQuery(
            std::string_view,
            otterstax::function_ref_t<std::unique_ptr<components::vector::data_chunk_t>(const ch::select_result_t&)>)
            override {
            co_return std::unique_ptr<components::vector::data_chunk_t>{};
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)>) override {
            co_return int64_t{0};
        }

        // Answers the schema probe with the replayed blocks; system.columns
        // with no rows (a table without overrides).
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t replayed;
            if (query.find("system.columns") == std::string_view::npos) {
                replayed.blocks = blocks_;
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(replayed));
        }

    private:
        ch::connect_params params_;
        std::string alias_;
        std::vector<clickhouse::Block> blocks_;
    };

    std::unique_ptr<ch::IConnector>
    ch_empty_probe_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        return std::make_unique<ch_replay_connector>(std::move(params),
                                                     std::move(alias),
                                                     std::vector<clickhouse::Block>{});
    }

    std::unique_ptr<ch::IConnector>
    ch_columnless_block_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        std::vector<clickhouse::Block> blocks;
        blocks.emplace_back();
        return std::make_unique<ch_replay_connector>(std::move(params), std::move(alias), std::move(blocks));
    }

    // The probe answers a named column followed by a column without a name.
    std::unique_ptr<ch::IConnector>
    ch_unnamed_column_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        std::vector<clickhouse::Block> blocks;
        blocks.emplace_back();
        blocks.back().AppendColumn("id", std::make_shared<clickhouse::ColumnUInt32>());
        blocks.back().AppendColumn("", std::make_shared<clickhouse::ColumnUInt32>());
        return std::make_unique<ch_replay_connector>(std::move(params), std::move(alias), std::move(blocks));
    }

    config::PgConnectionDesc pg_connection(const std::string& alias,
                                const std::string& database,
                                const std::string& schema,
                                const std::string& table) {
        return config::PgConnectionDesc{.alias = alias,
                                        .host = "localhost",
                                        .port = "5432",
                                        .username = "user",
                                        .password = "pass",
                                        .database = database,
                                        .schema = schema,
                                        .table = table};
    }

    config::ChConnectionDesc ch_events(const std::string& alias, const std::string& database) {
        return config::ChConnectionDesc{.alias = alias, .host = "localhost", .database = database, .table = "events"};
    }

    // A stack over the PostgreSQL mock (each table it is asked about answers
    // (id int4, name varchar)) and the connections of `connections`.
    struct catalog_stack {
        otterstax::test::session_stack_owner owner;

        explicit catalog_stack(const std::string& leaf,
                               config::ConnectionsConfig connections,
                               ch::connector_factory ch_factory = &ch_mock_connector_factory)
            : owner(otterstax::test::unique_test_dir(leaf), options(std::move(connections), ch_factory)) {}

        static otterstax::test::stack_options_t options(config::ConnectionsConfig connections,
                                                        ch::connector_factory ch_factory) {
            otterstax::test::stack_options_t options;
            options.connectors.clickhouse = ch_factory;
            options.connections = std::move(connections);
            return options;
        }

        cat::rows_t read(std::string_view sql) {
            auto cursor = owner.engine().sql(std::string{sql});
            REQUIRE(cursor);
            INFO(sql << ": " << (cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
            cat::rows_t rows{owner.engine().resource()};
            for (const auto& chunk : cursor->chunks()) {
                rows.push_back(chunk.partial_copy(owner.engine().resource(), 0, chunk.size()));
            }
            return rows;
        }

        // Every table GetTables lists for `command`, as the frontend computes it.
        std::pmr::vector<table_info> tables(const cat::get_tables_command_t& command) {
            auto namespaces = read(cat::namespaces_sql);
            auto classes = read(cat::classes_sql);
            auto remote_tables = read(cat::remote_tables_sql);
            cat::rows_t attributes{owner.engine().resource()};
            cat::rows_t remote_columns{owner.engine().resource()};
            if (command.include_schema) {
                attributes = read(cat::attributes_sql);
                remote_columns = read(cat::remote_columns_sql);
            }
            auto listed = cat::list_tables(owner.engine().resource(),
                                           command,
                                           namespaces,
                                           classes,
                                           command.include_schema ? &attributes : nullptr,
                                           remote_tables,
                                           command.include_schema ? &remote_columns : nullptr);
            INFO((listed.has_error() ? listed.error().what.c_str() : "ok"));
            REQUIRE_FALSE(listed.has_error());
            return std::move(listed.value());
        }

        // Sorted "<catalog>.<schema>.<table>" of every listed table.
        std::vector<std::string> names(const cat::get_tables_command_t& command) {
            std::vector<std::string> out;
            for (const auto& table : tables(command)) {
                out.push_back(table.name.database.t + "." + table.name.schema.t + "." + table.name.collection.t);
            }
            std::sort(out.begin(), out.end());
            return out;
        }
    };

    config::ConnectionsConfig two_pg() {
        config::ConnectionsConfig connections;
        connections.postgresql.push_back(pg_connection("shop", "pgdb", "public", "orders"));
        connections.postgresql.push_back(pg_connection("crm", "crmdb", "sales", "customers"));
        return connections;
    }

} // namespace

// ── escape_sql_literal ───────────────────────────────────────────────────────
// MySQL (default sql_mode) and ClickHouse treat a backslash as an escape
// character inside a quoted literal; PostgreSQL (standard_conforming_strings)
// does not. Doubling only the apostrophe leaves `\'` able to close the literal.

TEST_CASE("escape_sql_literal: trailing backslash cannot swallow the closing quote") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;

    REQUIRE(cat::escape_sql_literal(resource, "a\\", backend_type_t::MySQL) == "'a\\\\'");
    REQUIRE(cat::escape_sql_literal(resource, "a\\", backend_type_t::ClickHouse) == "'a\\\\'");
    REQUIRE(cat::escape_sql_literal(resource, "a\\", backend_type_t::PostgreSQL) == "'a\\'");
}

TEST_CASE("escape_sql_literal: backslash-quote injection stays inside the literal") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;
    const std::string_view payload = "\\' OR 1=1 -- ";

    REQUIRE(cat::escape_sql_literal(resource, payload, backend_type_t::MySQL) == "'\\\\'' OR 1=1 -- '");
    REQUIRE(cat::escape_sql_literal(resource, payload, backend_type_t::ClickHouse) == "'\\\\'' OR 1=1 -- '");
    REQUIRE(cat::escape_sql_literal(resource, payload, backend_type_t::PostgreSQL) == "'\\'' OR 1=1 -- '");
}

TEST_CASE("escape_sql_literal: apostrophes are doubled on every backend") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;

    for (auto backend : {backend_type_t::MySQL, backend_type_t::PostgreSQL, backend_type_t::ClickHouse}) {
        INFO("backend = " << static_cast<int>(backend));
        REQUIRE(cat::escape_sql_literal(resource, "O'Brien", backend) == "'O''Brien'");
        REQUIRE(cat::escape_sql_literal(resource, "", backend) == "''");
    }
}

// ── GetTables ────────────────────────────────────────────────────────────────
// A remote table is listed as the backend spells it, a local one under its
// database with an empty schema; the federation metadata tables are hidden. The
// GetTables filters are the Flight SQL frontend's, applied over the listing
// (tests/system/test_flightsql_metadata.cpp).

TEST_CASE("catalog GetTables: local tables are listed beside the remote ones") {
    catalog_stack fx("test_catalog_get_tables_local", two_pg());
    REQUIRE_FALSE(fx.owner.engine().sql("CREATE DATABASE loc;")->is_error());
    REQUIRE_FALSE(fx.owner.engine().sql("CREATE TABLE loc.items (id bigint, label string);")->is_error());

    cat::get_tables_command_t command;
    REQUIRE(fx.names(command) ==
            std::vector<std::string>{"crmdb.sales.customers", "loc..items", "pgdb.public.orders"});
}

TEST_CASE("catalog GetTables: include_schema attaches the discovered columns") {
    config::ConnectionsConfig connections;
    connections.postgresql.push_back(pg_connection("shop", "pgdb", "public", "orders"));
    catalog_stack fx("test_catalog_include_schema", connections);

    cat::get_tables_command_t command;
    command.include_schema = true;
    auto listed = fx.tables(command);
    REQUIRE(listed.size() == 1);
    const auto& schema = listed.front().schema;
    REQUIRE(schema.type() == components::types::logical_type::STRUCT);
    REQUIRE(schema.child_types().size() == 2);
    CHECK(schema.child_types()[0].alias() == "id");
    CHECK(schema.child_types()[0].type() == components::types::logical_type::INTEGER);
    CHECK(schema.child_types()[1].alias() == "name");
    CHECK(schema.child_types()[1].type() == components::types::logical_type::STRING_LITERAL);
}

// ── startup discovery error paths ────────────────────────────────────────────
// A connection whose discovery fails is not kept (the connector manager drops
// it) and records nothing; the server still starts.

namespace {

    void require_left_out(catalog_stack& fx, const std::string& alias) {
        REQUIRE_FALSE(fx.owner.remote().clickhouse_connectors().hasConnection(alias));
        REQUIRE(fx.owner.remote().discovered().empty());
        REQUIRE(fx.names(cat::get_tables_command_t{}).empty());
    }

} // namespace

TEST_CASE("catalog discovery: a ClickHouse probe without a header block records nothing") {
    config::ConnectionsConfig connections;
    connections.clickhouse.push_back(ch_events("ev_alias", "ev"));
    catalog_stack fx("test_catalog_ch_no_header", connections, &ch_empty_probe_factory);
    require_left_out(fx, "ev_alias");
}

TEST_CASE("catalog discovery: a column-less ClickHouse header block records nothing") {
    config::ConnectionsConfig connections;
    connections.clickhouse.push_back(ch_events("ev_alias", "ev"));
    catalog_stack fx("test_catalog_ch_columnless", connections, &ch_columnless_block_factory);
    require_left_out(fx, "ev_alias");
}

// A recorded column is defined by its name: a column the backend answers
// without one fails the connection's discovery.
TEST_CASE("catalog discovery: a discovered column without a name records nothing") {
    config::ConnectionsConfig connections;
    connections.clickhouse.push_back(ch_events("ev_alias", "ev"));
    catalog_stack fx("test_catalog_ch_unnamed", connections, &ch_unnamed_column_factory);
    require_left_out(fx, "ev_alias");
}

TEST_CASE("catalog discovery: a ClickHouse connection without a database is refused before probing") {
    // The replay connector would answer a probe with no header block; the
    // refusal comes before it.
    config::ConnectionsConfig connections;
    connections.clickhouse.push_back(ch_events("nodb", ""));
    catalog_stack fx("test_catalog_ch_nodb", connections, &ch_empty_probe_factory);
    require_left_out(fx, "nodb");
}

TEST_CASE("catalog discovery: a PostgreSQL connection without a schema is discovered under public") {
    config::ConnectionsConfig connections;
    connections.postgresql.push_back(pg_connection("noschema", "pgdb", "", "orders"));
    catalog_stack fx("test_catalog_pg_noschema", connections);
    REQUIRE(fx.names(cat::get_tables_command_t{}) == std::vector<std::string>{"pgdb.public.orders"});
    auto read = otterstax::test::run_session_sql_payload(fx.owner.stack(), 1, "SELECT * FROM noschema.pgdb.public.orders;");
    INFO((read.has_error() ? read.error().what.c_str() : "ok"));
    REQUIRE_FALSE(read.has_error());
    REQUIRE(read.value().size() == 2);
}

// ── owned database names ─────────────────────────────────────────────────────
// A connection uid (an engine database of that name would capture the
// connection's remote names), the kafka object database and the federation
// metadata database. Unquoted identifiers reach the engine lower-cased, so the
// match ignores case.

TEST_CASE("catalog check_database_ownership: connection uids, kafka and federation are owned names") {
    config::ConnectionsConfig connections;
    connections.postgresql.push_back(pg_connection("PgAlias", "pgdb", "public", "orders"));
    catalog_stack fx("test_catalog_ownership", connections);
    auto* resource = fx.owner.engine().resource();

    for (const char* owned : {"PgAlias", "pgalias", "PGALIAS", "kafka", "KAFKA", "federation", "Federation"}) {
        INFO("database name = " << owned);
        auto verdict = cat::check_database_ownership(resource, owned);
        REQUIRE(verdict.contains_error());
        REQUIRE(verdict.type == core::error_code_t::invalid_parameter);
    }
    // The owner is named in the configured spelling.
    CHECK(std::string{cat::check_database_ownership(resource, "pgalias").what.c_str()} ==
          "database 'pgalias' is owned by connection 'PgAlias'");

    REQUIRE_FALSE(cat::check_database_ownership(resource, "orders").contains_error());
    REQUIRE_FALSE(cat::check_database_ownership(resource, "pgdb").contains_error());
}

// ── remote DDL ───────────────────────────────────────────────────────────────
// DDL never reaches the engine's host hooks, so a DDL statement naming a remote
// table runs on its backend directly (the generated statement) and the
// federation metadata follows: a dropped table's rows go.

TEST_CASE("catalog remote DDL: DROP TABLE runs on the backend and takes the table out of the metadata") {
    catalog_stack fx("test_catalog_remote_drop", two_pg());
    const auto s = fx.owner.stack();

    auto dropped = otterstax::test::run_session_sql_payload(s, 1, "DROP TABLE shop.pgdb.public.orders;");
    INFO((dropped.has_error() ? dropped.error().what.c_str() : "ok"));
    REQUIRE_FALSE(dropped.has_error());
    REQUIRE(fx.names(cat::get_tables_command_t{}) == std::vector<std::string>{"crmdb.sales.customers"});

    // A table the metadata never held: the statement still goes to the backend.
    auto unknown = otterstax::test::run_session_sql_payload(s, 2, "DROP TABLE crm.crmdb.sales.never_discovered;");
    INFO((unknown.has_error() ? unknown.error().what.c_str() : "ok"));
    REQUIRE_FALSE(unknown.has_error());
    REQUIRE(fx.names(cat::get_tables_command_t{}) == std::vector<std::string>{"crmdb.sales.customers"});
}
