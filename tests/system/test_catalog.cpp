// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The catalog building blocks over the federation, no query session needed
// except where a statement is the subject: literal escaping for the handwritten
// discovery queries; the startup discovery error paths, which must not register
// a half-known table; the database names user DDL must leave alone; and a
// remote DROP TABLE, which runs on its backend and takes the table out of the
// catalog.

#include "catalog/discovery.hpp"
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

        // Sorted "<connection>.<database>.<schema>.<table>" of every table the
        // catalog keeps of a connection's database.
        std::vector<std::string> names() {
            auto cursor = owner.engine().sql(
                "SELECT d.datconnection, d.datname, n.nspname, c.relname FROM pg_catalog.pg_database d JOIN "
                "pg_catalog.pg_namespace n ON n.nspdatabase = d.oid JOIN pg_catalog.pg_class c ON c.relnamespace = "
                "n.oid WHERE d.datconnection <> '' AND c.relkind = 'r';");
            REQUIRE(cursor);
            INFO((cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE_FALSE(cursor->is_error());
            std::vector<std::string> out;
            for (const auto& chunk : cursor->chunks()) {
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    std::string name;
                    for (std::uint64_t column = 0; column < 4; ++column) {
                        name += (column ? "." : "") + std::string{chunk.data[column].get_value<std::string_view>(row)};
                    }
                    out.push_back(std::move(name));
                }
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
// character inside a quoted literal; PostgreSQL does in '...' only under
// standard_conforming_strings = off, and always in E'...': a PostgreSQL value
// holding a backslash is written E'...' with it doubled, which reads the same
// under either setting. Doubling only the apostrophe leaves `\'` able to close
// the literal.

TEST_CASE("escape_sql_literal: trailing backslash cannot swallow the closing quote") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;

    REQUIRE(cat::escape_sql_literal(resource, "a\\", backend_type_t::MySQL) == "'a\\\\'");
    REQUIRE(cat::escape_sql_literal(resource, "a\\", backend_type_t::ClickHouse) == "'a\\\\'");
    REQUIRE(cat::escape_sql_literal(resource, "a\\", backend_type_t::PostgreSQL) == "E'a\\\\'");
}

TEST_CASE("escape_sql_literal: backslash-quote injection stays inside the literal") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;
    const std::string_view payload = "\\' OR 1=1 -- ";

    REQUIRE(cat::escape_sql_literal(resource, payload, backend_type_t::MySQL) == "'\\\\'' OR 1=1 -- '");
    REQUIRE(cat::escape_sql_literal(resource, payload, backend_type_t::ClickHouse) == "'\\\\'' OR 1=1 -- '");
    REQUIRE(cat::escape_sql_literal(resource, payload, backend_type_t::PostgreSQL) == "E'\\\\'' OR 1=1 -- '");
}

// The listing of every user schema leaves out pg_toast* / pg_temp* by a LIKE whose `_` is escaped:
// written E'pg\\_toast%', a server under standard_conforming_strings = off reads the same
// pattern (in 'pg\_toast%' it would read `\_` as `_`, a wildcard).
TEST_CASE("make_list_tables_query: the PostgreSQL LIKE patterns read the same under either standard_conforming_strings") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    const auto query = std::string{cat::make_list_tables_query(&arena, backend_type_t::PostgreSQL, "")};
    INFO(query);
    CHECK(query.find("NOT LIKE E'pg\\\\_toast%'") != std::string::npos);
    CHECK(query.find("NOT LIKE E'pg\\\\_temp%'") != std::string::npos);
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

// ── startup discovery error paths ────────────────────────────────────────────
// A connection whose discovery fails keeps its actor (its statements fail at the
// connection) and records nothing; the server still starts.

namespace {

    void require_left_out(catalog_stack& fx, const std::string& alias) {
        REQUIRE(fx.owner.remote().actor(alias) != nullptr);
        for (const auto& discovery : fx.owner.remote().discovered()) {
            REQUIRE_FALSE(discovery.discovered);
        }
        REQUIRE(fx.names().empty());
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

// A registered column is defined by its name: a table whose discovery answers
// a column without one is left out of the catalog. The discovery itself
// answered, so the connection stays open (its other tables would register).
TEST_CASE("catalog discovery: a discovered column without a name records nothing") {
    config::ConnectionsConfig connections;
    connections.clickhouse.push_back(ch_events("ev_alias", "ev"));
    catalog_stack fx("test_catalog_ch_unnamed", connections, &ch_unnamed_column_factory);
    REQUIRE(fx.owner.remote().discovered().size() == 1);
    REQUIRE(fx.owner.remote().discovered().front().discovered);
    REQUIRE(fx.names().empty());
}

TEST_CASE("catalog discovery: a ClickHouse connection without a database is refused before probing") {
    // The replay connector would answer a probe with no header block; the
    // refusal comes before it.
    config::ConnectionsConfig connections;
    connections.clickhouse.push_back(ch_events("nodb", ""));
    catalog_stack fx("test_catalog_ch_nodb", connections, &ch_empty_probe_factory);
    require_left_out(fx, "nodb");
}

// No `schema:` is every user schema of the database. The mock's
// information_schema listing answers one table, `public`.`table`, so the
// connection names no table (every table of every user schema).
TEST_CASE("catalog discovery: a PostgreSQL connection without a schema registers every user schema") {
    config::ConnectionsConfig connections;
    connections.postgresql.push_back(pg_connection("noschema", "pgdb", "", ""));
    catalog_stack fx("test_catalog_pg_noschema", connections);
    REQUIRE(fx.names() == std::vector<std::string>{"noschema.pgdb.public.table"});
    auto read =
        otterstax::test::run_session_sql_payload(fx.owner.stack(), 1, "SELECT * FROM noschema.pgdb.public.\"table\";");
    INFO((read.has_error() ? read.error().what.c_str() : "ok"));
    REQUIRE_FALSE(read.has_error());
    REQUIRE(read.value().size() == 2);
}

// ── owned database names ─────────────────────────────────────────────────────
// A connection of config.yaml (a local database of that name would read like
// the connection) and OtterStax's own database. Unquoted identifiers reach the engine lower-cased, so the
// match ignores case. `kafka` is not owned: the Kafka objects are the schema main.kafka.

TEST_CASE("catalog check_database_ownership: connection uids and federation are owned names") {
    config::ConnectionsConfig connections;
    connections.postgresql.push_back(pg_connection("PgAlias", "pgdb", "public", "orders"));
    catalog_stack fx("test_catalog_ownership", connections);
    auto* resource = fx.owner.engine().resource();
    const auto aliases = fx.owner.remote().aliases();

    for (const char* owned : {"PgAlias", "pgalias", "PGALIAS", "federation", "Federation"}) {
        INFO("database name = " << owned);
        auto verdict = cat::check_database_ownership(resource, owned, aliases);
        REQUIRE(verdict.contains_error());
        REQUIRE(verdict.type == core::error_code_t::invalid_parameter);
    }
    // The owner is named in the configured spelling.
    CHECK(std::string{cat::check_database_ownership(resource, "pgalias", aliases).what.c_str()} ==
          "database 'pgalias' is owned by connection 'PgAlias'");

    REQUIRE_FALSE(cat::check_database_ownership(resource, "orders", aliases).contains_error());
    REQUIRE_FALSE(cat::check_database_ownership(resource, "pgdb", aliases).contains_error());
    REQUIRE_FALSE(cat::check_database_ownership(resource, "kafka", aliases).contains_error());
}

// ── remote DDL ───────────────────────────────────────────────────────────────
// A DROP TABLE of a connection's table is otterbrix's DDL plan: the backend
// actor runs the backend's DROP at the COMMIT, the catalog's rows go with it.

TEST_CASE("catalog remote DDL: DROP TABLE runs on the backend and takes the table out of the catalog") {
    catalog_stack fx("test_catalog_remote_drop", two_pg());
    const auto s = fx.owner.stack();
    REQUIRE(fx.names() == std::vector<std::string>{"crm.crmdb.sales.customers", "shop.pgdb.public.orders"});

    auto dropped = otterstax::test::run_session_sql_payload(s, 1, "DROP TABLE shop.pgdb.public.orders;");
    INFO((dropped.has_error() ? dropped.error().what.c_str() : "ok"));
    REQUIRE_FALSE(dropped.has_error());
    REQUIRE(fx.names() == std::vector<std::string>{"crm.crmdb.sales.customers"});

    // Another table of a connection configured with one `table:` (crm is
    // `customers` only) is no DDL target of it: the session refuses the DROP
    // before the engine (the mock backend would answer any table it is asked
    // about) and the catalog keeps what it had.
    auto unknown = otterstax::test::run_session_sql_payload(s, 2, "DROP TABLE crm.crmdb.sales.never_discovered;");
    REQUIRE(unknown.has_error());
    INFO(unknown.error().what.c_str());
    CHECK(unknown.error().type == core::error_code_t::invalid_parameter);
    CHECK(std::string{unknown.error().what.c_str()}.find("connection \"crm\" is configured with table \"customers\"") !=
          std::string::npos);
    REQUIRE(fx.names() == std::vector<std::string>{"crm.crmdb.sales.customers"});

    // A four-part name whose first part is no connection of config.yaml is
    // not discovered: otterbrix's "does not exist".
    auto foreign = otterstax::test::run_session_sql_payload(s, 3, "DROP TABLE nowhere.crmdb.sales.customers;");
    REQUIRE(foreign.has_error());
    REQUIRE(fx.names() == std::vector<std::string>{"crm.crmdb.sales.customers"});
}
