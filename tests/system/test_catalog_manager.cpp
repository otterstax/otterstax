// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// CatalogManager contracts that do not need a Scheduler: literal escaping for
// the handwritten discovery queries, the FlightSQL GetTables filter semantics,
// backend classification of DDL targets, and the discovery error paths that
// must not register a half-known table.
#include "catalog/catalog_manager.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "test_helpers.hpp"

#include "../mock/ch_db_connector.hpp"
#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"
#include "../mock/pg_db_connector.hpp"
#include "../mock/sql_db_connector.hpp"

#include <actor-zeta.hpp>
#include <boost/asio.hpp>
#include <clickhouse/columns/numeric.h>
#include <components/logical_plan/node_drop.hpp>
#include <core/result_wrapper.hpp>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <chrono>
#include <memory_resource>
#include <string>
#include <thread>
#include <vector>

namespace {

    using otterstax::test::wait_until_ready;

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

        // Answers every metadata query (named types and the schema probe)
        // with the same replayed blocks.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t replayed;
            replayed.blocks = blocks_;
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

    // ClickHouse connector whose schema probe answers a header block with a
    // named column followed by a column without a name; the named-types query
    // (system.columns) answers no rows, as for a table without overrides.
    class ch_unnamed_column_connector final : public ch::IConnector {
    public:
        ch_unnamed_column_connector(ch::connect_params params, std::string alias)
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

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t answer;
            if (query.find("system.columns") == std::string_view::npos) {
                clickhouse::Block header;
                header.AppendColumn("id", std::make_shared<clickhouse::ColumnUInt32>());
                header.AppendColumn("", std::make_shared<clickhouse::ColumnUInt32>());
                answer.blocks.push_back(std::move(header));
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(answer));
        }

    private:
        ch::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<ch::IConnector>
    ch_unnamed_column_factory(std::pmr::memory_resource*, ch::connect_params params, std::string alias) {
        return std::make_unique<ch_unnamed_column_connector>(std::move(params), std::move(alias));
    }

    // Catalog actor plus the three connector managers over mocked connectors
    // and the three backend actors that drive them (the catalog runs every
    // discovery through those actors, and their constructors start the
    // connector pools). Members are declared so that the actors are destroyed
    // before the managers and the managers before the catalog.
    struct catalog_fixture {
        std::pmr::memory_resource* resource;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog;
        std::unique_ptr<mysql::ConnectorManager> mysql_conn;
        std::unique_ptr<pg::ConnectorManager> pg_conn;
        std::unique_ptr<ch::ConnectorManager> ch_conn;
        std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> mysql_manager;
        std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> pg_manager;
        std::unique_ptr<db::ClickhouseManager, actor_zeta::pmr::deleter_t> ch_manager;

        explicit catalog_fixture(std::pmr::memory_resource* res,
                                 ch::connector_factory ch_factory = &ch_mock_connector_factory)
            : catalog_fixture(res,
                              std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res}),
                              ch_factory) {}

        // `engine` is the data manager behind the OtterbrixManager the catalog
        // registers into, for tests that need a particular engine verdict.
        catalog_fixture(std::pmr::memory_resource* res,
                        std::unique_ptr<IDataManager> engine,
                        ch::connector_factory ch_factory)
            : resource(res)
            , otterbrix_manager(actor_zeta::spawn<db::OtterbrixManager>(res, std::move(engine)))
            , catalog(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager->address()))
            , mysql_conn(std::make_unique<mysql::ConnectorManager>(res,
                                                                   catalog->address(),
                                                                   &mysql_mock_connector_factory,
                                                                   /*pool_size*/ 1))
            , pg_conn(std::make_unique<pg::ConnectorManager>(res, catalog->address(), &pg_mock_connector_factory, 1))
            , ch_conn(std::make_unique<ch::ConnectorManager>(res, catalog->address(), ch_factory, 1))
            , mysql_manager(actor_zeta::spawn<db::MySQLManager>(res, mysql_conn.get()))
            , pg_manager(actor_zeta::spawn<db::PostgressManager>(res, pg_conn.get()))
            , ch_manager(actor_zeta::spawn<db::ClickhouseManager>(res, ch_conn.get())) {
            catalog->set_backend_managers(mysql_manager->address(), pg_manager->address(), ch_manager->address());
        }

        // Registers a PostgreSQL alias whose single table is discovered eagerly by
        // addConnection (through the catalog actor).
        void add_pg(const std::string& alias,
                    const std::string& database,
                    const std::string& schema,
                    const std::string& table) {
            conn::api_server::PgConnectionParams params;
            params.alias = alias;
            params.host = "localhost";
            params.port = "5432";
            params.username = "user";
            params.password = "pass";
            params.database = database;
            params.schema = schema;
            params.table = table;
            auto added = pg_conn->addConnection(params);
            REQUIRE_FALSE(added.has_error());
        }

        core::error_t add_connection_schema(qualified_name_t name, catalog_ext::ConnectionType type) {
            auto [needs_sched, future] = actor_zeta::send(catalog->address(),
                                                          &mysql::CatalogManager::add_connection_schema,
                                                          std::move(name),
                                                          type);
            wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::error_t check_database_ownership(std::string dbname) {
            auto [needs_sched, future] = actor_zeta::send(catalog->address(),
                                                          &mysql::CatalogManager::check_database_ownership,
                                                          std::move(dbname));
            wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::result_wrapper_t<ParsedQueryDataPtr> update_backend_type(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(catalog->address(), &mysql::CatalogManager::update_backend_type, id, std::move(data));
            wait_until_ready(future);
            return std::move(future).take_ready();
        }

        // Sorted "<db>.<schema>.<table>" of every row get_tables returns.
        std::vector<std::string> list_tables(const arrow::flight::sql::GetTables& command) {
            auto [needs_sched, future] =
                actor_zeta::send(catalog->address(), &mysql::CatalogManager::get_tables, command);
            wait_until_ready(future);
            auto result = std::move(future).take_ready();
            REQUIRE_FALSE(result.has_error());
            std::vector<std::string> names;
            for (const auto& table : result.value()) {
                names.push_back(table.name.database + "." + table.name.schema + "." + table.name.collection);
            }
            std::sort(names.begin(), names.end());
            return names;
        }
    };

    // An engine that stamps the same oid on every collection it creates and
    // keeps the statements it is handed: the second registration collides in
    // the catalog's store, and the undo the catalog issues is observable.
    class constant_oid_data_manager final : public SimpleMockOtterbrixManager {
    public:
        explicit constant_oid_data_manager(mock_config config)
            : SimpleMockOtterbrixManager(config) {}

        components::cursor::cursor_t_ptr execute_sql(const std::string& query) override {
            statements.push_back(query);
            return components::cursor::make_cursor(resource());
        }

        components::cursor::cursor_t_ptr create_collection(const std::string&,
                                                           const std::string&,
                                                           std::vector<components::table::column_definition_t>,
                                                           components::catalog::oid_t& out_oid) override {
            out_oid = components::catalog::FIRST_USER_OID;
            return components::cursor::make_cursor(resource());
        }

        std::vector<std::string> statements;
    };

    arrow::flight::sql::GetTables get_tables_command() {
        arrow::flight::sql::GetTables command;
        command.include_schema = false;
        return command;
    }

} // namespace

// ── escape_sql_literal ───────────────────────────────────────────────────────
// MySQL (default sql_mode) and ClickHouse treat a backslash as an escape
// character inside a quoted literal; PostgreSQL (standard_conforming_strings)
// does not. Doubling only the apostrophe leaves `\'` able to close the literal.

TEST_CASE("escape_sql_literal: trailing backslash cannot swallow the closing quote") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;

    REQUIRE(otterstax::catalog::escape_sql_literal(resource, "a\\", backend_type_t::MySQL) == "'a\\\\'");
    REQUIRE(otterstax::catalog::escape_sql_literal(resource, "a\\", backend_type_t::ClickHouse) == "'a\\\\'");
    REQUIRE(otterstax::catalog::escape_sql_literal(resource, "a\\", backend_type_t::PostgreSQL) == "'a\\'");
}

TEST_CASE("escape_sql_literal: backslash-quote injection stays inside the literal") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;
    const std::string_view payload = "\\' OR 1=1 -- ";

    REQUIRE(otterstax::catalog::escape_sql_literal(resource, payload, backend_type_t::MySQL) == "'\\\\'' OR 1=1 -- '");
    REQUIRE(otterstax::catalog::escape_sql_literal(resource, payload, backend_type_t::ClickHouse) ==
            "'\\\\'' OR 1=1 -- '");
    REQUIRE(otterstax::catalog::escape_sql_literal(resource, payload, backend_type_t::PostgreSQL) ==
            "'\\'' OR 1=1 -- '");
}

TEST_CASE("escape_sql_literal: apostrophes are doubled on every backend") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;

    for (auto backend : {backend_type_t::MySQL, backend_type_t::PostgreSQL, backend_type_t::ClickHouse}) {
        INFO("backend = " << static_cast<int>(backend));
        REQUIRE(otterstax::catalog::escape_sql_literal(resource, "O'Brien", backend) == "'O''Brien'");
        REQUIRE(otterstax::catalog::escape_sql_literal(resource, "", backend) == "''");
    }
}

// ── get_tables ───────────────────────────────────────────────────────────────
// FlightSQL GetTables: `catalog` is an exact match, the two *_filter_pattern
// fields are SQL LIKE patterns (% and _), and `table_types` restricts the
// result to the listed types.

TEST_CASE("catalog get_tables: schema and table filters are LIKE patterns") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);
    fx.add_pg("shop", "pgdb", "public", "orders");
    fx.add_pg("crm", "crmdb", "sales", "customers");

    auto command = get_tables_command();
    REQUIRE(fx.list_tables(command) == std::vector<std::string>{"crmdb.sales.customers", "pgdb.public.orders"});

    SECTION("catalog is an exact match on the database part") {
        command.catalog = "pgdb";
        REQUIRE(fx.list_tables(command) == std::vector<std::string>{"pgdb.public.orders"});
        command.catalog = "pg%";
        REQUIRE(fx.list_tables(command).empty());
    }

    SECTION("db_schema_filter_pattern honours % and _") {
        command.db_schema_filter_pattern = "pub%";
        REQUIRE(fx.list_tables(command) == std::vector<std::string>{"pgdb.public.orders"});
        command.db_schema_filter_pattern = "s_les";
        REQUIRE(fx.list_tables(command) == std::vector<std::string>{"crmdb.sales.customers"});
        command.db_schema_filter_pattern = "%";
        REQUIRE(fx.list_tables(command).size() == 2);
        command.db_schema_filter_pattern = "public";
        REQUIRE(fx.list_tables(command) == std::vector<std::string>{"pgdb.public.orders"});
    }

    SECTION("table_name_filter_pattern is applied") {
        command.table_name_filter_pattern = "%s";
        REQUIRE(fx.list_tables(command).size() == 2);
        command.table_name_filter_pattern = "cust_mers";
        REQUIRE(fx.list_tables(command) == std::vector<std::string>{"crmdb.sales.customers"});
        command.table_name_filter_pattern = "%der%";
        REQUIRE(fx.list_tables(command) == std::vector<std::string>{"pgdb.public.orders"});
        command.table_name_filter_pattern = "order";
        REQUIRE(fx.list_tables(command).empty());
    }

    SECTION("filters combine") {
        command.catalog = "crmdb";
        command.db_schema_filter_pattern = "s%";
        command.table_name_filter_pattern = "c%";
        REQUIRE(fx.list_tables(command) == std::vector<std::string>{"crmdb.sales.customers"});
        command.table_name_filter_pattern = "o%";
        REQUIRE(fx.list_tables(command).empty());
    }
}

TEST_CASE("catalog get_tables: table_types restricts the listing") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);
    fx.add_pg("shop", "pgdb", "public", "orders");

    auto command = get_tables_command();
    command.table_types = {"VIEW"};
    REQUIRE(fx.list_tables(command).empty());

    command.table_types = {std::string{catalog_ext::table_type_name}};
    REQUIRE(fx.list_tables(command) == std::vector<std::string>{"pgdb.public.orders"});

    command.table_types = {"VIEW", std::string{catalog_ext::table_type_name}};
    REQUIRE(fx.list_tables(command) == std::vector<std::string>{"pgdb.public.orders"});
}

TEST_CASE("catalog get_tables: include_schema attaches the discovered STRUCT") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);
    fx.add_pg("shop", "pgdb", "public", "orders");

    auto command = get_tables_command();
    command.include_schema = true;
    auto [needs_sched, future] = actor_zeta::send(fx.catalog->address(), &mysql::CatalogManager::get_tables, command);
    wait_until_ready(future);
    auto result = std::move(future).take_ready();
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.value().size() == 1);
    REQUIRE(result.value().front().schema.type() == components::types::logical_type::STRUCT);
}

// ── backend classification ───────────────────────────────────────────────────

TEST_CASE("catalog update_backend_type: classifying an already classified statement is an error") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);
    fx.add_pg("products", "pgdb", "public", "products");

    GreenplumParser parser(resource);
    auto parsed = parser.parse("SELECT id, name FROM products.pgdb.public.products;");
    REQUIRE_FALSE(parsed.has_error());

    auto first = fx.update_backend_type(session_hash_t{1}, std::move(parsed.value()));
    REQUIRE_FALSE(first.has_error());
    auto classified = std::move(first.value());
    REQUIRE(classified->backend_type == backend_type_t::PostgreSQL);

    auto second = fx.update_backend_type(session_hash_t{1}, std::move(classified));
    REQUIRE(second.has_error());
    REQUIRE(second.error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("catalog update_backend_type: DROP TABLE needs no registered schema and is routed to its backend") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);
    fx.add_pg("products", "pgdb", "public", "products");

    GreenplumParser parser(resource);

    SECTION("registered table") {
        auto parsed = parser.parse("DROP TABLE products.pgdb.public.products;");
        REQUIRE_FALSE(parsed.has_error());
        auto result = fx.update_backend_type(session_hash_t{1}, std::move(parsed.value()));
        REQUIRE_FALSE(result.has_error());
        REQUIRE(result.value()->backend_type == backend_type_t::PostgreSQL);
    }

    SECTION("table the catalog never discovered — no discovery is attempted") {
        auto parsed = parser.parse("DROP TABLE products.pgdb.public.never_discovered;");
        REQUIRE_FALSE(parsed.has_error());
        auto result = fx.update_backend_type(session_hash_t{2}, std::move(parsed.value()));
        REQUIRE_FALSE(result.has_error());
        REQUIRE(result.value()->backend_type == backend_type_t::PostgreSQL);

        auto listed = fx.list_tables(get_tables_command());
        REQUIRE(listed == std::vector<std::string>{"pgdb.public.products"});
    }
}

TEST_CASE("catalog update_backend_type: only collection/index drop kinds may carry an external target") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);
    fx.add_pg("products", "pgdb", "public", "products");

    GreenplumParser parser(resource);

    auto with_drop_kind = [&](components::logical_plan::drop_target_kind kind) {
        auto parsed = parser.parse("DROP TABLE products.pgdb.public.products;");
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        REQUIRE(data->otterbrix_params->external_nodes.size() == 1);
        REQUIRE(data->otterbrix_params->external_nodes.front().size() == 1);
        // Re-point the external entry at a drop node of the requested kind; the
        // resolved target (alias products) is kept as the parser produced it.
        data->otterbrix_params->node = components::logical_plan::make_node_drop(resource, kind);
        data->otterbrix_params->external_nodes.front().front().node = &data->otterbrix_params->node;
        return data;
    };

    SECTION("index kind is accepted") {
        auto result = fx.update_backend_type(session_hash_t{1},
                                             with_drop_kind(components::logical_plan::drop_target_kind::index));
        REQUIRE_FALSE(result.has_error());
        REQUIRE(result.value()->backend_type == backend_type_t::PostgreSQL);
    }

    SECTION("view / sequence / type / macro / database kinds are contract violations") {
        for (auto kind : {components::logical_plan::drop_target_kind::view,
                          components::logical_plan::drop_target_kind::sequence,
                          components::logical_plan::drop_target_kind::type,
                          components::logical_plan::drop_target_kind::macro,
                          components::logical_plan::drop_target_kind::database}) {
            INFO("drop kind = " << static_cast<int>(kind));
            auto result = fx.update_backend_type(session_hash_t{1}, with_drop_kind(kind));
            REQUIRE(result.has_error());
            REQUIRE(result.error().type == core::error_code_t::invalid_parameter);
        }
    }
}

// ── discovery error paths ────────────────────────────────────────────────────

// addConnection registers eagerly through the catalog and keeps the
// connection only when that succeeded, so a discovery failure surfaces as the
// addConnection verdict, with the catalog's error code, and nothing is listed.

TEST_CASE("catalog add_connection_schema: a ClickHouse probe without a header block registers nothing") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource, &ch_empty_probe_factory);

    ch::connect_params params;
    params.host = "localhost";
    params.database = "ev";
    params.table = "events";
    auto added = fx.ch_conn->addConnection(params, "ev_alias");
    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::schema_error);
    REQUIRE_FALSE(fx.ch_conn->hasConnection("ev_alias"));
    REQUIRE(fx.list_tables(get_tables_command()).empty());
}

TEST_CASE("catalog add_connection_schema: a column-less ClickHouse header block registers nothing") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource, &ch_columnless_block_factory);

    ch::connect_params params;
    params.host = "localhost";
    params.database = "ev";
    params.table = "events";
    auto added = fx.ch_conn->addConnection(params, "ev_alias");
    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::schema_error);
    REQUIRE_FALSE(fx.ch_conn->hasConnection("ev_alias"));
    REQUIRE(fx.list_tables(get_tables_command()).empty());
}

// A mirrored column is defined by its name. A discovered column the backend
// answers without one is a schema_error for the whole registration: its type
// carries no alias, and the engine's alias() has no null guard for such a type.
TEST_CASE("catalog add_connection_schema: a discovered column without a name registers nothing") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource, &ch_unnamed_column_factory);

    ch::connect_params params;
    params.host = "localhost";
    params.database = "ev";
    params.table = "events";
    auto added = fx.ch_conn->addConnection(params, "ev_alias");
    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::schema_error);
    REQUIRE_FALSE(fx.ch_conn->hasConnection("ev_alias"));
    REQUIRE(fx.list_tables(get_tables_command()).empty());
}

TEST_CASE("catalog add_connection_schema: ClickHouse connection without a database is rejected before probing") {
    auto* resource = std::pmr::new_delete_resource();
    // The replay connector would answer a probe with no header block; a
    // schema_error verdict would therefore mean the probe ran.
    catalog_fixture fx(resource, &ch_empty_probe_factory);

    ch::connect_params params;
    params.host = "localhost";
    params.database = "";
    params.table = "events";
    auto added = fx.ch_conn->addConnection(params, "nodb");
    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::missing_field);
    REQUIRE_FALSE(fx.ch_conn->hasConnection("nodb"));
    REQUIRE(fx.list_tables(get_tables_command()).empty());
}

TEST_CASE("catalog add_connection_schema: PostgreSQL connection without a schema is rejected before probing") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);

    // addConnection substitutes the driver default for an empty schema, so
    // the registration succeeds under "public"; the catalog message is the
    // path on which an empty schema can reach the discovery.
    pg::connect_params params;
    params.host = "localhost";
    params.database = "pgdb";
    params.schema = "";
    params.table = "orders";
    auto added = fx.pg_conn->addConnection(params, "noschema");
    REQUIRE_FALSE(added.has_error());

    auto err = fx.add_connection_schema(qualified_name_t("noschema", "pgdb", "", "orders"),
                                        catalog_ext::ConnectionType::PostgreSQL);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::missing_field);
    REQUIRE(fx.list_tables(get_tables_command()) == std::vector<std::string>{"pgdb.public.orders"});
}

TEST_CASE("catalog add_connection_schema: unknown uid is an error, not a silent skip") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);

    for (auto type : {catalog_ext::ConnectionType::MySQL,
                      catalog_ext::ConnectionType::PostgreSQL,
                      catalog_ext::ConnectionType::ClickHouse}) {
        INFO("backend type = " << static_cast<int>(type));
        auto err = fx.add_connection_schema(qualified_name_t("ghost", "db", "public", "t"), type);
        REQUIRE(err.contains_error());
        REQUIRE(err.type == core::error_code_t::do_not_exists);
    }
    REQUIRE(fx.list_tables(get_tables_command()).empty());
}

TEST_CASE("catalog add_connection_schema: a uid keeps the backend it was registered with") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);
    fx.add_pg("shop", "pgdb", "public", "orders");

    auto err = fx.add_connection_schema(qualified_name_t("shop", "pgdb", "", "orders"),
                                        catalog_ext::ConnectionType::MySQL);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::invalid_parameter);
    REQUIRE(fx.list_tables(get_tables_command()) == std::vector<std::string>{"pgdb.public.orders"});
}

TEST_CASE("catalog add_connection_schema: no backend actor for the type is an error, not a hang") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);
    fx.catalog->set_backend_managers(actor_zeta::address_t::empty_address(),
                                     actor_zeta::address_t::empty_address(),
                                     actor_zeta::address_t::empty_address());

    ch::connect_params params;
    params.host = "localhost";
    params.database = "ev";
    params.table = "events";
    auto added = fx.ch_conn->addConnection(params, "ev_alias");
    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::do_not_exists);
    REQUIRE_FALSE(fx.ch_conn->hasConnection("ev_alias"));
    REQUIRE(fx.list_tables(get_tables_command()).empty());
}

// A table the engine registered but the catalog could not mirror must not stay
// in the engine catalog: the catalog undoes the registration before it reports
// the failure, and neither the table nor its uid becomes visible.
TEST_CASE("catalog add_connection_schema: a table the store cannot mirror is dropped from the engine again") {
    auto* resource = std::pmr::new_delete_resource();
    auto engine = std::make_unique<constant_oid_data_manager>(mock_config{.resource = resource});
    auto* recorder = engine.get();
    catalog_fixture fx(resource, std::move(engine), &ch_mock_connector_factory);

    fx.add_pg("shop", "pgdb", "public", "orders");
    REQUIRE(fx.list_tables(get_tables_command()) == std::vector<std::string>{"pgdb.public.orders"});
    REQUIRE(recorder->statements == std::vector<std::string>{"CREATE DATABASE \"shop\""});

    conn::api_server::PgConnectionParams params;
    params.alias = "crm";
    params.host = "localhost";
    params.port = "5432";
    params.username = "user";
    params.password = "pass";
    params.database = "crmdb";
    params.schema = "sales";
    params.table = "customers";
    auto added = fx.pg_conn->addConnection(params);

    REQUIRE(added.has_error());
    REQUIRE(added.error().type == core::error_code_t::already_exists);
    REQUIRE(recorder->statements ==
            std::vector<std::string>{"CREATE DATABASE \"shop\"",
                                     "CREATE DATABASE \"crm\"",
                                     "DROP TABLE \"crm\".\"crmdb:sales:customers\""});
    REQUIRE_FALSE(fx.pg_conn->hasConnection("crm"));
    REQUIRE(fx.list_tables(get_tables_command()) == std::vector<std::string>{"pgdb.public.orders"});
}

// The engine database named after a connection uid holds that connection's
// mirror, and the kafka object database belongs to the KafkaManager; a user
// database-level DDL on either name is refused. Unquoted identifiers reach the
// engine lower-cased, so the match ignores case.
TEST_CASE("catalog check_database_ownership: connection uids and the kafka database are owned names") {
    auto* resource = std::pmr::new_delete_resource();
    catalog_fixture fx(resource);
    fx.add_pg("PgAlias", "pgdb", "public", "orders");

    for (const char* owned : {"PgAlias", "pgalias", "PGALIAS", "kafka", "KAFKA"}) {
        INFO("database name = " << owned);
        auto verdict = fx.check_database_ownership(owned);
        REQUIRE(verdict.contains_error());
        REQUIRE(verdict.type == core::error_code_t::invalid_parameter);
    }

    REQUIRE_FALSE(fx.check_database_ownership("orders").contains_error());
    REQUIRE_FALSE(fx.check_database_ownership("pgdb").contains_error());
}
