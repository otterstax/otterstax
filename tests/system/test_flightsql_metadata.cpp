// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The Flight SQL metadata answers over a real stack: SessionEngine::catalogs()
// / db_schemas() / tables() read the engine's catalog — the local databases
// and the databases of the connections alike — and GetCatalogs /
// GetDbSchemas / GetTables are built from them the way the frontend builds
// them (core/commands.hpp). A catalog is a database as SQL writes it (`d`,
// `c.d`), every catalog and every schema is listed once, in catalog_name then
// db_schema_name order, the filters run in the engine, and the database
// `federation` and the system schemas are hidden.

#include "frontend/flight_sql/session_engine.hpp"
#include "session_stack.hpp"

#include "core/commands.hpp"

#include <catch2/catch_all.hpp>

#include <arrow/api.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

    using row_t = std::pair<std::string, std::string>;

    std::vector<std::string> strings_of(const std::shared_ptr<arrow::Array>& array) {
        std::vector<std::string> out;
        const auto& strings = static_cast<const arrow::StringArray&>(*array);
        for (int64_t i = 0; i < strings.length(); ++i) {
            out.push_back(strings.IsNull(i) ? std::string{"<null>"} : strings.GetString(i));
        }
        return out;
    }

    std::vector<row_t> schemas_of(const std::vector<flight::core::DbSchema>& schemas) {
        auto batch = flight::core::db_schemas_batch(schemas);
        REQUIRE(batch->num_columns() == 2);
        const auto catalogs = strings_of(batch->column(0));
        const auto names = strings_of(batch->column(1));
        std::vector<row_t> out;
        for (size_t i = 0; i < catalogs.size(); ++i) {
            out.emplace_back(catalogs[i], names[i]);
        }
        return out;
    }

    // "<catalog>|<schema>|<table>" of every GetTables row, in answer order.
    std::vector<std::string> tables_of(const std::pmr::vector<table_info>& tables) {
        auto batch = flight::core::tables_batch(tables, false);
        const auto catalogs = strings_of(batch->column(0));
        const auto schemas = strings_of(batch->column(1));
        const auto names = strings_of(batch->column(2));
        std::vector<std::string> out;
        for (size_t i = 0; i < names.size(); ++i) {
            out.push_back(catalogs[i] + "|" + schemas[i] + "|" + names[i]);
        }
        return out;
    }

    // Two MySQL connections m1 / m2 (database db, its one table `table`), two
    // local databases and a schema of main.
    struct metadata_stack {
        otterstax::test::session_stack_owner owner;
        flight::engine::SessionEngine engine;

        explicit metadata_stack(const std::string& leaf)
            : owner(otterstax::test::unique_test_dir(leaf),
                    otterstax::test::stack_options_t{
                        .connections = config::ConnectionsConfig{.mysql = otterstax::test::mock_mysql({"m1", "m2"})}})
            , engine(owner.sessions(), owner.engine().resource()) {
            REQUIRE(engine.execute_update("CREATE DATABASE loc") == 0);
            REQUIRE(engine.execute_update("CREATE TABLE loc.public.items (id bigint)") == 0);
            REQUIRE(engine.execute_update("CREATE TABLE loc.public.labels (id bigint)") == 0);
            REQUIRE(engine.execute_update("CREATE DATABASE alpha") == 0);
            REQUIRE(engine.execute_update("CREATE TABLE alpha.public.t (id bigint)") == 0);
            REQUIRE(engine.execute_update("CREATE SCHEMA sales") == 0);
            REQUIRE(engine.execute_update("CREATE TABLE sales.leads (id bigint)") == 0);
        }
    };

} // namespace

TEST_CASE("flight metadata: GetCatalogs lists every database once, as SQL writes it") {
    metadata_stack fx("otterstax_flight_metadata_catalogs");
    auto batch = flight::core::catalogs_batch(fx.engine.catalogs());
    REQUIRE(batch->num_columns() == 1);
    CHECK(strings_of(batch->column(0)) == std::vector<std::string>{"alpha", "loc", "m1.db", "m2.db", "main"});
}

TEST_CASE("flight metadata: GetDbSchemas keeps each schema under its own catalog") {
    metadata_stack fx("otterstax_flight_metadata_schemas");

    CHECK(schemas_of(fx.engine.db_schemas(std::nullopt, "")) == std::vector<row_t>{{"alpha", "public"},
                                                                                     {"loc", "public"},
                                                                                     {"m1.db", "public"},
                                                                                     {"m2.db", "public"},
                                                                                     {"main", "public"},
                                                                                     {"main", "sales"}});

    SECTION("catalog is an exact match") {
        CHECK(schemas_of(fx.engine.db_schemas("m1.db", "")) == std::vector<row_t>{{"m1.db", "public"}});
        CHECK(schemas_of(fx.engine.db_schemas("main", "")) ==
              std::vector<row_t>{{"main", "public"}, {"main", "sales"}});
        CHECK(fx.engine.db_schemas("m%", "").empty());
        CHECK(fx.engine.db_schemas("", "").empty());
        CHECK(fx.engine.db_schemas("federation", "").empty());
    }

    SECTION("db_schema_filter_pattern is a LIKE pattern") {
        CHECK(schemas_of(fx.engine.db_schemas(std::nullopt, "s%")) == std::vector<row_t>{{"main", "sales"}});
        CHECK(fx.engine.db_schemas(std::nullopt, "p_blic").size() == 5);
        CHECK(fx.engine.db_schemas(std::nullopt, "pg_%").empty());
    }

    SECTION("filters combine") {
        CHECK(fx.engine.db_schemas("loc", "s%").empty());
        CHECK(schemas_of(fx.engine.db_schemas("main", "s%")) == std::vector<row_t>{{"main", "sales"}});
    }
}

TEST_CASE("flight metadata: GetTables applies the catalog, schema and table filters") {
    metadata_stack fx("otterstax_flight_metadata_tables");

    CHECK(tables_of(fx.engine.tables(std::nullopt, "", "", false)) == std::vector<std::string>{"alpha|public|t",
                                                                                             "loc|public|items",
                                                                                             "loc|public|labels",
                                                                                             "m1.db|public|table",
                                                                                             "m2.db|public|table",
                                                                                             "main|sales|leads"});
    CHECK(tables_of(fx.engine.tables("loc", "", "", false)) ==
          std::vector<std::string>{"loc|public|items", "loc|public|labels"});
    CHECK(tables_of(fx.engine.tables("m2.db", "", "", false)) == std::vector<std::string>{"m2.db|public|table"});
    CHECK(tables_of(fx.engine.tables(std::nullopt, "sal%", "", false)) == std::vector<std::string>{"main|sales|leads"});
    CHECK(tables_of(fx.engine.tables("loc", "pub%", "l%", false)) == std::vector<std::string>{"loc|public|labels"});
}

TEST_CASE("flight metadata: GetTables with include_schema describes each table in the engine") {
    metadata_stack fx("otterstax_flight_metadata_columns");

    const auto tables = fx.engine.tables(std::nullopt, "", "", true);
    REQUIRE(tables.size() == 6);
    const auto columns_of = [&](std::string_view catalog_and_table) {
        std::vector<std::string> names;
        for (const auto& table : tables) {
            if (flight::core::catalog_of(table.name.connection.t, table.name.database.t) + "." +
                    table.name.collection.t ==
                catalog_and_table) {
                REQUIRE(table.schema.type() == components::types::logical_type::STRUCT);
                for (const auto& field : table.schema.child_types()) {
                    names.push_back(field.alias());
                }
            }
        }
        return names;
    };
    CHECK(columns_of("loc.items") == std::vector<std::string>{"id"});
    // A table of a connection: its registered columns, no SQL to the backend.
    CHECK(columns_of("m1.db.table") == std::vector<std::string>{"id", "name"});

    auto batch = flight::core::tables_batch(tables, true);
    REQUIRE(batch->num_rows() == 6);
    CHECK(batch->column(4)->null_count() == 0);
}
