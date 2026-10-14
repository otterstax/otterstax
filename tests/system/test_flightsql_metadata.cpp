// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The Flight SQL metadata answers over a real stack: SessionEngine::metadata()
// lists the engine's own tables beside the remote ones, and GetCatalogs /
// GetDbSchemas / GetTables are built from that listing the way the frontend
// builds them (core/commands.hpp). Every catalog is listed once, every schema
// once under the catalog that holds it, in catalog_name then db_schema_name
// order, and the catalog / schema filters are applied.

#include "frontend/flight_sql/session_engine.hpp"
#include "session_stack.hpp"

#include "core/commands.hpp"

#include <FlightSql.pb.h>

#include <catch2/catch_all.hpp>

#include <arrow/api.h>

#include <string>
#include <utility>
#include <vector>

namespace {

    namespace fps = arrow::flight::protocol::sql;
    using row_t = std::pair<std::string, std::string>;

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

    // Two remote databases: pgdb with one schema; crmdb with two schemas, one
    // of them holding two tables (from two connections).
    otterstax::test::stack_options_t remote_databases() {
        otterstax::test::stack_options_t options;
        options.connections.postgresql.push_back(pg_connection("shop", "pgdb", "public", "orders"));
        options.connections.postgresql.push_back(pg_connection("crm", "crmdb", "sales", "customers"));
        options.connections.postgresql.push_back(pg_connection("leads", "crmdb", "sales", "leads"));
        options.connections.postgresql.push_back(pg_connection("hr", "crmdb", "hr", "staff"));
        return options;
    }

    std::vector<std::string> strings_of(const std::shared_ptr<arrow::Array>& array) {
        std::vector<std::string> out;
        const auto& strings = static_cast<const arrow::StringArray&>(*array);
        for (int64_t i = 0; i < strings.length(); ++i) {
            out.push_back(strings.IsNull(i) ? std::string{"<null>"} : strings.GetString(i));
        }
        return out;
    }

    std::vector<std::string> catalogs_of(const flight::core::EngineMetadata& md) {
        auto batch = flight::core::catalogs_batch(md);
        REQUIRE(batch->num_columns() == 1);
        return strings_of(batch->column(0));
    }

    std::vector<row_t> schemas_of(const flight::core::EngineMetadata& md, const fps::CommandGetDbSchemas& cmd) {
        auto batch = flight::core::db_schemas_batch(cmd, md);
        REQUIRE(batch->num_columns() == 2);
        const auto catalogs = strings_of(batch->column(0));
        const auto schemas = strings_of(batch->column(1));
        std::vector<row_t> out;
        for (size_t i = 0; i < catalogs.size(); ++i) {
            out.emplace_back(catalogs[i], schemas[i]);
        }
        return out;
    }

    // "<catalog>.<schema>.<table>" of every GetTables row, in answer order.
    std::vector<std::string> tables_of(const flight::core::EngineMetadata& md, const fps::CommandGetTables& cmd) {
        auto batch = flight::core::tables_batch(cmd, md);
        const auto catalogs = strings_of(batch->column(0));
        const auto schemas = strings_of(batch->column(1));
        const auto names = strings_of(batch->column(2));
        std::vector<std::string> out;
        for (size_t i = 0; i < names.size(); ++i) {
            out.push_back(catalogs[i] + "." + schemas[i] + "." + names[i]);
        }
        return out;
    }

    struct metadata_stack {
        otterstax::test::session_stack_owner owner;
        flight::engine::SessionEngine engine;

        explicit metadata_stack(const std::string& leaf)
            : owner(otterstax::test::unique_test_dir(leaf), remote_databases())
            , engine(owner.sessions(), owner.engine().resource()) {
            // Two local databases, one of them with two tables (local tables
            // are listed with an empty schema).
            REQUIRE(engine.execute_update("CREATE DATABASE loc") == 0);
            REQUIRE(engine.execute_update("CREATE TABLE loc.items (id bigint)") == 0);
            REQUIRE(engine.execute_update("CREATE TABLE loc.labels (id bigint)") == 0);
            REQUIRE(engine.execute_update("CREATE DATABASE alpha") == 0);
            REQUIRE(engine.execute_update("CREATE TABLE alpha.t (id bigint)") == 0);
        }
    };

} // namespace

TEST_CASE("flight metadata: GetCatalogs lists every catalog once in order") {
    metadata_stack fx("otterstax_flight_metadata_catalogs");
    const auto md = fx.engine.metadata();
    CHECK(catalogs_of(md) == std::vector<std::string>{"alpha", "crmdb", "loc", "pgdb"});
}

TEST_CASE("flight metadata: GetDbSchemas keeps each schema under its own catalog") {
    metadata_stack fx("otterstax_flight_metadata_schemas");
    const auto md = fx.engine.metadata();

    fps::CommandGetDbSchemas cmd;
    CHECK(schemas_of(md, cmd) == std::vector<row_t>{{"alpha", ""},
                                                    {"crmdb", "hr"},
                                                    {"crmdb", "sales"},
                                                    {"loc", ""},
                                                    {"pgdb", "public"}});

    SECTION("catalog is an exact match") {
        cmd.set_catalog("crmdb");
        CHECK(schemas_of(md, cmd) == std::vector<row_t>{{"crmdb", "hr"}, {"crmdb", "sales"}});
        cmd.set_catalog("loc");
        CHECK(schemas_of(md, cmd) == std::vector<row_t>{{"loc", ""}});
        cmd.set_catalog("crm%");
        CHECK(schemas_of(md, cmd).empty());
    }

    SECTION("db_schema_filter_pattern is a LIKE pattern") {
        cmd.set_db_schema_filter_pattern("s%");
        CHECK(schemas_of(md, cmd) == std::vector<row_t>{{"crmdb", "sales"}});
        cmd.set_db_schema_filter_pattern("p_blic");
        CHECK(schemas_of(md, cmd) == std::vector<row_t>{{"pgdb", "public"}});
    }

    SECTION("filters combine") {
        cmd.set_catalog("pgdb");
        cmd.set_db_schema_filter_pattern("s%");
        CHECK(schemas_of(md, cmd).empty());
    }
}

TEST_CASE("flight metadata: GetTables applies the catalog and schema filters") {
    metadata_stack fx("otterstax_flight_metadata_tables");
    const auto md = fx.engine.metadata();

    fps::CommandGetTables cmd;
    CHECK(tables_of(md, cmd) == std::vector<std::string>{"alpha..t",
                                                         "crmdb.hr.staff",
                                                         "crmdb.sales.customers",
                                                         "crmdb.sales.leads",
                                                         "loc..items",
                                                         "loc..labels",
                                                         "pgdb.public.orders"});
    cmd.set_catalog("crmdb");
    CHECK(tables_of(md, cmd) ==
          std::vector<std::string>{"crmdb.hr.staff", "crmdb.sales.customers", "crmdb.sales.leads"});
    cmd.set_db_schema_filter_pattern("sal%");
    CHECK(tables_of(md, cmd) == std::vector<std::string>{"crmdb.sales.customers", "crmdb.sales.leads"});
    cmd.set_table_name_filter_pattern("l%");
    CHECK(tables_of(md, cmd) == std::vector<std::string>{"crmdb.sales.leads"});
}
