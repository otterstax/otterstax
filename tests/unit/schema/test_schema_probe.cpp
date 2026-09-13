// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The engine-side schema probe behind OtterbrixDataManager::get_schema. The
// answer is read off the validated plan, never off row data, so it must be the
// same for an empty table and a populated one, must come back for relations
// that have no rows to derive anything from (a zero-column table, a VIEW whose
// body the engine splices in place of the probe), and must carry the concrete
// column types, not placeholders.

#include <catch2/catch_all.hpp>

#include "otterbrix/config.hpp"
#include "otterbrix/operators/execute_plan.hpp"

#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <otterbrix/otterbrix.hpp>

#include <filesystem>
#include <string>

using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    struct probe_fixture {
        db::otterbrix_engine_ptr engine;
        data_manager_ptr manager;
        std::pmr::memory_resource* resource;

        explicit probe_fixture(const char* data_dir) {
            std::filesystem::remove_all(data_dir);
            engine = db::make_otterbrix_engine(make_create_config(data_dir));
            manager = make_otterbrix_manager(engine);
            resource = engine->dispatcher()->resource();
        }

        void sql(const std::string& statement) {
            auto cursor = manager->execute_sql(statement);
            INFO(statement << " -> " << (cursor && cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE(cursor);
            REQUIRE_FALSE(cursor->is_error());
        }

        void create(const std::string& database,
                    const std::string& collection,
                    std::vector<components::table::column_definition_t> columns) {
            components::catalog::oid_t oid = components::catalog::INVALID_OID;
            auto cursor = manager->create_collection(database, collection, std::move(columns), oid);
            INFO(database << "." << collection << " -> "
                          << (cursor && cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE(cursor);
            REQUIRE_FALSE(cursor->is_error());
            REQUIRE(oid != components::catalog::INVALID_OID);
        }

        // Probes one relation; the schema is type_data()[0] of the answer.
        components::cursor::cursor_t_ptr probe(const std::string& database, const std::string& collection) {
            OtterbrixSchemaParams params(resource);
            params.emplace_back(database, collection);
            return manager->get_schema(params);
        }
    };

    const complex_logical_type& struct_of(const components::cursor::cursor_t_ptr& cursor, size_t index) {
        REQUIRE(cursor);
        INFO("probe error: " << (cursor->is_error() ? cursor->get_error().what.c_str() : ""));
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->type_data().size() > index);
        REQUIRE(cursor->type_data()[index].type() == logical_type::STRUCT);
        return cursor->type_data()[index];
    }

    std::vector<components::table::column_definition_t> three_columns() {
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", complex_logical_type(logical_type::INTEGER));
        cols.emplace_back("name", complex_logical_type(logical_type::STRING_LITERAL));
        cols.emplace_back("weight", complex_logical_type(logical_type::DOUBLE));
        return cols;
    }

    void require_three_columns(const complex_logical_type& schema) {
        REQUIRE(schema.child_types().size() == 3);
        REQUIRE(schema.child_types()[0].alias() == "id");
        REQUIRE(schema.child_types()[0].type() == logical_type::INTEGER);
        REQUIRE(schema.child_types()[1].alias() == "name");
        REQUIRE(schema.child_types()[1].type() == logical_type::STRING_LITERAL);
        REQUIRE(schema.child_types()[2].alias() == "weight");
        REQUIRE(schema.child_types()[2].type() == logical_type::DOUBLE);
    }

} // namespace

TEST_CASE("schema probe: column names and concrete types of an empty table") {
    probe_fixture fx("/tmp/otterstax_schema_probe_empty");
    fx.sql("CREATE DATABASE probe_db;");
    fx.create("probe_db", "people", three_columns());

    require_three_columns(struct_of(fx.probe("probe_db", "people"), 0));
}

TEST_CASE("schema probe: the answer does not depend on the table's rows") {
    probe_fixture fx("/tmp/otterstax_schema_probe_rows");
    fx.sql("CREATE DATABASE probe_db;");
    fx.create("probe_db", "people", three_columns());
    auto before = fx.probe("probe_db", "people");

    fx.sql("INSERT INTO probe_db.people (id, name, weight) VALUES (1, 'a', 1.5), (2, 'b', 2.5);");
    auto after = fx.probe("probe_db", "people");

    require_three_columns(struct_of(before, 0));
    require_three_columns(struct_of(after, 0));
}

TEST_CASE("schema probe: a table without columns is an empty schema, not an error") {
    probe_fixture fx("/tmp/otterstax_schema_probe_zero_columns");
    fx.sql("CREATE DATABASE probe_db;");
    fx.create("probe_db", "bare", {});

    auto cursor = fx.probe("probe_db", "bare");
    const auto& schema = struct_of(cursor, 0);
    REQUIRE(schema.child_types().empty());
}

TEST_CASE("schema probe: a VIEW answers with its body's columns") {
    probe_fixture fx("/tmp/otterstax_schema_probe_view");
    fx.sql("CREATE DATABASE probe_db;");
    fx.sql("CREATE TABLE probe_db.t (col_a STRING, col_b BIGINT);");
    fx.sql("CREATE VIEW probe_db.v AS SELECT col_a FROM probe_db.t WHERE col_b > 10;");

    auto table_cursor = fx.probe("probe_db", "t");
    const auto& table = struct_of(table_cursor, 0);
    REQUIRE(table.child_types().size() == 2);
    REQUIRE(table.child_types()[0].alias() == "col_a");

    // The view projects col_a only, with the type the base table gives it — and
    // the base table is empty, so nothing here can come from row data.
    auto view_cursor = fx.probe("probe_db", "v");
    const auto& view = struct_of(view_cursor, 0);
    REQUIRE(view.child_types().size() == 1);
    REQUIRE(view.child_types()[0].alias() == "col_a");
    REQUIRE(view.child_types()[0].type() == table.child_types()[0].type());
}

TEST_CASE("schema probe: identifiers that need quoting resolve as given") {
    probe_fixture fx("/tmp/otterstax_schema_probe_quoted");
    fx.sql("CREATE DATABASE probe_db;");

    SECTION("mixed case and a dash") {
        fx.create("probe_db", "Mixed-Case", three_columns());
        require_three_columns(struct_of(fx.probe("probe_db", "Mixed-Case"), 0));
    }
    SECTION("a reserved word") {
        fx.create("probe_db", "select", three_columns());
        require_three_columns(struct_of(fx.probe("probe_db", "select"), 0));
    }
}

TEST_CASE("schema probe: an unknown relation is an error, not an empty schema") {
    probe_fixture fx("/tmp/otterstax_schema_probe_missing");
    fx.sql("CREATE DATABASE probe_db;");

    auto cursor = fx.probe("probe_db", "nowhere");
    REQUIRE(cursor);
    REQUIRE(cursor->is_error());
}

TEST_CASE("schema probe: dependencies keep their positional slots") {
    probe_fixture fx("/tmp/otterstax_schema_probe_positional");
    fx.sql("CREATE DATABASE probe_db;");
    fx.create("probe_db", "people", three_columns());
    std::vector<components::table::column_definition_t> one;
    one.emplace_back("flag", complex_logical_type(logical_type::BOOLEAN));
    fx.create("probe_db", "flags", std::move(one));

    OtterbrixSchemaParams params(fx.resource);
    params.emplace_back("probe_db", "people");
    // An external dependency (empty collection) is not probed locally and keeps
    // an empty slot so the indices of its neighbours stay valid.
    params.emplace_back("", "");
    params.emplace_back("probe_db", "flags");
    auto cursor = fx.manager->get_schema(params);

    require_three_columns(struct_of(cursor, 0));
    REQUIRE(struct_of(cursor, 1).child_types().empty());
    const auto& flags = struct_of(cursor, 2);
    REQUIRE(flags.child_types().size() == 1);
    REQUIRE(flags.child_types()[0].alias() == "flag");
    REQUIRE(flags.child_types()[0].type() == logical_type::BOOLEAN);
}
