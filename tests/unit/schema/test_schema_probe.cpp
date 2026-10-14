// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The engine-side schema probe of a relation (schema_probe::make_table_probe /
// read_columns: the per-relation step of otterstax::engine::describe). The answer is read off the validated
// plan, never off row data, so it must be the same for an empty table and a
// populated one, must come back for relations that have no rows to derive
// anything from (a zero-column table, a VIEW whose body the engine splices in
// place of the probe), and must carry the concrete column types, not
// placeholders. Over a real engine in a temp dir.

#include "../../system/session_stack.hpp"
#include "otterbrix/operators/schema_probe.hpp"

#include <catch2/catch_all.hpp>

#include <components/types/types.hpp>

#include <string>

using components::types::logical_type;

namespace {

    using columns_result_t = core::result_wrapper_t<otterstax::engine::columns_t>;

    struct probe_fixture {
        otterstax::test::test_engine_t engine;

        explicit probe_fixture(const char* data_dir)
            : engine(data_dir) {}

        void sql(const std::string& statement) {
            auto cursor = engine.sql(statement);
            INFO(statement << " -> " << (cursor && cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
            REQUIRE(cursor);
            REQUIRE_FALSE(cursor->is_error());
        }

        // Probes one relation: its output columns.
        columns_result_t probe(const std::string& database, const std::string& collection) {
            auto* resource = engine.resource();
            auto probe = otterstax::schema_probe::make_table_probe(resource, database, collection);
            auto cursor = engine.drive(
                otterstax::engine::send_plan(engine.dispatcher(), engine.session(), probe.execution_plan(resource)));
            auto read = otterstax::schema_probe::read_columns(probe, cursor, resource);
            if (read.has_error()) {
                return read.convert_error<otterstax::engine::columns_t>();
            }
            return std::move(read.value().columns);
        }
    };

    const otterstax::engine::columns_t& columns_of(const columns_result_t& result) {
        INFO("probe error: " << (result.has_error() ? result.error().what.c_str() : ""));
        REQUIRE_FALSE(result.has_error());
        return result.value();
    }

    // The column list of an (id, name, weight) table.
    constexpr const char* three_columns = " (id INT, name STRING, weight DOUBLE);";

    void require_three_columns(const otterstax::engine::columns_t& columns) {
        REQUIRE(columns.size() == 3);
        REQUIRE(columns[0].alias() == "id");
        REQUIRE(columns[0].type() == logical_type::INTEGER);
        REQUIRE(columns[1].alias() == "name");
        REQUIRE(columns[1].type() == logical_type::STRING_LITERAL);
        REQUIRE(columns[2].alias() == "weight");
        REQUIRE(columns[2].type() == logical_type::DOUBLE);
    }

} // namespace

TEST_CASE("schema probe: column names and concrete types of an empty table") {
    probe_fixture fx("/tmp/otterstax_schema_probe_empty");
    fx.sql("CREATE DATABASE probe_db;");
    fx.sql(std::string{"CREATE TABLE probe_db.people"} + three_columns);

    require_three_columns(columns_of(fx.probe("probe_db", "people")));
}

TEST_CASE("schema probe: the answer does not depend on the table's rows") {
    probe_fixture fx("/tmp/otterstax_schema_probe_rows");
    fx.sql("CREATE DATABASE probe_db;");
    fx.sql(std::string{"CREATE TABLE probe_db.people"} + three_columns);
    auto before = fx.probe("probe_db", "people");

    fx.sql("INSERT INTO probe_db.people (id, name, weight) VALUES (1, 'a', 1.5), (2, 'b', 2.5);");
    auto after = fx.probe("probe_db", "people");

    require_three_columns(columns_of(before));
    require_three_columns(columns_of(after));
}

TEST_CASE("schema probe: a table without columns is an empty schema, not an error") {
    probe_fixture fx("/tmp/otterstax_schema_probe_zero_columns");
    fx.sql("CREATE DATABASE probe_db;");
    fx.sql("CREATE TABLE probe_db.bare ();");

    auto probed = fx.probe("probe_db", "bare");
    REQUIRE(columns_of(probed).empty());
}

TEST_CASE("schema probe: a VIEW answers with its body's columns") {
    probe_fixture fx("/tmp/otterstax_schema_probe_view");
    fx.sql("CREATE DATABASE probe_db;");
    fx.sql("CREATE TABLE probe_db.t (col_a STRING, col_b BIGINT);");
    fx.sql("CREATE VIEW probe_db.v AS SELECT col_a FROM probe_db.t WHERE col_b > 10;");

    auto table_probe = fx.probe("probe_db", "t");
    const auto& table = columns_of(table_probe);
    REQUIRE(table.size() == 2);
    REQUIRE(table[0].alias() == "col_a");

    // The view projects col_a only, with the type the base table gives it — and
    // the base table is empty, so nothing here can come from row data.
    auto view_probe = fx.probe("probe_db", "v");
    const auto& view = columns_of(view_probe);
    REQUIRE(view.size() == 1);
    REQUIRE(view[0].alias() == "col_a");
    REQUIRE(view[0].type() == table[0].type());
}

TEST_CASE("schema probe: identifiers that need quoting resolve as given") {
    probe_fixture fx("/tmp/otterstax_schema_probe_quoted");
    fx.sql("CREATE DATABASE probe_db;");

    SECTION("mixed case and a dash") {
        fx.sql(std::string{"CREATE TABLE probe_db.\"Mixed-Case\""} + three_columns);
        require_three_columns(columns_of(fx.probe("probe_db", "Mixed-Case")));
    }
    SECTION("a reserved word") {
        fx.sql(std::string{"CREATE TABLE probe_db.\"select\""} + three_columns);
        require_three_columns(columns_of(fx.probe("probe_db", "select")));
    }
}

TEST_CASE("schema probe: an unknown relation is an error, not an empty schema") {
    probe_fixture fx("/tmp/otterstax_schema_probe_missing");
    fx.sql("CREATE DATABASE probe_db;");

    auto probed = fx.probe("probe_db", "nowhere");
    REQUIRE(probed.has_error());
    CHECK(probed.error().type == core::error_code_t::table_not_exists);
}

TEST_CASE("schema probe: two relations probed one after the other keep their own columns") {
    probe_fixture fx("/tmp/otterstax_schema_probe_positional");
    fx.sql("CREATE DATABASE probe_db;");
    fx.sql(std::string{"CREATE TABLE probe_db.people"} + three_columns);
    fx.sql("CREATE TABLE probe_db.flags (flag BOOLEAN);");

    auto people = fx.probe("probe_db", "people");
    auto flags_probe = fx.probe("probe_db", "flags");
    require_three_columns(columns_of(people));
    const auto& flags = columns_of(flags_probe);
    REQUIRE(flags.size() == 1);
    REQUIRE(flags[0].alias() == "flag");
    REQUIRE(flags[0].type() == logical_type::BOOLEAN);
}
