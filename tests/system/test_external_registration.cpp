// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// External-table registration against the real engine: the planner stamps the
// pg_class oid onto the create node during execute_plan — that is the only
// channel for reading the oid back (pg_catalog is not reachable via plain SQL
// SELECT).
#include <catch2/catch_all.hpp>

#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <otterbrix/otterbrix.hpp>

#include "otterbrix/config.hpp"
#include "otterbrix/operators/execute_plan.hpp"

#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <string>
#include <vector>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;

TEST_CASE("external registration: engine stamps pg_class oid on create") {
    std::filesystem::remove_all("/tmp/otterstax_test_registration");
    auto cfg = make_create_config("/tmp/otterstax_test_registration");
    auto inst = db::make_otterbrix_engine(cfg);
    auto manager = make_otterbrix_manager(inst);

    auto db_cursor = manager->execute_sql("CREATE DATABASE \"11111111-2222-3333-4444-555555555555\";");
    REQUIRE(db_cursor);
    REQUIRE_FALSE(db_cursor->is_error());

    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", complex_logical_type(logical_type::INTEGER));
    cols.emplace_back("name", complex_logical_type(logical_type::STRING_LITERAL));

    components::catalog::oid_t oid = components::catalog::INVALID_OID;
    auto create_cursor = manager->create_collection("11111111-2222-3333-4444-555555555555",
                                                    "pgdb:public:products",
                                                    std::move(cols),
                                                    oid);
    REQUIRE(create_cursor);
    REQUIRE_FALSE(create_cursor->is_error());
    REQUIRE(oid != components::catalog::INVALID_OID);

    std::vector<components::table::column_definition_t> cols2;
    cols2.emplace_back("id", complex_logical_type(logical_type::INTEGER));
    components::catalog::oid_t oid2 = components::catalog::INVALID_OID;
    auto create_cursor2 = manager->create_collection("11111111-2222-3333-4444-555555555555",
                                                     "pgdb:public:orders",
                                                     std::move(cols2),
                                                     oid2);
    REQUIRE(create_cursor2);
    REQUIRE_FALSE(create_cursor2->is_error());
    REQUIRE(oid2 != components::catalog::INVALID_OID);
    REQUIRE(oid2 != oid);
}

namespace {

    std::vector<components::table::column_definition_t> id_name_columns() {
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", complex_logical_type(logical_type::BIGINT));
        cols.emplace_back("name", complex_logical_type(logical_type::STRING_LITERAL));
        return cols;
    }

    // `rows` rows (first_id + i, "name_<first_id + i>").
    components::vector::data_chunk_t
    id_name_rows(std::pmr::memory_resource* resource, std::int64_t first_id, std::size_t rows) {
        std::pmr::vector<complex_logical_type> types(resource);
        types.emplace_back(logical_type::BIGINT);
        types.back().set_alias("id");
        types.emplace_back(logical_type::STRING_LITERAL);
        types.back().set_alias("name");
        components::vector::data_chunk_t chunk{resource, types, rows};
        for (std::size_t i = 0; i < rows; ++i) {
            const std::int64_t row_id = first_id + static_cast<std::int64_t>(i);
            chunk.set_value(0, i, logical_value_t(resource, row_id));
            chunk.set_value(1, i, logical_value_t(resource, "name_" + std::to_string(row_id)));
        }
        chunk.set_cardinality(rows);
        return chunk;
    }

    void require_ok(const components::cursor::cursor_t_ptr& cursor) {
        REQUIRE(cursor);
        INFO("error: " << (cursor->is_error() ? cursor->get_error().what.c_str() : ""));
        REQUIRE_FALSE(cursor->is_error());
    }

    void require_error(const components::cursor::cursor_t_ptr& cursor,
                       core::error_code_t code,
                       const std::string& what) {
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        REQUIRE(cursor->get_error().type == code);
        REQUIRE(std::string{cursor->get_error().what.c_str()} == what);
    }

    // (id, name) of every row of an (id, name) result, in result order.
    std::vector<std::pair<std::int64_t, std::string>> id_name_pairs(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<std::pair<std::int64_t, std::string>> rows;
        for (std::uint64_t row = 0; row < cursor->size(); ++row) {
            rows.emplace_back(cursor->value(0, row).value<std::int64_t>(),
                              std::string{cursor->value(1, row).value<std::string_view>()});
        }
        return rows;
    }

    void require_id_name_struct(const complex_logical_type& schema) {
        REQUIRE(schema.type() == logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 2);
        REQUIRE(schema.child_types()[0].alias() == "id");
        REQUIRE(schema.child_types()[0].type() == logical_type::BIGINT);
        REQUIRE(schema.child_types()[1].alias() == "name");
        REQUIRE(schema.child_types()[1].type() == logical_type::STRING_LITERAL);
    }

} // namespace

// insert_data creates database.collection and inserts the rows under the
// catalog-resolve wrap: the rows land in the named database and nowhere else,
// and a table that already exists is refused rather than appended to.
TEST_CASE("external registration: insert_data fills a table in its own database only") {
    std::filesystem::remove_all("/tmp/otterstax_test_insert_data");
    auto cfg = make_create_config("/tmp/otterstax_test_insert_data");
    auto inst = db::make_otterbrix_engine(cfg);
    auto* resource = inst->dispatcher()->resource();
    auto manager = make_otterbrix_manager(inst);

    require_ok(manager->execute_sql("CREATE DATABASE mixdb;"));
    require_ok(manager->execute_sql("CREATE DATABASE otherdb;"));

    auto first = manager->insert_data("mixdb", "t", id_name_columns(), id_name_rows(resource, 1, 3));
    require_ok(first);
    REQUIRE(first->size() == 3);

    // A database that does not exist: the rows are refused and nothing is created.
    require_error(manager->insert_data("nodb", "t", id_name_columns(), id_name_rows(resource, 100, 1)),
                  core::error_code_t::table_not_exists,
                  "INSERT target collection does not exist");
    require_error(manager->execute_sql("SELECT * FROM nodb.t;"),
                  core::error_code_t::database_not_exists,
                  "database does not exist: nodb");

    // The same collection name in another database is a separate table.
    auto other = manager->insert_data("otherdb", "t", id_name_columns(), id_name_rows(resource, 100, 1));
    require_ok(other);
    REQUIRE(other->size() == 1);

    // A second insert_data of an existing table is refused, and the table keeps
    // only the rows of the first load.
    require_error(manager->insert_data("mixdb", "t", id_name_columns(), id_name_rows(resource, 200, 2)),
                  core::error_code_t::table_already_exists,
                  "insert_data: mixdb.t already exists");

    auto mixed = manager->execute_sql("SELECT id, name FROM mixdb.t ORDER BY id;");
    require_ok(mixed);
    REQUIRE(id_name_pairs(mixed) ==
            std::vector<std::pair<std::int64_t, std::string>>{{1, "name_1"}, {2, "name_2"}, {3, "name_3"}});
    auto others = manager->execute_sql("SELECT id, name FROM otherdb.t ORDER BY id;");
    require_ok(others);
    REQUIRE(id_name_pairs(others) == std::vector<std::pair<std::int64_t, std::string>>{{100, "name_100"}});
}

// DROP TABLE then CREATE EXTERNAL TABLE under the same name — the reload the
// python external-table tests and the benchmark's cold loads run: once the
// table is dropped, insert_data creates it again with only the new rows.
TEST_CASE("external registration: insert_data loads a dropped table again") {
    std::filesystem::remove_all("/tmp/otterstax_test_insert_data_reload");
    auto cfg = make_create_config("/tmp/otterstax_test_insert_data_reload");
    auto inst = db::make_otterbrix_engine(cfg);
    auto* resource = inst->dispatcher()->resource();
    auto manager = make_otterbrix_manager(inst);

    require_ok(manager->execute_sql("CREATE DATABASE mixdb;"));
    require_ok(manager->insert_data("mixdb", "t", id_name_columns(), id_name_rows(resource, 1, 3)));
    require_ok(manager->execute_sql("DROP TABLE mixdb.t;"));

    auto reloaded = manager->insert_data("mixdb", "t", id_name_columns(), id_name_rows(resource, 200, 2));
    require_ok(reloaded);
    REQUIRE(reloaded->size() == 2);

    auto rows = manager->execute_sql("SELECT id, name FROM mixdb.t ORDER BY id;");
    require_ok(rows);
    REQUIRE(id_name_pairs(rows) ==
            std::vector<std::pair<std::int64_t, std::string>>{{200, "name_200"}, {201, "name_201"}});
}

// get_schema reads the columns off the plan, never off row data: an empty and
// a populated table of one shape answer the same STRUCT.
TEST_CASE("external registration: get_schema of an empty and a populated table") {
    std::filesystem::remove_all("/tmp/otterstax_test_get_schema");
    auto cfg = make_create_config("/tmp/otterstax_test_get_schema");
    auto inst = db::make_otterbrix_engine(cfg);
    auto* resource = inst->dispatcher()->resource();
    auto manager = make_otterbrix_manager(inst);

    require_ok(manager->execute_sql("CREATE DATABASE mixdb;"));
    components::catalog::oid_t oid = components::catalog::INVALID_OID;
    require_ok(manager->create_collection("mixdb", "empty_t", id_name_columns(), oid));
    require_ok(manager->insert_data("mixdb", "full_t", id_name_columns(), id_name_rows(resource, 1, 3)));

    OtterbrixSchemaParams params(resource);
    params.emplace_back("mixdb", "empty_t");
    params.emplace_back("mixdb", "full_t");
    params.emplace_back("", "");
    auto schema = manager->get_schema(params);
    require_ok(schema);
    REQUIRE(schema->type_data().size() == 3);
    require_id_name_struct(schema->type_data()[0]);
    require_id_name_struct(schema->type_data()[1]);
    // A slot without a collection keeps its position with an empty STRUCT.
    REQUIRE(schema->type_data()[2].type() == logical_type::STRUCT);
    REQUIRE(schema->type_data()[2].child_types().empty());

    OtterbrixSchemaParams missing_table(resource);
    missing_table.emplace_back("mixdb", "nope");
    require_error(manager->get_schema(missing_table),
                  core::error_code_t::table_not_exists,
                  "collection does not exist: mixdb.nope");

    OtterbrixSchemaParams missing_database(resource);
    missing_database.emplace_back("nodb", "t");
    require_error(manager->get_schema(missing_database),
                  core::error_code_t::database_not_exists,
                  "database does not exist: nodb");
}
