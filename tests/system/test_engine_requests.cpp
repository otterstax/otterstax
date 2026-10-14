// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The engine requests (otterbrix/operators/engine_requests) and the DML-shape
// rules they apply (types/otterbrix.hpp), against a real engine in a temp dir:
// the write cursor of a local DML, the rule that tells a no-RETURNING DML apart
// from everything else, and the load behind CREATE EXTERNAL TABLE.

#include "otterbrix/operators/engine_requests.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "session_stack.hpp"
#include "types/otterbrix.hpp"

#include <actor-zeta.hpp>
#include <catch2/catch_all.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <memory>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::data_chunk_t;
using otterstax::test::test_engine_t;
namespace engine_requests = otterstax::engine;

namespace {

    components::cursor::cursor_t_ptr run_execute(test_engine_t& engine, OtterbrixStatementPtr statement) {
        return engine.drive(engine_requests::execute_statement(engine.resource(),
                                                               engine.dispatcher(),
                                                               engine.session(),
                                                               std::move(statement)));
    }

    OtterbrixStatementPtr parsed_statement(std::pmr::memory_resource* resource, const std::string& sql) {
        GreenplumParser parser(resource);
        auto parsed = parser.parse(sql);
        INFO(sql << " -> " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value()->otterbrix_params);
    }

    core::result_wrapper_t<bool> run_create_table(test_engine_t& engine, std::string database, data_chunk_t chunk) {
        return engine.drive(engine_requests::create_table(engine.resource(),
                                                          engine.dispatcher(),
                                                          engine.session(),
                                                          std::move(database),
                                                          "people",
                                                          std::move(chunk)));
    }

    data_chunk_t two_named_columns(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> types(resource);
        types.emplace_back(logical_type::INTEGER);
        types.back().set_alias("id");
        types.emplace_back(logical_type::STRING_LITERAL);
        types.back().set_alias("name");
        data_chunk_t chunk{resource, types, 1};
        chunk.set_value(0, 0, components::types::logical_value_t(resource, static_cast<int32_t>(1)));
        chunk.set_value(1, 0, components::types::logical_value_t(resource, std::string{"one"}));
        chunk.set_cardinality(1);
        return chunk;
    }

    void require_ok(const components::cursor::cursor_t_ptr& cursor) {
        REQUIRE(cursor);
        INFO((cursor->is_error() ? cursor->get_error().what.c_str() : ""));
        REQUIRE_FALSE(cursor->is_error());
    }

} // namespace

TEST_CASE("execute_statement: a local no-RETURNING DML answers its count as a write") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();
    require_ok(engine.sql("CREATE DATABASE wdb;"));
    require_ok(engine.sql("CREATE TABLE wdb.t (id bigint);"));

    auto cursor = run_execute(engine, parsed_statement(engine.resource(), "INSERT INTO wdb.t (id) VALUES (1), (2), (3);"));
    require_ok(cursor);
    REQUIRE(cursor->is_write());
    REQUIRE(cursor->affected_rows() == 3);
    REQUIRE(cursor->column_count() == 0);
    // The payload a frontend reads from it (no columns, the count on its own) is
    // pinned through the session in test_dml_result_shape.cpp.
}

// One builder serves a DELETE by one column and by several (the metadata's key
// columns): the terms are ANDed, every value bound as a parameter.
TEST_CASE("delete_rows_plan: the WHERE terms are ANDed and only the matching rows go") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();
    auto* resource = engine.resource();
    require_ok(engine.sql("CREATE DATABASE ddb;"));
    require_ok(engine.sql("CREATE TABLE ddb.t (uid string, name string);"));
    require_ok(engine.sql("INSERT INTO ddb.t (uid, name) VALUES ('a', 'x'), ('a', 'y'), ('b', 'x'), ('b', 'y');"));

    auto count = [&] {
        auto cursor = engine.sql("SELECT uid FROM ddb.t;");
        require_ok(cursor);
        return cursor->size();
    };

    const otterstax::engine::column_equals_t both[] = {
        {"uid", components::types::logical_value_t(resource, std::string{"a"})},
        {"name", components::types::logical_value_t(resource, std::string{"x"})}};
    auto deleted = engine.plan(otterstax::engine::delete_rows_plan(resource, "ddb", "t", both));
    require_ok(deleted);
    CHECK(deleted->affected_rows() == 1);
    CHECK(count() == 3);

    const otterstax::engine::column_equals_t one[] = {
        {"uid", components::types::logical_value_t(resource, std::string{"b"})}};
    deleted = engine.plan(otterstax::engine::delete_rows_plan(resource, "ddb", "t", one));
    require_ok(deleted);
    CHECK(deleted->affected_rows() == 2);
    CHECK(count() == 1);
}

TEST_CASE("execute_statement: a column-less result of a SELECT plan is a schema_error cursor") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();

    {
        // The engine answers a hugeint column it cannot type without columns.
        require_ok(engine.sql("CREATE DATABASE hdb;"));
        require_ok(engine.sql("CREATE TABLE hdb.h (big hugeint);"));
        require_ok(engine.sql("INSERT INTO hdb.h (big) VALUES (1);"));
        auto cursor = run_execute(engine, parsed_statement(engine.resource(), "SELECT big FROM hdb.h;"));
        REQUIRE(cursor);
        if (!cursor->is_error()) {
            // An engine that types the column answers it with columns.
            REQUIRE(cursor->column_count() > 0);
        } else {
            REQUIRE(cursor->get_error().type == core::error_code_t::schema_error);
            REQUIRE(std::string{cursor->get_error().what.c_str()}.find("SELECT") != std::string::npos);
            REQUIRE(std::string{cursor->get_error().what.c_str()}.find("without columns") != std::string::npos);
        }
    }
}

TEST_CASE("execute_statement: a multi-chunk result keeps every row's values") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();
    constexpr size_t rows = 2500;
    require_ok(engine.sql("CREATE DATABASE mdb;"));
    require_ok(engine.sql("CREATE TABLE mdb.t (id bigint, name string);"));
    for (size_t first = 0; first < rows; first += 500) {
        std::string sql = "INSERT INTO mdb.t (id, name) VALUES ";
        for (size_t i = first; i < first + 500; ++i) {
            sql += (i == first ? "(" : ", (") + std::to_string(i) + ", 'name_" + std::to_string(i) + "')";
        }
        require_ok(engine.sql(sql + ";"));
    }

    auto cursor = run_execute(engine, parsed_statement(engine.resource(), "SELECT id, name FROM mdb.t ORDER BY id;"));

    require_ok(cursor);
    REQUIRE(cursor->size() == rows);
    REQUIRE(cursor->column_count() == 2);
    REQUIRE(cursor->chunks().size() >= 3);
    // Rows on both sides of a chunk boundary and the very last one.
    for (size_t row : {size_t{0}, size_t{1023}, size_t{1024}, size_t{2047}, size_t{2048}, rows - 1}) {
        INFO("row " << row);
        REQUIRE(cursor->value(0, row).value<int64_t>() == static_cast<int64_t>(row));
        REQUIRE(std::string{cursor->value(1, row).value<std::string_view>()} == "name_" + std::to_string(row));
    }
}

TEST_CASE("execute_statement: an engine refusal is an error cursor with the engine's code") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();

    auto cursor = run_execute(engine, parsed_statement(engine.resource(), "SELECT id FROM nodb.t;"));

    REQUIRE(cursor);
    REQUIRE(cursor->is_error());
    REQUIRE(cursor->get_error().type == core::error_code_t::database_not_exists);
}

TEST_CASE("dml_without_returning: only INSERT/UPDATE/DELETE without RETURNING qualify") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    GreenplumParser parser(&arena);
    auto plan_of = [&](const char* sql) {
        auto parsed = parser.parse(sql);
        INFO(sql << " -> " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value()->otterbrix_params->node);
    };

    REQUIRE(dml_without_returning(plan_of("INSERT INTO db.t (id) VALUES (1);")));
    REQUIRE(dml_without_returning(plan_of("UPDATE db.t SET id = 2 WHERE id = 1;")));
    REQUIRE(dml_without_returning(plan_of("DELETE FROM db.t WHERE id = 1;")));

    REQUIRE_FALSE(dml_without_returning(plan_of("INSERT INTO db.t (id) VALUES (1) RETURNING id;")));
    REQUIRE_FALSE(dml_without_returning(plan_of("UPDATE db.t SET id = 2 WHERE id = 1 RETURNING id;")));
    REQUIRE_FALSE(dml_without_returning(plan_of("DELETE FROM db.t WHERE id = 1 RETURNING id;")));

    REQUIRE_FALSE(dml_without_returning(plan_of("SELECT id FROM db.t;")));
    REQUIRE_FALSE(dml_without_returning(components::logical_plan::node_ptr{}));
    // A bare DML node (no resolve wrapper) is the same statement.
    REQUIRE(dml_without_returning(components::logical_plan::make_node_insert(&arena)));
}

TEST_CASE("row_producing_statement: SELECT and RETURNING DML answer rows and nothing else does") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    GreenplumParser parser(&arena);
    auto plan_of = [&](const char* sql) {
        auto parsed = parser.parse(sql);
        INFO(sql << " -> " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value()->otterbrix_params->node);
    };
    auto kind_of = [&](const char* sql) {
        const char* kind = row_producing_statement(plan_of(sql));
        return kind ? std::string{kind} : std::string{};
    };

    REQUIRE(kind_of("SELECT id FROM db.t;") == "SELECT");
    REQUIRE(kind_of("SELECT id FROM db.t WHERE id > 1 ORDER BY id LIMIT 3;") == "SELECT");
    REQUIRE(kind_of("SELECT count(*) FROM db.t;") == "SELECT");
    REQUIRE(kind_of("SELECT a.id FROM db.a JOIN db.b ON a.id = b.id;") == "SELECT");
    REQUIRE(kind_of("SELECT id FROM db.t UNION SELECT id FROM db.u;") == "SELECT");
    REQUIRE(kind_of("INSERT INTO db.t (id) VALUES (1) RETURNING id;") == "INSERT ... RETURNING");
    REQUIRE(kind_of("UPDATE db.t SET id = 2 WHERE id = 1 RETURNING id;") == "UPDATE ... RETURNING");
    REQUIRE(kind_of("DELETE FROM db.t WHERE id = 1 RETURNING id;") == "DELETE ... RETURNING");

    REQUIRE(kind_of("INSERT INTO db.t (id) VALUES (1);").empty());
    REQUIRE(kind_of("UPDATE db.t SET id = 2 WHERE id = 1;").empty());
    REQUIRE(kind_of("DELETE FROM db.t WHERE id = 1;").empty());
    REQUIRE(kind_of("CREATE DATABASE db;").empty());
    REQUIRE(kind_of("CREATE TABLE db.t (id INT);").empty());
    REQUIRE(kind_of("DROP TABLE db.t;").empty());

    // A missing plan answers no rows.
    REQUIRE(row_producing_statement(components::logical_plan::node_ptr{}) == nullptr);
    // A bare aggregate node (no resolve wrapper) is the same statement.
    REQUIRE(std::string{row_producing_statement(components::logical_plan::make_node_aggregate(
                &arena, qualified_name_t{core::dbname_t{"db"}, core::relname_t{"t"}}))} == "SELECT");
}

TEST_CASE("create_table: the engine's verdict on the database is returned with its code") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();

    auto created = run_create_table(engine, "", two_named_columns(engine.resource()));

    REQUIRE(created.has_error());
    REQUIRE(created.error().type != core::error_code_t::none);
    REQUIRE(std::string{created.error().what.c_str()}.find("create_database") != std::string::npos);
}

TEST_CASE("create_table: the engine's verdict on the insert is returned with its code") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();
    REQUIRE_FALSE(run_create_table(engine, "filedb", two_named_columns(engine.resource())).has_error());

    auto created = run_create_table(engine, "filedb", two_named_columns(engine.resource()));

    REQUIRE(created.has_error());
    REQUIRE(created.error().type == core::error_code_t::table_already_exists);
    REQUIRE(std::string{created.error().what.c_str()}.find("insert_data") != std::string::npos);
}

TEST_CASE("create_table: a nameless column cannot define a table") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();

    std::pmr::vector<complex_logical_type> types(engine.resource());
    types.emplace_back(logical_type::INTEGER);
    types.back().set_alias("id");
    types.emplace_back(logical_type::DOUBLE);
    data_chunk_t chunk{engine.resource(), types, 0};

    auto created = run_create_table(engine, "filedb", std::move(chunk));

    REQUIRE(created.has_error());
    REQUIRE(created.error().type == core::error_code_t::invalid_parameter);
    REQUIRE(std::string{created.error().what.c_str()}.find("column 1") != std::string::npos);
}

TEST_CASE("create_table: a well-formed chunk is accepted and loaded") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();

    auto created = run_create_table(engine, "filedb", two_named_columns(engine.resource()));

    REQUIRE_FALSE(created.has_error());
    REQUIRE(created.value() == true);
    auto rows = engine.sql("SELECT id, name FROM filedb.people;");
    require_ok(rows);
    REQUIRE(rows->size() == 1);
}
