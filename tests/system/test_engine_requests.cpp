// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The engine requests (otterbrix/operators/engine_requests) and the DML-shape
// rules they apply (types/otterbrix.hpp), against a real engine in a temp dir:
// the write cursor of a local DML, the rule that tells a no-RETURNING DML apart
// from everything else. The load behind CREATE EXTERNAL TABLE is
// test_external_registration.cpp's.

#include "otterbrix/operators/engine_requests.hpp"
#include "../mock/cell.hpp"
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

    components::cursor::cursor_t_ptr run_execute(test_engine_t& engine, ExecutionPlanPtr statement) {
        return engine.drive(engine_requests::execute_statement(engine.resource(),
                                                               engine.dispatcher(),
                                                               engine.session(),
                                                               std::move(statement)));
    }

    ExecutionPlanPtr parsed_statement(std::pmr::memory_resource* resource, const std::string& sql) {
        GreenplumParser parser(resource);
        auto parsed = parser.parse(sql);
        INFO(sql << " -> " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value()->plan);
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
    require_ok(engine.sql("CREATE SCHEMA wdb;"));
    require_ok(engine.sql("CREATE TABLE wdb.t (id bigint);"));

    auto cursor = run_execute(engine, parsed_statement(engine.resource(), "INSERT INTO wdb.t (id) VALUES (1), (2), (3);"));
    require_ok(cursor);
    REQUIRE(cursor->is_write());
    REQUIRE(cursor->affected_rows() == 3);
    REQUIRE(cursor->column_count() == 0);
    // The payload a frontend reads from it (no columns, the count on its own) is
    // pinned through the session in test_dml_result_shape.cpp.
}

TEST_CASE("execute_statement: a column-less result of a SELECT plan is an internal_error cursor") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();

    {
        // The engine answers a hugeint column it cannot type without columns.
        require_ok(engine.sql("CREATE SCHEMA hdb;"));
        require_ok(engine.sql("CREATE TABLE hdb.h (big hugeint);"));
        require_ok(engine.sql("INSERT INTO hdb.h (big) VALUES (1);"));
        auto cursor = run_execute(engine, parsed_statement(engine.resource(), "SELECT big FROM hdb.h;"));
        REQUIRE(cursor);
        if (!cursor->is_error()) {
            // An engine that types the column answers it with columns.
            REQUIRE(cursor->column_count() > 0);
        } else {
            REQUIRE(cursor->get_error().type == core::error_code_t::internal_error);
            REQUIRE(std::string{cursor->get_error().what.c_str()}.find("SELECT") != std::string::npos);
            REQUIRE(std::string{cursor->get_error().what.c_str()}.find("without columns") != std::string::npos);
        }
    }
}

TEST_CASE("execute_statement: a multi-chunk result keeps every row's values") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();
    constexpr size_t rows = 2500;
    require_ok(engine.sql("CREATE SCHEMA mdb;"));
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
        REQUIRE(cursor->get<int64_t>(0, row).value() == static_cast<int64_t>(row));
        REQUIRE(std::string{cursor->get<std::string_view>(1, row).value()} == "name_" + std::to_string(row));
    }
}

TEST_CASE("execute_statement: an engine refusal is an error cursor with the engine's code") {
    test_engine_t engine(otterstax::test::unique_test_dir("test_engine_requests"));
    engine.remove_on_exit();

    auto cursor = run_execute(engine, parsed_statement(engine.resource(), "SELECT id FROM nodb.public.t;"));

    REQUIRE(cursor);
    REQUIRE(cursor->is_error());
    // otterbrix answers a name it cannot resolve, its database missing included, as a missing table.
    REQUIRE(cursor->get_error().type == core::error_code_t::table_not_exists);
}

TEST_CASE("dml_without_returning: only INSERT/UPDATE/DELETE without RETURNING qualify") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    GreenplumParser parser(&arena);
    auto plan_of = [&](const char* sql) {
        auto parsed = parser.parse(sql);
        INFO(sql << " -> " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(parsed.has_error());
        return plan_root(*parsed.value()->plan);
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
        return plan_root(*parsed.value()->plan);
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
                &arena, qualified_name_t{core::schema_t{"db"}, core::relname_t{"t"}}))} == "SELECT");
}

