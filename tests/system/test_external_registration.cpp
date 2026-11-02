// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// External-table registration against the real engine, through the engine
// requests (otterbrix/operators/engine_requests): the load behind CREATE
// EXTERNAL TABLE (engine::create_table_plan + engine::load_table) on a session.
#include "otterbrix/operators/engine_requests.hpp"
#include "../mock/cell.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <string>
#include <vector>

using components::types::complex_logical_type;
using components::types::logical_type;
using otterstax::test::test_engine_t;
namespace engine_requests = otterstax::engine;

namespace {

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
            chunk.set_value(0, i, row_id);
            chunk.set_value(1, i, std::string_view{"name_" + std::to_string(row_id)});
        }
        chunk.set_cardinality(rows);
        return chunk;
    }

    void require_ok(const components::cursor::cursor_t_ptr& cursor) {
        REQUIRE(cursor);
        INFO("error: " << (cursor->is_error() ? cursor->get_error().what.c_str() : ""));
        REQUIRE_FALSE(cursor->is_error());
    }

    template<typename T>
    void require_ok(const core::result_wrapper_t<T>& result) {
        INFO("error: " << (result.has_error() ? result.error().what.c_str() : ""));
        REQUIRE_FALSE(result.has_error());
    }

    // The load on the test's engine session: CREATE TABLE `target` from the
    // chunk's columns, then the rows (a run of one chunk, as a loader answers a
    // small file); `own_block` as the query session passes it.
    components::cursor::cursor_t_ptr load(test_engine_t& engine,
                                          const qualified_name_t& target,
                                          components::vector::data_chunk_t chunk,
                                          bool own_block = true) {
        std::pmr::vector<components::vector::data_chunk_t> rows(engine.resource());
        rows.push_back(std::move(chunk));
        auto create = engine_requests::create_table_plan(engine.resource(), target, rows);
        REQUIRE_FALSE(create.has_error());
        return engine.drive(engine_requests::load_table(engine.resource(),
                                                        engine.dispatcher(),
                                                        engine.session(),
                                                        std::move(create.value()),
                                                        target,
                                                        std::move(rows),
                                                        own_block));
    }

    qualified_name_t local(const char* database, const char* table) {
        return qualified_name_t{core::dbname_t{database}, core::schema_t{"public"}, core::relname_t{table}};
    }

    // (id, name) of every row of an (id, name) result, in result order.
    std::vector<std::pair<std::int64_t, std::string>> id_name_pairs(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<std::pair<std::int64_t, std::string>> rows;
        for (std::uint64_t row = 0; row < cursor->size(); ++row) {
            rows.emplace_back(cursor->get<std::int64_t>(0, row).value(),
                              std::string{cursor->get<std::string_view>(1, row).value()});
        }
        return rows;
    }

} // namespace

// The load (behind CREATE EXTERNAL TABLE) creates the table as CREATE TABLE
// names it and inserts the rows: they land in the named database and nowhere
// else, a database that does not exist is not created, and a table that already
// exists is refused rather than appended to.
TEST_CASE("external registration: a load fills a table in its own database only") {
    test_engine_t engine("/tmp/otterstax_test_insert_data");
    engine.remove_on_exit();
    auto* resource = engine.resource();

    require_ok(engine.sql("CREATE DATABASE mixdb;"));
    require_ok(load(engine, local("mixdb", "t"), id_name_rows(resource, 1, 3)));

    // A database that does not exist is the engine's refusal; nothing is made.
    auto missing = load(engine, local("newdb", "t"), id_name_rows(resource, 50, 1));
    REQUIRE(missing->is_error());
    CHECK(missing->transaction_status() == components::session::transaction_status_t::idle);
    CHECK(engine.sql("SELECT id, name FROM newdb.public.t;")->is_error());

    // The same table name in another database is a separate table.
    require_ok(engine.sql("CREATE DATABASE otherdb;"));
    require_ok(load(engine, local("otherdb", "t"), id_name_rows(resource, 100, 1)));

    // A second load of an existing table is refused, and the table keeps only
    // the rows of the first load.
    auto again = load(engine, local("mixdb", "t"), id_name_rows(resource, 200, 2));
    REQUIRE(again->is_error());
    REQUIRE(again->get_error().type == core::error_code_t::table_already_exists);

    auto mixed = engine.sql("SELECT id, name FROM mixdb.public.t ORDER BY id;");
    require_ok(mixed);
    REQUIRE(id_name_pairs(mixed) ==
            std::vector<std::pair<std::int64_t, std::string>>{{1, "name_1"}, {2, "name_2"}, {3, "name_3"}});
    auto others = engine.sql("SELECT id, name FROM otherdb.public.t ORDER BY id;");
    require_ok(others);
    REQUIRE(id_name_pairs(others) == std::vector<std::pair<std::int64_t, std::string>>{{100, "name_100"}});
}

// DROP TABLE then CREATE EXTERNAL TABLE under the same name — the reload the
// python external-table tests and the benchmark's cold loads run: once the
// table is dropped, the load creates it again with only the new rows.
TEST_CASE("external registration: a load fills a dropped table again") {
    test_engine_t engine("/tmp/otterstax_test_insert_data_reload");
    engine.remove_on_exit();
    auto* resource = engine.resource();

    const qualified_name_t t{core::relname_t{"t"}};
    require_ok(load(engine, t, id_name_rows(resource, 1, 3)));
    require_ok(engine.sql("DROP TABLE t;"));
    require_ok(load(engine, t, id_name_rows(resource, 200, 2)));

    auto rows = engine.sql("SELECT id, name FROM t ORDER BY id;");
    require_ok(rows);
    REQUIRE(id_name_pairs(rows) ==
            std::vector<std::pair<std::int64_t, std::string>>{{200, "name_200"}, {201, "name_201"}});
}

// A load of its own block leaves the session outside one; a load inside the
// session's block (own_block false) is part of it: ROLLBACK takes the table
// and its rows back.
TEST_CASE("external registration: a load inside the session's block is rolled back with it") {
    test_engine_t engine("/tmp/otterstax_test_insert_data_block");
    engine.remove_on_exit();
    auto* resource = engine.resource();
    using components::session::transaction_status_t;

    auto own = load(engine, qualified_name_t{core::relname_t{"own"}}, id_name_rows(resource, 1, 2));
    require_ok(own);
    CHECK(own->transaction_status() == transaction_status_t::idle);

    require_ok(engine.sql("BEGIN;"));
    auto inside = load(engine, qualified_name_t{core::relname_t{"inside"}}, id_name_rows(resource, 1, 2), false);
    require_ok(inside);
    CHECK(inside->transaction_status() == transaction_status_t::in_transaction);
    auto seen = engine.sql("SELECT id, name FROM inside ORDER BY id;");
    require_ok(seen);
    CHECK(seen->size() == 2);
    require_ok(engine.sql("ROLLBACK;"));
    CHECK(engine.sql("SELECT id, name FROM inside;")->is_error());
    CHECK(engine.sql("SELECT id, name FROM own;")->size() == 2);
}

// A column without a name cannot define a table: refused before the engine.
TEST_CASE("external registration: a nameless column cannot define a table") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<complex_logical_type> types(&arena);
    types.emplace_back(logical_type::BIGINT);
    std::pmr::vector<components::vector::data_chunk_t> rows(&arena);
    rows.emplace_back(&arena, types, 1);
    auto plan = engine_requests::create_table_plan(&arena, qualified_name_t{core::relname_t{"t"}}, rows);
    REQUIRE(plan.has_error());
    CHECK(plan.error().type == core::error_code_t::invalid_parameter);
    CHECK(std::string{plan.error().what.c_str()} == "CREATE TABLE t: column 0 has no name");
}

// An empty run names no column: refused before the engine.
TEST_CASE("external registration: an empty run cannot define a table") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    std::pmr::vector<components::vector::data_chunk_t> rows(&arena);
    auto plan = engine_requests::create_table_plan(&arena, qualified_name_t{core::relname_t{"t"}}, rows);
    REQUIRE(plan.has_error());
    CHECK(plan.error().type == core::error_code_t::invalid_parameter);
    CHECK(std::string{plan.error().what.c_str()} == "CREATE TABLE t: the input has no columns");
}
