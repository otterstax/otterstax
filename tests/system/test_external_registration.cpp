// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// External-table registration against the real engine, through the engine
// requests (otterbrix/operators/engine_requests): the file / s3 load
// (engine::create_table) and the relation probe of its result.
#include "otterbrix/operators/engine_requests.hpp"
#include "otterbrix/operators/schema_probe.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <components/types/types.hpp>
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

    template<typename T>
    void require_ok(const core::result_wrapper_t<T>& result) {
        INFO("error: " << (result.has_error() ? result.error().what.c_str() : ""));
        REQUIRE_FALSE(result.has_error());
    }

    // The file / s3 load: database made to exist, the table created from the
    // chunk's columns, the rows inserted.
    core::result_wrapper_t<bool> load(test_engine_t& engine,
                                      const std::string& database,
                                      const std::string& table,
                                      components::vector::data_chunk_t rows) {
        return engine.drive(engine_requests::create_table(engine.resource(),
                                                          engine.dispatcher(),
                                                          engine.session(),
                                                          database,
                                                          table,
                                                          std::move(rows)));
    }

    // The relation probe of engine::describe: a LIMIT 0 read of database.collection.
    core::result_wrapper_t<engine_requests::columns_t>
    probe(test_engine_t& engine, const std::string& database, const std::string& collection) {
        auto* resource = engine.resource();
        auto probe = otterstax::schema_probe::make_table_probe(resource, database, collection);
        auto cursor =
            engine.drive(engine_requests::send_plan(engine.dispatcher(), engine.session(), probe.execution_plan(resource)));
        auto read = otterstax::schema_probe::read_columns(probe, cursor, resource);
        if (read.has_error()) {
            return read.convert_error<engine_requests::columns_t>();
        }
        return std::move(read.value().columns);
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

// The load (engine::create_table, behind CREATE EXTERNAL TABLE) makes the
// database exist, creates database.collection and inserts the rows: they land
// in the named database and nowhere else, and a table that already exists is
// refused rather than appended to.
TEST_CASE("external registration: a load fills a table in its own database only") {
    test_engine_t engine("/tmp/otterstax_test_insert_data");
    engine.remove_on_exit();
    auto* resource = engine.resource();

    require_ok(engine.sql("CREATE DATABASE mixdb;"));
    require_ok(load(engine, "mixdb", "t", id_name_rows(resource, 1, 3)));

    // A database that does not exist yet is created by the load.
    require_ok(load(engine, "newdb", "t", id_name_rows(resource, 50, 1)));
    auto created = engine.sql("SELECT id, name FROM newdb.t;");
    require_ok(created);
    REQUIRE(id_name_pairs(created) == std::vector<std::pair<std::int64_t, std::string>>{{50, "name_50"}});

    // The same collection name in another database is a separate table.
    require_ok(load(engine, "otherdb", "t", id_name_rows(resource, 100, 1)));

    // A second load of an existing table is refused, and the table keeps only
    // the rows of the first load.
    auto again = load(engine, "mixdb", "t", id_name_rows(resource, 200, 2));
    REQUIRE(again.has_error());
    REQUIRE(again.error().type == core::error_code_t::table_already_exists);
    REQUIRE(std::string{again.error().what.c_str()}.find("mixdb.t already exists") != std::string::npos);

    auto mixed = engine.sql("SELECT id, name FROM mixdb.t ORDER BY id;");
    require_ok(mixed);
    REQUIRE(id_name_pairs(mixed) ==
            std::vector<std::pair<std::int64_t, std::string>>{{1, "name_1"}, {2, "name_2"}, {3, "name_3"}});
    auto others = engine.sql("SELECT id, name FROM otherdb.t ORDER BY id;");
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

    require_ok(load(engine, "mixdb", "t", id_name_rows(resource, 1, 3)));
    require_ok(engine.sql("DROP TABLE mixdb.t;"));
    require_ok(load(engine, "mixdb", "t", id_name_rows(resource, 200, 2)));

    auto rows = engine.sql("SELECT id, name FROM mixdb.t ORDER BY id;");
    require_ok(rows);
    REQUIRE(id_name_pairs(rows) ==
            std::vector<std::pair<std::int64_t, std::string>>{{200, "name_200"}, {201, "name_201"}});
}

// The relation probe reads the columns off the engine's reply to a LIMIT 0
// read, never off row data: an empty and a populated table of one shape answer
// the same STRUCT.
TEST_CASE("external registration: the probe of an empty and a populated table") {
    test_engine_t engine("/tmp/otterstax_test_get_schema");
    engine.remove_on_exit();
    auto* resource = engine.resource();

    require_ok(engine.sql("CREATE DATABASE mixdb;"));
    require_ok(engine.sql("CREATE TABLE mixdb.empty_t (id BIGINT, name STRING);"));
    require_ok(load(engine, "mixdb", "full_t", id_name_rows(resource, 1, 3)));

    for (const char* table : {"empty_t", "full_t"}) {
        auto columns = probe(engine, "mixdb", table);
        require_ok(columns);
        require_id_name_struct(complex_logical_type::create_struct("", columns.value()));
    }

    auto missing_table = probe(engine, "mixdb", "nope");
    REQUIRE(missing_table.has_error());
    REQUIRE(missing_table.error().type == core::error_code_t::table_not_exists);
    REQUIRE(std::string{missing_table.error().what.c_str()} == "collection does not exist: mixdb.nope");

    auto missing_database = probe(engine, "nodb", "t");
    REQUIRE(missing_database.has_error());
    REQUIRE(missing_database.error().type == core::error_code_t::database_not_exists);
    REQUIRE(std::string{missing_database.error().what.c_str()} == "database does not exist: nodb");
}
