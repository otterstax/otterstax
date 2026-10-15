// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include <catch2/catch_all.hpp>

#include "drive_future.hpp"
#include "integration/kafka/detail/kafka_poller.hpp"
#include "integration/kafka/detail/kafka_reader.hpp"
#include "integration/otterbrix/otterbrix_engine.hpp"
#include "otterbrix/config.hpp"
#include "tests/temporary_directory.hpp"

#include <filesystem>

namespace kd = otterstax::kafka::detail;
namespace ty = components::types;

namespace {
    std::map<int32_t, int64_t> checked_offsets(const components::cursor::cursor_t_ptr& cursor) {
        auto parsed = kd::parse_offsets(cursor);
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value());
    }
}

TEST_CASE("kafka transactional offsets: repeated batches replace only their partitions") {
    otterstax::test::temporary_directory directory("otterstax-kafka-offsets-replace");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto addr = engine->engine_dispatcher_address();
    auto query = [&](const std::string& sql) {
        auto cursor = drive_until_ready(kd::kafka_query(addr, resource, sql));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        return cursor;
    };
    query("CREATE DATABASE kafka;");
    query("CREATE TABLE kafka.s (id INT);");
    query("CREATE TABLE kafka.s__offsets (partition_id INT, committed_offset BIGINT);");
    // Existing append-only state must collapse when its partition next advances.
    query("INSERT INTO kafka.s__offsets (partition_id, committed_offset) VALUES (0, 1), (0, 2), (1, 8);");
    const std::vector<otterstax::kafka::kafka_column_t> columns{{"id", ty::logical_type::INTEGER}};
    for (int32_t batch = 0; batch < 32; ++batch) {
        INFO("batch " << batch);
        const std::map<int32_t, int64_t> next{{0, 3 + batch}, {2, 1 + batch}};
        auto error = kd::ingest_transactional(
            addr, resource, "kafka", "s", next,
            kd::json_to_chunk(resource, columns, {"{\"id\":" + std::to_string(batch) + "}"}));
        REQUIRE_FALSE(error.contains_error());
        auto stored = query("SELECT partition_id, committed_offset FROM kafka.s__offsets;");
        REQUIRE(stored->size() == 3);
        REQUIRE(checked_offsets(stored) == std::map<int32_t, int64_t>{{0, 3 + batch}, {1, 8}, {2, 1 + batch}});
    }
    auto rows = query("SELECT id FROM kafka.s;");
    REQUIRE(rows->size() == 32);
    int32_t expected = 0;
    for (const auto& chunk : rows->chunks()) {
        for (uint64_t row = 0; row < chunk.size(); ++row) {
            CHECK(chunk.value(0, row).value<int32_t>() == expected++);
        }
    }
    // An empty converted batch still advances offsets atomically.
    REQUIRE_FALSE(kd::ingest_transactional(addr, resource, "kafka", "s", {{0, 35}},
                                           kd::json_to_chunk(resource, columns, {})).contains_error());
    CHECK(query("SELECT id FROM kafka.s;")->size() == 32);
    auto stored = query("SELECT partition_id, committed_offset FROM kafka.s__offsets;");
    REQUIRE(stored->size() == 3);
    CHECK(checked_offsets(stored).at(0) == 35);
}

TEST_CASE("kafka transactional offsets: failed replacement rolls back both deletion and data") {
    otterstax::test::temporary_directory directory("otterstax-kafka-offsets-rollback");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto addr = engine->engine_dispatcher_address();
    auto query = [&](const std::string& sql) {
        auto cursor = drive_until_ready(kd::kafka_query(addr, resource, sql));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        return cursor;
    };
    query("CREATE DATABASE kafka;");
    query("CREATE TABLE kafka.s (id INT);");
    query("CREATE TABLE kafka.s__offsets (partition_id INT, committed_offset BIGINT CHECK (committed_offset < 100));");
    query("INSERT INTO kafka.s__offsets (partition_id, committed_offset) VALUES (0, 7), (1, 9);");
    const std::vector<otterstax::kafka::kafka_column_t> columns{{"id", ty::logical_type::INTEGER}};
    auto ingest = [&](int64_t offset) {
        return kd::ingest_transactional(addr, resource, "kafka", "s", {{0, offset}},
                                        kd::json_to_chunk(resource, columns, {R"({"id":1})"}));
    };
    auto error = ingest(100);
    REQUIRE(error.contains_error());
    CHECK(std::string(error.what).find("offsets insert") != std::string::npos);
    CHECK(query("SELECT id FROM kafka.s;")->size() == 0);
    auto stored = query("SELECT partition_id, committed_offset FROM kafka.s__offsets;");
    REQUIRE(stored->size() == 2);
    CHECK(checked_offsets(stored) == std::map<int32_t, int64_t>{{0, 7}, {1, 9}});
    REQUIRE_FALSE(ingest(8).contains_error());
    CHECK(query("SELECT id FROM kafka.s;")->size() == 1);
    stored = query("SELECT partition_id, committed_offset FROM kafka.s__offsets;");
    REQUIRE(stored->size() == 2);
    CHECK(checked_offsets(stored) == std::map<int32_t, int64_t>{{0, 8}, {1, 9}});
}

TEST_CASE("kafka offsets: read failures and corrupt positions cannot become an empty checkpoint") {
    otterstax::test::temporary_directory directory("otterstax-kafka-offsets-read-errors");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto addr = engine->engine_dispatcher_address();
    auto query = [&](const std::string& sql) {
        return drive_until_ready(kd::kafka_query(addr, resource, sql));
    };
    REQUIRE(kd::parse_offsets({}).has_error());
    auto missing = query("SELECT partition_id, committed_offset FROM kafka.missing__offsets;");
    REQUIRE(missing);
    REQUIRE(missing->is_error());
    auto parsed = kd::parse_offsets(missing);
    REQUIRE(parsed.has_error());
    CHECK(parsed.error().type == missing->get_error().type);
    REQUIRE_FALSE(query("CREATE DATABASE kafka;")->is_error());
    REQUIRE_FALSE(query("CREATE TABLE kafka.positions (partition_id INT, committed_offset BIGINT);")->is_error());
    auto empty = kd::parse_offsets(query("SELECT * FROM kafka.positions;"));
    REQUIRE_FALSE(empty.has_error());
    CHECK(empty.value().empty());
    const auto bad_row = GENERATE("(0, -1)", "(-1, 1)", "(NULL, 1)", "(0, NULL)");
    REQUIRE_FALSE(query(std::string{"INSERT INTO kafka.positions (partition_id, committed_offset) VALUES "} + bad_row + ";")->is_error());
    CHECK(kd::parse_offsets(query("SELECT * FROM kafka.positions;")).has_error());
    CHECK(kd::parse_offsets(query("SELECT partition_id FROM kafka.positions;")).has_error());
}
