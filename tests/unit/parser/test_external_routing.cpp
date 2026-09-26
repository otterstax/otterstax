// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Verifies the s3/file grammar extensions are registered in GreenplumParser and
// that CREATE EXTERNAL TABLE / COPY (...) TO lower into an external_node_t
// carrying the fields the Scheduler routes on. (The end-to-end routing through
// the file/s3 managers is exercised by the system / minio tests.)

#include <catch2/catch_all.hpp>

#include "otterbrix/parser/grammar_extention/external_node.hpp"
#include "otterbrix/parser/parser.hpp"

#include <memory_resource>

using otterstax::external::external_node_t;
using otterstax::external::external_op_t;

namespace {
    // external_node_t is tagged node_type::unused (the engine never executes
    // it) and the parser produces no other unused-tagged root, so the tag alone
    // identifies it.
    external_node_t* as_external(const components::logical_plan::node_ptr& node) {
        if (!node || node->type() != components::logical_plan::node_type::unused) {
            return nullptr;
        }
        return static_cast<external_node_t*>(node.get());
    }
} // namespace

TEST_CASE("external: CREATE EXTERNAL TABLE on a local path lowers to external_node_t") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    auto r = parser.parse(
        "CREATE EXTERNAL TABLE file.people WITH (location = '/tmp/people.parquet', format = 'parquet')");
    REQUIRE_FALSE(r.has_error());

    auto* node = as_external(r.value()->otterbrix_params->node);
    REQUIRE(node != nullptr);
    CHECK(node->op() == external_op_t::create_external_table);
    CHECK_FALSE(node->is_s3());
    CHECK(node->database() == "file");
    CHECK(node->table() == "people");
    CHECK(node->location() == "/tmp/people.parquet");
    CHECK(node->object_path() == "/tmp/people.parquet");
    CHECK(node->inner_sql().empty());
}

TEST_CASE("external: CREATE EXTERNAL TABLE on an s3 URI lowers to external_node_t") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    auto r = parser.parse("CREATE EXTERNAL TABLE s3.trades WITH ("
                          "  s3_alias = 'minio1', location = 's3://bucket/trades.parquet', format = 'parquet')");
    REQUIRE_FALSE(r.has_error());

    auto* node = as_external(r.value()->otterbrix_params->node);
    REQUIRE(node != nullptr);
    CHECK(node->op() == external_op_t::create_external_table);
    CHECK(node->is_s3());
    CHECK(node->s3_alias() == "minio1");
    CHECK(node->location() == "s3://bucket/trades.parquet");
    CHECK(node->object_path() == "bucket/trades.parquet"); // scheme stripped for the s3 manager
}

TEST_CASE("external: COPY (...) TO a local path captures the inner query") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    auto r = parser.parse("COPY (SELECT * FROM file.people) TO '/tmp/out.csv' WITH (format = 'csv')");
    REQUIRE_FALSE(r.has_error());

    auto* node = as_external(r.value()->otterbrix_params->node);
    REQUIRE(node != nullptr);
    CHECK(node->op() == external_op_t::copy_to);
    CHECK_FALSE(node->is_s3());
    CHECK(node->location() == "/tmp/out.csv");
    CHECK(node->inner_sql() == "SELECT * FROM file.people");
}

TEST_CASE("external: COPY (...) TO an s3 URI captures the inner query and alias") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    auto r = parser.parse(
        "COPY (SELECT 1) TO 's3://bucket/out.parquet' WITH (s3_alias = 'minio1', format = 'parquet')");
    REQUIRE_FALSE(r.has_error());

    auto* node = as_external(r.value()->otterbrix_params->node);
    REQUIRE(node != nullptr);
    CHECK(node->op() == external_op_t::copy_to);
    CHECK(node->is_s3());
    CHECK(node->s3_alias() == "minio1");
    CHECK(node->object_path() == "bucket/out.parquet");
    CHECK(node->inner_sql() == "SELECT 1");
}

TEST_CASE("external: a plain SELECT is not claimed by the extensions") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    auto r = parser.parse("SELECT 1 AS x");
    REQUIRE_FALSE(r.has_error());
    CHECK(as_external(r.value()->otterbrix_params->node) == nullptr);
    CHECK(r.value()->extension_kind == extension_kind_t::none);
}

// The three extension roots share node_type::unused, so the parser records
// which extension claimed the statement; the Worker routes on that record.
TEST_CASE("external: the claiming extension is recorded on ParsedQueryData") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    SECTION("file extension") {
        auto r = parser.parse("CREATE EXTERNAL TABLE file.t WITH (location = '/tmp/a.parquet')");
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value()->extension_kind == extension_kind_t::external);
    }
    SECTION("s3 extension") {
        auto r = parser.parse("COPY (SELECT * FROM s3.t) TO 's3://bucket/out.csv' "
                              "WITH (s3_alias = 'm', format = 'csv')");
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value()->extension_kind == extension_kind_t::external);
    }
    SECTION("kafka extension") {
        auto r = parser.parse("CREATE SOURCE orders (id BIGINT, note VARCHAR) "
                              "WITH (KAFKA_TOPIC='orders_topic', value_format='JSON')");
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value()->extension_kind == extension_kind_t::kafka);
        REQUIRE(r.value()->otterbrix_params->node != nullptr);
        CHECK(r.value()->otterbrix_params->node->type() == components::logical_plan::node_type::unused);
    }
    SECTION("an engine statement") {
        auto r = parser.parse("CREATE DATABASE plain_db");
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value()->extension_kind == extension_kind_t::none);
    }
}

TEST_CASE("external: the format option is optional (auto-detected downstream)") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    auto r = parser.parse("CREATE EXTERNAL TABLE file.t WITH (location = '/tmp/a.parquet')");
    REQUIRE_FALSE(r.has_error());

    auto* node = as_external(r.value()->otterbrix_params->node);
    REQUIRE(node != nullptr);
    CHECK(node->op() == external_op_t::create_external_table);
    CHECK(node->format().empty());
    CHECK(node->location() == "/tmp/a.parquet");
}

TEST_CASE("external: a malformed external statement surfaces a parse error (no crash)") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    // Claimed by the s3 extension (CREATE EXTERNAL TABLE + WITH) but the option
    // value is missing — the extension grammar rejects it; GreenplumParser must
    // turn the thrown parser_exception_t into a clean error result.
    auto r = parser.parse("CREATE EXTERNAL TABLE s3.t WITH (location =)");
    CHECK(r.has_error());
}
