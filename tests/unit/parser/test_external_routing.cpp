// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Verifies the s3/file grammar extensions are registered in GreenplumParser and
// that CREATE EXTERNAL TABLE / COPY (...) TO lower into an external_node_t
// carrying the fields the Scheduler routes on. (The end-to-end routing through
// the file/s3 managers is exercised by the system / minio tests.)

#include <catch2/catch.hpp>

#include "otterbrix/parser/grammar_extention/external_node.hpp"
#include "otterbrix/parser/parser.hpp"

#include <memory_resource>

using otterstax::external::external_node_t;
using otterstax::external::external_op_t;

TEST_CASE("external: CREATE EXTERNAL TABLE on a local path lowers to external_node_t") {
    GreenplumParser parser(std::pmr::get_default_resource());
    auto r = parser.parse(
        "CREATE EXTERNAL TABLE file.people WITH (location = '/tmp/people.parquet', format = 'parquet')");
    REQUIRE_FALSE(r.has_error());

    auto* node = dynamic_cast<external_node_t*>(r.value()->otterbrix_params->node.get());
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
    GreenplumParser parser(std::pmr::get_default_resource());
    auto r = parser.parse("CREATE EXTERNAL TABLE s3.trades WITH ("
                          "  s3_alias = 'minio1', location = 's3://bucket/trades.parquet', format = 'parquet')");
    REQUIRE_FALSE(r.has_error());

    auto* node = dynamic_cast<external_node_t*>(r.value()->otterbrix_params->node.get());
    REQUIRE(node != nullptr);
    CHECK(node->op() == external_op_t::create_external_table);
    CHECK(node->is_s3());
    CHECK(node->s3_alias() == "minio1");
    CHECK(node->location() == "s3://bucket/trades.parquet");
    CHECK(node->object_path() == "bucket/trades.parquet"); // scheme stripped for the s3 manager
}

TEST_CASE("external: COPY (...) TO a local path captures the inner query") {
    GreenplumParser parser(std::pmr::get_default_resource());
    auto r = parser.parse("COPY (SELECT * FROM file.people) TO '/tmp/out.csv' WITH (format = 'csv')");
    REQUIRE_FALSE(r.has_error());

    auto* node = dynamic_cast<external_node_t*>(r.value()->otterbrix_params->node.get());
    REQUIRE(node != nullptr);
    CHECK(node->op() == external_op_t::copy_to);
    CHECK_FALSE(node->is_s3());
    CHECK(node->location() == "/tmp/out.csv");
    CHECK(node->inner_sql() == "SELECT * FROM file.people");
}

TEST_CASE("external: COPY (...) TO an s3 URI captures the inner query and alias") {
    GreenplumParser parser(std::pmr::get_default_resource());
    auto r = parser.parse(
        "COPY (SELECT 1) TO 's3://bucket/out.parquet' WITH (s3_alias = 'minio1', format = 'parquet')");
    REQUIRE_FALSE(r.has_error());

    auto* node = dynamic_cast<external_node_t*>(r.value()->otterbrix_params->node.get());
    REQUIRE(node != nullptr);
    CHECK(node->op() == external_op_t::copy_to);
    CHECK(node->is_s3());
    CHECK(node->s3_alias() == "minio1");
    CHECK(node->object_path() == "bucket/out.parquet");
    CHECK(node->inner_sql() == "SELECT 1");
}

TEST_CASE("external: a plain SELECT is not claimed by the extensions") {
    GreenplumParser parser(std::pmr::get_default_resource());
    auto r = parser.parse("SELECT 1 AS x");
    REQUIRE_FALSE(r.has_error());
    CHECK(dynamic_cast<external_node_t*>(r.value()->otterbrix_params->node.get()) == nullptr);
}

TEST_CASE("external: the format option is optional (auto-detected downstream)") {
    GreenplumParser parser(std::pmr::get_default_resource());
    auto r = parser.parse("CREATE EXTERNAL TABLE file.t WITH (location = '/tmp/a.parquet')");
    REQUIRE_FALSE(r.has_error());

    auto* node = dynamic_cast<external_node_t*>(r.value()->otterbrix_params->node.get());
    REQUIRE(node != nullptr);
    CHECK(node->op() == external_op_t::create_external_table);
    CHECK(node->format().empty());
    CHECK(node->location() == "/tmp/a.parquet");
}

TEST_CASE("external: a malformed external statement surfaces a parse error (no crash)") {
    GreenplumParser parser(std::pmr::get_default_resource());
    // Claimed by the s3 extension (CREATE EXTERNAL TABLE + WITH) but the option
    // value is missing — the extension grammar rejects it; GreenplumParser must
    // turn the thrown parser_exception_t into a clean error result.
    auto r = parser.parse("CREATE EXTERNAL TABLE s3.t WITH (location =)");
    CHECK(r.has_error());
}
