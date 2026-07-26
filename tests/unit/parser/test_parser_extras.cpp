// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"
#include "otterbrix/schema/schema_utils.hpp"

#include <catch2/catch_all.hpp>

#include <components/logical_plan/node.hpp>
#include <memory_resource>
#include <string>
#include <vector>

namespace {

ParsedQueryDataPtr parse_or_die(GreenplumParser& p, const std::string& sql) {
    auto r = p.parse(sql);
    REQUIRE_FALSE(r.has_error());
    return std::move(r.value());
}

size_t flat_count(const ParsedQueryDataPtr& parsed) {
    size_t n = 0;
    for (const auto& batch : parsed->otterbrix_params->external_nodes)
        n += batch.size();
    return n;
}

} // namespace

// ── Error handling ────────────────────────────────────────────────────────────

TEST_CASE("parse: empty string returns error") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    auto r = parser.parse("");
    REQUIRE(r.has_error());
}

TEST_CASE("parse: invalid SQL syntax returns error") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    auto r = parser.parse("SELECTT * FORM nowhere");
    REQUIRE(r.has_error());
}

TEST_CASE("parse: incomplete statement returns error") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    // Note: a bare "SELECT" is VALID in the PostgreSQL grammar (empty target
    // list), so an actually incomplete statement is used here.
    auto r = parser.parse("SELECT id FROM");
    REQUIRE(r.has_error());
}

// ── NodeTag ───────────────────────────────────────────────────────────────────

TEST_CASE("parse: SELECT sets T_SelectStmt tag") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "SELECT id, name FROM orders WHERE id = 1;");
    REQUIRE(parsed->tag == NodeTag::T_SelectStmt);
}

TEST_CASE("parse: INSERT sets T_InsertStmt tag") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "INSERT INTO orders (id, name) VALUES (1, 'test');");
    REQUIRE(parsed->tag == NodeTag::T_InsertStmt);
}

TEST_CASE("parse: UPDATE sets T_UpdateStmt tag") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "UPDATE orders SET name = 'x' WHERE id = 1;");
    REQUIRE(parsed->tag == NodeTag::T_UpdateStmt);
}

TEST_CASE("parse: DELETE sets T_DeleteStmt tag") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "DELETE FROM orders WHERE id = 1;");
    REQUIRE(parsed->tag == NodeTag::T_DeleteStmt);
}

// ── parameters_count ─────────────────────────────────────────────────────────

TEST_CASE("parse: no placeholders → parameters_count == 0") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "SELECT id FROM orders WHERE status = 'active';");
    REQUIRE(parsed->otterbrix_params->parameters_count == 0);
}

TEST_CASE("parse: one placeholder → parameters_count == 1") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "SELECT id FROM orders WHERE id = $1;");
    REQUIRE(parsed->otterbrix_params->parameters_count == 1);
}

TEST_CASE("parse: two placeholders → parameters_count == 2") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "SELECT id FROM orders WHERE id = $1 AND status = $2;");
    REQUIRE(parsed->otterbrix_params->parameters_count == 2);
}

// ── external_nodes_count consistency ─────────────────────────────────────────

TEST_CASE("parse: external_nodes_count matches flat node count") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "SELECT o.id, p.name "
        "FROM uid1.db1.sch1.orders o "
        "INNER JOIN uid2.db2.sch2.products p ON o.product_id = p.id;");

    REQUIRE(parsed->otterbrix_params->external_nodes_count == flat_count(parsed));
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 2);
}

// ── Same-UID batching ─────────────────────────────────────────────────────────
// When two tables share a unique_identifier, get_external_nodes() places them
// in separate batches so the scheduler can issue two sequential round-trips
// to the same connection.

TEST_CASE("parse: same UID in two tables → two batches, one node each") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "SELECT a.id, b.name "
        "FROM uid1.db1.sch1.table_a a "
        "INNER JOIN uid1.db1.sch1.table_b b ON a.id = b.id;");

    // Two external nodes from the same uid → two separate batches
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 2);
    REQUIRE(parsed->otterbrix_params->external_nodes.size() == 2);
    REQUIRE(parsed->otterbrix_params->external_nodes[0].size() == 1);
    REQUIRE(parsed->otterbrix_params->external_nodes[1].size() == 1);

    // Both nodes must carry the same uid (each external entry carries its
    // resolved target — nodes themselves no longer carry names)
    REQUIRE(parsed->otterbrix_params->external_nodes[0][0].target.name.unique_identifier == "uid1");
    REQUIRE(parsed->otterbrix_params->external_nodes[1][0].target.name.unique_identifier == "uid1");
}

TEST_CASE("parse: different UIDs in two tables → one batch, two nodes") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "SELECT a.id, b.name "
        "FROM uid1.db1.sch1.table_a a "
        "INNER JOIN uid2.db2.sch2.table_b b ON a.id = b.id;");

    REQUIRE(parsed->otterbrix_params->external_nodes_count == 2);
    // Different UIDs do not collide: both nodes fit in a single batch
    REQUIRE(parsed->otterbrix_params->external_nodes.size() == 1);
    REQUIRE(parsed->otterbrix_params->external_nodes[0].size() == 2);
}

// ── Statefulness: parser instance reuse ──────────────────────────────────────

TEST_CASE("parse: same parser handles multiple successive calls correctly") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    auto r1 = parse_or_die(parser, "SELECT id FROM orders;");
    auto r2 = parse_or_die(parser, "SELECT name FROM products WHERE id = 1;");

    REQUIRE(r1->otterbrix_params->external_nodes_count == 0);
    REQUIRE(r2->otterbrix_params->external_nodes_count == 0);
    REQUIRE(r1->tag == NodeTag::T_SelectStmt);
    REQUIRE(r2->tag == NodeTag::T_SelectStmt);
}

TEST_CASE("parse: error then success on same parser instance") {
    auto* resource = std::pmr::get_default_resource();
    GreenplumParser parser(resource);

    REQUIRE(parser.parse("NOT VALID SQL !!").has_error());
    auto parsed = parse_or_die(parser, "SELECT id FROM orders;");
    REQUIRE(parsed->tag == NodeTag::T_SelectStmt);
}
