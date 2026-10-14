// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/parser/parser.hpp"

#include <catch2/catch_all.hpp>

#include <components/logical_plan/node.hpp>
#include <core/result_wrapper.hpp>
#include <memory_resource>
#include <string>
#include <vector>

namespace {

ParsedQueryDataPtr parse_or_die(GreenplumParser& p, const std::string& sql) {
    auto r = p.parse(sql);
    REQUIRE_FALSE(r.has_error());
    return std::move(r.value());
}

} // namespace

// ── Error handling ────────────────────────────────────────────────────────────

TEST_CASE("parse: empty string returns error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto r = parser.parse("");
    REQUIRE(r.has_error());
}

TEST_CASE("parse: a lone ';' has no statement and is a parse error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    // The grammar accepts the input but yields an empty statement list; there
    // is nothing to plan, so it must surface as sql_parse_error rather than
    // being read as a statement.
    auto r = parser.parse(";");
    REQUIRE(r.has_error());
    REQUIRE(r.error().type == core::error_code_t::sql_parse_error);
}

TEST_CASE("parse: a comment-only input has no statement and is a parse error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto r = parser.parse("-- c");
    REQUIRE(r.has_error());
    REQUIRE(r.error().type == core::error_code_t::sql_parse_error);
}

TEST_CASE("parse: invalid SQL syntax returns error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto r = parser.parse("SELECTT * FORM nowhere");
    REQUIRE(r.has_error());
}

TEST_CASE("parse: incomplete statement returns error") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    // Note: a bare "SELECT" is VALID in the PostgreSQL grammar (empty target
    // list), so an actually incomplete statement is used here.
    auto r = parser.parse("SELECT id FROM");
    REQUIRE(r.has_error());
}

// ── NodeTag ───────────────────────────────────────────────────────────────────

TEST_CASE("parse: SELECT sets T_SelectStmt tag") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "SELECT id, name FROM orders WHERE id = 1;");
    REQUIRE(parsed->tag == NodeTag::T_SelectStmt);
}

TEST_CASE("parse: INSERT sets T_InsertStmt tag") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "INSERT INTO orders (id, name) VALUES (1, 'test');");
    REQUIRE(parsed->tag == NodeTag::T_InsertStmt);
}

TEST_CASE("parse: UPDATE sets T_UpdateStmt tag") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "UPDATE orders SET name = 'x' WHERE id = 1;");
    REQUIRE(parsed->tag == NodeTag::T_UpdateStmt);
}

TEST_CASE("parse: DELETE sets T_DeleteStmt tag") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "DELETE FROM orders WHERE id = 1;");
    REQUIRE(parsed->tag == NodeTag::T_DeleteStmt);
}

// ── parameters_count ─────────────────────────────────────────────────────────

TEST_CASE("parse: no placeholders → parameters_count == 0") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "SELECT id FROM orders WHERE status = 'active';");
    REQUIRE(parsed->otterbrix_params->parameters_count == 0);
}

TEST_CASE("parse: one placeholder → parameters_count == 1") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "SELECT id FROM orders WHERE id = $1;");
    REQUIRE(parsed->otterbrix_params->parameters_count == 1);
}

TEST_CASE("parse: two placeholders → parameters_count == 2") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);
    auto parsed = parse_or_die(parser, "SELECT id FROM orders WHERE id = $1 AND status = $2;");
    REQUIRE(parsed->otterbrix_params->parameters_count == 2);
}

// ── Statefulness: parser instance reuse ──────────────────────────────────────

TEST_CASE("parse: same parser handles multiple successive calls correctly") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto r1 = parse_or_die(parser, "SELECT id FROM orders;");
    auto r2 = parse_or_die(parser, "SELECT name FROM products WHERE id = 1;");

    REQUIRE(r1->sql == "SELECT id FROM orders;");
    REQUIRE(r2->sql == "SELECT name FROM products WHERE id = 1;");
    REQUIRE(r1->tag == NodeTag::T_SelectStmt);
    REQUIRE(r2->tag == NodeTag::T_SelectStmt);
}

TEST_CASE("parse: error then success on same parser instance") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    REQUIRE(parser.parse("NOT VALID SQL !!").has_error());
    auto parsed = parse_or_die(parser, "SELECT id FROM orders;");
    REQUIRE(parsed->tag == NodeTag::T_SelectStmt);
}
