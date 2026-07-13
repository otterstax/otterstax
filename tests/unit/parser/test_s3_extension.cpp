// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Unit tests for the s3 SQL grammar extension
// (otterbrix/parser/grammar_extention/s3). They exercise the parse stage
// directly (s3_ext::parse), the transform stage, and routing through the
// otterbrix parser registry (raw_parser) — mirroring otterbrix's own
// components/sql/test/test_parser_extension.cpp.

#include <catch2/catch_all.hpp>

#include <cstring>
#include <memory_resource>
#include <string>

#include <components/logical_plan/node.hpp>
#include <components/sql/parser/extension.hpp>
#include <components/sql/parser/nodes/parsenodes.h>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/parser/pg_std_list.h>
#include <components/sql/transformer/transformer.hpp>

#include "file_ast.hpp"       // file_stmt, for the disambiguation assertions
#include "file_extension.hpp" // for the registry (routing/disambiguation tests)
#include "otterbrix/parser/grammar_extention/external_node.hpp" // transform target
#include "s3_ast.hpp"
#include "s3_extension.hpp"

using namespace components::sql::parser;

namespace {

bool ceq(const char* a, const char* b) { return a != nullptr && b != nullptr && std::strcmp(a, b) == 0; }

const s3_ext::s3_stmt* s3_parse_ok(std::pmr::memory_resource* r, const std::string& sql) {
    auto res = s3_ext::parse(r, sql);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value() != NIL);
    const auto* stmt = extension_payload<s3_ext::s3_stmt>(res.value(), "s3");
    REQUIRE(stmt != nullptr);
    return stmt;
}

parser_extension_registry_t make_registry() {
    parser_extension_registry_t registry;
    REQUIRE_FALSE(registry.add(make_s3_extension()).has_error());
    REQUIRE_FALSE(registry.add(make_file_extension()).has_error());
    return registry;
}

} // namespace

// ── parse stage ─────────────────────────────────────────────────────────────

// All SQL strings below end with a trailing ';' — the canonical form a
// SQL-script driver (psql -f, the demo's run-queries.sh) sends. Grammar
// tolerates it via opt_semicolon (s3_gram.y); without ';' is exercised by
// the no-terminator cases further down.

TEST_CASE("s3: CREATE EXTERNAL TABLE is parsed into the AST") {
    std::pmr::monotonic_buffer_resource arena;
    const auto* s = s3_parse_ok(&arena,
                                "CREATE EXTERNAL TABLE s3.trades WITH ("
                                "  s3_alias = 'my_s3_alias',"
                                "  location = 's3://bucket/data.parquet',"
                                "  format   = 'parquet' );");
    CHECK(s->kind == s3_ext::s3_stmt_kind::create_external_table);
    CHECK(ceq(s->db, "s3"));
    CHECK(ceq(s->table, "trades"));
    CHECK(ceq(s->location, "s3://bucket/data.parquet"));
    CHECK(ceq(s->s3_alias, "my_s3_alias"));
    CHECK(ceq(s->format, "parquet"));
    CHECK(s->inner_sql == nullptr);
}

TEST_CASE("s3: COPY (SELECT ...) TO captures the inner query and target") {
    std::pmr::monotonic_buffer_resource arena;
    const auto* s = s3_parse_ok(&arena,
                                "COPY (SELECT * FROM s3.trades) TO 's3://bucket/trades2.parquet'"
                                " WITH ( s3_alias = 'my_s3_alias', format = 'parquet' );");
    CHECK(s->kind == s3_ext::s3_stmt_kind::copy_to);
    CHECK(ceq(s->location, "s3://bucket/trades2.parquet"));
    CHECK(ceq(s->s3_alias, "my_s3_alias"));
    CHECK(ceq(s->format, "parquet"));
    CHECK(ceq(s->inner_sql, "SELECT * FROM s3.trades"));
}

TEST_CASE("s3: COPY inner query survives a ')' inside a string literal") {
    std::pmr::monotonic_buffer_resource arena;
    const auto* s = s3_parse_ok(&arena,
                                "COPY (SELECT * FROM s3.trades WHERE name = 'a)b') TO 's3://b/o.parquet'"
                                " WITH ( format = 'csv' );");
    CHECK(s->kind == s3_ext::s3_stmt_kind::copy_to);
    CHECK(ceq(s->inner_sql, "SELECT * FROM s3.trades WHERE name = 'a)b'"));
    CHECK(ceq(s->location, "s3://b/o.parquet"));
    CHECK(ceq(s->format, "csv"));
}

TEST_CASE("s3: COPY requires the WITH (...) clause") {
    std::pmr::monotonic_buffer_resource arena;
    // WITH is mandatory in our COPY syntax — without it the statement is rejected
    // (and the core parser would claim a bare COPY ... TO as its own CopyStmt).
    auto res = s3_ext::parse(&arena, "COPY (SELECT * FROM s3.trades) TO 's3://b/o.parquet';");
    CHECK(res.has_error());
}

TEST_CASE("s3: an omitted format option resolves to nullptr") {
    std::pmr::monotonic_buffer_resource arena;
    const auto* s = s3_parse_ok(&arena, "CREATE EXTERNAL TABLE s3.t WITH (s3_alias='a', location='s3://b/d.parquet');");
    CHECK(s->format == nullptr);
    CHECK(ceq(s->s3_alias, "a"));
}

TEST_CASE("s3: a malformed statement is claimed as an error") {
    std::pmr::monotonic_buffer_resource arena;
    auto res = s3_ext::parse(&arena, "CREATE EXTERNAL TABLE s3.t WITH (location=);");
    CHECK(res.has_error());
}

TEST_CASE("s3: trailing ';' is optional — no-terminator form also parses") {
    // mysql.connector / psycopg2 strip the terminator before send, so the
    // grammar must accept both forms; opt_semicolon → ( /*empty*/ | ';' ).
    std::pmr::monotonic_buffer_resource arena;
    const auto* c = s3_parse_ok(
        &arena,
        "CREATE EXTERNAL TABLE s3.t WITH (s3_alias='a', location='s3://b/d.parquet', format='parquet')");
    CHECK(c->kind == s3_ext::s3_stmt_kind::create_external_table);

    const auto* cp = s3_parse_ok(
        &arena,
        "COPY (SELECT * FROM s3.t) TO 's3://b/out.csv' WITH (s3_alias='a', format='csv')");
    CHECK(cp->kind == s3_ext::s3_stmt_kind::copy_to);
}

TEST_CASE("s3: core SQL and local-path statements are not claimed") {
    std::pmr::monotonic_buffer_resource arena;

    auto plain = s3_ext::parse(&arena, "SELECT 1");
    CHECK_FALSE(plain.has_error());
    CHECK(plain.value() == NIL);

    // a local-path location belongs to the `file` extension
    auto local = s3_ext::parse(&arena, "CREATE EXTERNAL TABLE s3.t WITH (location='/data/d.parquet');");
    CHECK_FALSE(local.has_error());
    CHECK(local.value() == NIL);
}

// ── registry routing through raw_parser ─────────────────────────────────────

TEST_CASE("s3 registry: an s3 statement is routed to the s3 extension") {
    std::pmr::monotonic_buffer_resource arena;
    auto registry = make_registry();
    auto* tree = raw_parser(&arena, "CREATE EXTERNAL TABLE s3.trades WITH (location='s3://b/d.parquet');", registry);
    REQUIRE(tree != NIL);
    auto* node = reinterpret_cast<Node*>(linitial(tree));
    REQUIRE(nodeTag(node) == T_ExtensionNode);
    CHECK(extension_payload<s3_ext::s3_stmt>(tree, "s3") != nullptr);
    CHECK(extension_payload<file_ext::file_stmt>(tree, "file") == nullptr);
}

TEST_CASE("s3 registry: core SQL is claimed by neither extension") {
    std::pmr::monotonic_buffer_resource arena;
    auto registry = make_registry();
    auto* tree = raw_parser(&arena, "SELECT 1", registry);
    REQUIRE(tree != NIL);
    CHECK(nodeTag(reinterpret_cast<Node*>(linitial(tree))) == T_SelectStmt);
}

TEST_CASE("s3 registry: a malformed external statement surfaces a parser error") {
    std::pmr::monotonic_buffer_resource arena;
    auto registry = make_registry();
    CHECK_THROWS_AS(raw_parser(&arena, "CREATE EXTERNAL TABLE s3.t WITH (location=);", registry), parser_exception_t);
}

// ── transform stage (lowers to an external_node_t the Scheduler routes) ──────

TEST_CASE("s3 transform: ExtensionNode lowers to an external_node_t") {
    std::pmr::monotonic_buffer_resource arena;
    auto registry = make_registry();
    auto* node = reinterpret_cast<Node*>(linitial(
        raw_parser(&arena, "CREATE EXTERNAL TABLE s3.t WITH (s3_alias='a', location='s3://b/d.parquet');", registry)));
    REQUIRE(nodeTag(node) == T_ExtensionNode);

    components::sql::transform::transformer tr(&arena, nullptr, &registry);
    auto result = tr.transform(*node);
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.node_ptr() != nullptr);
    auto* ext = dynamic_cast<otterstax::external::external_node_t*>(result.node_ptr().get());
    REQUIRE(ext != nullptr);
    CHECK(ext->op() == otterstax::external::external_op_t::create_external_table);
    CHECK(ext->is_s3());
    CHECK(ext->object_path() == "b/d.parquet");
}
