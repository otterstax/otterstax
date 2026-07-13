// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Unit tests for the file SQL grammar extension
// (otterbrix/parser/grammar_extention/file). They exercise the parse stage
// directly (file_ext::parse), the transform stage, and routing through the
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

#include "file_ast.hpp"
#include "file_extension.hpp"
#include "otterbrix/parser/grammar_extention/external_node.hpp" // transform target
#include "s3_ast.hpp"       // s3_stmt, for the disambiguation assertions
#include "s3_extension.hpp" // for the registry (routing/disambiguation tests)

using namespace components::sql::parser;

namespace {

bool ceq(const char* a, const char* b) { return a != nullptr && b != nullptr && std::strcmp(a, b) == 0; }

const file_ext::file_stmt* file_parse_ok(std::pmr::memory_resource* r, const std::string& sql) {
    auto res = file_ext::parse(r, sql);
    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value() != NIL);
    const auto* stmt = extension_payload<file_ext::file_stmt>(res.value(), "file");
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
// tolerates it via opt_semicolon (file_gram.y); without ';' is exercised by
// the no-terminator cases further down.

TEST_CASE("file: CREATE EXTERNAL TABLE (local path) is parsed into the AST") {
    std::pmr::monotonic_buffer_resource arena;
    const auto* s = file_parse_ok(
        &arena, "CREATE EXTERNAL TABLE file.trades WITH (location='/data/trades.parquet', format='parquet');");
    CHECK(s->kind == file_ext::file_stmt_kind::create_external_table);
    CHECK(ceq(s->db, "file"));
    CHECK(ceq(s->table, "trades"));
    CHECK(ceq(s->location, "/data/trades.parquet"));
    CHECK(ceq(s->format, "parquet"));
}

TEST_CASE("file: COPY (SELECT ...) TO local path captures the inner query") {
    std::pmr::monotonic_buffer_resource arena;
    const auto* s = file_parse_ok(&arena,
                                  "COPY (SELECT * FROM file.trades) TO '/data/out.csv' WITH ( format = 'csv' );");
    CHECK(s->kind == file_ext::file_stmt_kind::copy_to);
    CHECK(ceq(s->location, "/data/out.csv"));
    CHECK(ceq(s->inner_sql, "SELECT * FROM file.trades"));
    CHECK(ceq(s->format, "csv"));
}

TEST_CASE("file: COPY requires the WITH (...) clause") {
    std::pmr::monotonic_buffer_resource arena;
    // WITH is mandatory in our COPY syntax — without it the statement is rejected
    // (and the core parser would claim a bare COPY ... TO as its own CopyStmt).
    auto res = file_ext::parse(&arena, "COPY (SELECT * FROM file.t) TO '/data/out.parquet';");
    CHECK(res.has_error());
}

TEST_CASE("file: an omitted format option resolves to nullptr") {
    std::pmr::monotonic_buffer_resource arena;
    const auto* s = file_parse_ok(&arena, "CREATE EXTERNAL TABLE file.t WITH (location='/data/d.parquet');");
    CHECK(s->format == nullptr);
}

TEST_CASE("file: a malformed statement is claimed as an error") {
    std::pmr::monotonic_buffer_resource arena;
    auto res = file_ext::parse(&arena, "CREATE EXTERNAL TABLE file.t WITH (format=);");
    CHECK(res.has_error());
}

TEST_CASE("file: trailing ';' is optional — no-terminator form also parses") {
    // mysql.connector / psycopg2 strip the terminator before send, so the
    // grammar must accept both forms; opt_semicolon → ( /*empty*/ | ';' ).
    std::pmr::monotonic_buffer_resource arena;
    const auto* c = file_parse_ok(
        &arena, "CREATE EXTERNAL TABLE file.t WITH (location='/data/d.parquet', format='parquet')");
    CHECK(c->kind == file_ext::file_stmt_kind::create_external_table);

    const auto* cp = file_parse_ok(
        &arena, "COPY (SELECT * FROM file.t) TO '/data/out.csv' WITH (format = 'csv')");
    CHECK(cp->kind == file_ext::file_stmt_kind::copy_to);
}

TEST_CASE("file: core SQL and s3:// statements are not claimed") {
    std::pmr::monotonic_buffer_resource arena;

    auto plain = file_ext::parse(&arena, "SELECT 1");
    CHECK_FALSE(plain.has_error());
    CHECK(plain.value() == NIL);

    // an s3:// location belongs to the `s3` extension
    auto s3 = file_ext::parse(&arena, "CREATE EXTERNAL TABLE file.t WITH (location='s3://b/d.parquet');");
    CHECK_FALSE(s3.has_error());
    CHECK(s3.value() == NIL);
}

// ── registry routing through raw_parser ─────────────────────────────────────

TEST_CASE("file registry: a local-path statement is routed to the file extension") {
    std::pmr::monotonic_buffer_resource arena;
    auto registry = make_registry();
    // The WITH ( ... = ... ) clause is required: the core parser accepts a bare
    // `COPY (SELECT ...) TO 'file'` as its own CopyStmt, so without it the
    // extension would never be consulted.
    auto* tree =
        raw_parser(&arena, "COPY (SELECT * FROM file.t) TO '/data/out.parquet' WITH (format='parquet');", registry);
    REQUIRE(tree != NIL);
    auto* node = reinterpret_cast<Node*>(linitial(tree));
    REQUIRE(nodeTag(node) == T_ExtensionNode);
    CHECK(extension_payload<file_ext::file_stmt>(tree, "file") != nullptr);
    CHECK(extension_payload<s3_ext::s3_stmt>(tree, "s3") == nullptr);
}

// ── transform stage (lowers to an external_node_t the Scheduler routes) ──────

TEST_CASE("file transform: ExtensionNode lowers to an external_node_t") {
    std::pmr::monotonic_buffer_resource arena;
    auto registry = make_registry();
    auto* node = reinterpret_cast<Node*>(
        linitial(raw_parser(&arena, "CREATE EXTERNAL TABLE file.t WITH (location='/data/d.parquet');", registry)));
    REQUIRE(nodeTag(node) == T_ExtensionNode);

    components::sql::transform::transformer tr(&arena, nullptr, &registry);
    auto result = tr.transform(*node);
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.node_ptr() != nullptr);
    auto* ext = dynamic_cast<otterstax::external::external_node_t*>(result.node_ptr().get());
    REQUIRE(ext != nullptr);
    CHECK(ext->op() == otterstax::external::external_op_t::create_external_table);
    CHECK_FALSE(ext->is_s3());
    CHECK(ext->location() == "/data/d.parquet");
}
