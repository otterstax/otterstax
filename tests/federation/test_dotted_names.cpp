// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// catalog::split_dotted_names: a quoted "c.ns" qualifier is the name c.ns; nothing else of the text moves.

#include "catalog/dotted_names.hpp"

#include <catch2/catch_all.hpp>

#include <string>
#include <vector>

namespace cat = otterstax::catalog;

namespace {
    std::string split(const std::string& sql) {
        static const std::vector<cat::configured_connection_t> connections{
            {"pg", backend_type_t::PostgreSQL, "shop_db", "shop", "", nullptr},
            {"my", backend_type_t::MySQL, "bill", "", "", nullptr}};
        return cat::split_dotted_names(sql, connections);
    }
} // namespace

TEST_CASE("dotted names: a quoted connection.namespace qualifier is split") {
    CHECK(split(R"(SELECT * FROM "pg.public"."items")") == R"(SELECT * FROM "pg"."public"."items")");
    CHECK(split(R"(SELECT * FROM "pg.public".items)") == R"(SELECT * FROM "pg"."public".items)");
    CHECK(split(R"(SELECT "pg.public".items.id FROM "pg.public".items)") ==
          R"(SELECT "pg"."public".items.id FROM "pg"."public".items)");
    CHECK(split(R"(INSERT INTO "my.bill"."orders" (id) VALUES (1))") ==
          R"(INSERT INTO "my"."bill"."orders" (id) VALUES (1))");
    CHECK(split(R"(COPY "pg.public"."items" (a, b) FROM STDIN)") == R"(COPY "pg"."public"."items" (a, b) FROM STDIN)");
    // local is the first part of the clients' own tables.
    CHECK(split(R"(SELECT * FROM "local.eg".regions)") == R"(SELECT * FROM "local"."eg".regions)");
    // A blank or a comment before the dot.
    CHECK(split(R"(SELECT * FROM "pg.public" . items)") == R"(SELECT * FROM "pg"."public" . items)");
    CHECK(split("SELECT * FROM \"pg.public\" /* x */ .items") == "SELECT * FROM \"pg\".\"public\" /* x */ .items");
    // Three segments in one identifier.
    CHECK(split(R"(SELECT "pg.public.items".id FROM pg.public.items)") ==
          R"(SELECT "pg"."public"."items".id FROM pg.public.items)");
}

TEST_CASE("dotted names: everything else is the text as it was") {
    const std::vector<std::string> unchanged{
        // Not a qualifier: an alias, a schema's own name, a SET value.
        R"(SELECT sum(id) AS "pg.total" FROM pg.public.items)",
        R"(CREATE SCHEMA "pg.x")",
        R"(SELECT id FROM t ORDER BY "pg.total")",
        // The first segment names no connection, or in another case (a quoted name is case-sensitive).
        R"(SELECT * FROM "nope.public".items)",
        R"(SELECT * FROM "PG.public".items)",
        R"(SELECT * FROM "pgx.public".items)",
        // An empty segment, an escaped quote.
        R"(SELECT * FROM "pg..x".items)",
        R"(SELECT * FROM "pg.".items)",
        R"(SELECT * FROM "pg.a""b".items)",
        // Literals and comments.
        R"(SELECT '"pg.public".items')",
        R"(SELECT 'it''s "pg.public".x')",
        R"(SELECT E'\' "pg.public".x')",
        R"(SELECT $$ "pg.public".x $$)",
        R"(SELECT $q$ "pg.public".x $q$)",
        "SELECT 1 -- \"pg.public\".x",
        "SELECT /* /* \"pg.public\".x */ \"pg.public\".x */ 1",
        R"(SELECT U&"pg.public".x FROM t)",
        // No quote at all.
        "SELECT id FROM pg.public.items WHERE id = $1",
        // Unterminated: the engine refuses it.
        R"(SELECT * FROM "pg.public)",
    };
    for (const auto& sql : unchanged) {
        INFO(sql);
        CHECK(split(sql) == sql);
    }
}

TEST_CASE("dotted names: a literal before the name does not hide it") {
    CHECK(split(R"(SELECT 'a"b', $1, $$x$$ FROM "pg.public".items)") ==
          R"(SELECT 'a"b', $1, $$x$$ FROM "pg"."public".items)");
    CHECK(split(R"(SELECT e'it\'s' FROM "pg.public".items)") == R"(SELECT e'it\'s' FROM "pg"."public".items)");
    CHECK(split(R"(SELECT a$b FROM "pg.public".items)") == R"(SELECT a$b FROM "pg"."public".items)");
}

// A host function's call — bare or pg_catalog's — is written local.public.f(): otterbrix's catalog cannot hold a
// host's function in pg_catalog, and a call naming local.public reaches it from any place.
TEST_CASE("host calls: version() and pg_catalog.version() are written local.public.version()") {
    CHECK(cat::qualify_host_calls("select pg_catalog.version()") == "select local.public.version()");
    CHECK(cat::qualify_host_calls("SELECT version()") == "SELECT local.public.version()");
    // pg_table_is_visible: the sub-query over the session's place, its argument kept whole (nested parentheses).
    const auto visible = cat::qualify_host_calls("SELECT 1 FROM t WHERE pg_catalog.pg_table_is_visible((c.oid)) AND x");
    CHECK(visible.rfind("SELECT 1 FROM t WHERE (((c.oid)) IN (SELECT otx_vc.oid FROM pg_catalog.pg_class otx_vc", 0) == 0);
    CHECK(visible.ends_with("otx_vd.datconnection = current_connection()))) AND x"));
    CHECK(cat::qualify_host_calls("SELECT pg_table_is_visible(c.oid").find("IN (") == std::string::npos); // unbalanced
    CHECK(cat::qualify_host_calls("SELECT 'pg_table_is_visible(1)'") == "SELECT 'pg_table_is_visible(1)'");
    CHECK(cat::qualify_host_calls("SELECT VERSION ( )") == "SELECT local.public.VERSION ( )");
    CHECK(cat::qualify_host_calls(R"(SELECT "pg_catalog".version())") == "SELECT local.public.version()");
    CHECK(cat::qualify_host_calls("SELECT PG_CATALOG . version()") == "SELECT local.public.version()");
    CHECK(cat::qualify_host_calls("SELECT upper(version()), version() AS v FROM t") ==
          "SELECT upper(local.public.version()), local.public.version() AS v FROM t");
}

TEST_CASE("host calls: everything else is the text as it was") {
    for (const char* sql : {
             "SELECT 1",
             "SELECT version FROM t",                          // a column
             "SELECT x.version() FROM t",                      // a schema of its own
             "SELECT local.public.version()",                  // already qualified
             "SELECT db.pg_catalog.version()",                 // pg_catalog of a named database
             "SELECT 'version()'",                             // a literal
             "SELECT $$ version() $$",                         // a dollar quote
             "SELECT 1 -- version()",                          // a comment
             "SELECT \"version\"()",                           // a quoted name
             "INSERT INTO version (a) VALUES (1)",             // a table so named
             "CREATE TABLE version (id bigint)",
             "CREATE TABLE IF NOT EXISTS version (id bigint)",
             "CREATE INDEX i ON version (id)",
             "CREATE TABLE t (id bigint REFERENCES version (id))",
             "SELECT versions()",                              // another function
         }) {
        INFO(sql);
        CHECK(cat::qualify_host_calls(sql) == sql);
    }
}
