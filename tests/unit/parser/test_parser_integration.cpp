// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// GreenplumParser over the engine's parser and transformer: a name is carried on its plan node
// whole (a remote name keeps its uid and schema slots; the engine's host hooks resolve it), a
// table-level DDL statement keeps its target as written, flattened sub-queries travel with the
// statement, and the statements the pipeline cannot run are refused before a plan exists.

#include "otterbrix/parser/parser.hpp"

#include <catch2/catch_all.hpp>
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node.hpp>
#include <components/sql/parser/extension.hpp>
#include <components/sql/parser/pg_std_list.h>
#include <core/result_wrapper.hpp>

#include <algorithm>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

namespace {
    ParsedQueryDataPtr parse_or_die(GreenplumParser& parser, const std::string& sql) {
        auto result = parser.parse(sql);
        INFO(sql << ": " << (result.has_error() ? result.error().what.c_str() : "ok"));
        REQUIRE_FALSE(result.has_error());
        return std::move(result.value());
    }

    // Every relation the plan reads (aggregate nodes naming one), in plan order.
    void collect_reads(const components::logical_plan::node_ptr& node, std::vector<qualified_name_t>& out) {
        if (!node) {
            return;
        }
        if (node->type() == components::logical_plan::node_type::aggregate_t && !node->target().collection.t.empty()) {
            out.push_back(node->target());
        }
        for (const auto& child : node->children()) {
            collect_reads(child, out);
        }
    }

    std::vector<qualified_name_t> reads_of(const ParsedQueryDataPtr& parsed) {
        std::vector<qualified_name_t> out;
        for (const auto& sub_query : parsed->otterbrix_params->sub_queries) {
            collect_reads(sub_query, out);
        }
        collect_reads(parsed->otterbrix_params->node, out);
        return out;
    }

    const qualified_name_t* read_named(const std::vector<qualified_name_t>& reads, const std::string& table) {
        auto it = std::find_if(reads.begin(), reads.end(), [&table](const qualified_name_t& name) {
            return name.collection.t == table;
        });
        return it == reads.end() ? nullptr : &*it;
    }

    void require_name(const qualified_name_t* name,
                      const std::string& uid,
                      const std::string& database,
                      const std::string& schema,
                      const std::string& table) {
        REQUIRE(name != nullptr);
        CHECK(name->unique_identifier.t == uid);
        CHECK(name->database.t == database);
        CHECK(name->schema.t == schema);
        CHECK(name->collection.t == table);
    }
} // namespace

// ── Names on the plan ─────────────────────────────────────────────────────────

TEST_CASE("integration: a 4-part name keeps every slot on its scan") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    auto parsed = parse_or_die(
        parser,
        "SELECT * FROM mysql.bill.schema.orders o INNER JOIN pg.shop.shop.products p ON o.product_id = p.id;");
    const auto reads = reads_of(parsed);
    require_name(read_named(reads, "orders"), "mysql", "bill", "schema", "orders");
    require_name(read_named(reads, "products"), "pg", "shop", "shop", "products");
}

TEST_CASE("integration: a 3-part name reads as database.schema.table") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    // The federation reads the database slot of uid.db.table as the uid.
    auto parsed = parse_or_die(parser, "SELECT id FROM sales.bill.orders;");
    require_name(read_named(reads_of(parsed), "orders"), "", "sales", "bill", "orders");
}

TEST_CASE("integration: a local name has no uid") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    auto parsed = parse_or_die(parser, "SELECT c.name FROM pg.shop.shop.customers c INNER JOIN loc.warehouses w ON c.id = w.id;");
    const auto reads = reads_of(parsed);
    require_name(read_named(reads, "customers"), "pg", "shop", "shop", "customers");
    require_name(read_named(reads, "warehouses"), "", "loc", "", "warehouses");
}

TEST_CASE("integration: a derived table over a remote table stays in the plan") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    auto parsed = parse_or_die(parser,
                               "SELECT p.category, COUNT(*) FROM ("
                               "SELECT product_id FROM mysql.bill.schema.orders WHERE ts >= '2026-04-18'"
                               ") o INNER JOIN pg.shop.shop.products p ON p.id = o.product_id "
                               "GROUP BY p.category;");
    const auto reads = reads_of(parsed);
    require_name(read_named(reads, "orders"), "mysql", "bill", "schema", "orders");
    require_name(read_named(reads, "products"), "pg", "shop", "shop", "products");
    CHECK(parsed->sql.find("2026-04-18") != std::string::npos);
}

TEST_CASE("integration: the parsed statement carries its text") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    const std::string sql = "SELECT id FROM demo_warehouses;";
    auto parsed = parse_or_die(parser, sql);
    CHECK(parsed->sql == sql);
    CHECK(parsed->ddl_target.collection.t.empty());
}

// ── Table-level DDL keeps its written target ──────────────────────────────────
// The transformer keeps only database and table on a DDL node; a DDL statement never reaches the
// engine's host hooks, so the session tells a remote target from a local one by this name.

TEST_CASE("integration: DROP TABLE keeps its target as written") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    auto four = parse_or_die(parser, "DROP TABLE conn1.db1.public.t;");
    require_name(&four->ddl_target, "conn1", "db1", "public", "t");
    auto three = parse_or_die(parser, "DROP TABLE conn1.db1.t;");
    require_name(&three->ddl_target, "", "conn1", "db1", "t");
    auto local = parse_or_die(parser, "DROP TABLE db.t;");
    require_name(&local->ddl_target, "", "db", "", "t");
}

TEST_CASE("integration: CREATE TABLE and CREATE INDEX keep their target as written") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    auto created = parse_or_die(parser, "CREATE TABLE mysql.public.public.t (id bigint);");
    require_name(&created->ddl_target, "mysql", "public", "public", "t");
    auto local = parse_or_die(parser, "CREATE TABLE t (id bigint);");
    require_name(&local->ddl_target, "", "", "", "t");
    auto index = parse_or_die(parser, "CREATE INDEX i ON conn1.db1.public.t (id);");
    require_name(&index->ddl_target, "conn1", "db1", "public", "t");
}

TEST_CASE("integration: DROP DATABASE rejects an alias qualifier") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    // The engine grammar spells the target as `database_name: ColId` — a single
    // identifier — so an alias-qualified DROP DATABASE cannot parse.
    auto result = parser.parse("DROP DATABASE conn1.db1;");
    REQUIRE(result.has_error());
    auto local = parse_or_die(parser, "DROP DATABASE db1;");
    CHECK(local->ddl_target.collection.t.empty());
}

TEST_CASE("integration: DROP of several objects in one statement is rejected") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    // The transformer lowers only the first object; accepting the list would
    // silently leave the rest in place.
    auto result = parser.parse("DROP TABLE a, b;");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("integration: CREATE TABLE AS SELECT over a remote table is refused") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    auto result = parser.parse("CREATE TABLE t AS SELECT * FROM mysql.public.t;");
    REQUIRE(result.has_error());
    CHECK(std::string{result.error().what.c_str()}.find("CREATE TABLE AS") != std::string::npos);
}

// ── One statement per parse() ─────────────────────────────────────────────────

TEST_CASE("integration: several statements in one query are rejected") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    // parse() builds exactly one plan; a trailing statement must fail loudly
    // rather than vanish.
    auto result = parser.parse("SELECT 1; DELETE FROM orders WHERE id = 1;");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::unimplemented_yet);
}

// ── EXPLAIN ───────────────────────────────────────────────────────────────────
// The transformer lowers EXPLAIN to the inner statement's plan and keeps the
// explain mode on a plan this pipeline discards, so an accepted EXPLAIN would
// execute the inner statement. parse() must reject it before that.

TEST_CASE("integration: EXPLAIN SELECT is rejected") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    auto result = parser.parse("EXPLAIN SELECT id FROM orders;");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("integration: EXPLAIN DELETE is rejected, not executed") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    auto result = parser.parse("EXPLAIN DELETE FROM orders WHERE id = 1;");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::unimplemented_yet);
}

// ── Transaction control ───────────────────────────────────────────────────────

TEST_CASE("integration: SAVEPOINT / ROLLBACK TO / RELEASE are refused") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    const std::string sql = GENERATE(Catch::Generators::as<std::string>{},
                                     "SAVEPOINT sp1;",
                                     "ROLLBACK TO SAVEPOINT sp1;",
                                     "ROLLBACK TO sp1;",
                                     "RELEASE SAVEPOINT sp1;",
                                     "RELEASE sp1;");
    CAPTURE(sql);
    // The transformer lowers none of the savepoint statements to a plan node.
    auto result = parser.parse(sql);
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::unimplemented_yet);
    REQUIRE(std::string{result.error().what.c_str()} == "unsupported transaction statement");
}

TEST_CASE("integration: BEGIN / COMMIT / ROLLBACK parse") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    // Control for the refusal above: plain transaction control is lowered.
    const std::string sql = GENERATE(Catch::Generators::as<std::string>{}, "BEGIN;", "COMMIT;", "ROLLBACK;");
    CAPTURE(sql);
    auto result = parser.parse(sql);
    INFO("error: " << result.error().what.c_str());
    REQUIRE_FALSE(result.has_error());
}

// ── Flattened sub-queries travel with the statement ───────────────────────────

TEST_CASE("integration: IN sub-query over local tables is carried with the statement") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    auto parsed = parse_or_die(parser, "SELECT * FROM sq.t WHERE id IN (SELECT id FROM sq.u);");
    const auto& statement = *parsed->otterbrix_params;
    REQUIRE(statement.sub_queries.size() == 1);
    REQUIRE(statement.sub_query_results.size() == 1);
    CHECK(statement.sub_queries.front()->target().collection.t == "u");
    CHECK(statement.node->target().collection.t == "t");
}

TEST_CASE("integration: a statement without a sub-query carries none") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    auto parsed = parse_or_die(parser, "SELECT * FROM sq.t WHERE id IN (1, 2);");
    CHECK(parsed->otterbrix_params->sub_queries.empty());
    CHECK(parsed->otterbrix_params->sub_query_results.empty());
}

// A sub-query over a remote table is an ordinary sub-query now: the engine runs it, resolving the
// remote name through the federation.
TEST_CASE("integration: a sub-query next to a remote table is carried with the statement") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);
    for (const char* sql : {"SELECT * FROM mysql.db.sch.t WHERE id IN (SELECT id FROM sq.u);",
                            "SELECT * FROM sq.t WHERE id IN (SELECT id FROM mysql.db.sch.u);",
                            "SELECT * FROM sq.t WHERE EXISTS (SELECT id FROM mysql.db.sch.u);",
                            "SELECT * FROM sq.t WHERE id = (SELECT max(id) FROM mysql.db.sch.u);"}) {
        auto parsed = parse_or_die(parser, sql);
        INFO(sql);
        CHECK_FALSE(parsed->otterbrix_params->sub_queries.empty());
        const auto reads = reads_of(parsed);
        CHECK(std::any_of(reads.begin(), reads.end(), [](const qualified_name_t& name) {
            return name.unique_identifier.t == "mysql";
        }));
    }
}

// The transformer keeps no uid slot on a DML target; the parser records the
// target as written when it has one, which the session runs on its backend.
TEST_CASE("integration: a four-part DML target is kept as written") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    GreenplumParser parser(&arena);

    auto updated = parse_or_die(parser, "UPDATE mysql.bill.schema.orders SET a = 1 WHERE id = 2;");
    require_name(&updated->dml_target, "mysql", "bill", "schema", "orders");
    auto inserted = parse_or_die(parser, "INSERT INTO pg.shop.public.items (id) VALUES (1);");
    require_name(&inserted->dml_target, "pg", "shop", "public", "items");
    auto deleted = parse_or_die(parser, "DELETE FROM ch.ev.schema.events WHERE id = 1;");
    require_name(&deleted->dml_target, "ch", "ev", "schema", "events");

    // Three parts read as database.schema.table and reach the federation: nothing recorded.
    auto three = parse_or_die(parser, "UPDATE sales.bill.orders SET a = 1;");
    CHECK(three->dml_target.collection.t.empty());
    auto local = parse_or_die(parser, "DELETE FROM loc.t WHERE id = 1;");
    CHECK(local->dml_target.collection.t.empty());
    auto select = parse_or_die(parser, "SELECT a FROM mysql.bill.schema.orders;");
    CHECK(select->dml_target.collection.t.empty());
}
