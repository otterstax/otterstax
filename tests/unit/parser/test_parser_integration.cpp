// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/parser/name_resolution.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/parser/subquery_extractor.hpp"
#include "otterbrix/schema/schema_utils.hpp"

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
        REQUIRE_FALSE(result.has_error());
        return std::move(result.value());
    }

    // One external slot: the node together with its parser-resolved target.
    struct flat_external_t {
        components::logical_plan::node_ptr node;
        otterstax::names::resolved_target_t target;
    };

    std::vector<flat_external_t> external_nodes_flat(const ParsedQueryDataPtr& parsed) {
        std::vector<flat_external_t> out;
        for (const auto& batch : parsed->otterbrix_params->external_nodes) {
            for (const auto& entry : batch) {
                // Every external slot must carry a plan-node reference.
                REQUIRE(entry.node != nullptr);
                out.push_back(flat_external_t{*entry.node, entry.target});
            }
        }
        return out;
    }

    flat_external_t find_by_uid(const std::vector<flat_external_t>& nodes, const std::string& uid) {
        auto it = std::find_if(nodes.begin(), nodes.end(), [&uid](const auto& n) {
            return n.target.name.unique_identifier == uid;
        });
        return it != nodes.end() ? *it : flat_external_t{};
    }

    bool is_schema_node_with_raw_sql(const components::logical_plan::node_ptr& n) {
        if (!n || n->type() != components::logical_plan::node_type::unused) {
            return false;
        }
        const auto& sn = static_cast<const schema_utils::schema_node_t&>(*n);
        return sn.has_raw_sql();
    }
} // namespace

TEST_CASE("integration: 4-part qualifier in cross-source JOIN") {
    // Scoped arena: libotterbrix_sql's parse tree allocates through this
    // resource and is never explicitly freed — the process-wide default
    // would leak it (LSAN).
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "SELECT * FROM mysql.bill.schema.orders o INNER JOIN pg.shop.shop.products p ON o.product_id = p.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 2);

    auto mysql_node = find_by_uid(nodes, "mysql");
    auto pg_node = find_by_uid(nodes, "pg");
    REQUIRE(mysql_node.node);
    REQUIRE(pg_node.node);

    REQUIRE_FALSE(is_schema_node_with_raw_sql(mysql_node.node));
    REQUIRE_FALSE(is_schema_node_with_raw_sql(pg_node.node));

    REQUIRE(mysql_node.target.name.database == "bill");
    REQUIRE(mysql_node.target.name.collection == "orders");
    REQUIRE(pg_node.target.name.schema == "shop");
    REQUIRE(pg_node.target.name.collection == "products");
}

TEST_CASE("integration: 4-part qualifier untouched") {
    // Scoped arena: libotterbrix_sql's parse tree allocates through this
    // resource and is never explicitly freed — the process-wide default
    // would leak it (LSAN).
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed =
        parse_or_die(parser,
                     "SELECT * FROM uid1.db1.sch1.test1 INNER JOIN uid2.db2.sch2.test2 ON test1.id = test2.id;");
    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 2);

    auto n1 = find_by_uid(nodes, "uid1");
    auto n2 = find_by_uid(nodes, "uid2");
    REQUIRE(n1.node);
    REQUIRE(n2.node);
    REQUIRE(n1.target.name.database == "db1");
    REQUIRE(n1.target.name.schema == "sch1");
    REQUIRE(n1.target.name.collection == "test1");
    REQUIRE(n2.target.name.collection == "test2");
}

TEST_CASE("integration: no external_node") {
    // Scoped arena: libotterbrix_sql's parse tree allocates through this
    // resource and is never explicitly freed — the process-wide default
    // would leak it (LSAN).
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "SELECT id FROM demo_warehouses;");
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 0);
}

// ── DROP routing by target kind ───────────────────────────────────────────────
// One drop_t node carries every DROP kind. Only the kinds whose target may be
// alias-qualified (collection, index) resolve through the name registry and can
// route to a backend; database / type / sequence / view / macro name engine-local
// objects and never produce an external node. Of the local kinds, only
// collection / database / index close the current external batch (mutable).

TEST_CASE("integration: DROP DATABASE is local — no external nodes") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser, "DROP DATABASE db1;");
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 0);
}

TEST_CASE("integration: DROP DATABASE rejects an alias qualifier") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // The engine grammar spells the target as `database_name: ColId` — a single
    // identifier — so an alias-qualified DROP DATABASE cannot parse. This is what
    // makes "database-level DDL is local by construction" true rather than lucky.
    auto result = parser.parse("DROP DATABASE conn1.db1;");
    REQUIRE(result.has_error());
}

TEST_CASE("integration: DROP TABLE keeps its alias-qualified external node") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // A collection-kind DROP with an alias qualifier routes to its backend.
    auto parsed = parse_or_die(parser, "DROP TABLE conn1.db1.public.t;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 1);
    REQUIRE(nodes[0].target.name.unique_identifier == "conn1");
}

TEST_CASE("integration: DROP INDEX with an alias resolves the table and the index") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // The leading parts name the indexed table, the trailing part the index; the
    // external slot carries the table as `name` and the index as `from_name`,
    // both under the connection uid.
    auto parsed = parse_or_die(parser, "DROP INDEX conn1.db1.public.t.idx;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 1);
    REQUIRE(nodes[0].node->type() == components::logical_plan::node_type::drop_t);
    REQUIRE(nodes[0].target.name.unique_identifier == "conn1");
    REQUIRE(nodes[0].target.name.database == "db1");
    REQUIRE(nodes[0].target.name.schema == "public");
    REQUIRE(nodes[0].target.name.collection == "t");
    REQUIRE(nodes[0].target.from_name.unique_identifier == "conn1");
    REQUIRE(nodes[0].target.from_name.database == "db1");
    REQUIRE(nodes[0].target.from_name.collection == "idx");
}

// The FROM / USING source of an UPDATE / DELETE is a child sub-plan of the DML
// node, not a second name on it. Resolving a plain-table source into the slot's
// from_name is what lets the generator write ONE two-table statement. The child
// must then NOT become an external slot of its own: batches are executed
// innermost-first, so a slot there would be fetched and replaced with raw data
// before the DML is generated, and the pushdown could never form.
TEST_CASE("integration: UPDATE ... FROM resolves its source into the DML slot") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "UPDATE conn1.db1.public.orders SET name = 'x' FROM conn1.db1.public.other WHERE orders.id = other.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 1);
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 1);
    REQUIRE(nodes[0].node->type() == components::logical_plan::node_type::update_t);
    REQUIRE(nodes[0].target.name.unique_identifier == "conn1");
    REQUIRE(nodes[0].target.name.collection == "orders");
    REQUIRE(nodes[0].target.from_name.unique_identifier == "conn1");
    REQUIRE(nodes[0].target.from_name.database == "db1");
    REQUIRE(nodes[0].target.from_name.schema == "public");
    REQUIRE(nodes[0].target.from_name.collection == "other");
}

TEST_CASE("integration: DELETE ... USING resolves its source into the DML slot") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(
        parser,
        "DELETE FROM conn1.db1.public.orders USING conn1.db1.public.other WHERE orders.id = other.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 1);
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 1);
    REQUIRE(nodes[0].node->type() == components::logical_plan::node_type::delete_t);
    REQUIRE(nodes[0].target.name.collection == "orders");
    REQUIRE(nodes[0].target.from_name.unique_identifier == "conn1");
    REQUIRE(nodes[0].target.from_name.collection == "other");
}

// A local source resolves too — to a name with no uid. The slot carries it so
// the generator can say the source is not on the target's backend; leaving
// from_name empty there would report the vaguer "not pushed down" instead.
TEST_CASE("integration: UPDATE ... FROM a local table resolves to a uid-less source") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed =
        parse_or_die(parser, "UPDATE conn1.db1.public.orders SET name = 'x' FROM other WHERE orders.id = other.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 1);
    REQUIRE(nodes[0].target.from_name.unique_identifier.empty());
    REQUIRE(nodes[0].target.from_name.collection == "other");
}

// Only a plain table is pushed down. A subquery source is a childless aggregate
// too, but it is a stub the parser still has to swap for the extracted raw SQL,
// and it only reaches the stub through a slot of its own. So from_name stays
// empty and the source keeps its slot — pushing it down would name the
// generated `__otterstax_subq_N` to the backend as if it were a real table.
TEST_CASE("integration: DELETE ... USING a subquery source keeps the source as its own slot") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "DELETE FROM conn1.db1.public.orders USING "
                               "(SELECT id FROM conn1.db1.public.other) s WHERE orders.id = s.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 2);
    auto del = find_by_uid(nodes, "conn1");
    REQUIRE(del.node);
    REQUIRE(del.node->type() == components::logical_plan::node_type::delete_t);
    REQUIRE(del.target.from_name.collection.empty());
}

TEST_CASE("integration: DROP TABLE without an alias is local and closes the batch") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // Control for the local-kind cases below: a mutable local DROP produces no
    // external node but still opens a fresh batch, which survives as a single
    // empty batch after the trailing one is trimmed.
    auto parsed = parse_or_die(parser, "DROP TABLE t;");
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 0);
    REQUIRE(parsed->otterbrix_params->external_nodes.size() == 1);
    REQUIRE(parsed->otterbrix_params->external_nodes[0].empty());
}

TEST_CASE("integration: DROP VIEW / SEQUENCE / TYPE / FUNCTION are local and not mutable") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    const std::string sql = GENERATE(Catch::Generators::as<std::string>{},
                                     "DROP VIEW v;",
                                     "DROP SEQUENCE s;",
                                     "DROP TYPE tier_t;",
                                     "DROP FUNCTION f();");
    CAPTURE(sql);
    auto parsed = parse_or_die(parser, sql);
    REQUIRE(parsed->tag == NodeTag::T_DropStmt);
    REQUIRE(parsed->otterbrix_params->external_nodes_count == 0);
    // No batch boundary either: an engine-local object cannot change what a
    // backend batch observes.
    REQUIRE(parsed->otterbrix_params->external_nodes.empty());
}

TEST_CASE("integration: DROP of several objects in one statement is rejected") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // The transformer lowers only the first object; accepting the list would
    // silently leave the rest in place.
    auto result = parser.parse("DROP TABLE a, b;");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::unimplemented_yet);
}

// ── One statement per parse() ─────────────────────────────────────────────────

TEST_CASE("integration: several statements in one query are rejected") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

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
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto result = parser.parse("EXPLAIN SELECT id FROM orders;");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::unimplemented_yet);
}

TEST_CASE("integration: EXPLAIN DELETE is rejected, not executed") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto result = parser.parse("EXPLAIN DELETE FROM orders WHERE id = 1;");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::unimplemented_yet);
}

// ── Transaction control ───────────────────────────────────────────────────────

TEST_CASE("integration: SAVEPOINT / ROLLBACK TO / RELEASE are refused") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

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
    auto* resource = &arena;
    GreenplumParser parser(resource);

    // Control for the refusal above: plain transaction control is lowered.
    const std::string sql = GENERATE(Catch::Generators::as<std::string>{}, "BEGIN;", "COMMIT;", "ROLLBACK;");
    CAPTURE(sql);
    auto result = parser.parse(sql);
    INFO("error: " << result.error().what.c_str());
    REQUIRE_FALSE(result.has_error());
}

// ── Extension registration ────────────────────────────────────────────────────

namespace {
    components::sql::parser::parse_extension_result_t claim_nothing(std::pmr::memory_resource*,
                                                                    const std::string&) {
        return NIL;
    }
} // namespace

TEST_CASE("integration: a failed extension registration fails every parse()") {
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;

    // A seed entry named like a built-in extension makes the built-in
    // registration fail; the parser must not come up with that extension
    // silently missing.
    components::sql::parser::parser_extension_registry_t seed;
    REQUIRE_FALSE(seed.add(components::sql::parser::parser_extension_t{"s3", &claim_nothing}).has_error());
    GreenplumParser parser(resource, std::move(seed));

    auto result = parser.parse("SELECT id FROM orders;");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::already_exists);
}

TEST_CASE("integration: derived table into schema_node") {
    // Scoped arena: libotterbrix_sql's parse tree allocates through this
    // resource and is never explicitly freed — the process-wide default
    // would leak it (LSAN).
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "SELECT p.category, COUNT(*) FROM ("
                               "SELECT product_id FROM mysql.bill.schema.orders WHERE ts >= '2026-04-18'"
                               ") o INNER JOIN pg.shop.shop.products p ON p.id = o.product_id "
                               "GROUP BY p.category;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 2);

    auto mysql_stub = find_by_uid(nodes, "mysql");
    auto pg_real = find_by_uid(nodes, "pg");
    REQUIRE(mysql_stub.node);
    REQUIRE(pg_real.node);

    REQUIRE(is_schema_node_with_raw_sql(mysql_stub.node));
    const auto& raw = static_cast<const schema_utils::schema_node_t&>(*mysql_stub.node).raw_sql();
    REQUIRE(raw.find("FROM mysql.bill.schema.orders") != std::string::npos);
    REQUIRE(raw.find("ts >= '2026-04-18'") != std::string::npos);
    REQUIRE_FALSE(static_cast<const schema_utils::schema_node_t&>(*mysql_stub.node).qualifiers().empty());

    // The schema node itself carries the resolved stub name as well.
    REQUIRE(static_cast<const schema_utils::schema_node_t&>(*mysql_stub.node).name() == mysql_stub.target.name);

    REQUIRE_FALSE(is_schema_node_with_raw_sql(pg_real.node));
    REQUIRE(pg_real.target.name.schema == "shop");
}

TEST_CASE("integration: qualified + local untouched") {
    // Scoped arena: libotterbrix_sql's parse tree allocates through this
    // resource and is never explicitly freed — the process-wide default
    // would leak it (LSAN).
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed = parse_or_die(parser,
                               "SELECT c.name FROM pg.shop.shop.customers c "
                               "INNER JOIN demo_warehouses w ON c.id = w.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 1);
    REQUIRE(nodes[0].target.name.unique_identifier == "pg");

    // It's a real aggregate, NOT a wrapped raw_sql — proves wrap was skipped.
    REQUIRE_FALSE(is_schema_node_with_raw_sql(nodes[0].node));
    REQUIRE(nodes[0].target.name.schema == "shop");
    REQUIRE(nodes[0].target.name.collection == "customers");
}

TEST_CASE("integration: cross-source subqueries") {
    // Scoped arena: libotterbrix_sql's parse tree allocates through this
    // resource and is never explicitly freed — the process-wide default
    // would leak it (LSAN).
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
    auto* resource = &arena;
    GreenplumParser parser(resource);

    auto parsed =
        parse_or_die(parser,
                     "SELECT c.name FROM pg.shop.shop.customers c "
                     "INNER JOIN (SELECT user_id FROM ch.ev.schema.sessions WHERE ts >= '2026-04-12') s "
                     "  ON s.user_id = c.id "
                     "INNER JOIN (SELECT customer_id FROM mysql.bill.schema.orders WHERE ts >= '2026-04-12') o "
                     "  ON o.customer_id = c.id;");

    auto nodes = external_nodes_flat(parsed);
    REQUIRE(nodes.size() == 3);

    auto pg_node = find_by_uid(nodes, "pg");
    auto ch_stub = find_by_uid(nodes, "ch");
    auto mysql_stub = find_by_uid(nodes, "mysql");
    REQUIRE(pg_node.node);
    REQUIRE(ch_stub.node);
    REQUIRE(mysql_stub.node);

    REQUIRE_FALSE(is_schema_node_with_raw_sql(pg_node.node));

    REQUIRE(is_schema_node_with_raw_sql(ch_stub.node));
    REQUIRE(is_schema_node_with_raw_sql(mysql_stub.node));

    const auto& ch_raw = static_cast<const schema_utils::schema_node_t&>(*ch_stub.node).raw_sql();
    const auto& mysql_raw = static_cast<const schema_utils::schema_node_t&>(*mysql_stub.node).raw_sql();
    REQUIRE(ch_raw.find("FROM ch.ev.schema.sessions") != std::string::npos);
    REQUIRE(mysql_raw.find("FROM mysql.bill.schema.orders") != std::string::npos);
}
