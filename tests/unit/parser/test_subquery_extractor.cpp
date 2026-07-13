// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/parser/subquery_extractor.hpp"

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <string>

using otterstax::parser::extraction_result_t;
using otterstax::parser::k_stub_prefix;
using otterstax::parser::prepare_sql;

namespace {
    std::string stub_id(int n) { return std::string(k_stub_prefix) + std::to_string(n); }

    // The Postgres grammar wrapped in libotterbrix_sql allocates parse-tree nodes
    // through the supplied memory_resource and never tracks per-call cleanup —
    // it relies on the caller passing an arena that dies at end of parse. Using
    // the default (new_delete) resource leaks the full parse tree (LSAN trips).
    // Mirror parser.cpp:294 and discard a monotonic arena per call; the
    // `extraction_result_t` return is by-value (std::string / std::vector) and
    // does not point into the arena.
    extraction_result_t prep(std::string_view sql) {
        std::pmr::monotonic_buffer_resource arena;
        return prepare_sql(sql, &arena);
    }
} // namespace

TEST_CASE("subquery in FROM") {
    auto r = prep("SELECT p.category, COUNT(*) FROM ("
                  "SELECT product_id FROM mysql.bill.orders WHERE ts >= '2026-04-18'"
                  ") o INNER JOIN pg.shop.products p ON p.id = o.product_id GROUP BY p.category;");

    REQUIRE(r.stubs.size() == 1);
    REQUIRE(r.stubs[0].source_uid == "mysql");
    REQUIRE(r.stubs[0].stub_id == stub_id(0));

    SECTION("source uid first") { REQUIRE(r.modified_sql.find("mysql.subq.subq." + stub_id(0)) != std::string::npos); }

    SECTION("alias survives in the modified SQL") {
        REQUIRE(r.modified_sql.find("ON p.id = o.product_id") != std::string::npos);
    }

    SECTION("preserve original SELECT") {
        REQUIRE(r.stubs[0].raw_sql.find("SELECT product_id") == 0);
        REQUIRE(r.stubs[0].raw_sql.find("ts >= '2026-04-18'") != std::string::npos);
    }

    SECTION("preserve original qualifier") {
        REQUIRE(r.stubs[0].raw_sql.find("FROM mysql.bill.orders") != std::string::npos);
        REQUIRE_FALSE(r.stubs[0].qualifiers.empty());
        REQUIRE(r.stubs[0].qualifiers[0].name.unique_identifier == "mysql");
    }
}

TEST_CASE("subquery in JOIN") {
    auto r = prep("SELECT * FROM pg.shop.customers c INNER JOIN ("
                  "SELECT id FROM mysql.bill.orders WHERE status = 'paid'"
                  ") AS o ON o.id = c.id;");

    REQUIRE(r.stubs.size() == 1);
    REQUIRE(r.stubs[0].source_uid == "mysql");
    REQUIRE(r.modified_sql.find("mysql.subq.subq." + stub_id(0)) != std::string::npos);
    // `AS o ON o.id = c.id` is left untouched in source, only replace the `(...)` slice.
    REQUIRE(r.modified_sql.find("AS o ON o.id = c.id") != std::string::npos);
}

TEST_CASE("subqueries from different sources") {
    auto r = prep("SELECT c.name FROM pg.shop.customers c INNER JOIN ("
                  "SELECT user_id FROM ch.ev.sessions WHERE ts >= '2026-04-12'"
                  ") s ON s.user_id = c.id INNER JOIN ("
                  "SELECT customer_id FROM mysql.bill.orders WHERE ts >= '2026-04-12'"
                  ") o ON o.customer_id = c.id;");

    REQUIRE(r.stubs.size() == 2);
    REQUIRE(r.stubs[0].source_uid == "ch");
    REQUIRE(r.stubs[0].stub_id == stub_id(0));
    REQUIRE(r.stubs[1].source_uid == "mysql");
    REQUIRE(r.stubs[1].stub_id == stub_id(1));

    REQUIRE(r.stubs[0].raw_sql.find("FROM ch.ev.sessions") != std::string::npos);
    REQUIRE(r.stubs[1].raw_sql.find("FROM mysql.bill.orders") != std::string::npos);

    REQUIRE(r.modified_sql.find("ch.subq.subq." + stub_id(0)) != std::string::npos);
    REQUIRE(r.modified_sql.find("mysql.subq.subq." + stub_id(1)) != std::string::npos);
}

TEST_CASE("tricky string literals") {
    SECTION("string with literal parens is not split mid-quote") {
        auto r = prep("SELECT * FROM ("
                      "SELECT name FROM pg.shop.customers WHERE name = 'foo)bar('"
                      ") c;");
        REQUIRE(r.stubs.size() == 1);
        REQUIRE(r.stubs[0].source_uid == "pg");
        REQUIRE(r.stubs[0].raw_sql.find("'foo)bar('") != std::string::npos);
    }

    SECTION("subquery boundary detection") {
        auto r = prep("SELECT * FROM ("
                      "SELECT id FROM mysql.bill.orders WHERE status IN ('paid','shipped')"
                      ") o;");
        REQUIRE(r.stubs.size() == 1);
        REQUIRE(r.stubs[0].raw_sql.find("status IN ('paid','shipped')") != std::string::npos);
    }

    SECTION("escaped single-quote") {
        auto r = prep("SELECT * FROM ("
                      "SELECT id FROM mysql.bill.orders WHERE name = 'O''Brien'"
                      ") o;");
        REQUIRE(r.stubs.size() == 1);
        REQUIRE(r.stubs[0].raw_sql.find("'O''Brien'") != std::string::npos);
    }
}

TEST_CASE("qualifier inside subquery") {
    auto r = prep("SELECT * FROM ("
                  "SELECT id FROM cluster01.mysql.bill.invoices WHERE ts >= '2026-03-20'"
                  ") i;");
    REQUIRE(r.stubs.size() == 1);
    REQUIRE(r.stubs[0].source_uid == "cluster01");
    REQUIRE(r.stubs[0].raw_sql.find("FROM cluster01.mysql.bill.invoices") != std::string::npos);
    REQUIRE_FALSE(r.stubs[0].qualifiers.empty());
    REQUIRE(r.stubs[0].qualifiers[0].name.unique_identifier == "cluster01");
}

TEST_CASE("nested subquery") {
    auto r = prep("SELECT * FROM ("
                  "SELECT id FROM mysql.bill.orders WHERE id IN (SELECT inner_id FROM ch.ev.sessions)"
                  ") o;");
    REQUIRE(r.stubs.size() == 1);
    REQUIRE(r.stubs[0].source_uid == "mysql");
    REQUIRE(r.stubs[0].raw_sql.find("(SELECT inner_id FROM ch.ev.sessions)") != std::string::npos);
}

TEST_CASE("cross-source SELECT") {
    auto r = prep("SELECT * FROM mysql.bill.orders o JOIN pg.shop.products p ON o.product_id = p.id;");
    REQUIRE(r.stubs.empty());
    REQUIRE(r.modified_sql.find("mysql.bill.orders o JOIN pg.shop.products p") != std::string::npos);
}

TEST_CASE("single-source SELECT") {
    auto r = prep("SELECT id, name FROM mysql.bill.schema.orders WHERE ts >= '2026-04-18' LIMIT 10;");
    REQUIRE(r.stubs.empty());
    REQUIRE(r.modified_sql.find("mysql.bill.schema.orders") != std::string::npos);
}

TEST_CASE("DDL/DML untouched") {
    SECTION("CREATE TYPE") {
        auto r = prep("CREATE TYPE tier_t AS ENUM('bronze','silver','gold');");
        REQUIRE(r.stubs.empty());
    }
    SECTION("INSERT") {
        auto r = prep("INSERT INTO mysql.bill.orders VALUES (1, 2, 3);");
        REQUIRE(r.stubs.empty());
    }
    SECTION("UPDATE") {
        auto r = prep("UPDATE mysql.bill.orders SET status = 'paid';");
        REQUIRE(r.stubs.empty());
    }
}

TEST_CASE("local references") {
    auto r = prep("SELECT id FROM demo_warehouses WHERE active = true;");
    REQUIRE(r.stubs.empty());
}

TEST_CASE("qualified + local-table mix") {
    auto r = prep("SELECT c.name, c.addr.* FROM pg.shop.customers c "
                  "INNER JOIN demo_warehouses w ON c.id = w.id;");
    REQUIRE(r.stubs.empty());
}
