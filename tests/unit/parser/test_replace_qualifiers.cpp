// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/parser/subquery_extractor.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"

#include <catch2/catch.hpp>

#include <memory_resource>
#include <string>

namespace {
    // See test_subquery_extractor.cpp:prep — must use a scoped arena, otherwise
    // libotterbrix_sql's parse-tree allocations leak through the default
    // (new_delete) resource and LSAN fails the test.
    otterstax::parser::extraction_result_t prep(const std::string& sql) {
        std::pmr::monotonic_buffer_resource arena;
        return otterstax::parser::prepare_sql(sql, &arena);
    }

    std::string render(const otterstax::parser::subquery_stub_t& stub, backend_type_t backend) {
        return sql_gen::replace_qualifiers(stub.raw_sql, stub.qualifiers, backend);
    }
} // namespace

TEST_CASE("MySQL qualifier to db.collection") {
    auto r = prep("SELECT p.category FROM ("
                  "SELECT product_id FROM mysql.bill.schema.orders WHERE ts >= '2026-04-18'"
                  ") o INNER JOIN pg.shop.shop.products p ON p.id = o.product_id;");
    REQUIRE(r.stubs.size() == 1);

    auto out = render(r.stubs[0], backend_type_t::MySQL);
    REQUIRE(out.find("FROM `bill`.`orders`") != std::string::npos);
    REQUIRE(out.find("mysql.bill.schema.orders") == std::string::npos);
    REQUIRE(out.find("schema") == std::string::npos);
    // Date predicate inside the raw_sql is preserved verbatim.
    REQUIRE(out.find("ts >= '2026-04-18'") != std::string::npos);
}

TEST_CASE("PG qualifier to schema.collection") {
    auto r = prep("SELECT * FROM ("
                  "SELECT id FROM pg.shop.shop.customers WHERE tier = 'gold'"
                  ") c;");
    REQUIRE(r.stubs.size() == 1);

    auto out = render(r.stubs[0], backend_type_t::PostgreSQL);
    REQUIRE(out.find("FROM \"shop\".\"customers\"") != std::string::npos);
    REQUIRE(out.find("pg.shop.shop.customers") == std::string::npos);
    REQUIRE(out.find("tier = 'gold'") != std::string::npos);
}

TEST_CASE("CH qualifier to db.collection AND (expr).field") {
    auto r = prep("SELECT * FROM ("
                  "SELECT (s.props).channel FROM ch.ev.schema.sessions s WHERE (s.ship_addr).country = 'DE'"
                  ") s;");
    REQUIRE(r.stubs.size() == 1);

    auto out = render(r.stubs[0], backend_type_t::ClickHouse);
    REQUIRE(out.find("FROM `ev`.`sessions`") != std::string::npos);
    REQUIRE(out.find("ch.ev.schema.sessions") == std::string::npos);
    // CH dialect fixup: `(expr).field` → `expr.field`.
    REQUIRE(out.find("s.props.channel") != std::string::npos);
    REQUIRE(out.find("(s.props).channel") == std::string::npos);
    REQUIRE(out.find("s.ship_addr.country") != std::string::npos);
}

TEST_CASE("3-part — first segment promoted to uid") {
    auto r = prep("SELECT * FROM ("
                  "SELECT id FROM mysql.bill.orders WHERE status = 'paid'"
                  ") o;");
    REQUIRE(r.stubs.size() == 1);

    auto out = render(r.stubs[0], backend_type_t::MySQL);
    REQUIRE(out.find("FROM `bill`.`orders`") != std::string::npos);
    REQUIRE(out.find("mysql.bill.orders") == std::string::npos);
    REQUIRE(out.find("status = 'paid'") != std::string::npos);
}

TEST_CASE("PG (expr).field untouched") {
    auto r = prep("SELECT * FROM ("
                  "SELECT (s.props).channel FROM pg.shop.shop.sessions s"
                  ") s;");
    REQUIRE(r.stubs.size() == 1);

    auto out = render(r.stubs[0], backend_type_t::PostgreSQL);
    REQUIRE(out.find("(s.props).channel") != std::string::npos);
    REQUIRE(out.find("FROM \"shop\".\"sessions\"") != std::string::npos);
}

TEST_CASE("empty qualifiers") {
    std::vector<otterstax::parser::qualifier_rewrite_t> empty;
    std::string out = sql_gen::replace_qualifiers("SELECT 1", empty, backend_type_t::MySQL);
    REQUIRE(out == "SELECT 1");
}
