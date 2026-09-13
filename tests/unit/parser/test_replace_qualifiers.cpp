// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix/parser/subquery_extractor.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"

#include <catch2/catch_all.hpp>

#include <memory_resource>
#include <string>

namespace {
    // See test_subquery_extractor.cpp:prep — must use a scoped arena, otherwise
    // libotterbrix_sql's parse-tree allocations leak through the default
    // (new_delete) resource and LSAN fails the test.
    otterstax::parser::extraction_result_t prep(const std::string& sql) {
        std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};
        return otterstax::parser::prepare_sql(sql, &arena, std::pmr::new_delete_resource());
    }

    std::string render(const otterstax::parser::subquery_stub_t& stub, backend_type_t backend) {
        auto rendered =
            sql_gen::replace_qualifiers(stub.raw_sql, stub.qualifiers, backend, std::pmr::new_delete_resource());
        REQUIRE_FALSE(rendered.has_error());
        return std::move(rendered.value());
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
    std::pmr::vector<otterstax::parser::qualifier_rewrite_t> empty{std::pmr::new_delete_resource()};
    auto out = sql_gen::replace_qualifiers("SELECT 1", empty, backend_type_t::MySQL, std::pmr::new_delete_resource());
    REQUIRE_FALSE(out.has_error());
    REQUIRE(out.value() == "SELECT 1");
}

// A slot the extractor placed outside the text cannot be rewritten; leaving
// the qualifier in place would send an unknown name to the backend, so the
// rewrite refuses instead of skipping the slot.
TEST_CASE("a qualifier slot outside the SQL text is an error, not a skipped rewrite") {
    auto* resource = std::pmr::new_delete_resource();
    const std::string sql = "SELECT id FROM mysql.bill.schema.orders";
    const qualified_name_t name("mysql", "bill", "schema", "orders");

    SECTION("end past the text") {
        std::pmr::vector<otterstax::parser::qualifier_rewrite_t> quals{resource};
        quals.push_back({15, 100, name});
        auto out = sql_gen::replace_qualifiers(sql, quals, backend_type_t::MySQL, resource);
        REQUIRE(out.has_error());
        REQUIRE(out.error().type == core::error_code_t::invalid_parameter);
    }
    SECTION("negative start") {
        std::pmr::vector<otterstax::parser::qualifier_rewrite_t> quals{resource};
        quals.push_back({-1, 5, name});
        auto out = sql_gen::replace_qualifiers(sql, quals, backend_type_t::MySQL, resource);
        REQUIRE(out.has_error());
        REQUIRE(out.error().type == core::error_code_t::invalid_parameter);
    }
    SECTION("zero length") {
        std::pmr::vector<otterstax::parser::qualifier_rewrite_t> quals{resource};
        quals.push_back({15, 0, name});
        auto out = sql_gen::replace_qualifiers(sql, quals, backend_type_t::MySQL, resource);
        REQUIRE(out.has_error());
        REQUIRE(out.error().type == core::error_code_t::invalid_parameter);
    }
    SECTION("a slot that fits is rewritten") {
        std::pmr::vector<otterstax::parser::qualifier_rewrite_t> quals{resource};
        quals.push_back({15, 24, name});
        auto out = sql_gen::replace_qualifiers(sql, quals, backend_type_t::MySQL, resource);
        REQUIRE_FALSE(out.has_error());
        REQUIRE(out.value() == "SELECT id FROM `bill`.`orders`");
    }
}

// Unknown and Mixed are routing verdicts and Otterbrix is the local engine:
// none of them is a dialect a qualifier could be rewritten into, and an empty
// name has no table to write. Either way the rewrite refuses rather than
// guessing a spelling.
TEST_CASE("a qualifier without a dialect or a name is an error and never a guessed table reference") {
    auto* resource = std::pmr::new_delete_resource();
    const std::string sql = "SELECT id FROM mysql.bill.schema.orders";

    SECTION("backend without a SQL dialect") {
        std::pmr::vector<otterstax::parser::qualifier_rewrite_t> quals{resource};
        quals.push_back({15, 24, qualified_name_t("mysql", "bill", "schema", "orders")});
        for (auto backend : {backend_type_t::Unknown, backend_type_t::Mixed, backend_type_t::Otterbrix}) {
            INFO("backend = " << static_cast<int>(backend));
            auto out = sql_gen::replace_qualifiers(sql, quals, backend, resource);
            REQUIRE(out.has_error());
            REQUIRE(out.error().type == core::error_code_t::invalid_parameter);
        }
    }
    SECTION("empty qualified name") {
        std::pmr::vector<otterstax::parser::qualifier_rewrite_t> quals{resource};
        quals.push_back({15, 24, qualified_name_t{}});
        auto out = sql_gen::replace_qualifiers(sql, quals, backend_type_t::MySQL, resource);
        REQUIRE(out.has_error());
        REQUIRE(out.error().type == core::error_code_t::invalid_parameter);
    }
}
