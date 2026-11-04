// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// Engine defect D1 over the four-part model: a remote table JOINed with a local composite table (the
// demo's step_3a shape: an ENUM, two composites, bigints) on a struct-field key and on a plain key,
// over an empty and a seeded local table — a build side left with zero chunks once crashed
// operator_join_t. Four separate TEST_CASEs, never SECTIONs: a crash aborts the process and each
// variant must be attributable.

#include "federation_fixture.hpp"

using namespace fedtest;

namespace {

    // pg.shop.public.customers: Ann in Berlin, Bob in Tel Aviv. Local otter.public.warehouses;
    // `seed_rows` adds the single warehouse BER-1 located in Berlin. A correct engine answers
    // `expected_rows`.
    void run_remote_join_over_local_composite(const char* sql, bool seed_rows, std::size_t expected_rows) {
        fed_stack_t stack({{"pg", backend_type_t::PostgreSQL, "shop", "public"}});
        stack.backend("pg").add_table({"shop",
                                       "public",
                                       "customers",
                                       {column("name", logical_type::STRING_LITERAL),
                                        column("addr_city", logical_type::STRING_LITERAL)},
                                       {{std::string{"Ann"}, std::string{"Berlin"}},
                                        {std::string{"Bob"}, std::string{"Tel Aviv"}}}});
        stack.open();
        for (const char* ddl : {"CREATE DATABASE otter;",
                                "CREATE TYPE tier_t AS ENUM('bronze','silver','gold');",
                                "CREATE TYPE address_t AS (city STRING, country STRING, zip STRING);",
                                "CREATE TYPE spec_t AS (weight_1 INT, weight_2 INT, weight_3 INT, weight_4 INT, "
                                "weight_5 INT, weight_6 INT, primary_barcode STRING, secondary_barcode STRING, "
                                "photo STRING);",
                                "CREATE TABLE otter.public.warehouses (warehouse_id STRING, code STRING, tier tier_t, "
                                "location address_t, spec spec_t, priority_high BIGINT, priority_med BIGINT, "
                                "priority_low BIGINT);"}) {
            static_cast<void>(stack.ok(ddl));
        }
        if (seed_rows) {
            static_cast<void>(stack.ok("INSERT INTO otter.public.warehouses (warehouse_id, code, tier, location, "
                                       "priority_high) VALUES ('w-1', 'BER-1', 'silver', ROW('Berlin','DE','10115'), 1);"));
        }
        auto joined = stack.ok(sql);
        CHECK(rows_of(joined).size() == expected_rows);
    }

    // Struct field access on the JOIN key and the demo's local-side predicate (step_4).
    constexpr const char* d1_struct_key = "SELECT c.name, w.code FROM pg.shop.public.customers c "
                                          "INNER JOIN otter.public.warehouses w ON c.addr_city = (w.location).city "
                                          "WHERE (w.location).country IN ('DE','IL','US');";
    // Plain column on the JOIN key; the same build side and predicate.
    constexpr const char* d1_plain_key = "SELECT c.name, w.code FROM pg.shop.public.customers c "
                                         "INNER JOIN otter.public.warehouses w ON c.addr_city = w.code "
                                         "WHERE (w.location).country IN ('DE','IL','US');";

} // namespace

// No warehouse: the predicate leaves the build side with zero chunks.
TEST_CASE("D1: remote table JOIN local composite table on struct key over empty local table", "[engine-defect-d1]") {
    run_remote_join_over_local_composite(d1_struct_key, false, 0);
}

// BER-1 is located in Berlin, DE: Ann matches, Bob does not.
TEST_CASE("D1: remote table JOIN local composite table on struct key over seeded local table", "[engine-defect-d1]") {
    run_remote_join_over_local_composite(d1_struct_key, true, 1);
}

TEST_CASE("D1: remote table JOIN local composite table on plain key over empty local table", "[engine-defect-d1]") {
    run_remote_join_over_local_composite(d1_plain_key, false, 0);
}

// The code 'BER-1' is no customer's city: the JOIN runs to completion with no match.
TEST_CASE("D1: remote table JOIN local composite table on plain key over seeded local table", "[engine-defect-d1]") {
    run_remote_join_over_local_composite(d1_plain_key, true, 0);
}
