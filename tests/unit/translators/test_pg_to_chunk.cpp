// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// Row-level pg_to_chunk tests are not feasible at unit-test level:
// PQmakeEmptyPGresult() creates a result with zero columns and zero rows,
// and libpq has no public API for inserting synthetic rows without going
// through the wire protocol.  The row-level conversion paths are exercised
// by the system tests (tests/system/) which run the full pipeline against
// real PostgreSQL.
//
// What we CAN test here:
//   - pg_to_chunk / pg_to_struct do not crash on an empty PGresult
//   - Both enum-overload variants compile and behave identically

#include "otterbrix/translators/input/pg_to_chunk.hpp"

#include <catch2/catch.hpp>

#include <libpq-fe.h>

#include <memory_resource>

using namespace components::types;

namespace {
struct PGresultGuard {
    PGresult* res;
    explicit PGresultGuard(PGresult* r) : res(r) {}
    ~PGresultGuard() {
        if (res) PQclear(res);
    }
    PGresultGuard(const PGresultGuard&) = delete;
    PGresultGuard& operator=(const PGresultGuard&) = delete;
};
} // namespace

TEST_CASE("pg_to_struct: empty PGresult produces empty STRUCT") {
    PGresultGuard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    REQUIRE(g.res != nullptr);

    auto s = tsl::pg_to_struct(std::pmr::get_default_resource(), g.res);

    REQUIRE(s.type() == logical_type::STRUCT);
    REQUIRE(s.child_types().empty());
}

TEST_CASE("pg_to_struct (enum overload): empty PGresult produces empty STRUCT") {
    PGresultGuard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    tsl::pg_enum_oid_map empty_map;

    auto s = tsl::pg_to_struct(std::pmr::get_default_resource(), g.res, empty_map);

    REQUIRE(s.type() == logical_type::STRUCT);
    REQUIRE(s.child_types().empty());
}

TEST_CASE("pg_to_chunk: empty PGresult produces zero-row chunk") {
    PGresultGuard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    auto* res = std::pmr::get_default_resource();

    auto chunk = tsl::pg_to_chunk(res, g.res);

    REQUIRE(chunk.size() == 0);
    REQUIRE(chunk.column_count() == 0);
}

TEST_CASE("pg_to_chunk (enum overload): empty PGresult produces zero-row chunk") {
    PGresultGuard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    auto* res = std::pmr::get_default_resource();
    tsl::pg_enum_oid_map empty_map;

    auto chunk = tsl::pg_to_chunk(res, g.res, empty_map);

    REQUIRE(chunk.size() == 0);
    REQUIRE(chunk.column_count() == 0);
}
