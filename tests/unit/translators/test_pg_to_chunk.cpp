// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// PGresult objects are manufactured without a connection: tuples through libpq's
// public PQsetResultAttrs / PQsetvalue, command tags through the layout header
// (see pg_result_fixture.cpp). Both pg_to_chunk overloads are exercised on every
// shape, since the enum overload is the one the integration layer calls.

#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "counting_resource.hpp"
#include "pg_result_fixture.hpp"

#include <catch2/catch_all.hpp>

#include <libpq-fe.h>

#include <cstdint>
#include <memory_resource>
#include <string_view>

using namespace components::types;
using otterstax::test::counting_resource;
using otterstax::test::make_command_result;
using otterstax::test::make_tuples_result;
using otterstax::test::pg_column;
using otterstax::test::pg_result_guard;

namespace {

    constexpr Oid INT4OID = 23;
    constexpr Oid INT8OID = 20;
    constexpr Oid TEXTOID = 25;

    // Runs `check` against both pg_to_chunk overloads on the same result.
    template<typename Check>
    void for_both_overloads(std::pmr::memory_resource* res, PGresult* result, Check&& check) {
        {
            INFO("overload without enum map");
            auto converted = tsl::pg_to_chunk(res, result);
            check(converted);
        }
        {
            INFO("overload with enum map");
            tsl::pg_enum_oid_map empty_map;
            auto converted = tsl::pg_to_chunk(res, result, empty_map);
            check(converted);
        }
    }

} // namespace

TEST_CASE("pg_to_struct: empty PGresult produces empty STRUCT") {
    pg_result_guard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    REQUIRE(g.get() != nullptr);

    auto s = tsl::pg_to_struct(std::pmr::new_delete_resource(), g.get());

    REQUIRE(s.type() == logical_type::STRUCT);
    REQUIRE(s.child_types().empty());
}

TEST_CASE("pg_to_struct (enum overload): empty PGresult produces empty STRUCT") {
    pg_result_guard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    tsl::pg_enum_oid_map empty_map;

    auto s = tsl::pg_to_struct(std::pmr::new_delete_resource(), g.get(), empty_map);

    REQUIRE(s.type() == logical_type::STRUCT);
    REQUIRE(s.child_types().empty());
}

TEST_CASE("pg_to_struct: column names and types come from the result attributes") {
    pg_result_guard g(make_tuples_result({{"id", INT4OID}, {"label", TEXTOID}}, {}));

    auto s = tsl::pg_to_struct(std::pmr::new_delete_resource(), g.get());

    REQUIRE(s.child_types().size() == 2);
    REQUIRE(s.child_types()[0].alias() == "id");
    REQUIRE(s.child_types()[0].type() == logical_type::INTEGER);
    REQUIRE(s.child_types()[1].alias() == "label");
    REQUIRE(s.child_types()[1].type() == logical_type::STRING_LITERAL);
}

TEST_CASE("pg_to_chunk: empty PGresult produces zero-row chunk") {
    pg_result_guard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    auto* res = std::pmr::new_delete_resource();

    auto converted = tsl::pg_to_chunk(res, g.get());
    REQUIRE_FALSE(converted.has_error());

    REQUIRE(converted.value().size() == 0);
    REQUIRE(converted.value().column_count() == 0);
}

TEST_CASE("pg_to_chunk (enum overload): empty PGresult produces zero-row chunk") {
    pg_result_guard g(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK));
    auto* res = std::pmr::new_delete_resource();
    tsl::pg_enum_oid_map empty_map;

    auto converted = tsl::pg_to_chunk(res, g.get(), empty_map);
    REQUIRE_FALSE(converted.has_error());

    REQUIRE(converted.value().size() == 0);
    REQUIRE(converted.value().column_count() == 0);
}

// ── tuples ────────────────────────────────────────────────────────────────────

TEST_CASE("pg_to_chunk: tuples are converted by column type, NULL cells stay NULL") {
    pg_result_guard g(make_tuples_result({{"id", INT4OID}, {"big", INT8OID}, {"label", TEXTOID}},
                                         {{"1", "10000000000", "one"},
                                          {"2", nullptr, "two"},
                                          {nullptr, "3", nullptr}}));
    auto* res = std::pmr::new_delete_resource();

    for_both_overloads(res, g.get(), [](auto& converted) {
        REQUIRE_FALSE(converted.has_error());
        const auto& chunk = converted.value();
        REQUIRE(chunk.column_count() == 3);
        REQUIRE(chunk.size() == 3);
        REQUIRE(chunk.value(0, 0).template value<int32_t>() == 1);
        REQUIRE(chunk.value(1, 0).template value<int64_t>() == 10000000000LL);
        REQUIRE(chunk.value(2, 0).template value<std::string_view>() == "one");
        REQUIRE(chunk.value(0, 1).template value<int32_t>() == 2);
        REQUIRE(chunk.value(1, 1).is_null());
        REQUIRE(chunk.value(2, 1).template value<std::string_view>() == "two");
        REQUIRE(chunk.value(0, 2).is_null());
        REQUIRE(chunk.value(1, 2).template value<int64_t>() == 3);
        REQUIRE(chunk.value(2, 2).is_null());
    });
}

// ── DML: the affected-row count travels in the command tag ────────────────────

TEST_CASE("pg_to_chunk: a DML command tag becomes a column-less chunk carrying the affected-row count") {
    pg_result_guard g(make_command_result("UPDATE 7"));
    REQUIRE(PQnfields(g.get()) == 0);
    REQUIRE(PQntuples(g.get()) == 0);
    auto* res = std::pmr::new_delete_resource();

    for_both_overloads(res, g.get(), [](auto& converted) {
        REQUIRE_FALSE(converted.has_error());
        REQUIRE(converted.value().column_count() == 0);
        REQUIRE(converted.value().size() == 7);
    });
}

TEST_CASE("pg_to_chunk: an INSERT tag carries the count after the oid") {
    pg_result_guard g(make_command_result("INSERT 0 12"));
    auto* res = std::pmr::new_delete_resource();

    for_both_overloads(res, g.get(), [](auto& converted) {
        REQUIRE_FALSE(converted.has_error());
        REQUIRE(converted.value().column_count() == 0);
        REQUIRE(converted.value().size() == 12);
    });
}

TEST_CASE("pg_to_chunk: a statement that counts nothing reports zero affected rows") {
    pg_result_guard g(make_command_result("CREATE TABLE"));
    auto* res = std::pmr::new_delete_resource();

    for_both_overloads(res, g.get(), [](auto& converted) {
        REQUIRE_FALSE(converted.has_error());
        REQUIRE(converted.value().column_count() == 0);
        REQUIRE(converted.value().size() == 0);
    });
}

// A DML routinely touches more rows than DEFAULT_VECTOR_CAPACITY (1024). The
// carrier has no columns, so nothing per row may be allocated for it.
TEST_CASE("pg_to_chunk: a DML affecting more than 1024 rows keeps the count without per-row allocation") {
    constexpr std::uint64_t affected = 5000;
    pg_result_guard g(make_command_result("DELETE 5000"));

    SECTION("overload without enum map") {
        counting_resource res{std::pmr::new_delete_resource()};
        auto converted = tsl::pg_to_chunk(&res, g.get());
        REQUIRE_FALSE(converted.has_error());
        REQUIRE(converted.value().column_count() == 0);
        REQUIRE(converted.value().size() == affected);
        INFO("bytes allocated for the carrier: " << res.allocated_bytes());
        REQUIRE(res.allocated_bytes() < affected);
    }
    SECTION("overload with enum map") {
        counting_resource res{std::pmr::new_delete_resource()};
        tsl::pg_enum_oid_map empty_map;
        auto converted = tsl::pg_to_chunk(&res, g.get(), empty_map);
        REQUIRE_FALSE(converted.has_error());
        REQUIRE(converted.value().column_count() == 0);
        REQUIRE(converted.value().size() == affected);
        INFO("bytes allocated for the carrier: " << res.allocated_bytes());
        REQUIRE(res.allocated_bytes() < affected);
    }
}

// A counting tag whose number is unreadable is a protocol violation; reading it
// as 0 would report "0 rows affected" for a statement that did change rows.
TEST_CASE("pg_to_chunk: a counting tag with a non-numeric count is an error, not zero") {
    pg_result_guard g(make_command_result("UPDATE abc"));
    auto* res = std::pmr::new_delete_resource();

    for_both_overloads(res, g.get(), [](auto& converted) {
        REQUIRE(converted.has_error());
        REQUIRE(converted.error().type == core::error_code_t::conversion_failure);
        REQUIRE(std::string_view{converted.error().what.c_str()}.find("UPDATE abc") != std::string_view::npos);
    });
}

TEST_CASE("pg_to_chunk: a counting tag with no count at all is an error, not zero") {
    pg_result_guard g(make_command_result("DELETE "));
    auto* res = std::pmr::new_delete_resource();

    for_both_overloads(res, g.get(), [](auto& converted) {
        REQUIRE(converted.has_error());
        REQUIRE(converted.error().type == core::error_code_t::conversion_failure);
    });
}

TEST_CASE("pg_to_chunk: a count that does not fit in 64 bits is an error, not a wrapped value") {
    pg_result_guard g(make_command_result("INSERT 0 99999999999999999999999"));
    auto* res = std::pmr::new_delete_resource();

    for_both_overloads(res, g.get(), [](auto& converted) {
        REQUIRE(converted.has_error());
        REQUIRE(converted.error().type == core::error_code_t::conversion_failure);
    });
}

TEST_CASE("pg_to_chunk: the error message lives on the caller's resource") {
    pg_result_guard g(make_command_result("UPDATE abc"));
    std::pmr::monotonic_buffer_resource arena{std::pmr::new_delete_resource()};

    auto converted = tsl::pg_to_chunk(&arena, g.get());
    REQUIRE(converted.has_error());
    REQUIRE(converted.error().what.get_allocator().resource() == &arena);
}
