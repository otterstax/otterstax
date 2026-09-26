// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// A remote DML result carries its row count in the OK packet, not in rows.
//
// boost.mysql hands an INSERT/UPDATE/DELETE back as a results with empty meta()
// and empty rows(); the affected-row count lives in affected_rows(). The shape
// asserted here is the same one the ENGINE produces for a local DML — a
// column-less carrier whose cardinality IS the count — so nothing downstream
// (session_payload::size(), the PG CommandComplete tag, the MySQL OK packet,
// FlightSQL DoPutCommandStatementUpdate) needs to know where the statement ran.
// tests/system/test_dml_result_shape.cpp pins the local half of that contract;
// test_pg_to_chunk.cpp pins the PostgreSQL half.

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "counting_resource.hpp"

#include <catch2/catch_all.hpp>

#include <boost/mysql/detail/access.hpp>
#include <boost/mysql/detail/ok_view.hpp>
#include <boost/mysql/detail/resultset_encoding.hpp>
#include <boost/mysql/diagnostics.hpp>
#include <boost/mysql/metadata_mode.hpp>
#include <boost/mysql/results.hpp>

#include <memory_resource>

using otterstax::test::counting_resource;

namespace {

    // A results holding nothing but an OK packet — exactly what boost.mysql
    // produces for INSERT/UPDATE/DELETE.
    boost::mysql::results make_dml_results(std::uint64_t affected_rows) {
        boost::mysql::results r;
        auto& impl = boost::mysql::detail::access::get_impl(r);
        impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::minimal);
        boost::mysql::diagnostics diag;
        auto ec = impl.on_head_ok_packet(boost::mysql::detail::ok_view{affected_rows, 0, 0, 0, {}}, diag);
        REQUIRE(!ec);
        return r;
    }

} // namespace

TEST_CASE("mysql_to_chunk: a DML OK packet becomes a column-less chunk carrying the affected-row count") {
    std::pmr::unsynchronized_pool_resource resource{std::pmr::new_delete_resource()};

    auto results = make_dml_results(7);
    REQUIRE(results.meta().empty());
    REQUIRE(results.rows().empty());
    REQUIRE(results.affected_rows() == 7);

    auto converted = tsl::mysql_to_chunk(&resource, results);
    REQUIRE_FALSE(converted.has_error());
    const auto& chunk = converted.value();

    // No columns — there is nothing to project, and inventing some is how the
    // local DELETE path leaked uninitialised heap.
    REQUIRE(chunk.column_count() == 0);
    // ...but the count must survive: this is the number the wire protocols report.
    REQUIRE(chunk.size() == 7);
}

TEST_CASE("mysql_to_chunk: a DML that affected nothing yields an empty carrier, not a broken one") {
    std::pmr::unsynchronized_pool_resource resource{std::pmr::new_delete_resource()};

    auto results = make_dml_results(0);
    auto converted = tsl::mysql_to_chunk(&resource, results);
    REQUIRE_FALSE(converted.has_error());

    REQUIRE(converted.value().column_count() == 0);
    REQUIRE(converted.value().size() == 0);
}

// A DML routinely touches more rows than DEFAULT_VECTOR_CAPACITY (1024). The
// carrier has no columns, so nothing per row may be allocated for it: the count
// is a number, not a row buffer.
TEST_CASE("mysql_to_chunk: a DML affecting more than 1024 rows keeps the count without per-row allocation") {
    constexpr std::uint64_t affected = 5000;
    counting_resource resource{std::pmr::new_delete_resource()};

    auto results = make_dml_results(affected);
    auto converted = tsl::mysql_to_chunk(&resource, results);
    REQUIRE_FALSE(converted.has_error());

    REQUIRE(converted.value().column_count() == 0);
    REQUIRE(converted.value().size() == affected);
    INFO("bytes allocated for the carrier: " << resource.allocated_bytes());
    REQUIRE(resource.allocated_bytes() < affected);
}

// The other half of the contract — a result WITH columns reports its ROW count
// and never the OK packet's counter — is not unit-testable here: boost.mysql
// offers no public way to inject synthetic rows into a results, so a SELECT
// result cannot be manufactured. It is covered by every SELECT assertion in the
// python suite, which would break loudly if the count came from the wrong place.
