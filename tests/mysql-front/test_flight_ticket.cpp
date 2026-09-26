// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// FlightSQL ticket codec: GetFlightInfo encodes `<sql>:<txn>:<session hash>`,
// DoGet decodes it. The SQL text may contain ':' itself (`SELECT 1::int`), and
// a ticket forged or corrupted by a client must come back as a Status, never
// as an abort.

#include "frontend/flight_sql_server/server.hpp"

#include <catch2/catch_all.hpp>

#include <limits>
#include <string>

namespace {

    std::string round_trip_handle(const TicketData& data) {
        auto ticket = EncodeTransactionQuery(data);
        REQUIRE(ticket.ok());
        auto handle = arrow::flight::sql::StatementQueryTicket::Deserialize(ticket->ticket);
        REQUIRE(handle.ok());
        return handle->statement_handle;
    }

} // namespace

TEST_CASE("flight ticket: round-trips a plain query") {
    const auto handle = round_trip_handle({"SELECT a FROM t", "txn-1", 42});
    auto decoded = DecodeTransactionQuery(handle);
    REQUIRE(decoded.ok());
    REQUIRE(decoded->sql_query == "SELECT a FROM t");
    REQUIRE(decoded->transaction_id == "txn-1");
    REQUIRE(decoded->session_hash == 42);
}

TEST_CASE("flight ticket: SQL containing '::' survives the round trip") {
    const auto handle = round_trip_handle({"SELECT 1::int, 'a:b'::text", "", 7});
    auto decoded = DecodeTransactionQuery(handle);
    REQUIRE(decoded.ok());
    REQUIRE(decoded->sql_query == "SELECT 1::int, 'a:b'::text");
    REQUIRE(decoded->transaction_id.empty());
    REQUIRE(decoded->session_hash == 7);
}

TEST_CASE("flight ticket: the largest session hash round-trips") {
    const session_hash_t max_hash = std::numeric_limits<session_hash_t>::max();
    const auto handle = round_trip_handle({"SELECT 1", "", max_hash});
    auto decoded = DecodeTransactionQuery(handle);
    REQUIRE(decoded.ok());
    REQUIRE(decoded->session_hash == max_hash);
}

TEST_CASE("flight ticket: malformed handles are rejected with a Status") {
    SECTION("no divider at all") {
        auto decoded = DecodeTransactionQuery("SELECT 1");
        REQUIRE_FALSE(decoded.ok());
        REQUIRE(decoded.status().IsInvalid());
    }
    SECTION("only one divider") {
        auto decoded = DecodeTransactionQuery("SELECT 1:42");
        REQUIRE_FALSE(decoded.ok());
        REQUIRE(decoded.status().IsInvalid());
    }
    SECTION("session hash is not a number") {
        auto decoded = DecodeTransactionQuery("SELECT 1::abc");
        REQUIRE_FALSE(decoded.ok());
        REQUIRE(decoded.status().IsInvalid());
    }
    SECTION("session hash has trailing garbage") {
        auto decoded = DecodeTransactionQuery("SELECT 1::42x");
        REQUIRE_FALSE(decoded.ok());
        REQUIRE(decoded.status().IsInvalid());
    }
    SECTION("session hash is empty") {
        auto decoded = DecodeTransactionQuery("SELECT 1:txn:");
        REQUIRE_FALSE(decoded.ok());
        REQUIRE(decoded.status().IsInvalid());
    }
    SECTION("session hash is negative") {
        auto decoded = DecodeTransactionQuery("SELECT 1::-1");
        REQUIRE_FALSE(decoded.ok());
        REQUIRE(decoded.status().IsInvalid());
    }
    SECTION("session hash overflows") {
        auto decoded = DecodeTransactionQuery("SELECT 1::99999999999999999999999");
        REQUIRE_FALSE(decoded.ok());
        REQUIRE(decoded.status().IsInvalid());
    }
    SECTION("empty handle") {
        auto decoded = DecodeTransactionQuery("");
        REQUIRE_FALSE(decoded.ok());
        REQUIRE(decoded.status().IsInvalid());
    }
}
