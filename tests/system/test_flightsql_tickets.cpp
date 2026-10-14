// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The Flight SQL ticket cache (flight::core::FlightSqlCore): a result is held
// from GetFlightInfo to the DoGet that redeems its ticket and no longer
// (Flight tickets are single use), and what is never redeemed is bounded by
// count and by bytes. It used to keep every result until 1024 newer ones had
// pushed it out — a DoGet did not release it — so the server grew by every
// result it served (~60 MB over 800 benchmark RPCs), and a DoGet streamed from
// a raw pointer an eviction could free under it. A result has one owner at a
// time: the cache, then the DoGet that took it out.

#include "core/core.hpp"
#include "frontend/flight_sql/session_engine.hpp"
#include "session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <arrow/api.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace {
    using flight::core::CachedResultPtr;
    using flight::core::FlightSqlCore;

    // A one-column int64 result of `rows` rows, serialized for the wire.
    CachedResultPtr result_of(int64_t rows) {
        arrow::Int64Builder builder;
        for (int64_t i = 0; i < rows; ++i) {
            REQUIRE(builder.Append(i).ok());
        }
        std::shared_ptr<arrow::Array> column;
        REQUIRE(builder.Finish(&column).ok());
        auto schema = arrow::schema({arrow::field("v", arrow::int64())});
        return flight::core::materialize(schema, {arrow::RecordBatch::Make(schema, rows, {column})});
    }

    // The client identity an anonymous service gives every RPC.
    const std::string kAnonymous;

    std::string register_ok(FlightSqlCore& core, CachedResultPtr result, const std::string& owner = kAnonymous) {
        std::string ticket;
        REQUIRE(core.register_result(std::move(result), owner, &ticket).ok());
        return ticket;
    }

    FlightSqlCore make_core(flight::core::ResultCacheLimits limits) {
        // The cache never touches the engine.
        return FlightSqlCore(flight::core::AuthService::anonymous(), nullptr, limits);
    }
} // namespace

TEST_CASE("flight tickets: a result is held until its DoGet and the ticket is single use") {
    auto core = make_core(flight::core::kDefaultResultCacheLimits);
    auto result = result_of(100);
    REQUIRE(result->total_rows == 100);
    REQUIRE(result->bytes > 100 * sizeof(int64_t));
    const auto* registered = result.get();
    const auto bytes = result->bytes;

    const auto ticket = register_ok(core, std::move(result));
    REQUIRE(core.cached_results() == 1);
    REQUIRE(core.cached_bytes() == bytes);

    CachedResultPtr taken;
    REQUIRE(core.take_result(ticket, kAnonymous, &taken).ok());
    REQUIRE(taken.get() == registered);
    REQUIRE(core.cached_results() == 0);
    REQUIRE(core.cached_bytes() == 0);

    CachedResultPtr again;
    const auto second = core.take_result(ticket, kAnonymous, &again);
    REQUIRE(second.error_code() == grpc::StatusCode::NOT_FOUND);
    REQUIRE(again == nullptr);
}

TEST_CASE("flight tickets: redeemed results leave nothing behind, however many were served") {
    auto core = make_core(flight::core::kDefaultResultCacheLimits);
    for (int i = 0; i < 5000; ++i) {
        CachedResultPtr taken;
        REQUIRE(core.take_result(register_ok(core, result_of(10)), kAnonymous, &taken).ok());
    }
    REQUIRE(core.cached_results() == 0);
    REQUIRE(core.cached_bytes() == 0);
}

TEST_CASE("flight tickets: unredeemed results are bounded by count, the oldest evicted first") {
    auto core = make_core({3, std::size_t{1} << 30});
    std::vector<std::string> tickets;
    for (int i = 0; i < 5; ++i) {
        tickets.push_back(register_ok(core, result_of(1)));
    }
    REQUIRE(core.cached_results() == 3);
    CachedResultPtr taken;
    REQUIRE(core.take_result(tickets[0], kAnonymous, &taken).error_code() == grpc::StatusCode::NOT_FOUND);
    REQUIRE(core.take_result(tickets[1], kAnonymous, &taken).error_code() == grpc::StatusCode::NOT_FOUND);
    for (int i = 2; i < 5; ++i) {
        REQUIRE(core.take_result(tickets[i], kAnonymous, &taken).ok());
    }
}

TEST_CASE("flight tickets: unredeemed results are bounded by bytes; the newest stays even alone over the bound") {
    auto big = result_of(10000);
    const auto big_bytes = big->bytes;
    auto core = make_core({1024, big_bytes / 2});

    const auto first = register_ok(core, result_of(10));
    const auto second = register_ok(core, result_of(10));
    REQUIRE(core.cached_results() == 2);

    // Over the byte bound: everything older goes, the result just registered stays.
    const auto large = register_ok(core, std::move(big));
    REQUIRE(core.cached_results() == 1);
    REQUIRE(core.cached_bytes() == big_bytes);
    CachedResultPtr taken;
    REQUIRE(core.take_result(first, kAnonymous, &taken).error_code() == grpc::StatusCode::NOT_FOUND);
    REQUIRE(core.take_result(second, kAnonymous, &taken).error_code() == grpc::StatusCode::NOT_FOUND);

    // The next registration pushes the oversized one out.
    const auto after = register_ok(core, result_of(10));
    REQUIRE(core.cached_results() == 1);
    REQUIRE(core.take_result(large, kAnonymous, &taken).error_code() == grpc::StatusCode::NOT_FOUND);
    REQUIRE(core.take_result(after, kAnonymous, &taken).ok());
}

TEST_CASE("flight tickets: a DoGet keeps the result it took while evictions run") {
    auto core = make_core({1, std::size_t{1} << 30});
    auto result = result_of(1000);
    const auto* registered = result.get();
    const auto bytes = result->bytes;
    const auto ticket = register_ok(core, std::move(result));

    // The DoGet owns what it took: the evictions after it run over other
    // entries and the cache accounts for none of its bytes.
    CachedResultPtr streaming;
    REQUIRE(core.take_result(ticket, kAnonymous, &streaming).ok());
    REQUIRE(streaming.get() == registered);
    for (int i = 0; i < 10; ++i) {
        register_ok(core, result_of(1));
    }
    REQUIRE(core.cached_results() == 1);
    REQUIRE(core.cached_bytes() < bytes);
    REQUIRE(streaming->total_rows == 1000);
    REQUIRE(streaming->bytes == bytes);
}

// Tickets and prepared-statement handles used to be counters
// (`otterstax-tickets/<n>-<hash of n>`, `otterstax-prepared/<n>`): another
// client could guess the next one and DoGet someone else's result, or run /
// close someone else's statement. They are now 128 bits of
// frontend::secure_random_bytes and bound to the client that obtained them.

namespace {
    bool is_random_id(const std::string& id, std::string_view prefix) {
        if (!id.starts_with(prefix) || id.size() != prefix.size() + FlightSqlCore::kIdBytes * 2) {
            return false;
        }
        return std::all_of(id.begin() + static_cast<std::ptrdiff_t>(prefix.size()), id.end(), [](char c) {
            return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        });
    }
} // namespace

TEST_CASE("flight tickets: a ticket is 128 random bits and unique") {
    auto core = make_core(flight::core::kDefaultResultCacheLimits);
    std::set<std::string> distinct;
    constexpr size_t draws = 2000;
    for (size_t i = 0; i < draws; ++i) {
        const auto ticket = register_ok(core, result_of(1));
        REQUIRE(is_random_id(ticket, "otterstax-tickets/"));
        distinct.insert(ticket);
        CachedResultPtr taken;
        REQUIRE(core.take_result(ticket, kAnonymous, &taken).ok());
    }
    REQUIRE(distinct.size() == draws);
}

TEST_CASE("flight tickets: another client's ticket is not found and stays for its owner") {
    auto core = make_core(flight::core::kDefaultResultCacheLimits);
    auto auth = flight::core::AuthService::basic("u", "p");
    const std::string alice = auth.client_of_bearer("token-of-alice");
    const std::string bob = auth.client_of_bearer("token-of-bob");
    REQUIRE_FALSE(alice.empty());
    REQUIRE(alice != bob);

    auto result = result_of(5);
    const auto* registered = result.get();
    const auto ticket = register_ok(core, std::move(result), alice);

    CachedResultPtr taken;
    REQUIRE(core.take_result(ticket, bob, &taken).error_code() == grpc::StatusCode::NOT_FOUND);
    REQUIRE(core.take_result(ticket, kAnonymous, &taken).error_code() == grpc::StatusCode::NOT_FOUND);
    REQUIRE(taken == nullptr);
    REQUIRE(core.cached_results() == 1);

    REQUIRE(core.take_result(ticket, alice, &taken).ok());
    REQUIRE(taken.get() == registered);
    REQUIRE(core.cached_results() == 0);
}

TEST_CASE("flight prepared handles: random unique handles that only their creator can use") {
    otterstax::test::session_stack_owner owner(otterstax::test::unique_test_dir("otterstax_flight_handles"));
    auto engine = std::make_unique<flight::engine::SessionEngine>(owner.sessions(), owner.engine().resource());
    REQUIRE(engine->execute_update("CREATE DATABASE fh") == 0);
    REQUIRE(engine->execute_update("CREATE TABLE fh.t (id bigint)") == 0);
    FlightSqlCore core(flight::core::AuthService::anonymous(), std::move(engine));

    auto auth = flight::core::AuthService::basic("u", "p");
    const std::string alice = auth.client_of_bearer("token-of-alice");
    const std::string bob = auth.client_of_bearer("token-of-bob");

    std::set<std::string> handles;
    for (int i = 0; i < 8; ++i) {
        std::string handle;
        REQUIRE(core.create_prepared("SELECT id FROM fh.t", alice, &handle).ok());
        REQUIRE(is_random_id(handle, "otterstax-prepared/"));
        handles.insert(handle);
    }
    REQUIRE(handles.size() == 8);

    const std::string handle = *handles.begin();
    flight::core::PreparedStatementState state;
    // Bob can neither read, bind nor close Alice's statement — it is unknown to him.
    REQUIRE(core.prepared_lookup(handle, bob, &state).error_code() == grpc::StatusCode::NOT_FOUND);
    REQUIRE(state.prepared.query.empty());
    REQUIRE(core.prepared_bind(handle, bob, {}).error_code() == grpc::StatusCode::NOT_FOUND);
    REQUIRE(core.close_prepared(handle, bob).error_code() == grpc::StatusCode::NOT_FOUND);
    REQUIRE(core.close_prepared(handle, kAnonymous).error_code() == grpc::StatusCode::NOT_FOUND);

    // Alice still can.
    REQUIRE(core.prepared_lookup(handle, alice, &state).ok());
    REQUIRE(state.prepared.query == "SELECT id FROM fh.t");
    REQUIRE(core.prepared_bind(handle, alice, {}).ok());
    for (const auto& h : handles) {
        REQUIRE(core.close_prepared(h, alice).ok());
    }
    REQUIRE(core.prepared_lookup(handle, alice, &state).error_code() == grpc::StatusCode::NOT_FOUND);
}
