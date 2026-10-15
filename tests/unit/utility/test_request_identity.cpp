// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#include "utility/session.hpp"
#include <catch2/catch_all.hpp>
#include <array>
#include <thread>
#include <unordered_set>
#include <vector>

TEST_CASE("request identities stay unique across concurrent frontend lanes") {
    constexpr size_t lanes = 8;
    constexpr size_t per_lane = 4096;
    std::array<std::vector<session_hash_t>, lanes> identifiers;
    std::vector<std::jthread> threads;
    for (auto& lane : identifiers) {
        threads.emplace_back([&lane] {
            lane.reserve(per_lane);
            for (size_t i = 0; i < per_lane; ++i) lane.push_back(next_request_id());
        });
    }
    threads.clear(); // join before inspecting thread-owned results
    std::unordered_set<session_hash_t> unique;
    for (const auto& lane : identifiers) {
        for (auto id : lane) {
            REQUIRE(id != 0);
            REQUIRE(unique.insert(id).second);
        }
    }
    CHECK(unique.size() == lanes * per_lane);
}
