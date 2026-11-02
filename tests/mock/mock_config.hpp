// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <algorithm>
#include <chrono>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

struct mock_config {
    // No default: every mock allocates on the resource its test hands it.
    std::pmr::memory_resource* resource;
    bool can_throw = false;
    bool return_empty = false;
    std::chrono::milliseconds wait_time = std::chrono::milliseconds(50);
    std::string error_message = "";
};

// The columns of the mocks' table (id, name) a data-path query reads, in the order its SELECT list
// names them (`quote` the dialect's identifier quote): a read of the projected columns answers only
// those, as a backend does. A list naming neither (SELECT *) reads both.
inline std::vector<std::size_t> mock_projection(std::string_view query, char quote) {
    const auto from = query.find(" FROM ");
    const auto list = query.substr(0, from);
    std::vector<std::pair<std::size_t, std::size_t>> named;
    const std::string columns[2] = {std::string(1, quote) + "id" + quote, std::string(1, quote) + "name" + quote};
    for (std::size_t c = 0; c < 2; ++c) {
        if (const auto at = list.find(columns[c]); at != std::string_view::npos) {
            named.emplace_back(at, c);
        }
    }
    if (named.empty()) {
        return {0, 1};
    }
    std::sort(named.begin(), named.end());
    std::vector<std::size_t> out;
    for (auto [at, c] : named) {
        out.push_back(c);
    }
    return out;
}
