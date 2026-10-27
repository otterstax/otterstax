// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstddef>
#include <functional>
#include <memory_resource>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace otterstax {

    // A transparent string hash: a map keyed by std::pmr::string looks up a std::string_view,
    // a std::string or a literal without building a key on its resource.
    struct string_hash {
        using is_transparent = void;
        std::size_t operator()(std::string_view text) const noexcept { return std::hash<std::string_view>{}(text); }
    };

    // Its equality, through string_view: std::equal_to<> cannot compare a std::pmr::string key
    // with a std::string (basic_strings of two allocators have no operator==).
    struct string_equal {
        using is_transparent = void;
        bool operator()(std::string_view a, std::string_view b) const noexcept { return a == b; }
    };

    // The string-keyed containers of the codebase: allocated on a memory_resource, looked up
    // by any string-like key.
    template<class Value>
    using pmr_string_map_t = std::pmr::unordered_map<std::pmr::string, Value, string_hash, string_equal>;
    using pmr_string_set_t = std::pmr::unordered_set<std::pmr::string, string_hash, string_equal>;

} // namespace otterstax
