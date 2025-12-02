// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <string_view>

template<typename T, typename... Rest>
void hash_combine(std::size_t& seed, const T& v, const Rest&... rest) {
    seed ^= std::hash<T>{}(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    (hash_combine(seed, rest), ...);
}

inline std::size_t connection_hash(std::string_view addr, std::string_view user, std::string_view db) {
    size_t combined_hash = 42;
    hash_combine(combined_hash, addr, user, db);
    return combined_hash;
}