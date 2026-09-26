// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstddef>
#include <functional>

// Order-sensitive combination of several hashable parts into one seed
// (boost::hash_combine's mixing step): equal parts in different positions
// produce different seeds, unlike a plain XOR fold.
template<typename T, typename... Rest>
void hash_combine(std::size_t& seed, const T& v, const Rest&... rest) {
    seed ^= std::hash<T>{}(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    (hash_combine(seed, rest), ...);
}
