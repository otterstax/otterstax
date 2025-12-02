// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "mysql_defs/field_type.hpp"

#include <components/types/types.hpp>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

namespace mysql_front {
    template<typename T, size_t N>
    T read_nth_byte(const std::vector<uint8_t>& data, size_t pos) requires(std::numeric_limits<T>::is_exact) {
        return static_cast<T>(data.at(pos + N)) << (N * 8);
    }

    template<size_t N>
    uint8_t extract_nth_byte(uint64_t value) {
        return static_cast<uint8_t>((value >> (N * 8)) & 0xFF);
    }

    namespace detail {
        template<typename T, size_t... Idx>
        T merge_n_bytes_impl(const std::vector<uint8_t>& data,
                             size_t pos,
                             std::index_sequence<Idx...>) requires(std::numeric_limits<T>::is_exact) {
            return (read_nth_byte<T, Idx>(data, pos) | ...);
        }

        template<size_t... Idx>
        void push_nth_bytes_impl(std::vector<uint8_t>& payload, uint64_t value, std::index_sequence<Idx...>) {
            (payload.push_back(extract_nth_byte<Idx>(value)), ...);
        }
    } // namespace detail

    template<typename T, size_t N>
    T merge_n_bytes(const std::vector<uint8_t>& data, size_t pos) requires(std::numeric_limits<T>::is_exact) {
        return detail::merge_n_bytes_impl<T>(data, pos, std::make_index_sequence<N>());
    }

    template<size_t N>
    void push_nth_bytes(std::vector<uint8_t>& payload, uint64_t value) {
        detail::push_nth_bytes_impl(payload, value, std::make_index_sequence<N>());
    }

    field_type get_field_type(components::types::logical_type log_type);

    std::string hex_dump(const std::vector<uint8_t>& data, size_t max_bytes = 32);
} // namespace mysql_front