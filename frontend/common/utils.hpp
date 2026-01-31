// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../mysql_server/mysql_defs/field_type.hpp"
#include "../postgres_server/postgres_defs/field_type.hpp"

#include <components/types/types.hpp>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace frontend {
    // TODO: little-endian assumption, use boost::endian?
    enum class endian
    {
        LITTLE,
        BIG
    };

    template<typename T, size_t ByteIndex>
    constexpr T read_nth_byte_le(const std::vector<uint8_t>& data, size_t pos) {
        return static_cast<T>(data.at(pos + ByteIndex)) << (ByteIndex * 8);
    }

    template<typename T, size_t ByteIndex, size_t N>
    constexpr T read_nth_byte_be(const std::vector<uint8_t>& data, size_t pos) {
        constexpr size_t shift = (N - 1 - ByteIndex) * 8;
        return static_cast<T>(data.at(pos + ByteIndex)) << shift;
    }

    template<size_t ByteIndex>
    uint8_t extract_nth_byte_le(uint64_t value) {
        return static_cast<uint8_t>((value >> (ByteIndex * 8)) & 0xFF);
    }

    template<size_t ByteIndex, size_t N>
    constexpr uint8_t extract_nth_byte_be(uint64_t value) {
        constexpr size_t shift = (N - 1 - ByteIndex) * 8;
        return static_cast<uint8_t>((value >> shift) & 0xFF);
    }

    namespace detail {
        template<typename T, endian Order, size_t N, size_t... Idx>
        T merge_n_bytes_impl(const std::vector<uint8_t>& data, size_t pos, std::index_sequence<Idx...>) {
            if constexpr (Order == endian::LITTLE) {
                return (read_nth_byte_le<T, Idx>(data, pos) | ...);
            } else {
                return (read_nth_byte_be<T, Idx, N>(data, pos) | ...);
            }
        }

        template<endian Order, size_t N, size_t... Idx>
        void push_nth_bytes_impl(std::vector<uint8_t>& payload, uint64_t value, std::index_sequence<Idx...>) {
            if constexpr (Order == endian::LITTLE) {
                (payload.push_back(extract_nth_byte_le<Idx>(value)), ...);
            } else {
                (payload.push_back(extract_nth_byte_be<Idx, N>(value)), ...);
            }
        }

    } // namespace detail

    template<typename T, size_t N, endian Order>
    T merge_n_bytes(const std::vector<uint8_t>& data, size_t pos)
        requires(std::numeric_limits<T>::is_exact)
    {
        using U = std::make_unsigned_t<T>;
        U raw = detail::merge_n_bytes_impl<U, Order, N>(data, pos, std::make_index_sequence<N>{});

        return static_cast<T>(raw); // sign-extension here
    }

    template<typename T, endian Order>
    T merge_data_bytes(const std::vector<uint8_t>& data, size_t pos)
        requires(std::numeric_limits<T>::is_exact)
    {
        return merge_n_bytes<T, sizeof(T), Order>(data, pos);
    }

    template<typename T, size_t N, endian Order>
    void push_nth_bytes(std::vector<uint8_t>& payload, T value)
        requires(std::numeric_limits<T>::is_exact)
    {
        using U = std::make_unsigned_t<T>;
        U raw = static_cast<U>(value);

        detail::push_nth_bytes_impl<Order, N>(payload, static_cast<uint64_t>(raw), std::make_index_sequence<N>{});
    }

    template<typename T, endian Order>
    void push_data_bytes(std::vector<uint8_t>& payload, T value)
        requires(std::numeric_limits<T>::is_exact)
    {
        return push_nth_bytes<T, sizeof(T), Order>(payload, value);
    }

    namespace mysql {
        field_type get_field_type(components::types::logical_type log_type);
    } // namespace mysql

    namespace postgres {
        field_type get_field_type(components::types::logical_type log_type);
    } // namespace postgres

    std::vector<uint8_t> generate_backend_key(size_t size);

    std::string hex_dump(const std::vector<uint8_t>& data, size_t max_bytes = 32);
} // namespace frontend
