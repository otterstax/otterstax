// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../mysql_server/mysql_defs/field_type.hpp"
#include "../postgres_server/postgres_defs/field_type.hpp"

#include <components/types/types.hpp>
#include <array>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace frontend {
    // TODO: little-endian assumption, use boost::endian?
    enum class endian
    {
        LITTLE,
        BIG
    };

    // The byte-merging helpers below read data[pos, pos + N) unchecked: the
    // caller has the bytes — a header read to its exact size, or a
    // packet_reader whose check_bounds passed. Nothing here throws.
    template<typename T, size_t ByteIndex>
    constexpr T read_nth_byte_le(const std::vector<uint8_t>& data, size_t pos) {
        return static_cast<T>(data[pos + ByteIndex]) << (ByteIndex * 8);
    }

    template<typename T, size_t ByteIndex, size_t N>
    constexpr T read_nth_byte_be(const std::vector<uint8_t>& data, size_t pos) {
        constexpr size_t shift = (N - 1 - ByteIndex) * 8;
        return static_cast<T>(data[pos + ByteIndex]) << shift;
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

    // The wire type of a result column, nullopt for a logical type the protocol
    // has no type for (UHUGEINT, DATE, TIMESTAMP, UNKNOWN, ...). Nothing throws:
    // an unmapped column is refused by the connection with a protocol error
    // (see find_unsupported_column in resultset_utils.hpp).
    namespace mysql {
        std::optional<field_type> get_field_type(components::types::logical_type log_type);
    } // namespace mysql

    namespace postgres {
        std::optional<field_type> get_field_type(components::types::logical_type log_type);
    } // namespace postgres

    // The enumerator's name, for error messages ("HUGEINT", "UNKNOWN").
    std::string_view logical_type_name(components::types::logical_type type);

    // `size` bytes from a cryptographically secure generator (OpenSSL RAND_bytes),
    // or nullopt when it cannot produce them (no entropy source). Callable from
    // any thread. What the wire servers hand out as secrets comes from it: the
    // MySQL auth scramble and the PostgreSQL BackendKeyData secret, which a
    // CancelRequest presents — a client that saw other connections' values must
    // not be able to predict the next one (a shared std::mt19937 was both a data
    // race between concurrent handshakes and predictable from its outputs).
    std::optional<std::vector<uint8_t>> secure_random_bytes(size_t size);

    // Lower-case hex of `bytes` (two characters a byte).
    std::string hex_encode(const std::vector<uint8_t>& bytes);

    // `size` bytes of secure_random_bytes as lower-case hex (2 * size
    // characters), or nullopt when no secure bytes can be had. What the Flight
    // SQL server names its tickets and prepared-statement handles with.
    std::optional<std::string> secure_random_hex(size_t size);

    // The RFC 9562 version-4 UUID string (`xxxxxxxx-xxxx-4xxx-Yxxx-xxxxxxxxxxxx`,
    // lower case, Y one of 8 9 a b) made of these 16 bytes: the version and
    // variant bits are set, the other 122 bits are the bytes' own.
    std::string uuid_v4_string(const std::array<uint8_t, 16>& bytes);

    // A random version-4 UUID from secure_random_bytes, or nullopt when no secure
    // bytes can be had. The Spark Connect response ids.
    std::optional<std::string> secure_uuid_v4();

    std::string hex_dump(const std::vector<uint8_t>& data, size_t max_bytes = 32);
} // namespace frontend
