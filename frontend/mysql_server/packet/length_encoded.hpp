// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>

namespace frontend::mysql {
    constexpr uint8_t TWO_BYTE_INT_MARKER = 0xFC;
    constexpr uint8_t THREE_BYTE_INT_MARKER = 0xFD;
    constexpr uint8_t EIGHT_BYTE_INT_MARKER = 0xFE;

    // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_basic_dt_integers.html
    enum class length_encoded_int_size : uint8_t
    {
        ONE_BYTE = 1,
        THREE_BYTES = 3, // marker + 2-byte int
        FOUR_BYTES = 4,  // marker + 3-byte int
        NINE_BYTES = 9,  // marker + 8-byte int
    };

    length_encoded_int_size get_length_encoded_int_size(uint64_t value);
    std::size_t get_length_encoded_string_size(uint64_t string_size);
} // namespace frontend::mysql
