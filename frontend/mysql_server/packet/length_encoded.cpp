// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "length_encoded.hpp"

namespace frontend::mysql {
    length_encoded_int_size get_length_encoded_int_size(uint64_t value) {
        return (value < 251)        ? length_encoded_int_size::ONE_BYTE
               : (value < 65536)    ? length_encoded_int_size::THREE_BYTES
               : (value < 16777216) ? length_encoded_int_size::FOUR_BYTES
                                    : length_encoded_int_size::NINE_BYTES;
    }

    std::size_t get_length_encoded_string_size(uint64_t string_size) {
        return static_cast<uint8_t>(get_length_encoded_int_size(string_size)) + string_size;
    }
} // namespace frontend::mysql
