// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "../utils.hpp"
#include "length_encoded.hpp"
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace mysql_front {
    class packet_reader {
    public:
        packet_reader(std::vector<uint8_t> data);

        uint8_t read_uint8();
        uint16_t read_uint16_le();
        uint32_t read_uint32_le();
        uint64_t read_uint64_le();
        uint64_t read_length_encoded_integer();
        std::string read_string_null();
        std::string read_string_eof();
        std::string read_length_encoded_string();

        void skip_bytes(size_t n);
        size_t remaining() const;

    private:
        void check_bounds(size_t needed) const;

        std::vector<uint8_t> data_;
        size_t pos_;
    };
} // namespace mysql_front