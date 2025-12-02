// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "../utils.hpp"
#include "length_encoded.hpp"
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace mysql_front {
    class packet_writer {
    public:
        void reserve_payload(size_t length);

        void write_uint8(uint8_t value);
        void write_uint16_le(uint16_t value);
        void write_uint32_le(uint32_t value);
        void write_uint64_le(uint64_t value);
        void write_zeros(size_t count);

        // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_basic_dt_integers.html
        void write_length_encoded_integer(uint64_t value);

        // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_basic_dt_strings.html
        void write_string_null(std::string str);
        void write_string_fixed(std::string str);
        void write_length_encoded_string(std::string str);

        void write_null();

        std::vector<uint8_t> build_from_payload(uint8_t sequence_id);

    private:
        std::vector<uint8_t> payload_;
        bool is_reserved_ = false;
    };
} // namespace mysql_front