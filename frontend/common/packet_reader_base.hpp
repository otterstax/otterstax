// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utils.hpp"
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace frontend {
    class packet_reader_base {
    public:
        packet_reader_base(std::vector<uint8_t> data);
        virtual ~packet_reader_base() = default;

        uint8_t read_uint8();
        std::string read_string_null();
        std::string read_string_eof();

        // functions are virtual, since int byte order are protocol-dependant
        virtual int16_t read_int16() = 0;
        virtual uint16_t read_uint16() = 0;

        virtual int32_t read_int32() = 0;
        virtual uint32_t read_uint32() = 0;

        virtual int64_t read_int64() = 0;
        virtual uint64_t read_uint64() = 0;

        void skip_bytes(size_t n);
        size_t remaining() const;

    protected:
        void check_bounds(size_t needed) const;

        std::vector<uint8_t> data_;
        size_t pos_;
    };
} // namespace frontend