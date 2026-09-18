// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utils.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace frontend {
    // Why a read failed. The state is sticky: the first fault is kept and every
    // later read fails with it, so a caller may check once after a group of
    // reads. A failed read moves nothing and its returned value is not data.
    enum class packet_fault : uint8_t
    {
        none,
        // Fewer bytes are left than the field needs (a NUL-terminated string
        // without its terminator included).
        underflow,
        // A byte that is no length-encoded-integer prefix (0xFB NULL, 0xFF).
        invalid_marker,
    };

    // Decodes one client packet. Nothing here throws: a packet shorter than
    // the fields read from it is reported through fault(), and the connection
    // answers a protocol error.
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

        bool ok() const;
        packet_fault fault() const;

    protected:
        // True when `needed` bytes are left and no read has failed; false
        // records the underflow.
        bool check_bounds(size_t needed);
        void set_fault(packet_fault fault);

        std::vector<uint8_t> data_;
        size_t pos_;
        packet_fault fault_;
    };
} // namespace frontend
