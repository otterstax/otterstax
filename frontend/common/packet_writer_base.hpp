// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utils.hpp"
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace frontend {
    // Appends one built packet to a buffer of packets.
    inline void append_packet(std::vector<uint8_t>& out, const std::vector<uint8_t>& packet) {
        out.insert(out.end(), packet.begin(), packet.end());
    }

    class packet_writer_base {
    public:
        virtual ~packet_writer_base() = default;

        void reserve_payload(size_t length, size_t header_length);

        void write_uint8(uint8_t value);
        void write_string_null(std::string_view str);
        void write_string_fixed(std::string_view str);

        virtual void write_int16(int16_t value) = 0;
        virtual void write_uint16(uint16_t value) = 0;

        virtual void write_int32(int32_t value) = 0;
        virtual void write_uint32(uint32_t value) = 0;

        virtual void write_int64(int64_t value) = 0;
        virtual void write_uint64(uint64_t value) = 0;

        std::vector<uint8_t> extract_payload();

    protected:
        std::vector<uint8_t> payload_;
        bool is_reserved_ = false;
    };
} // namespace frontend