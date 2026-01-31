// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../common/packet_writer_base.hpp"
#include "../protocol_const.hpp"

namespace frontend::postgres {
    class packet_writer : public packet_writer_base {
    public:
        using packet_writer_base::packet_writer_base;
        void reserve_payload(size_t length);

        void write_int16(int16_t value) override;
        void write_uint16(uint16_t value) override;
        void write_int32(int32_t value) override;
        void write_uint32(uint32_t value) override;
        void write_int64(int64_t value) override;
        void write_uint64(uint64_t value) override;
        // null-writing is handled in DataRow message

        std::vector<uint8_t> build_from_payload(char message_type);

    private:
        using packet_writer_base::reserve_payload;
    };
} // namespace frontend::postgres
