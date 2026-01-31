// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../common/packet_writer_base.hpp"
#include "../protocol_const.hpp"
#include "length_encoded.hpp"

namespace frontend::mysql {
    class packet_writer : public packet_writer_base {
    public:
        void reserve_payload(size_t length);

        void write_int16(int16_t value) override;
        void write_uint16(uint16_t value) override;
        void write_int32(int32_t value) override;
        void write_uint32(uint32_t value) override;
        void write_int64(int64_t value) override;
        void write_uint64(uint64_t value) override;
        void write_zeros(size_t count);

        // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_basic_dt_integers.html
        void write_length_encoded_integer(uint64_t value);

        // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_basic_dt_strings.html
        void write_length_encoded_string(std::string str);

        void write_null();

        std::vector<uint8_t> build_from_payload(uint8_t sequence_id);

    private:
        using packet_writer_base::reserve_payload;
    };
} // namespace frontend::mysql
