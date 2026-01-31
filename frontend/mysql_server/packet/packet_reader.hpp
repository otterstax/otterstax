// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../../common/packet_reader_base.hpp"
#include "length_encoded.hpp"

namespace frontend::mysql {
    class packet_reader : public packet_reader_base {
    public:
        using packet_reader_base::packet_reader_base;

        int16_t read_int16() override;
        uint16_t read_uint16() override;
        int32_t read_int32() override;
        uint32_t read_uint32() override;
        int64_t read_int64() override;
        uint64_t read_uint64() override;
        uint64_t read_length_encoded_integer();
        std::string read_length_encoded_string();
    };
} // namespace frontend::mysql