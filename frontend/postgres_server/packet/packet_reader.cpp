// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_reader.hpp"

namespace frontend::postgres {
    int16_t packet_reader::read_int16() {
        auto v = merge_data_bytes<int16_t, endian::BIG>(data_, pos_);
        pos_ += 2;
        return v;
    }

    uint16_t packet_reader::read_uint16() {
        auto v = merge_data_bytes<uint16_t, endian::BIG>(data_, pos_);
        pos_ += 2;
        return v;
    }

    int32_t packet_reader::read_int32() {
        auto v = merge_data_bytes<int32_t, endian::BIG>(data_, pos_);
        pos_ += 4;
        return v;
    }

    uint32_t packet_reader::read_uint32() {
        auto v = merge_data_bytes<uint32_t, endian::BIG>(data_, pos_);
        pos_ += 4;
        return v;
    }

    int64_t packet_reader::read_int64() {
        auto v = merge_data_bytes<int64_t, endian::BIG>(data_, pos_);
        pos_ += 8;
        return v;
    }

    uint64_t packet_reader::read_uint64() {
        auto v = merge_data_bytes<uint64_t, endian::BIG>(data_, pos_);
        pos_ += 8;
        return v;
    }
} // namespace frontend::postgres
