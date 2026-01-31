// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_reader.hpp"

namespace frontend::mysql {
    int16_t packet_reader::read_int16() {
        auto v = merge_data_bytes<int16_t, endian::LITTLE>(data_, pos_);
        pos_ += 2;
        return v;
    }

    uint16_t packet_reader::read_uint16() {
        auto v = merge_data_bytes<uint16_t, endian::LITTLE>(data_, pos_);
        pos_ += 2;
        return v;
    }

    int32_t packet_reader::read_int32() {
        auto v = merge_data_bytes<int32_t, endian::LITTLE>(data_, pos_);
        pos_ += 4;
        return v;
    }

    uint32_t packet_reader::read_uint32() {
        auto val = merge_data_bytes<uint32_t, endian::LITTLE>(data_, pos_);
        pos_ += 4;
        return val;
    }

    int64_t packet_reader::read_int64() {
        auto v = merge_data_bytes<int64_t, endian::LITTLE>(data_, pos_);
        pos_ += 8;
        return v;
    }

    uint64_t packet_reader::read_uint64() {
        auto v = merge_data_bytes<uint64_t, endian::LITTLE>(data_, pos_);
        pos_ += 8;
        return v;
    }

    uint64_t packet_reader::read_length_encoded_integer() {
        uint8_t first_byte = data_.at(pos_++);

        if (first_byte < 251) {
            return first_byte;
        } else if (first_byte == TWO_BYTE_INT_MARKER) {
            return read_uint16();
        } else if (first_byte == THREE_BYTE_INT_MARKER) {
            auto val = merge_n_bytes<uint32_t, 3, endian::LITTLE>(data_, pos_);
            pos_ += 3;
            return val;
        } else if (first_byte == EIGHT_BYTE_INT_MARKER) {
            return read_int64();
        } else { // 0xFB - NULL marker
            throw std::runtime_error("NULL value in length-encoded integer");
        }
    }

    std::string packet_reader::read_length_encoded_string() {
        uint64_t length = read_length_encoded_integer();
        if (length == 0) {
            return {};
        }

        check_bounds(length);
        std::string result(data_.begin() + pos_, data_.begin() + pos_ + length);
        pos_ += length;
        return result;
    }
} // namespace frontend::mysql
