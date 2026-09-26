// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_reader.hpp"

namespace frontend::mysql {
    int16_t packet_reader::read_int16() {
        if (!check_bounds(2)) {
            return 0;
        }
        auto v = merge_data_bytes<int16_t, endian::LITTLE>(data_, pos_);
        pos_ += 2;
        return v;
    }

    uint16_t packet_reader::read_uint16() {
        if (!check_bounds(2)) {
            return 0;
        }
        auto v = merge_data_bytes<uint16_t, endian::LITTLE>(data_, pos_);
        pos_ += 2;
        return v;
    }

    int32_t packet_reader::read_int32() {
        if (!check_bounds(4)) {
            return 0;
        }
        auto v = merge_data_bytes<int32_t, endian::LITTLE>(data_, pos_);
        pos_ += 4;
        return v;
    }

    uint32_t packet_reader::read_uint32() {
        if (!check_bounds(4)) {
            return 0;
        }
        auto val = merge_data_bytes<uint32_t, endian::LITTLE>(data_, pos_);
        pos_ += 4;
        return val;
    }

    int64_t packet_reader::read_int64() {
        if (!check_bounds(8)) {
            return 0;
        }
        auto v = merge_data_bytes<int64_t, endian::LITTLE>(data_, pos_);
        pos_ += 8;
        return v;
    }

    uint64_t packet_reader::read_uint64() {
        if (!check_bounds(8)) {
            return 0;
        }
        auto v = merge_data_bytes<uint64_t, endian::LITTLE>(data_, pos_);
        pos_ += 8;
        return v;
    }

    uint64_t packet_reader::read_length_encoded_integer() {
        if (!check_bounds(1)) {
            return 0;
        }
        uint8_t first_byte = data_[pos_];

        // The marker and its integer are one field: neither is consumed unless
        // both fit.
        if (first_byte < 251) {
            pos_++;
            return first_byte;
        } else if (first_byte == TWO_BYTE_INT_MARKER) {
            if (!check_bounds(1 + 2)) {
                return 0;
            }
            pos_++;
            return read_uint16();
        } else if (first_byte == THREE_BYTE_INT_MARKER) {
            if (!check_bounds(1 + 3)) {
                return 0;
            }
            pos_++;
            auto val = merge_n_bytes<uint32_t, 3, endian::LITTLE>(data_, pos_);
            pos_ += 3;
            return val;
        } else if (first_byte == EIGHT_BYTE_INT_MARKER) {
            if (!check_bounds(1 + 8)) {
                return 0;
            }
            pos_++;
            return read_uint64();
        } else { // 0xFB - NULL marker, 0xFF - no integer prefix at all
            set_fault(packet_fault::invalid_marker);
            return 0;
        }
    }

    std::string packet_reader::read_length_encoded_string() {
        // The length prefix and the bytes are one field: consumed together or
        // not at all.
        const size_t start = pos_;
        uint64_t length = read_length_encoded_integer();
        if (!ok() || length == 0) {
            return {};
        }

        if (!check_bounds(length)) {
            pos_ = start;
            return {};
        }
        std::string result(data_.begin() + pos_, data_.begin() + pos_ + length);
        pos_ += length;
        return result;
    }
} // namespace frontend::mysql
