// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "packet_reader.hpp"

namespace mysql_front {
    packet_reader::packet_reader(std::vector<uint8_t> data)
        : data_(std::move(data))
        , pos_(0) {}

    uint8_t packet_reader::read_uint8() { return data_.at(pos_++); }

    uint16_t packet_reader::read_uint16_le() {
        auto val = merge_n_bytes<uint16_t, 2>(data_, pos_);
        pos_ += 2;
        return val;
    }

    uint32_t packet_reader::read_uint32_le() {
        auto val = merge_n_bytes<uint32_t, 4>(data_, pos_);
        pos_ += 4;
        return val;
    }

    uint64_t packet_reader::read_uint64_le() {
        auto val = merge_n_bytes<uint64_t, 8>(data_, pos_);
        pos_ += 8;
        return val;
    }

    uint64_t packet_reader::read_length_encoded_integer() {
        uint8_t first_byte = data_.at(pos_++);

        if (first_byte < 251) {
            return first_byte;
        } else if (first_byte == TWO_BYTE_INT_MARKER) {
            return read_uint16_le();
        } else if (first_byte == THREE_BYTE_INT_MARKER) {
            auto val = merge_n_bytes<uint32_t, 3>(data_, pos_);
            pos_ += 3;
            return val;
        } else if (first_byte == EIGHT_BYTE_INT_MARKER) {
            auto val = merge_n_bytes<uint64_t, 8>(data_, pos_);
            return val;
        } else { // 0xFB - NULL marker
            throw std::runtime_error("NULL value in length-encoded integer");
        }
    }

    std::string packet_reader::read_string_null() {
        size_t start = pos_;

        while (pos_ < data_.size() && data_[pos_] != 0) {
            pos_++;
        }

        std::string result(data_.begin() + start, data_.begin() + pos_);
        if (pos_ < data_.size()) {
            pos_++;
        }

        return result;
    }

    std::string packet_reader::read_string_eof() {
        std::string result(data_.begin() + pos_, data_.end());
        pos_ = data_.size();
        return result;
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

    void packet_reader::skip_bytes(size_t n) {
        check_bounds(n);
        pos_ += n;
    }

    size_t packet_reader::remaining() const { return data_.size() - pos_; }

    void packet_reader::check_bounds(size_t needed) const {
        if (pos_ + needed > data_.size()) {
            throw std::out_of_range("Buffer underflow");
        }
    }
} // namespace mysql_front