// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_reader_base.hpp"

namespace frontend {
    packet_reader_base::packet_reader_base(std::vector<uint8_t> data)
        : data_(std::move(data))
        , pos_(0) {}

    uint8_t packet_reader_base::read_uint8() { return data_.at(pos_++); }

    std::string packet_reader_base::read_string_null() {
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

    std::string packet_reader_base::read_string_eof() {
        std::string result(data_.begin() + pos_, data_.end());
        pos_ = data_.size();
        return result;
    }

    void packet_reader_base::skip_bytes(size_t n) {
        check_bounds(n);
        pos_ += n;
    }

    size_t packet_reader_base::remaining() const { return data_.size() - pos_; }

    void packet_reader_base::check_bounds(size_t needed) const {
        if (pos_ + needed > data_.size()) {
            throw std::out_of_range("Buffer underflow");
        }
    }
} // namespace frontend
