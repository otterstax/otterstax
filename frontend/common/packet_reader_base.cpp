// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_reader_base.hpp"

namespace frontend {
    packet_reader_base::packet_reader_base(std::vector<uint8_t> data)
        : data_(std::move(data))
        , pos_(0)
        , fault_(packet_fault::none) {}

    uint8_t packet_reader_base::read_uint8() {
        if (!check_bounds(1)) {
            return 0;
        }
        return data_[pos_++];
    }

    std::string packet_reader_base::read_string_null() {
        if (fault_ != packet_fault::none) {
            return {};
        }

        size_t end = pos_;
        while (end < data_.size() && data_[end] != 0) {
            end++;
        }

        if (end == data_.size()) {
            set_fault(packet_fault::underflow);
            return {};
        }

        std::string result(data_.begin() + pos_, data_.begin() + end);
        pos_ = end + 1;
        return result;
    }

    std::string packet_reader_base::read_string_eof() {
        std::string result(data_.begin() + pos_, data_.end());
        pos_ = data_.size();
        return result;
    }

    void packet_reader_base::skip_bytes(size_t n) {
        if (!check_bounds(n)) {
            return;
        }
        pos_ += n;
    }

    size_t packet_reader_base::remaining() const { return data_.size() - pos_; }

    bool packet_reader_base::ok() const { return fault_ == packet_fault::none; }

    packet_fault packet_reader_base::fault() const { return fault_; }

    bool packet_reader_base::check_bounds(size_t needed) {
        if (fault_ != packet_fault::none) {
            return false;
        }
        // remaining() is never smaller than 0, so no sum can wrap here.
        if (needed > remaining()) {
            set_fault(packet_fault::underflow);
            return false;
        }
        return true;
    }

    void packet_reader_base::set_fault(packet_fault fault) {
        if (fault_ == packet_fault::none) {
            fault_ = fault;
        }
    }
} // namespace frontend
