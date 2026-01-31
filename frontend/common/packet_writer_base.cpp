// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_writer_base.hpp"

namespace frontend {
    void packet_writer_base::reserve_payload(size_t length, size_t header_length) {
        payload_.clear(); // should always be no-op
        payload_.reserve(length + header_length);
        payload_.resize(header_length); // reserve for header
        is_reserved_ = true;
    }

    void packet_writer_base::write_uint8(uint8_t value) { payload_.push_back(value); }

    void packet_writer_base::write_string_null(std::string str) {
        payload_.insert(payload_.end(), std::make_move_iterator(str.begin()), std::make_move_iterator(str.end()));
        payload_.push_back(0);
    }

    void packet_writer_base::write_string_fixed(std::string str) {
        payload_.insert(payload_.end(), std::make_move_iterator(str.begin()), std::make_move_iterator(str.end()));
    }

    std::vector<uint8_t> packet_writer_base::extract_payload() {
        is_reserved_ = false;
        return std::exchange(payload_, {}); // moves payload out & resets it
    }
} // namespace frontend
