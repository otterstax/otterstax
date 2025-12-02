// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "packet_writer.hpp"

namespace mysql_front {
    constexpr uint8_t PACKET_HEADER_SIZE = 4;

    void packet_writer::reserve_payload(size_t length) {
        payload_.clear(); // should always be no-op
        payload_.reserve(length + PACKET_HEADER_SIZE);
        payload_.resize(PACKET_HEADER_SIZE); // reserve for header
        is_reserved_ = true;
    }

    void packet_writer::write_uint8(uint8_t value) { payload_.push_back(value); }

    void packet_writer::write_uint16_le(uint16_t value) { push_nth_bytes<2>(payload_, value); }

    void packet_writer::write_uint32_le(uint32_t value) { push_nth_bytes<4>(payload_, value); }

    void packet_writer::write_uint64_le(uint64_t value) { push_nth_bytes<8>(payload_, value); }

    void packet_writer::write_zeros(size_t count) { payload_.insert(payload_.end(), count, 0); }

    void packet_writer::write_length_encoded_integer(uint64_t value) {
        switch (get_length_encoded_int_size(value)) {
            case length_encoded_int_size::ONE_BYTE:
                payload_.push_back(static_cast<uint8_t>(value));
                break;
            case length_encoded_int_size::THREE_BYTES:
                payload_.push_back(TWO_BYTE_INT_MARKER);
                write_uint16_le(static_cast<uint16_t>(value));
                break;
            case length_encoded_int_size::FOUR_BYTES:
                payload_.push_back(THREE_BYTE_INT_MARKER);
                push_nth_bytes<3>(payload_, value);
                break;
            case length_encoded_int_size::NINE_BYTES:
                payload_.push_back(EIGHT_BYTE_INT_MARKER);
                push_nth_bytes<8>(payload_, value);
                break;
        }
    }

    void packet_writer::write_string_null(std::string str) {
        payload_.insert(payload_.end(), std::make_move_iterator(str.begin()), std::make_move_iterator(str.end()));
        payload_.push_back(0);
    }

    void packet_writer::write_string_fixed(std::string str) {
        payload_.insert(payload_.end(), std::make_move_iterator(str.begin()), std::make_move_iterator(str.end()));
    }

    void packet_writer::write_length_encoded_string(std::string str) {
        write_length_encoded_integer(str.size());
        write_string_fixed(std::move(str));
    }

    void packet_writer::write_null() {
        payload_.push_back(0xFB); // NULL marker
    }

    std::vector<uint8_t> packet_writer::build_from_payload(uint8_t sequence_id) {
        if (!is_reserved_) {
            // todo: add warning?
            payload_.insert(payload_.begin(), 4, 0);
        }

        uint32_t length = static_cast<uint32_t>(payload_.size()) - PACKET_HEADER_SIZE;
        payload_[0] = extract_nth_byte<0>(length);
        payload_[1] = extract_nth_byte<1>(length);
        payload_[2] = extract_nth_byte<2>(length);
        payload_[3] = sequence_id;

        is_reserved_ = false;
        return std::exchange(payload_, {}); // moves payload out & resets it
    }
} // namespace mysql_front