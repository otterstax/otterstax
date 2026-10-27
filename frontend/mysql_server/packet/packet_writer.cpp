// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_writer.hpp"

#include <algorithm>

namespace frontend::mysql {
    void packet_writer::reserve_payload(size_t length) {
        packet_writer_base::reserve_payload(length, PACKET_HEADER_SIZE);
    }

    void packet_writer::write_int16(int16_t value) { push_data_bytes<int16_t, endian::LITTLE>(payload_, value); }
    void packet_writer::write_uint16(uint16_t value) { push_data_bytes<uint16_t, endian::LITTLE>(payload_, value); }
    void packet_writer::write_int32(int32_t value) { push_data_bytes<int32_t, endian::LITTLE>(payload_, value); }
    void packet_writer::write_uint32(uint32_t value) { push_data_bytes<uint32_t, endian::LITTLE>(payload_, value); }
    void packet_writer::write_int64(int64_t value) { push_data_bytes<int64_t, endian::LITTLE>(payload_, value); }
    void packet_writer::write_uint64(uint64_t value) { push_data_bytes<uint64_t, endian::LITTLE>(payload_, value); }

    void packet_writer::write_zeros(size_t count) { payload_.insert(payload_.end(), count, 0); }

    void packet_writer::write_length_encoded_integer(uint64_t value) {
        switch (get_length_encoded_int_size(value)) {
            case length_encoded_int_size::ONE_BYTE:
                payload_.push_back(static_cast<uint8_t>(value));
                break;
            case length_encoded_int_size::THREE_BYTES:
                payload_.push_back(TWO_BYTE_INT_MARKER);
                write_uint16(static_cast<uint16_t>(value));
                break;
            case length_encoded_int_size::FOUR_BYTES:
                payload_.push_back(THREE_BYTE_INT_MARKER);
                push_nth_bytes<uint64_t, 3, endian::LITTLE>(payload_, value);
                break;
            case length_encoded_int_size::NINE_BYTES:
                payload_.push_back(EIGHT_BYTE_INT_MARKER);
                push_data_bytes<uint64_t, endian::LITTLE>(payload_, value);
                break;
        }
    }

    void packet_writer::write_length_encoded_string(std::string_view str) {
        write_length_encoded_integer(str.size());
        write_string_fixed(str);
    }

    void packet_writer::write_null() {
        payload_.push_back(0xFB); // NULL marker
    }

    std::vector<uint8_t> packet_writer::build_from_payload(uint8_t sequence_id) {
        if (!is_reserved_) {
            // todo: add warning?
            payload_.insert(payload_.begin(), PACKET_HEADER_SIZE, 0);
        }

        uint32_t length = static_cast<uint32_t>(payload_.size()) - PACKET_HEADER_SIZE;
        payload_[0] = extract_nth_byte_le<0>(length);
        payload_[1] = extract_nth_byte_le<1>(length);
        payload_[2] = extract_nth_byte_le<2>(length);
        payload_[3] = sequence_id;

        return extract_payload();
    }

    namespace {
        // The largest payload one packet carries; a longer one continues in the next packet.
        constexpr size_t max_packet_payload = 0xFFFFFF;
    } // namespace

    void packet_writer::append_frame(std::vector<uint8_t>& out) {
        const size_t body_start = is_reserved_ ? PACKET_HEADER_SIZE : 0;
        const uint8_t* body = payload_.data() + body_start;
        size_t remaining = payload_.size() - body_start;
        size_t fragment = 0;
        do {
            fragment = std::min(remaining, max_packet_payload);
            const uint32_t length = static_cast<uint32_t>(fragment);
            const uint8_t header[PACKET_HEADER_SIZE] = {extract_nth_byte_le<0>(length),
                                                         extract_nth_byte_le<1>(length),
                                                         extract_nth_byte_le<2>(length),
                                                         0};
            out.insert(out.end(), header, header + PACKET_HEADER_SIZE);
            out.insert(out.end(), body, body + fragment);
            body += fragment;
            remaining -= fragment;
        } while (fragment == max_packet_payload);
        payload_.clear();
        is_reserved_ = false;
    }

    void assign_sequence_ids(std::span<uint8_t> frames, uint8_t& sequence_id) {
        size_t position = 0;
        while (position + PACKET_HEADER_SIZE <= frames.size()) {
            const size_t length = static_cast<size_t>(frames[position]) |
                                  (static_cast<size_t>(frames[position + 1]) << 8) |
                                  (static_cast<size_t>(frames[position + 2]) << 16);
            frames[position + 3] = sequence_id++;
            position += PACKET_HEADER_SIZE + length;
        }
    }
} // namespace frontend::mysql
