// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_writer.hpp"

#include <utility>

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
            payload_.insert(payload_.begin(), PACKET_HEADER_SIZE, 0);
        }

        uint32_t length = static_cast<uint32_t>(payload_.size()) - PACKET_HEADER_SIZE;
        payload_[0] = extract_nth_byte_le<0>(length);
        payload_[1] = extract_nth_byte_le<1>(length);
        payload_[2] = extract_nth_byte_le<2>(length);
        payload_[3] = sequence_id;

        return extract_payload();
    }

    std::vector<std::vector<uint8_t>> packet_writer::build_from_payload_pieces() {
        if (!is_reserved_) {
            payload_.insert(payload_.begin(), PACKET_HEADER_SIZE, 0);
            is_reserved_ = true;
        }
        const size_t payload_size = payload_.size() - PACKET_HEADER_SIZE;
        if (payload_size < MAX_PACKET_SIZE) {
            std::vector<std::vector<uint8_t>> one;
            one.push_back(build_from_payload(0));
            return one;
        }

        const uint32_t full_pieces = static_cast<uint32_t>(payload_size / MAX_PACKET_SIZE);
        const uint32_t tail = static_cast<uint32_t>(payload_size % MAX_PACKET_SIZE);

        const auto piece = [this](size_t offset, uint32_t length) {
            const uint8_t head[PACKET_HEADER_SIZE] = {extract_nth_byte_le<0>(length),
                                                      extract_nth_byte_le<1>(length),
                                                      extract_nth_byte_le<2>(length),
                                                      0}; // the caller stamps the pieces' sequence ids

            std::vector<uint8_t> packet;
            packet.reserve(PACKET_HEADER_SIZE + length);
            packet.insert(packet.end(), head, head + PACKET_HEADER_SIZE);
            packet.insert(packet.end(),
                          payload_.begin() + static_cast<std::ptrdiff_t>(offset),
                          payload_.begin() + static_cast<std::ptrdiff_t>(offset) + length);
            return std::move(packet);
        };

        std::vector<std::vector<uint8_t>> packets;
        packets.reserve(full_pieces + 1);

        size_t offset = PACKET_HEADER_SIZE;
        for (uint32_t i = 0; i < full_pieces; ++i) {
            packets.push_back(piece(offset, MAX_PACKET_SIZE));
            offset += MAX_PACKET_SIZE;
        }
        packets.push_back(piece(offset, tail));

        std::ignore = extract_payload(); // the payload went out in pieces; reset the writer
        return packets;
    }
} // namespace frontend::mysql
