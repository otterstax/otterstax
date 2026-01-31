// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_writer.hpp"

#include "../../common/protocol_config.hpp"

namespace frontend::postgres {
    void packet_writer::reserve_payload(size_t length) {
        packet_writer_base::reserve_payload(length, PACKET_HEADER_SIZE);
    }

    void packet_writer::write_int16(int16_t value) { push_data_bytes<int16_t, endian::BIG>(payload_, value); }
    void packet_writer::write_uint16(uint16_t value) { push_data_bytes<uint16_t, endian::BIG>(payload_, value); }
    void packet_writer::write_int32(int32_t value) { push_data_bytes<int32_t, endian::BIG>(payload_, value); }
    void packet_writer::write_uint32(uint32_t value) { push_data_bytes<uint32_t, endian::BIG>(payload_, value); }
    void packet_writer::write_int64(int64_t value) { push_data_bytes<int64_t, endian::BIG>(payload_, value); }
    void packet_writer::write_uint64(uint64_t value) { push_data_bytes<uint64_t, endian::BIG>(payload_, value); }

    void write_uint64_be(uint64_t value);

    std::vector<uint8_t> packet_writer::build_from_payload(char message_type) {
        if (!is_reserved_) {
            // todo: add warning?
            payload_.insert(payload_.begin(), PACKET_HEADER_SIZE, 0);
        }

        int32_t length = payload_.size() - 1; // type char does not count toward length
        payload_[0] = message_type;
        payload_[1] = extract_nth_byte_be<0, 4>(length);
        payload_[2] = extract_nth_byte_be<1, 4>(length);
        payload_[3] = extract_nth_byte_be<2, 4>(length);
        payload_[4] = extract_nth_byte_be<3, 4>(length);

        return extract_payload();
    }
} // namespace frontend::postgres
