// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

// A resultset's build_packets answers every packet back to back in one buffer (what one write
// sends); the tests read it packet by packet.
namespace otterstax::test {

    // MySQL: a 3-byte little-endian payload length, the sequence id, the payload.
    inline std::vector<std::vector<uint8_t>> split_mysql_packets(const std::vector<uint8_t>& frames) {
        std::vector<std::vector<uint8_t>> packets;
        std::size_t position = 0;
        while (position + 4 <= frames.size()) {
            const std::size_t length = static_cast<std::size_t>(frames[position]) |
                                       (static_cast<std::size_t>(frames[position + 1]) << 8) |
                                       (static_cast<std::size_t>(frames[position + 2]) << 16);
            packets.emplace_back(frames.begin() + static_cast<std::ptrdiff_t>(position),
                                 frames.begin() + static_cast<std::ptrdiff_t>(position + 4 + length));
            position += 4 + length;
        }
        return packets;
    }

    // PostgreSQL: the type byte, a 4-byte big-endian length counting itself, the body.
    inline std::vector<std::vector<uint8_t>> split_pg_messages(const std::vector<uint8_t>& frames) {
        std::vector<std::vector<uint8_t>> messages;
        std::size_t position = 0;
        while (position + 5 <= frames.size()) {
            const std::size_t length = (static_cast<std::size_t>(frames[position + 1]) << 24) |
                                       (static_cast<std::size_t>(frames[position + 2]) << 16) |
                                       (static_cast<std::size_t>(frames[position + 3]) << 8) |
                                       static_cast<std::size_t>(frames[position + 4]);
            messages.emplace_back(frames.begin() + static_cast<std::ptrdiff_t>(position),
                                  frames.begin() + static_cast<std::ptrdiff_t>(position + 1 + length));
            position += 1 + length;
        }
        return messages;
    }

} // namespace otterstax::test
