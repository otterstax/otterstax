// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../packet/packet_utils.hpp"

namespace frontend::postgres {
    class transaction_manager {
    public:
        std::vector<std::vector<uint8_t>> handle_begin(packet_writer& writer);
        std::vector<std::vector<uint8_t>> handle_commit(packet_writer& writer);
        std::vector<std::vector<uint8_t>> handle_rollback(packet_writer& writer);

        std::vector<std::vector<uint8_t>> handle_savepoint(packet_writer& writer, std::string name);
        std::vector<std::vector<uint8_t>> handle_rollback_to_savepoint(packet_writer& writer, std::string name);
        std::vector<std::vector<uint8_t>> handle_release_savepoint(packet_writer& writer, std::string name);

        transaction_status get_transaction_status() const;
        void mark_failed();

    private:
        transaction_status state_ = transaction_status::IDLE;
        std::vector<std::string> savepoints_{};
    };
} // namespace frontend::postgres