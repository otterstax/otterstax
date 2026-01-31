// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "transaction_manager.hpp"

namespace frontend::postgres {
    std::vector<std::vector<uint8_t>> transaction_manager::handle_begin(packet_writer& writer) {
        if (state_ != transaction_status::IDLE) {
            state_ = transaction_status::TRANSACTION_ERROR;

            return {build_error_response(writer,
                                         sql_state::ACTIVE_SQL_TRANSACTION,
                                         "There is already a transaction in progress",
                                         error_severity::error()),
                    build_ready_for_query(writer, state_)};
        }

        state_ = transaction_status::IN_TRANSACTION;
        return {build_command_complete(writer, command_complete_tag::begin()), build_ready_for_query(writer, state_)};
    }

    std::vector<std::vector<uint8_t>> transaction_manager::handle_commit(packet_writer& writer) {
        if (state_ == transaction_status::TRANSACTION_ERROR) {
            return {
                build_error_response(writer,
                                     sql_state::IN_FAILED_SQL_TRANSACTION,
                                     "Current transaction is aborted, commands ignored until end of transaction block",
                                     error_severity::error()),
                build_ready_for_query(writer, state_)};
        }

        // successful in postgres (even if were IDLE)
        state_ = transaction_status::IDLE;
        savepoints_.clear();
        return {build_command_complete(writer, command_complete_tag::commit()), build_ready_for_query(writer, state_)};
    }

    std::vector<std::vector<uint8_t>> transaction_manager::handle_rollback(packet_writer& writer) {
        state_ = transaction_status::IDLE;
        savepoints_.clear();
        return {build_command_complete(writer, command_complete_tag::rollback()),
                build_ready_for_query(writer, state_)};
    }

    std::vector<std::vector<uint8_t>> transaction_manager::handle_savepoint(packet_writer& writer, std::string name) {
        if (state_ != transaction_status::IN_TRANSACTION) {
            return {build_error_response(writer,
                                         sql_state::NO_ACTIVE_SQL_TRANSACTION,
                                         "SAVEPOINT can only be used in transaction blocks",
                                         error_severity::error()),
                    build_ready_for_query(writer, state_)};
        }

        savepoints_.emplace_back(std::move(name));
        return {build_command_complete(writer, command_complete_tag::savepoint()),
                build_ready_for_query(writer, state_)};
    }

    std::vector<std::vector<uint8_t>> transaction_manager::handle_rollback_to_savepoint(packet_writer& writer,
                                                                                        std::string name) {
        if (state_ == transaction_status::IDLE) {
            return {build_error_response(writer,
                                         sql_state::NO_ACTIVE_SQL_TRANSACTION,
                                         "SAVEPOINT can only be used in transaction blocks",
                                         error_severity::error()),
                    build_ready_for_query(writer, state_)};
        }

        auto it = std::find(savepoints_.begin(), savepoints_.end(), name);
        if (it == savepoints_.end()) {
            state_ = transaction_status::TRANSACTION_ERROR;
            return {build_error_response(writer,
                                         sql_state::INVALID_SAVEPOINT_SPECIFICATION,
                                         "Savepoint \"" + name + "\" does not exist",
                                         error_severity::error()),
                    build_ready_for_query(writer, state_)};
        }

        // rollback: erase all savepoints after
        savepoints_.erase(it + 1, savepoints_.end());
        state_ = transaction_status::IN_TRANSACTION;
        return {build_command_complete(writer, command_complete_tag::rollback()),
                build_ready_for_query(writer, state_)};
    }

    std::vector<std::vector<uint8_t>> transaction_manager::handle_release_savepoint(packet_writer& writer,
                                                                                    std::string name) {
        if (state_ != transaction_status::IN_TRANSACTION) {
            return {build_error_response(writer,
                                         sql_state::NO_ACTIVE_SQL_TRANSACTION,
                                         "SAVEPOINT can only be used in transaction blocks",
                                         error_severity::error()),
                    build_ready_for_query(writer, state_)};
        }

        auto it = std::find(savepoints_.begin(), savepoints_.end(), name);
        if (it == savepoints_.end()) {
            state_ = transaction_status::TRANSACTION_ERROR;
            return {build_error_response(writer,
                                         sql_state::INVALID_SAVEPOINT_SPECIFICATION,
                                         "Savepoint \"" + name + "\" does not exist",
                                         error_severity::error()),
                    build_ready_for_query(writer, state_)};
        }

        // erase all savepoint after
        savepoints_.erase(it, savepoints_.end());
        return {build_command_complete(writer, command_complete_tag::release()), build_ready_for_query(writer, state_)};
    }

    transaction_status transaction_manager::get_transaction_status() const { return state_; }

    void transaction_manager::mark_failed() { state_ = transaction_status::TRANSACTION_ERROR; }
} // namespace frontend::postgres