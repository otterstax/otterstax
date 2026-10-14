// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <cstdint>

// The transaction block of a connection's engine session as its query session
// knows it after each statement. The engine keeps the block itself (BEGIN /
// COMMIT / ROLLBACK, an error failing it until ROLLBACK) and answers no
// question about it, so the session follows it from what every statement of
// the connection was and how it ended. Every statement of the connection —
// whatever its route — passes through the session, and every failure is
// reported to the engine on that session (query_session_t::settle), which is
// what keeps the two in step.
namespace otterstax::session {

    enum class transaction_state_t : std::uint8_t
    {
        // No explicit transaction: every statement commits on its own.
        idle,
        // Inside BEGIN ... COMMIT / ROLLBACK.
        in_block,
        // The block failed (an error inside it): only ROLLBACK, or a COMMIT that
        // ends it as that rollback, is accepted until it ends.
        failed,
    };

    // The transaction statement a statement is, read off its plan root.
    enum class transaction_command_t : std::uint8_t
    {
        none,
        begin,
        commit,
        rollback,
    };

    // The block after a statement of kind `command` ran in a block that was
    // `before`. `succeeded` false with `error` the statement's error code
    // (after the failure was reported to the engine on the connection's session).
    //
    // Mirrors manager_dispatcher_t (otterbrix services/dispatcher/dispatcher.cpp):
    // - BEGIN opens a block, and inside one keeps it (a stray BEGIN does not
    //   restart it); COMMIT and ROLLBACK end it. COMMIT of a failed block runs
    //   the rollback and answers transaction_finalized: the block has ended.
    // - A COMMIT ends the block whatever it answers. Its operator
    //   (operator_commit_transaction_t) first drains the transaction
    //   (txn_commit_drain_msg: txn_manager_.commit, or abort for an empty one)
    //   and only then runs the steps that can fail (the index commit, the WAL
    //   marker); so when one fails, finish_failed_statement_ finds no
    //   transaction of the session and marks nothing failed — the engine
    //   session is idle, as PostgreSQL leaves it after a failed COMMIT.
    // - The engine refuses everything but COMMIT / ROLLBACK in a failed block as
    //   transaction_finalized: the block stays failed, whatever this session
    //   believed before.
    // - Any other failure fails an open block; outside a block nothing changes.
    constexpr transaction_state_t next_transaction_state(transaction_state_t before,
                                                         transaction_command_t command,
                                                         bool succeeded,
                                                         core::error_code_t error) noexcept {
        if (command == transaction_command_t::rollback || command == transaction_command_t::commit) {
            return transaction_state_t::idle;
        }
        if (succeeded) {
            // A failed block runs nothing but COMMIT / ROLLBACK (the engine
            // refuses the rest, the session refuses what does not reach the
            // engine), so a statement that ran proves there is none.
            const auto base = before == transaction_state_t::failed ? transaction_state_t::idle : before;
            if (command == transaction_command_t::begin && base == transaction_state_t::idle) {
                return transaction_state_t::in_block;
            }
            return base;
        }
        if (error == core::error_code_t::transaction_finalized) {
            return transaction_state_t::failed;
        }
        return before == transaction_state_t::idle ? transaction_state_t::idle : transaction_state_t::failed;
    }

} // namespace otterstax::session
