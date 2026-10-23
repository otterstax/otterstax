// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "statement.hpp"

#include <core/result_wrapper.hpp>

#include <memory>

namespace conn {

    // One backend connection. Not thread-safe: the pool thread that owns it makes every
    // call, one at a time. A call may block, but never past the driver's own timeouts, and
    // never throws: a driver exception is caught and classified inside (errors.hpp).
    //
    // Reconnecting belongs to the driver, because only the driver can tell a stale socket
    // (server restarted, idle connection closed by the server or a balancer) from a stalled
    // backend: the engine's error_code_t has no timeout value, so both arrive as io_error
    // and the pool could not tell them apart. execute() therefore
    //   - reconnects first when an earlier failure dropped the connection — this replaces
    //     the liveness check the old connector managers ran in front of every query;
    //   - on a stale connection reconnects and repeats the statement once, but only when
    //     statement.repeatable (a read);
    //   - never repeats after its deadline expired (the backend is stalled, and a repeat
    //     doubles the wait) and never repeats a statement that is not repeatable — a DML or
    //     DDL whose answer was lost may already have been applied. The result kind does not
    //     decide it: MySQL and PostgreSQL send a DML as a `rows` statement.
    class IDriver {
    public:
        virtual ~IDriver() = default;

        // One attempt. The startup retry ladder (service.connection_retry) stays with the
        // caller of connection_pool_t::add.
        virtual core::error_t connect() = 0;
        virtual statement_outcome_t execute(const statement_t& statement) = 0;
        // One round trip. A dead connection is dropped, so the next execute() reconnects
        // first. The pool sends it only to a connection that sat idle, never before every
        // statement.
        virtual core::error_t ping() = 0;
        virtual void close() noexcept = 0;
        // A new, disconnected driver for the same backend and parameters: how a pool opens
        // another connection. May be called from any thread.
        virtual std::unique_ptr<IDriver> clone_disconnected() const = 0;
    };

} // namespace conn
