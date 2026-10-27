// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "statement.hpp"

#include <core/result_wrapper.hpp>

#include <memory>

namespace conn {

    // One backend connection. Not thread-safe: the pool thread that owns it makes each call, one
    // at a time; calls may block within the driver's timeouts and never throw (errors.hpp).
    // Only the driver can tell a stale socket from a stalled backend: error_code_t has no timeout,
    // so both are io_error. execute() reconnects after a drop and repeats a stale statement once,
    // but only a statement.repeatable read before its deadline; a DML/DDL may already be applied.
    // The result kind cannot tell (MySQL/PostgreSQL send a DML as a `rows` statement).
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
