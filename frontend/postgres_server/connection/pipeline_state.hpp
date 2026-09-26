// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>

namespace frontend::postgres {
    // Error state of the extended-query pipeline of one connection. A pipeline
    // runs from its first extended-query message (Parse, Bind, Describe,
    // Execute, Close, Flush) to its Sync. An error inside it is answered with
    // ErrorResponse alone: every message up to the Sync is discarded, and the
    // Sync sends the one ReadyForQuery. An error outside a pipeline (a simple
    // Query) is answered with ErrorResponse and ReadyForQuery at once.
    class pipeline_state {
    public:
        // An extended-query message is being processed.
        void begin_pipeline();

        // Records an error. Answers true when the ReadyForQuery is deferred to
        // the Sync of the running pipeline, false when the error closes its
        // own exchange (no pipeline is running: nothing is recorded).
        [[nodiscard]] bool set_error();
        bool has_error() const;

        // The pipeline is over — its Sync arrived, or a simple Query, which
        // answers ReadyForQuery itself, is being processed.
        void end_pipeline();

    private:
        bool in_pipeline_ = false;
        bool has_error_ = false;
    };
} // namespace frontend::postgres
