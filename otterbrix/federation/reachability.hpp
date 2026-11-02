// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <atomic>
#include <cstdint>

namespace otterstax::federation {

    // What the agents of one connection know of its reachability (the backend actor owns it, the
    // agents read and write it on their pool threads). A connect that failed — a reconnect before a
    // statement or a discovery — marks the connection unreachable for `interval_ms` (10 s): until
    // then an agent that would have to connect answers at once, without trying; the first agent that
    // needs a connect after it tries again, and a connect that succeeds clears the mark. No timer: the
    // mark is read when a statement comes.
    struct reachability_t {
        std::atomic<std::int64_t> unreachable_until_ms{0}; // steady clock; 0: reachable
        std::atomic<std::int64_t> interval_ms{10000};
    };

} // namespace otterstax::federation
