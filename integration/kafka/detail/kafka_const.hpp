// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace otterstax::kafka::detail {
    // librdkafka's C++ API takes a raw millisecond count — convert ONLY at that
    // boundary so every duration stays unit-typed on our side
    inline constexpr std::int32_t to_ms(std::chrono::milliseconds timeout) noexcept {
        return static_cast<std::int32_t>(timeout.count());
    }

    // Worker loops (poller + stream)
    inline constexpr std::size_t MAX_BATCH = 500;                     // records per poll -> one transaction
    inline constexpr std::chrono::milliseconds POLL_TIMEOUT{200};     // consumer poll wait before re-checking stop_
    inline constexpr std::chrono::microseconds ENGINE_POLL_STEP{100}; // park while an engine future is not ready

    // KafkaManager's own event loop (distinct from ENGINE_POLL_STEP: this is the
    // manager parking on an idle inbox, not a worker waiting on an engine reply)
    inline constexpr std::chrono::microseconds LOOP_IDLE_STEP{100};

    // Exactly-once transaction lifecycle. init_transactions gets a retry budget
    // rather than a single shot: right after a crash-recovery relaunch it contends
    // with the pre-crash producer's not-yet-fenced transaction (same
    // transactional.id) and returns a RETRIABLE error until fencing completes
    inline constexpr std::chrono::milliseconds TXN_TIMEOUT{30000};      // begin/send_offsets/commit/abort bound
    inline constexpr std::chrono::milliseconds TXN_INIT_TIMEOUT{10000}; // one init_transactions attempt
    inline constexpr std::chrono::seconds TXN_INIT_DEADLINE{60};        // total budget across init_transactions retries
    inline constexpr std::chrono::milliseconds TXN_INIT_BACKOFF{200};   // pause between init_transactions retries

    inline constexpr std::chrono::milliseconds FLUSH_TIMEOUT{10000};         // at-least-once produce flush
    inline constexpr std::chrono::milliseconds PRODUCER_DRAIN_TIMEOUT{5000}; // best-effort drain in the producer dtor
    inline constexpr std::chrono::milliseconds SEEK_TIMEOUT{5000};           // consumer seek (rewind on abort)

    // A batch that fails (engine error, schema mismatch, produce failure) is rewound
    // and retried, never skipped. The retry pauses grow exponentially from
    // BATCH_RETRY_BACKOFF_MIN, capped at BATCH_RETRY_BACKOFF_MAX, so a persistently
    // failing batch does not spin the worker thread or flood the engine/log
    inline constexpr std::chrono::milliseconds BATCH_RETRY_BACKOFF_MIN{200};
    inline constexpr std::chrono::milliseconds BATCH_RETRY_BACKOFF_MAX{10000};
    // Slice of one retry pause between stop_ checks, so a worker being stopped
    // never waits out the whole (capped) pause
    inline constexpr std::chrono::milliseconds BATCH_RETRY_SLEEP_STEP{50};

    // Pause before the next attempt after `consecutive_failures` failed batches
    inline constexpr std::chrono::milliseconds batch_retry_backoff(unsigned consecutive_failures) noexcept {
        std::chrono::milliseconds pause = BATCH_RETRY_BACKOFF_MIN;
        for (unsigned i = 1; i < consecutive_failures && pause < BATCH_RETRY_BACKOFF_MAX; ++i) {
            pause *= 2;
        }
        return pause < BATCH_RETRY_BACKOFF_MAX ? pause : BATCH_RETRY_BACKOFF_MAX;
    }
} // namespace otterstax::kafka::detail
