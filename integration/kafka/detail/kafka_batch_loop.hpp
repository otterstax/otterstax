// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#pragma once

#include "kafka_const.hpp"
#include "kafka_consumer.hpp"

#include <atomic>

namespace otterstax::kafka::detail {
    // A source has one batch in flight. Keep its payloads until the engine
    // accepts them; a failed transaction retries that batch without fetching
    // later records or depending on a broker seek to replay it.
    template<typename Consumer, typename Apply, typename Acknowledge, typename Failure>
    void consume_batches(Consumer& consumer,
                         const std::atomic<bool>& stop,
                         Apply&& apply,
                         Acknowledge&& acknowledge,
                         Failure&& failure) {
        std::vector<kafka_record_t> records;
        while (!stop.load(std::memory_order_relaxed)) {
            if (records.empty()) {
                records = consumer.poll_batch(MAX_BATCH, POLL_TIMEOUT);
                if (records.empty()) {
                    continue;
                }
            }
            if (auto error = apply(records); error.contains_error()) {
                failure("apply", error);
                continue;
            }
            acknowledge(records);
            records.clear();
        }
    }
} // namespace otterstax::kafka::detail
