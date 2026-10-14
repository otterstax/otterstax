// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utility/logger.hpp"

#include <core/result_wrapper.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace otterstax::backends {

    // Why one attempt to open a backend failed — said by the attempt itself, at
    // the step that failed, never read off the error's code (a code may come
    // from the backend, the connector or the engine, and means nothing about
    // whether a retry can help).
    enum class open_failure_kind_t : std::uint8_t
    {
        // The connection or the discovery failed (a container reported healthy a
        // moment before it listens, a server not ready yet): retried.
        unreachable,
        // The backend answered, but what it answered cannot be used (a discovered
        // column the federation metadata cannot record): the same answer comes
        // back on every attempt, so it is not retried; the backend is left out.
        unusable,
    };

    struct open_failure_t {
        open_failure_kind_t kind;
        core::error_t error;
    };

    // Opens one configured backend at startup.
    //
    // `open` makes one attempt and answers std::nullopt when the backend opened,
    // or the open_failure_t that says why not. An unreachable backend is retried
    // up to `max_attempts` with `delay` between tries; an unusable one is not.
    // Either way a backend that did not open is reported as `false`: the server
    // still starts, without that backend, and the failure is already in the log.
    // A configuration error is not this helper's: the descriptor is checked
    // before the first attempt (remote_backends_t::open), so nothing an attempt
    // answers can abort startup.
    //
    // `sleep_for` takes the std::chrono::milliseconds to wait between attempts.
    template<typename Open, typename SleepFor>
    bool open_with_retry(log_t& log,
                         const char* kind,
                         const std::string& alias,
                         int max_attempts,
                         std::chrono::milliseconds delay,
                         Open&& open,
                         SleepFor&& sleep_for) {
        const int attempts = std::max(1, max_attempts);
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            std::optional<open_failure_t> failed = open();
            if (!failed) {
                log->info("Registered {} connection '{}'", kind, alias);
                return true;
            }
            if (failed->kind == open_failure_kind_t::unusable) {
                log->error("{} connection '{}' left out: {}", kind, alias, failed->error.what.c_str());
                return false;
            }
            if (attempt == attempts) {
                log->error("Failed to register {} connection '{}' after {} attempt(s): {}",
                           kind,
                           alias,
                           attempts,
                           failed->error.what.c_str());
                return false;
            }
            log->warn("{} connection '{}' not ready ({}/{}): {} — retrying in {} ms...",
                      kind,
                      alias,
                      attempt,
                      attempts,
                      failed->error.what.c_str(),
                      delay.count());
            sleep_for(delay);
        }
        return false;
    }

} // namespace otterstax::backends
