// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utility/logger.hpp"

#include <core/result_wrapper.hpp>

#include <algorithm>
#include <chrono>
#include <string>

namespace otterstax::startup {

    // Opens one configured backend at startup.
    //
    // A backend that refuses or half-accepts the connection (a container reported
    // healthy a moment before it listens, discovery hitting a not-yet-ready
    // server) is retried up to `max_attempts` with `delay` between tries. One that
    // stays unreachable is reported as `false`: the server still starts, without
    // that backend, and the failure is already in the log. A descriptor the
    // backend can never accept (`invalid_parameter`: a malformed port, a value
    // outside its range) is not something a retry can fix, so it comes back as
    // the error and startup aborts instead of running with a config nobody meant.
    //
    // `open` yields the connector manager's core::result_wrapper_t<std::string>;
    // `sleep_for` takes the std::chrono::milliseconds to wait between attempts.
    template<typename Open, typename SleepFor>
    core::result_wrapper_t<bool> open_with_retry(log_t& log,
                                                 const char* kind,
                                                 const std::string& alias,
                                                 int max_attempts,
                                                 std::chrono::milliseconds delay,
                                                 Open&& open,
                                                 SleepFor&& sleep_for) {
        const int attempts = std::max(1, max_attempts);
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            auto opened = open();
            if (!opened.has_error()) {
                log->info("Registered {} connection '{}'", kind, alias);
                return true;
            }
            if (opened.error().type == core::error_code_t::invalid_parameter) {
                log->error("Invalid {} connection '{}': {}", kind, alias, opened.error().what.c_str());
                return opened.template convert_error<bool>();
            }
            if (attempt == attempts) {
                log->error("Failed to register {} connection '{}' after {} attempt(s): {}",
                           kind,
                           alias,
                           attempts,
                           opened.error().what.c_str());
                return false;
            }
            log->warn("{} connection '{}' not ready ({}/{}): {} — retrying in {} ms...",
                      kind,
                      alias,
                      attempt,
                      attempts,
                      opened.error().what.c_str(),
                      delay.count());
            sleep_for(delay);
        }
        return false;
    }

} // namespace otterstax::startup
