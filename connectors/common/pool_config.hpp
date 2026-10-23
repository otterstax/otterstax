// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

namespace conn {

    // Sizing and timing of one work_pool_t: the `pool:` block of a connection in
    // config.yaml (config::PoolConfig), or service.file_transfers for local files.
    struct pool_config_t {
        std::size_t min_size{1};                            // opened at startup, never closed
        std::size_t max_size{4};                            // hard cap on sessions (connections) at once
        std::chrono::milliseconds idle_timeout{60'000};     // a session above min_size idle this long closes
        std::chrono::milliseconds check_after_idle{30'000}; // a connection idle this long is pinged before use
        std::chrono::milliseconds queue_timeout{30'000};    // a request waiting longer than this is refused
        std::chrono::milliseconds connect_backoff{1'000};   // wait after a failed connect before the next one
    };

    // Called on the pool thread once a request's answer is settled and its promise released,
    // for an awaiter that does not poll on its own: settling a promise schedules nobody. A
    // Worker parks while it awaits and its waker enqueues it again, or sends it a message when
    // it is not parked (wait.hpp, scheduler/parking.hpp). Empty for an awaiter that polls
    // until the answer is there (CatalogManager's enqueue_impl, otterstax::test::drive).
    using waker_t = std::function<void()>;

    // Alias → its pool (connection_pool_t, transfer_pools_t). The hash is transparent, so a
    // lookup by string_view builds no string.
    struct alias_hash {
        using is_transparent = void;
        std::size_t operator()(std::string_view alias) const noexcept { return std::hash<std::string_view>{}(alias); }
    };
    template<class Pool>
    using alias_map_t = std::unordered_map<std::string, std::unique_ptr<Pool>, alias_hash, std::equal_to<>>;

} // namespace conn
