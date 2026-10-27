// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utility/string_hash.hpp"

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>

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

    // Runs on the pool thread once a request's answer settles and its promise is released, for an awaiter
    // that does not poll: settling schedules nobody, so a Worker's waker re-enqueues it or sends Worker::wake
    // if not parked (wait.hpp, scheduler/parking.hpp); empty for a poller (CatalogManager, test::drive).
    using waker_t = std::function<void()>;

    // Alias → its pool (connection_pool_t, transfer_pools_t), on the registry's resource. The
    // hash is transparent, so a lookup by string_view builds no string.
    template<class Pool>
    using alias_map_t = otterstax::pmr_string_map_t<std::unique_ptr<Pool>>;

} // namespace conn
