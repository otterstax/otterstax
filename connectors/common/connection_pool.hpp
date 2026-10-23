// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "driver.hpp"
#include "pool_config.hpp"
#include "statement.hpp"
#include "work_pool.hpp"

#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <unordered_map>

namespace conn {

    // What one pool thread owns: one backend connection. Before running a statement on a
    // connection that sat idle longer than check_after_idle it pings it (PgBouncer's
    // server_check_delay): the server or a balancer may have dropped it meanwhile, and a
    // DML sent on a dead socket cannot be repeated (IDriver). A connection in steady use is
    // never pinged.
    class driver_session_t {
    public:
        using request_t = statement_t;
        using outcome_t = statement_outcome_t;
        using clock = std::chrono::steady_clock;

        driver_session_t(std::unique_ptr<IDriver> driver, std::chrono::milliseconds check_after_idle);

        core::error_t open();
        outcome_t run(const statement_t& statement);
        void close() noexcept;

    private:
        std::unique_ptr<IDriver> driver_;
        std::chrono::milliseconds check_after_idle_;
        clock::time_point last_used_;
    };

    using alias_pool_t = work_pool_t<driver_session_t>;
    using statement_future_t = alias_pool_t::future_t;

    // Every connection of one backend type, by alias: one alias_pool_t each. Filled on the
    // startup thread before the first statement (ComponentManager::register_connections) and
    // read-only afterwards, so execute() takes no lock; each alias_pool_t has its own.
    class connection_pool_t {
    public:
        explicit connection_pool_t(std::pmr::memory_resource* resource);
        ~connection_pool_t();

        connection_pool_t(const connection_pool_t&) = delete;
        connection_pool_t& operator=(const connection_pool_t&) = delete;

        // Startup thread only. `prototype` is a disconnected driver for the alias; every
        // connection the pool opens is prototype->clone_disconnected(). Opens min_size
        // connections; on failure the alias is not registered.
        [[nodiscard]] core::error_t add(const std::string& alias, std::unique_ptr<IDriver> prototype, pool_config_t config);
        // Startup thread only: a connection whose schema registration failed is not kept.
        void drop(const std::string& alias);
        bool contains(std::string_view alias) const noexcept;

        // Any thread; never blocks. An unknown alias gets an already-settled do_not_exists.
        // `waker` is called once the answer is settled (pool_config.hpp).
        statement_future_t execute(std::string_view alias, statement_t statement, waker_t waker = {});

        // The alias's pool, or null; for tests and plots.
        const alias_pool_t* find(std::string_view alias) const noexcept;

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

    private:
        std::pmr::memory_resource* resource_;
        alias_map_t<alias_pool_t> aliases_;
    };

} // namespace conn
