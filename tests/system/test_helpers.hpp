// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Helpers shared by the system tests that build an actor graph by hand rather
// than through scheduler_stack_owner: polling a cross-actor future from a plain
// thread, a default-config engine, and the connect params a mock MySQL
// connection needs to register with the catalog.

#pragma once

#include "integration/otterbrix/otterbrix_manager.hpp"
#include "utility/logger.hpp"

#include <boost/mysql/connect_params.hpp>
#include <otterbrix/otterbrix.hpp>

#include <catch2/catch_all.hpp>

#include <chrono>
#include <thread>
#include <utility>

namespace otterstax::test {

    // Polls `future` every 10 ms until it is ready or `timeout` has passed and
    // reports whether it settled. It asserts nothing, so a thread other than the
    // test thread may call it (Catch2 assertions are not thread-safe).
    template<typename Future>
    bool poll_until_ready(Future& future, std::chrono::milliseconds timeout) {
        using namespace std::chrono_literals;
        for (auto waited = 0ms; waited < timeout && !future.is_ready(); waited += 10ms) {
            std::this_thread::sleep_for(10ms);
        }
        return future.is_ready();
    }

    // Test-thread form: the future must settle within 10 s.
    template<typename Future>
    void wait_until_ready(Future& future) {
        using namespace std::chrono_literals;
        REQUIRE(poll_until_ready(future, 10s));
    }

    // A default-config engine (no data directory), for tests that never persist
    // anything — unlike scheduler_stack.hpp's init_test_otterbrix.
    inline db::otterbrix_engine_ptr init_default_test_otterbrix() {
        auto config = configuration::config::default_config();
        initialize_all_loggers(config.log.path.string());
        return db::make_otterbrix_engine(std::move(config));
    }

    // addConnection keeps a MySQL connection only if the catalog could register
    // its schema, and whole-database discovery needs a database name — so the
    // mock connection names one. The mock answers the table listing with zero
    // rows, so registration succeeds with no tables; a table a statement
    // references is then registered lazily on its first query.
    inline boost::mysql::connect_params mock_connect_params() {
        boost::mysql::connect_params params;
        params.database = "db";
        return params;
    }

} // namespace otterstax::test
