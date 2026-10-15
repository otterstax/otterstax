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
#include <boost/filesystem/operations.hpp>
#include <filesystem>
#include <system_error>
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

    // Even an otherwise empty engine persists its system catalog. Own one
    // unique directory per instance so parallel tests cannot share that catalog.
    class isolated_test_engine {
    public:
        isolated_test_engine() {
            do {
                directory_ = std::filesystem::temp_directory_path() /
                    boost::filesystem::unique_path("otterstax-engine-%%%%-%%%%-%%%%-%%%%").string();
            } while (!std::filesystem::create_directory(directory_));
            try {
                auto config = configuration::config::create_config(directory_);
                initialize_all_loggers(config.log.path.string());
                engine_ = db::make_otterbrix_engine(config);
            } catch (...) {
                std::error_code ignored;
                std::filesystem::remove_all(directory_, ignored);
                throw;
            }
        }
        isolated_test_engine(const isolated_test_engine&) = delete;
        isolated_test_engine& operator=(const isolated_test_engine&) = delete;
        ~isolated_test_engine() {
            engine_.reset();
            std::error_code ignored;
            std::filesystem::remove_all(directory_, ignored);
        }
        const db::otterbrix_engine_ptr& engine() const { return engine_; }
    private:
        std::filesystem::path directory_;
        db::otterbrix_engine_ptr engine_;
    };

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
