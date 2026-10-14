// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "runtime_fixture.hpp"

#include "utility/logger.hpp"

#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <memory>
#include <sstream>
#include <string>
#include <thread>

using namespace otterstax::runtime;
using namespace otterstax::runtime::test;

TEST_CASE("runtime: starts its sessions and backends pools and stops them with no engine") {
    runtime_t runtime(small_runtime());
    CHECK(runtime.resource() != nullptr);
    CHECK(runtime.sessions_scheduler() != nullptr);
    CHECK(runtime.backends_scheduler() != nullptr);

    // Idempotent, and the destructor stops what is left (the backends pool);
    // an engine pool never started is not stopped.
    runtime.stop_sessions();
    runtime.stop_sessions();
}

TEST_CASE("runtime: destroyed while running joins its pools") {
    for (int cycle = 0; cycle < 3; ++cycle) {
        runtime_t runtime(small_runtime());
        CHECK(runtime.sessions_scheduler() != nullptr);
    }
}

TEST_CASE("runtime: pool sizes come from the config and 0 sizes to the machine") {
    config::RuntimeConfig sized = small_runtime();
    sized.sessions.threads = 3;
    sized.backends.threads = 5;
    runtime_t runtime(sized);
    CHECK(runtime.sessions_scheduler()->num_workers() == 3);
    CHECK(runtime.backends_scheduler()->num_workers() == 5);

    const std::size_t machine = std::max<std::size_t>(2, std::thread::hardware_concurrency());
    CHECK(resolve_threads(0) == machine);
    CHECK(resolve_threads(7) == 7);
    runtime_t defaults;
    CHECK(defaults.sessions_scheduler()->num_workers() == machine);
    CHECK(defaults.backends_scheduler()->num_workers() == machine);
}

TEST_CASE("engine log: every engine writes through the Main logger's sinks and none is registered") {
    // A sink added to Main before the engines open is one their logs copy.
    auto main_log = get_logger(logger_tag::Main);
    REQUIRE(main_log.is_valid());
    std::ostringstream lines;
    main_log->sinks().push_back(std::make_shared<spdlog::sinks::ostream_sink_mt>(lines));

    {
        // Two engines in one process would collide in spdlog's registry.
        temp_dir_t first_dir("log_a");
        temp_dir_t second_dir("log_b");
        runtime_t first_runtime(small_runtime());
        runtime_t second_runtime(small_runtime());
        auto first = open_ok(first_runtime, first_dir.path());
        auto second = open_ok(second_runtime, second_dir.path());
        // The engine log runs at SPDLOG_ACTIVE_LEVEL (ERROR by default), where a clean open
        // writes nothing: a refused open (the pools are lent) writes its error through it.
        CHECK(engine_t::open(first_runtime, engine_config(first_dir.path())).has_error());
        CHECK(spdlog::get("otterbrix") == nullptr);
    }

    main_log->sinks().pop_back();
    CHECK(lines.str().find("[otterbrix]") != std::string::npos);
}
