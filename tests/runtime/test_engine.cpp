// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "runtime_fixture.hpp"


using namespace otterstax::runtime;
using namespace otterstax::runtime::test;

TEST_CASE("engine: rows written before a shutdown are read after a reopen on the same directory") {
    temp_dir_t dir("reopen");
    const components::session::session_id_t session;
    {
        runtime_t runtime(small_runtime());
        auto engine = open_ok(runtime, dir.path());
        REQUIRE(execute_sql(*engine, session, "CREATE DATABASE rt;")->is_success());
        REQUIRE(execute_sql(*engine, session, "CREATE TABLE rt.items (id BIGINT, name STRING);")->is_success());
        auto inserted =
            execute_sql(*engine, session, "INSERT INTO rt.items (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three');");
        REQUIRE(inserted->is_success());
        CHECK(inserted->affected_rows() == 3);
        // The shutdown checkpoint folds the journal into the table files.
        engine.reset();
    }
    CHECK(std::filesystem::exists(dir.path() / ".lock"));

    runtime_t runtime(small_runtime());
    auto engine = open_ok(runtime, dir.path());
    auto rows = execute_sql(*engine, session, "SELECT id, name FROM rt.items ORDER BY id;");
    REQUIRE(rows->is_success());
    REQUIRE(rows->size() == 3);
    CHECK(rows->value(0, 0).value<int64_t>() == 1);
    CHECK(rows->value(0, 2).value<int64_t>() == 3);
    CHECK(std::string{rows->value(1, 1).value<std::string_view>()} == "two");

    // The reopened engine keeps writing.
    REQUIRE(execute_sql(*engine, session, "INSERT INTO rt.items (id, name) VALUES (4, 'four');")->is_success());
    auto count = execute_sql(*engine, session, "SELECT id FROM rt.items;");
    REQUIRE(count->is_success());
    CHECK(count->size() == 4);
}

TEST_CASE("engine: a second engine on a directory another engine owns is refused") {
    temp_dir_t dir("locked");
    runtime_t first_runtime(small_runtime());
    auto first = open_ok(first_runtime, dir.path());

    runtime_t second_runtime(small_runtime());
    auto second = engine_t::open(second_runtime, engine_config(dir.path()));
    REQUIRE(second.has_error());
    CHECK(second.error().type == core::error_code_t::already_exists);
    CHECK(mentions(second.error(), "another engine already owns"));
    // The pools were lent to the refused open: the next engine needs a new runtime.
    temp_dir_t free_dir("locked_other");
    auto again = engine_t::open(second_runtime, engine_config(free_dir.path()));
    REQUIRE(again.has_error());
    CHECK(mentions(again.error(), "new runtime_t"));

    // The first engine is untouched by the refusal.
    const components::session::session_id_t session;
    CHECK(execute_sql(*first, session, "CREATE DATABASE still_alive;")->is_success());

    // Once the owner is gone, the directory opens (on a new runtime: the
    // refusal spent the second one's engine pools).
    first.reset();
    runtime_t third_runtime(small_runtime());
    auto reopened = open_ok(third_runtime, dir.path());
    CHECK(execute_sql(*reopened, session, "CREATE DATABASE after_owner;")->is_success());
}

TEST_CASE("engine: one runtime hosts one engine at a time and its pools are spent by it") {
    temp_dir_t first_dir("one_engine_a");
    temp_dir_t second_dir("one_engine_b");
    runtime_t runtime(small_runtime());
    auto engine = open_ok(runtime, first_dir.path());

    auto busy = engine_t::open(runtime, engine_config(second_dir.path()));
    REQUIRE(busy.has_error());
    CHECK(busy.error().type == core::error_code_t::invalid_parameter);
    CHECK(mentions(busy.error(), "already run an engine"));

    engine.reset();
    auto spent = engine_t::open(runtime, engine_config(second_dir.path()));
    REQUIRE(spent.has_error());
    CHECK(spent.error().type == core::error_code_t::invalid_parameter);
    CHECK(mentions(spent.error(), "new runtime_t"));
}

TEST_CASE("engine: a log that cannot be made is refused without taking the pools") {
    temp_dir_t dir("no_log");
    runtime_t runtime(small_runtime());
    // No data_dir and no log_dir: the engine log has no directory.
    auto refused = engine_t::open(runtime, config::EngineConfig{.data_dir = ""});
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::invalid_parameter);
    // The pools were not lent: the same runtime opens an engine.
    auto engine = open_ok(runtime, dir.path());
    CHECK(engine->dispatcher_address());
}

TEST_CASE("engine: opened and shut down with nothing in flight") {
    temp_dir_t dir("idle");
    for (int cycle = 0; cycle < 3; ++cycle) {
        runtime_t runtime(small_runtime());
        auto engine = open_ok(runtime, dir.path());
        CHECK(engine->dispatcher_address());
        CHECK(engine->resource() == runtime.resource());
    }
}

TEST_CASE("engine: the runtime's sessions pool stops independently of the engine") {
    temp_dir_t dir("pools_order");
    runtime_t runtime(small_runtime());
    auto engine = open_ok(runtime, dir.path());
    // The engine runs on its own pools, whatever became of the sessions pool.
    // (main stops the sessions pool before the engine and the backends pool only
    // after the remote backends are gone, in ~runtime_t.)
    runtime.stop_sessions();
    const components::session::session_id_t session;
    CHECK(execute_sql(*engine, session, "CREATE DATABASE after_pools;")->is_success());
    engine.reset();
}

TEST_CASE("engine: make_engine_configuration puts every directory in data_dir by default") {
    config::EngineConfig engine;
    auto configuration = make_engine_configuration(engine);
    const std::filesystem::path data_dir = config::DEFAULT_ENGINE_DATA_DIR;
    CHECK(configuration.main_path == data_dir);
    CHECK(configuration.wal.path == data_dir);
    CHECK(configuration.disk.path == data_dir);
    CHECK(configuration.log.path == data_dir);
    CHECK(configuration.log.level == static_cast<log_t::level>(SPDLOG_ACTIVE_LEVEL));
    const configuration::config_execution otterbrix_defaults{};
    CHECK(configuration.execution.executor_pool_size == otterbrix_defaults.executor_pool_size);
    CHECK(configuration.execution.dml_flush_row_threshold == otterbrix_defaults.dml_flush_row_threshold);
    CHECK(configuration.execution.pump.in_flight == otterbrix_defaults.pump.in_flight);
    CHECK(configuration.execution.pump.idle == otterbrix_defaults.pump.idle);
}

TEST_CASE("engine: make_engine_configuration takes every service.engine knob") {
    config::EngineConfig engine;
    engine.data_dir = "/srv/otterstax/data";
    engine.log_dir = "/srv/otterstax/log";
    auto configuration = make_engine_configuration(engine);
    CHECK(configuration.main_path == "/srv/otterstax/data");
    CHECK(configuration.wal.path == "/srv/otterstax/data");
    CHECK(configuration.disk.path == "/srv/otterstax/data");
    CHECK(configuration.log.path == "/srv/otterstax/log");
}
