// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "config/config.hpp"

#include <catch2/catch_all.hpp>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory_resource>
#include <string>
#include <string_view>
#include <unistd.h>

// `service.engine` and `service.runtime` through the whole-file load_config.

namespace {

    std::pmr::memory_resource* resource() { return std::pmr::new_delete_resource(); }

    // A config file in the temp dir, removed when the object goes.
    class temp_config_t final {
    public:
        explicit temp_config_t(const std::string& content) {
            static std::atomic<int> counter{0};
            path_ = (std::filesystem::temp_directory_path() /
                     ("otterstax_engine_cfg_" + std::to_string(static_cast<long>(::getpid())) + "_" +
                      std::to_string(counter++) + ".yaml"))
                        .string();
            std::ofstream out(path_);
            out << content;
        }
        temp_config_t(const temp_config_t&) = delete;
        temp_config_t& operator=(const temp_config_t&) = delete;
        ~temp_config_t() {
            std::error_code ec;
            std::filesystem::remove(path_, ec);
        }
        [[nodiscard]] const std::string& path() const noexcept { return path_; }

    private:
        std::string path_;
    };

    config::ServiceConfig load_ok(const std::string& content) {
        temp_config_t file(content);
        auto loaded = config::load_config(resource(), file.path());
        if (loaded.has_error()) {
            FAIL("load refused the file: " << loaded.error().what.c_str());
        }
        return std::move(loaded.value());
    }

    core::error_t load_error(const std::string& content) {
        temp_config_t file(content);
        auto loaded = config::load_config(resource(), file.path());
        REQUIRE(loaded.has_error());
        return loaded.error();
    }

    bool mentions(const core::error_t& error, std::string_view text) {
        return std::string_view{error.what.data(), error.what.size()}.find(text) != std::string_view::npos;
    }

    void check_default_engine(const config::EngineConfig& engine) {
        CHECK(engine.data_dir == config::DEFAULT_ENGINE_DATA_DIR);
        CHECK(engine.log_dir.empty());
    }

    void check_default_runtime(const config::RuntimeConfig& runtime) {
        CHECK(runtime.general.threads == 3);
        CHECK(runtime.exec.threads == 3);
        CHECK(runtime.disk.threads == 3);
        CHECK(runtime.sessions.threads == 0);
        CHECK(runtime.backends.threads == 0);
        CHECK(runtime.connector_io.threads == 0);
        for (const auto* pool : {&runtime.mysql_io, &runtime.postgres_io, &runtime.flight_sql_io, &runtime.spark_connect_io}) {
            CHECK(pool->threads == 0);
        }
    }

} // namespace

TEST_CASE("service.engine: absent section keeps the previous behaviour") {
    auto loaded = load_ok(R"(
service:
  mysql: { port: 8816 }
)");
    check_default_engine(loaded.engine);
    check_default_runtime(loaded.runtime);
}

TEST_CASE("service.engine: a missing file keeps the previous behaviour") {
    auto loaded = config::load_config(resource(), "/definitely/not/here/config.yaml");
    REQUIRE_FALSE(loaded.has_error());
    check_default_engine(loaded.value().engine);
    check_default_runtime(loaded.value().runtime);
}

TEST_CASE("service.engine: every key is read") {
    auto loaded = load_ok(R"(
service:
  engine:
    data_dir: /srv/otterstax/data
    log_dir: /srv/otterstax/log
)");
    const auto& engine = loaded.engine;
    CHECK(engine.data_dir == "/srv/otterstax/data");
    CHECK(engine.log_dir == "/srv/otterstax/log");
    check_default_runtime(loaded.runtime);
}

TEST_CASE("service.engine: one key alone overrides only its own default") {
    auto loaded = load_ok(R"(
service:
  engine:
    data_dir: /var/lib/otterstax
)");
    CHECK(loaded.engine.data_dir == "/var/lib/otterstax");
    CHECK(loaded.engine.log_dir.empty());
}

// The keys an older config.yaml may still carry are not read any more: like
// any key the reader does not know, they are ignored and the file loads.
TEST_CASE("service.engine: keys no longer read are ignored") {
    auto loaded = load_ok(R"(
service:
  engine:
    data_dir: /srv/otterstax/data
    wal_dir: /srv/otterstax/wal
    disk_dir: /srv/otterstax/disk
    log_level: warn
    executor_pool_size: 8
    dml_flush_row_threshold: 4096
    pump: { in_flight_us: 20, idle_us: 5000 }
  runtime:
    general:  { threads: 4, max_throughput: 500 }
    sessions: { threads: 16, max_throughput: 0 }
)");
    CHECK(loaded.engine.data_dir == "/srv/otterstax/data");
    CHECK(loaded.engine.log_dir.empty());
    CHECK(loaded.runtime.general.threads == 4);
    CHECK(loaded.runtime.sessions.threads == 16);
}

TEST_CASE("service.engine: invalid values are invalid_parameter naming the field") {
    const std::pair<const char*, const char*> cases[] = {
        {"service:\n  engine:\n    data_dir: \"\"\n", "service.engine.data_dir"},
        {"service:\n  engine:\n    log_dir: [a, b]\n", "service.engine.log_dir"},
        {"service:\n  engine: 5\n", "service.engine"},
    };
    for (const auto& [yaml, field] : cases) {
        INFO(yaml);
        auto error = load_error(yaml);
        CHECK(error.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(error, field));
    }
}

TEST_CASE("service.runtime: pool sizes are read") {
    auto loaded = load_ok(R"(
service:
  runtime:
    general:  { threads: 4 }
    exec:     { threads: 6 }
    disk:     { threads: 1 }
    sessions: { threads: 16 }
    backends: { threads: 0 }
)");
    const auto& runtime = loaded.runtime;
    CHECK(runtime.general.threads == 4);
    CHECK(runtime.exec.threads == 6);
    CHECK(runtime.disk.threads == 1);
    CHECK(runtime.sessions.threads == 16);
    CHECK(runtime.backends.threads == 0);
    CHECK(runtime.connector_io.threads == 0);
    check_default_engine(loaded.engine);
}

TEST_CASE("service.runtime: the connector io pools are read") {
    auto loaded = load_ok(R"(
service:
  runtime:
    connector_io: { threads: 6 }
)");
    CHECK(loaded.runtime.connector_io.threads == 6);
    // The actor-zeta pools keep their defaults.
    CHECK(loaded.runtime.backends.threads == 0);
    CHECK(loaded.runtime.general.threads == 3);

    // 0 is auto, as for sessions / backends.
    auto automatic = load_ok("service:\n  runtime:\n    connector_io: { threads: 0 }\n");
    CHECK(automatic.runtime.connector_io.threads == 0);
}

// Every pool of the server comes from config.yaml, the frontends' too.
TEST_CASE("service.runtime: the frontend pools are read") {
    auto loaded = load_ok(R"(
service:
  runtime:
    mysql_io:         { threads: 3 }
    postgres_io:      { threads: 5 }
    flight_sql_io:    { threads: 7 }
    spark_connect_io: { threads: 1 }
)");
    CHECK(loaded.runtime.mysql_io.threads == 3);
    CHECK(loaded.runtime.postgres_io.threads == 5);
    CHECK(loaded.runtime.flight_sql_io.threads == 7);
    CHECK(loaded.runtime.spark_connect_io.threads == 1);
    // The others keep their defaults.
    CHECK(loaded.runtime.connector_io.threads == 0);
    CHECK(loaded.runtime.sessions.threads == 0);

    // One alone overrides only its own; 0 is auto.
    auto one = load_ok("service:\n  runtime:\n    postgres_io: { threads: 0 }\n    flight_sql_io: { threads: 2 }\n");
    CHECK(one.runtime.mysql_io.threads == 0);
    CHECK(one.runtime.postgres_io.threads == 0);
    CHECK(one.runtime.flight_sql_io.threads == 2);
    CHECK(one.runtime.spark_connect_io.threads == 0);
}

TEST_CASE("service.runtime: invalid pool sizes are invalid_parameter naming the field") {
    const std::pair<const char*, const char*> cases[] = {
        // The engine's pools have no auto size.
        {"service:\n  runtime:\n    general: { threads: 0 }\n", "service.runtime.general.threads"},
        {"service:\n  runtime:\n    exec: { threads: 0 }\n", "service.runtime.exec.threads"},
        {"service:\n  runtime:\n    disk: { threads: 0 }\n", "service.runtime.disk.threads"},
        {"service:\n  runtime:\n    sessions: { threads: -1 }\n", "service.runtime.sessions.threads"},
        {"service:\n  runtime:\n    backends: { threads: 100000 }\n", "service.runtime.backends.threads"},
        {"service:\n  runtime:\n    sessions: { threads: lots }\n", "service.runtime.sessions.threads"},
        {"service:\n  runtime:\n    sessions: 4\n", "service.runtime.sessions"},
        {"service:\n  runtime: [1, 2]\n", "service.runtime"},
        {"service:\n  runtime:\n    connector_io: { threads: -1 }\n", "service.runtime.connector_io.threads"},
        {"service:\n  runtime:\n    connector_io: { threads: 5000 }\n", "service.runtime.connector_io.threads"},
        {"service:\n  runtime:\n    connector_io: { threads: many }\n", "service.runtime.connector_io.threads"},
        {"service:\n  runtime:\n    connector_io: 4\n", "service.runtime.connector_io"},
        {"service:\n  runtime:\n    mysql_io: { threads: -1 }\n", "service.runtime.mysql_io.threads"},
        {"service:\n  runtime:\n    postgres_io: { threads: 5000 }\n", "service.runtime.postgres_io.threads"},
        {"service:\n  runtime:\n    flight_sql_io: { threads: some }\n", "service.runtime.flight_sql_io.threads"},
        {"service:\n  runtime:\n    spark_connect_io: 2\n", "service.runtime.spark_connect_io"},
    };
    for (const auto& [yaml, field] : cases) {
        INFO(yaml);
        auto error = load_error(yaml);
        CHECK(error.type == core::error_code_t::invalid_parameter);
        CHECK(mentions(error, field));
    }
}

TEST_CASE("service.engine: the other service sections and connections load alongside it") {
    auto loaded = load_ok(R"(
service:
  mysql: { port: 9816 }
  engine: { data_dir: /data/otterstax }
  runtime: { backends: { threads: 3 } }
  connection_retry: { max_attempts: 4, delay_ms: 10 }
connections:
  mysql:
    - { alias: m, host: h, port: "3306", username: u, password: p, database: d, table: "" }
)");
    CHECK(loaded.mysql.port == 9816);
    CHECK(loaded.engine.data_dir == "/data/otterstax");
    CHECK(loaded.runtime.backends.threads == 3);
    CHECK(loaded.connection_retry.max_attempts == 4);
    REQUIRE(loaded.connections.mysql.size() == 1);
    CHECK(loaded.connections.mysql[0].alias == "m");
}

// Every log file of the server goes to log_dir, data_dir when it is not set:
// main.cpp creates the loggers there once the file is read.
TEST_CASE("service.engine: the log directory is log_dir, else data_dir") {
    config::EngineConfig engine;
    engine.data_dir = "/srv/otterstax/data";
    CHECK(config::log_directory(engine) == "/srv/otterstax/data");
    engine.log_dir = "/var/log/otterstax";
    CHECK(config::log_directory(engine) == "/var/log/otterstax");
    CHECK(config::log_directory(config::EngineConfig{}) == config::DEFAULT_ENGINE_DATA_DIR);
}
