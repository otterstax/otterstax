// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#pragma once

#include "connections/connection_config.hpp"

#include <core/result_wrapper.hpp>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string>

namespace config {

struct FlightSqlConfig {
    std::string host = "0.0.0.0";
    uint16_t port = 8815;
};

struct MysqlConfig {
    uint16_t port = 8816;
};

struct PostgresConfig {
    uint16_t port = 8817;
};

struct SparkConnectConfig {
    std::string host = "0.0.0.0";
    uint16_t port = 15002;
    // A Spark session that receives no RPC for this many seconds is closed (its
    // query session with it, an open transaction rolled back); 0 keeps sessions
    // until ReleaseSession. Spark Connect's own default
    // (spark.connect.session.manager.defaultSessionTimeout) is 60 minutes.
    std::uint32_t session_timeout_s = 3600;
};

// The directory main.cpp hard-coded as the engine's data dir before
// `service.engine.data_dir` existed; it stays the default.
inline constexpr const char* DEFAULT_ENGINE_DATA_DIR = "/tmp/test_collection_sql/base";

// `service.engine`: the embedded otterbrix engine (otterbrix
// configuration::config). Every key is optional; everything else the engine
// takes is otterbrix's own default. Plain data: the runtime/ target turns it
// into a configuration::config (otterstax::runtime::make_engine_configuration).
struct EngineConfig {
    // The engine's own directory: it holds the `.lock` that refuses a second
    // engine, the WAL and the tables.
    std::string data_dir = DEFAULT_ENGINE_DATA_DIR;
    // Where every log file of the server goes — otterstax's loggers and the
    // engine's lines, which share Main's sinks (config::log_directory: empty →
    // data_dir). main.cpp creates the loggers there once the file is read; what
    // goes wrong before (the arguments, the file itself) is written to stderr.
    std::string log_dir;
};

// The directory the server's log files are written to: `log_dir`, else `data_dir`.
inline const std::string& log_directory(const EngineConfig& engine) {
    return engine.log_dir.empty() ? engine.data_dir : engine.log_dir;
}

// One thread pool: an actor-zeta pool of runtime_t, the io pool of the
// connector managers or of a frontend server. What 0 (auto) means is the
// field's own (RuntimeConfig).
struct PoolConfig {
    std::size_t threads = 0;
};

// `service.runtime`: every thread pool of the server, so config.yaml sizes all
// of them. general / exec / disk are the engine's (otterbrix base_spaces sized
// them 3 threads); sessions hosts one query session actor per client
// connection (0 = auto, max(2, hardware threads)); backends runs the backends'
// statements — these five are actor-zeta pools otterstax::runtime::runtime_t
// owns. The rest are plain thread pools their owners build from these sizes.
struct RuntimeConfig {
    PoolConfig general{3};
    PoolConfig exec{3};
    PoolConfig disk{3};
    PoolConfig sessions{0};
    // A statement holds a backends thread for the whole remote call (the
    // connection checkout and the wait for the backend's answer), so with fewer
    // threads than connections the surplus connections idle and one slow alias
    // can take every thread. 0 (auto) is therefore max(2, hardware threads) and
    // at least the sum of pool_size of every remote connection; an explicit
    // value is taken as is (main logs a warning when it is below that sum).
    PoolConfig backends{0};
    // The asio io pool of each remote connector manager (MySQL / PostgreSQL /
    // ClickHouse): the threads its connections' statements run on. A statement
    // holds one of them while it runs — libpq and clickhouse-cpp block it for
    // the whole statement, Boost.MySQL completes its async operations on it —
    // and a connection runs one statement at a time, so a manager can never keep
    // more io threads busy than it has connections. 0 = auto: each manager gets
    // as many threads as config.yaml opens connections to its backend kind (the
    // sum of their pool_size), at least 1 (otterstax::backends::connector_io_threads).
    // N = N threads for each of the three managers.
    PoolConfig connector_io{};
    // The frontend servers' threads; 0 = auto, max(2, hardware threads)
    // (otterstax::runtime::resolve_threads), for each.
    //  - mysql_io / postgres_io: the asio pool of the wire server. A connection
    //    holds one of its threads while its statement runs (the bridge waits on
    //    it), so the pool bounds the statements in flight over that wire.
    //  - flight_sql_io: the GrpcContext threads of the Flight SQL server; its
    //    adapter blocks the calling thread for the length of a query, so the
    //    count bounds its queries in flight.
    //  - spark_connect_io: one GrpcContext (and completion queue) per thread;
    //    the handlers are coroutines and never block their thread.
    PoolConfig mysql_io{};
    PoolConfig postgres_io{};
    PoolConfig flight_sql_io{};
    PoolConfig spark_connect_io{};
};

struct ServiceConfig {
    FlightSqlConfig flight_sql;
    MysqlConfig mysql;
    PostgresConfig postgres;
    SparkConnectConfig spark_connect;
    // The embedded engine (`service.engine`) and the pools it and the rest of
    // the server run on (`service.runtime`).
    EngineConfig engine;
    RuntimeConfig runtime;
    // Startup retry policy for opening backend connections (from `service.connection_retry`).
    ConnectionRetryConfig connection_retry;
    // Remote backends + s3 aliases parsed from the `connections:` section of the
    // same config file. Single source of truth for connections — opened once at
    // startup: the backends by remote_backends_t::open, the s3 aliases by
    // backends::register_s3_aliases.
    ConnectionsConfig connections;
};

// Reads the whole config.yaml. A missing file is the default ServiceConfig; a
// file that is not YAML is a conversion_failure; a value that cannot be read
// as its type (a port outside 1..65535 included) or a connection entry that is
// incomplete is an invalid_parameter naming the field or the entry. No
// exception leaves it: the yaml-cpp calls are the only throwing sites and each
// is converted where it is made. Nothing is logged — the server reads the file
// before it creates its loggers and reports a refusal itself. `resource` owns
// the error messages.
[[nodiscard]] core::result_wrapper_t<ServiceConfig> load_config(std::pmr::memory_resource* resource,
                                                                const std::string& config_path);

}  // namespace config
