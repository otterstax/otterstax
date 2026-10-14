// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#include "config.hpp"
#include "connections/connection_config_reader.hpp"
#include "yaml_scalar.hpp"

#include "utility/tracy_profiler.hpp"

#include <array>
#include <fstream>
#include <string_view>
#include <utility>


#include <yaml-cpp/yaml.h>

namespace config {

namespace {

    core::error_t invalid(std::string_view field, const std::string& why, std::pmr::memory_resource* resource) {
        return core::error_t(core::error_code_t::invalid_parameter,
                             std::pmr::string{("field '" + std::string(field) + "': " + why).c_str(), resource});
    }

    // A section that is present must be a mapping: yaml-cpp throws on a
    // subscript of a scalar, so this is checked before any key is read.
    core::error_t require_map(const YAML::Node& node, std::string_view field, std::pmr::memory_resource* resource) {
        if (node.IsMap()) {
            return core::error_t::no_error();
        }
        return invalid(field, "expected a mapping of keys", resource);
    }

    // A present integer value in [min, max]; text, a fraction or a value out of
    // the range is an invalid_parameter naming the field.
    core::result_wrapper_t<long long> integer_in_range(const YAML::Node& node,
                                                       std::string_view field,
                                                       long long min,
                                                       long long max,
                                                       std::pmr::memory_resource* resource) {
        auto value = yaml_scalar<long long>(node, field, resource);
        if (value.has_error()) {
            return value;
        }
        if (value.value() < min || value.value() > max) {
            return invalid(field,
                           std::to_string(value.value()) + " is out of range [" + std::to_string(min) + ", " +
                               std::to_string(max) + "]",
                           resource);
        }
        return value;
    }

    // A present directory: a non-empty string.
    core::result_wrapper_t<std::string> directory(const YAML::Node& node,
                                                  std::string_view field,
                                                  std::pmr::memory_resource* resource) {
        auto value = yaml_scalar<std::string>(node, field, resource);
        if (value.has_error()) {
            return value;
        }
        if (value.value().empty()) {
            return invalid(field, "a directory must not be empty", resource);
        }
        return value;
    }

    // One `service.runtime.<pool>` entry; `allow_auto` admits threads: 0.
    core::result_wrapper_t<PoolConfig> pool_config(const YAML::Node& node,
                                                   const std::string& field,
                                                   PoolConfig pool,
                                                   bool allow_auto,
                                                   std::pmr::memory_resource* resource) {
        if (auto not_map = require_map(node, field, resource); not_map.contains_error()) {
            return not_map;
        }
        // Far beyond any machine; refuses a typo that would spawn a million threads.
        constexpr long long max_threads = 4096;
        if (node["threads"]) {
            auto threads = integer_in_range(node["threads"], field + ".threads", allow_auto ? 0 : 1, max_threads, resource);
            if (threads.has_error()) {
                return threads.convert_error<PoolConfig>();
            }
            pool.threads = static_cast<std::size_t>(threads.value());
        }
        return pool;
    }

    // A section's optional `port`: an integer in 1..65535 (a value outside it
    // would be cut to 16 bits and name another port). `field` names the section.
    core::error_t read_port(const YAML::Node& section,
                            const std::string& field,
                            uint16_t& port,
                            std::pmr::memory_resource* resource) {
        if (auto not_map = require_map(section, field, resource); not_map.contains_error()) {
            return not_map;
        }
        if (!section["port"]) {
            return core::error_t::no_error();
        }
        auto value = integer_in_range(section["port"], field + ".port", 1, 65535, resource);
        if (value.has_error()) {
            return value.error();
        }
        port = static_cast<uint16_t>(value.value());
        return core::error_t::no_error();
    }

    core::result_wrapper_t<FlightSqlConfig> parse_flight_sql(const YAML::Node& config,
                                                             std::pmr::memory_resource* resource) {
        if (auto not_map = require_map(config, "service.flight_sql", resource); not_map.contains_error()) {
            return not_map;
        }
        FlightSqlConfig flight_config;
        if (config["host"]) {
            auto host = yaml_scalar<std::string>(config["host"], "service.flight_sql.host", resource);
            if (host.has_error()) {
                return host.convert_error<FlightSqlConfig>();
            }
            flight_config.host = std::move(host.value());
        }
        if (auto port = read_port(config, "service.flight_sql", flight_config.port, resource);
            port.contains_error()) {
            return port;
        }
        return flight_config;
    }

    core::result_wrapper_t<SparkConnectConfig> parse_spark_connect(const YAML::Node& config,
                                                                   std::pmr::memory_resource* resource) {
        if (auto not_map = require_map(config, "service.spark_connect", resource); not_map.contains_error()) {
            return not_map;
        }
        SparkConnectConfig spark_config;
        if (config["host"]) {
            auto host = yaml_scalar<std::string>(config["host"], "service.spark_connect.host", resource);
            if (host.has_error()) {
                return host.convert_error<SparkConnectConfig>();
            }
            spark_config.host = std::move(host.value());
        }
        if (auto port = read_port(config, "service.spark_connect", spark_config.port, resource);
            port.contains_error()) {
            return port;
        }
        if (config["session_timeout_s"]) {
            // At most a year: refuses a typo, keeps the milliseconds in range.
            auto timeout = integer_in_range(config["session_timeout_s"],
                                            "service.spark_connect.session_timeout_s",
                                            0,
                                            365LL * 24 * 3600,
                                            resource);
            if (timeout.has_error()) {
                return timeout.convert_error<SparkConnectConfig>();
            }
            spark_config.session_timeout_s = static_cast<std::uint32_t>(timeout.value());
        }
        return spark_config;
    }

    core::result_wrapper_t<ConnectionRetryConfig> parse_connection_retry(const YAML::Node& config,
                                                                         std::pmr::memory_resource* resource) {
        if (auto not_map = require_map(config, "service.connection_retry", resource); not_map.contains_error()) {
            return not_map;
        }
        ConnectionRetryConfig retry;
        if (config["max_attempts"]) {
            auto max_attempts =
                yaml_scalar<int>(config["max_attempts"], "service.connection_retry.max_attempts", resource);
            if (max_attempts.has_error()) {
                return max_attempts.convert_error<ConnectionRetryConfig>();
            }
            retry.max_attempts = max_attempts.value();
        }
        if (config["delay_ms"]) {
            auto delay_ms = yaml_scalar<int>(config["delay_ms"], "service.connection_retry.delay_ms", resource);
            if (delay_ms.has_error()) {
                return delay_ms.convert_error<ConnectionRetryConfig>();
            }
            retry.delay_ms = delay_ms.value();
        }
        return retry;
    }

    core::result_wrapper_t<EngineConfig> parse_engine(const YAML::Node& config, std::pmr::memory_resource* resource) {
        if (auto not_map = require_map(config, "service.engine", resource); not_map.contains_error()) {
            return not_map;
        }
        EngineConfig engine;
        const std::array<std::pair<const char*, std::string*>, 2> directories{{
            {"data_dir", &engine.data_dir},
            {"log_dir", &engine.log_dir},
        }};
        for (const auto& [key, target] : directories) {
            if (config[key]) {
                auto dir = directory(config[key], std::string("service.engine.") + key, resource);
                if (dir.has_error()) {
                    return dir.convert_error<EngineConfig>();
                }
                *target = std::move(dir.value());
            }
        }
        return engine;
    }

    core::result_wrapper_t<RuntimeConfig> parse_runtime(const YAML::Node& config, std::pmr::memory_resource* resource) {
        if (auto not_map = require_map(config, "service.runtime", resource); not_map.contains_error()) {
            return not_map;
        }
        RuntimeConfig runtime;
        // The engine's pools need a thread each to run on; every other pool takes
        // 0 as auto.
        struct pool_entry_t {
            const char* key;
            PoolConfig* target;
            bool allow_auto;
        };
        const std::array<pool_entry_t, 10> pools{{
            {"general", &runtime.general, false},
            {"exec", &runtime.exec, false},
            {"disk", &runtime.disk, false},
            {"sessions", &runtime.sessions, true},
            {"backends", &runtime.backends, true},
            {"connector_io", &runtime.connector_io, true},
            {"mysql_io", &runtime.mysql_io, true},
            {"postgres_io", &runtime.postgres_io, true},
            {"flight_sql_io", &runtime.flight_sql_io, true},
            {"spark_connect_io", &runtime.spark_connect_io, true},
        }};
        for (const auto& entry : pools) {
            if (config[entry.key]) {
                auto pool = pool_config(config[entry.key],
                                        std::string("service.runtime.") + entry.key,
                                        *entry.target,
                                        entry.allow_auto,
                                        resource);
                if (pool.has_error()) {
                    return pool.convert_error<RuntimeConfig>();
                }
                *entry.target = pool.value();
            }
        }
        return runtime;
    }

} // namespace

core::result_wrapper_t<ServiceConfig> load_config(std::pmr::memory_resource* resource, const std::string& config_path) {
    OTX_ZONE_N("config::load_config");
    std::ifstream file(config_path);
    if (!file.good()) {
        return ServiceConfig{};
    }
    file.close();

    // LoadFile is a yaml-cpp call: a file that is not YAML is reported as the
    // parser's own message, converted here.
    YAML::Node config;
    try {
        config = YAML::LoadFile(config_path);
    } catch (const YAML::Exception& e) {
        return core::error_t(core::error_code_t::conversion_failure,
                             std::pmr::string{("malformed configuration file '" + config_path + "': " + e.what()).c_str(),
                                              resource});
    }

    // The document itself is a mapping (an empty file is a null node: the
    // defaults); yaml-cpp throws on a subscript of a scalar.
    if (!config.IsNull()) {
        if (auto not_map = require_map(config, "(document)", resource); not_map.contains_error()) {
            return not_map;
        }
    }

    ServiceConfig server_config;

    // Wire-server settings live under the top-level `service:` key so they never
    // collide with the backend sections under `connections:` (both use the names
    // mysql/postgres). `service.mysql`/`service.postgres` are wire ports;
    // `connections.mysql`/`connections.postgresql` are remote backends.
    if (const auto service = config["service"]) {
        if (auto not_map = require_map(service, "service", resource); not_map.contains_error()) {
            return not_map;
        }
        if (service["flight_sql"]) {
            auto flight_sql = parse_flight_sql(service["flight_sql"], resource);
            if (flight_sql.has_error()) {
                return flight_sql.error();
            }
            server_config.flight_sql = std::move(flight_sql.value());
        }
        if (service["mysql"]) {
            if (auto port = read_port(service["mysql"], "service.mysql", server_config.mysql.port, resource);
                port.contains_error()) {
                return port;
            }
        }
        if (service["postgres"]) {
            if (auto port =
                    read_port(service["postgres"], "service.postgres", server_config.postgres.port, resource);
                port.contains_error()) {
                return port;
            }
        }
        if (service["spark_connect"]) {
            auto spark_connect = parse_spark_connect(service["spark_connect"], resource);
            if (spark_connect.has_error()) {
                return spark_connect.error();
            }
            server_config.spark_connect = std::move(spark_connect.value());
        }
        if (service["connection_retry"]) {
            auto retry = parse_connection_retry(service["connection_retry"], resource);
            if (retry.has_error()) {
                return retry.error();
            }
            server_config.connection_retry = retry.value();
        }
        if (service["engine"]) {
            auto engine = parse_engine(service["engine"], resource);
            if (engine.has_error()) {
                return engine.error();
            }
            server_config.engine = std::move(engine.value());
        }
        if (service["runtime"]) {
            auto runtime = parse_runtime(service["runtime"], resource);
            if (runtime.has_error()) {
                return runtime.error();
            }
            server_config.runtime = runtime.value();
        }
    }

    // Connections live in the same file under the `connections:` key — the single
    // source of truth for remote backends and s3 aliases. parse_connections
    // validates required fields; its first invalid entry is the result, so
    // startup aborts (a broken connection must not be ignored).
    auto connections = parse_connections(config["connections"], resource);
    if (connections.has_error()) {
        return connections.error();
    }
    server_config.connections = std::move(connections.value());
    return server_config;
}

}  // namespace config
