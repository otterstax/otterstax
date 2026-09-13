// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#include "config.hpp"
#include "connections/connection_config_reader.hpp"
#include "yaml_scalar.hpp"

#include "utility/tracy_profiler.hpp"

#include <fstream>
#include <utility>


#include <yaml-cpp/yaml.h>

namespace config {

    ConfigReader::ConfigReader(std::pmr::memory_resource* resource)
        : resource_(resource)
        , log_(get_logger(logger_tag::Config)) {}

core::result_wrapper_t<ServiceConfig> ConfigReader::load(const std::string& config_path) {
    OTX_ZONE_N("config::ConfigReader::load");
    std::ifstream file(config_path);
    if (!file.good()) {
        log_->warn("Configuration file '{}' not found. Using default values (no connections).", config_path);
        return ServiceConfig{};
    }
    file.close();

    // Every failure is logged once here, under the config logger, before it is
    // handed to the caller as the result.
    const auto failed = [&](core::error_t error) -> core::result_wrapper_t<ServiceConfig> {
        log_->error("Failed to load configuration file '{}': {}", config_path, error.what.c_str());
        return std::move(error);
    };

    // LoadFile is a yaml-cpp call: a file that is not YAML is reported as the
    // parser's own message, converted here.
    YAML::Node config;
    try {
        config = YAML::LoadFile(config_path);
    } catch (const YAML::Exception& e) {
        return failed(core::error_t(core::error_code_t::conversion_failure,
                                    std::pmr::string{("malformed configuration file '" + config_path + "': " + e.what()).c_str(),
                                                     resource_}));
    }

    ServiceConfig server_config;

    // Wire-server settings live under the top-level `service:` key so they never
    // collide with the backend sections under `connections:` (both use the names
    // mysql/postgres). `service.mysql`/`service.postgres` are wire ports;
    // `connections.mysql`/`connections.postgresql` are remote backends.
    if (const auto service = config["service"]) {
        if (service["flight_sql"]) {
            auto flight_sql = parseFlightSqlConfig(service["flight_sql"], resource_);
            if (flight_sql.has_error()) {
                return failed(flight_sql.error());
            }
            server_config.flight_sql = std::move(flight_sql.value());
        }
        if (service["mysql"]) {
            auto mysql = parseMysqlConfig(service["mysql"], resource_);
            if (mysql.has_error()) {
                return failed(mysql.error());
            }
            server_config.mysql = mysql.value();
        }
        if (service["postgres"]) {
            auto postgres = parsePostgresConfig(service["postgres"], resource_);
            if (postgres.has_error()) {
                return failed(postgres.error());
            }
            server_config.postgres = postgres.value();
        }
        if (service["connection_retry"]) {
            auto retry = parseConnectionRetryConfig(service["connection_retry"], resource_);
            if (retry.has_error()) {
                return failed(retry.error());
            }
            server_config.connection_retry = retry.value();
        }
    }

    // Connections live in the same file under the `connections:` key — the single
    // source of truth for remote backends and s3 aliases. parse_connections
    // validates required fields; its first invalid entry is the result, so
    // startup aborts (a broken connection must not be ignored).
    auto connections = parse_connections(config["connections"], resource_);
    if (connections.has_error()) {
        return failed(connections.error());
    }
    server_config.connections = std::move(connections.value());

    log_->info("Configuration loaded from '{}'", config_path);
    log_->debug("Flight SQL: {}:{} ", server_config.flight_sql.host, server_config.flight_sql.port);
    log_->debug("MySQL port: {}", server_config.mysql.port);
    log_->debug("Postgres port: {}", server_config.postgres.port);
    log_->debug("Connection retry: {} attempt(s), {} ms delay",
                server_config.connection_retry.max_attempts,
                server_config.connection_retry.delay_ms);
    log_->debug("Connections: {} mysql, {} postgresql, {} clickhouse, {} s3",
                server_config.connections.mysql.size(),
                server_config.connections.postgresql.size(),
                server_config.connections.clickhouse.size(),
                server_config.connections.s3.size());

    return server_config;
}

core::result_wrapper_t<FlightSqlConfig> ConfigReader::parseFlightSqlConfig(const YAML::Node& config,
                                                                           std::pmr::memory_resource* resource) {
    FlightSqlConfig flight_config;

    if (config["host"]) {
        auto host = yaml_scalar<std::string>(config["host"], "service.flight_sql.host", resource);
        if (host.has_error()) {
            return host.convert_error<FlightSqlConfig>();
        }
        flight_config.host = std::move(host.value());
    }

    if (config["port"]) {
        auto port = yaml_scalar<int>(config["port"], "service.flight_sql.port", resource);
        if (port.has_error()) {
            return port.convert_error<FlightSqlConfig>();
        }
        flight_config.port = static_cast<uint16_t>(port.value());
    }

    return flight_config;
}

core::result_wrapper_t<MysqlConfig> ConfigReader::parseMysqlConfig(const YAML::Node& config,
                                                                   std::pmr::memory_resource* resource) {
    MysqlConfig mysql_config;

    if (config["port"]) {
        auto port = yaml_scalar<int>(config["port"], "service.mysql.port", resource);
        if (port.has_error()) {
            return port.convert_error<MysqlConfig>();
        }
        mysql_config.port = static_cast<uint16_t>(port.value());
    }

    return mysql_config;
}

core::result_wrapper_t<PostgresConfig> ConfigReader::parsePostgresConfig(const YAML::Node& config,
                                                                         std::pmr::memory_resource* resource) {
    PostgresConfig postgres_config;

    if (config["port"]) {
        auto port = yaml_scalar<int>(config["port"], "service.postgres.port", resource);
        if (port.has_error()) {
            return port.convert_error<PostgresConfig>();
        }
        postgres_config.port = static_cast<uint16_t>(port.value());
    }

    return postgres_config;
}

core::result_wrapper_t<ConnectionRetryConfig> ConfigReader::parseConnectionRetryConfig(
    const YAML::Node& config, std::pmr::memory_resource* resource) {
    ConnectionRetryConfig retry;

    if (config["max_attempts"]) {
        auto max_attempts = yaml_scalar<int>(config["max_attempts"], "service.connection_retry.max_attempts", resource);
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

}  // namespace config
