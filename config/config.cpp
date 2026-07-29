// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#include "config.hpp"
#include "connections/connection_config_reader.hpp"

#include <fstream>
#include <stdexcept>


#include <yaml-cpp/yaml.h>

namespace config {

    ConfigReader::ConfigReader() : log_(get_logger(logger_tag::Config)) {}

ServiceConfig ConfigReader::load(const std::string& config_path) {
    std::ifstream file(config_path);
    if (!file.good()) {
        log_->warn("Configuration file '{}' not found. Using default values (no connections).", config_path);
        return ServiceConfig{};
    }
    file.close();

    YAML::Node config;
    try {
        config = YAML::LoadFile(config_path);
    } catch (const YAML::Exception& e) {
        log_->error("Failed to parse configuration file '{}': {}", config_path, e.what());
        throw std::runtime_error("Failed to parse configuration file: " + std::string(e.what()));
    }

    ServiceConfig server_config;

    // Wire-server settings live under the top-level `service:` key so they never
    // collide with the backend sections under `connections:` (both use the names
    // mysql/postgres). `service.mysql`/`service.postgres` are wire ports;
    // `connections.mysql`/`connections.postgresql` are remote backends.
    if (const auto service = config["service"]) {
        if (service["flight_sql"]) {
            server_config.flight_sql = parseFlightSqlConfig(service["flight_sql"]);
        }
        if (service["mysql"]) {
            server_config.mysql = parseMysqlConfig(service["mysql"]);
        }
        if (service["postgres"]) {
            server_config.postgres = parsePostgresConfig(service["postgres"]);
        }
        if (service["connection_retry"]) {
            server_config.connection_retry = parseConnectionRetryConfig(service["connection_retry"]);
        }
    }

    // Connections live in the same file under the `connections:` key — the single
    // source of truth for remote backends and s3 aliases. parse_connections
    // validates required fields and throws on an incomplete entry; log it here
    // and rethrow so startup aborts (a broken connection must not be ignored).
    try {
        server_config.connections = parse_connections(config["connections"]);
    } catch (const std::exception& e) {
        log_->error("Invalid connections in '{}': {}", config_path, e.what());
        throw;
    }

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

FlightSqlConfig ConfigReader::parseFlightSqlConfig(const YAML::Node& config) {
    FlightSqlConfig flight_config;

    if (config["host"]) {
        flight_config.host = config["host"].as<std::string>();
    }

    if (config["port"]) {
        flight_config.port = static_cast<uint16_t>(config["port"].as<int>());
    }

    return flight_config;
}

MysqlConfig ConfigReader::parseMysqlConfig(const YAML::Node& config) {
    MysqlConfig mysql_config;

    if (config["port"]) {
        mysql_config.port = static_cast<uint16_t>(config["port"].as<int>());
    }

    return mysql_config;
}

PostgresConfig ConfigReader::parsePostgresConfig(const YAML::Node& config) {
    PostgresConfig postgres_config;

    if (config["port"]) {
        postgres_config.port = static_cast<uint16_t>(config["port"].as<int>());
    }

    return postgres_config;
}

ConnectionRetryConfig ConfigReader::parseConnectionRetryConfig(const YAML::Node& config) {
    ConnectionRetryConfig retry;

    if (config["max_attempts"]) {
        retry.max_attempts = config["max_attempts"].as<int>();
    }

    if (config["delay_ms"]) {
        retry.delay_ms = config["delay_ms"].as<int>();
    }

    return retry;
}

}  // namespace config
