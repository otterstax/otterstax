// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#pragma once

#include <yaml-cpp/yaml.h>
#include "utility/logger.hpp"
#include "connections/connection_config.hpp"

#include <cstdint>
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

struct ServiceConfig {
    FlightSqlConfig flight_sql;
    MysqlConfig mysql;
    PostgresConfig postgres;
    // Startup retry policy for opening backend connections (from `service.connection_retry`).
    ConnectionRetryConfig connection_retry;
    // Remote backends + s3 aliases parsed from the `connections:` section of the
    // same config file. Single source of truth for connections — registered once
    // at startup by ComponentManager::register_connections.
    ConnectionsConfig connections;
};

class ConfigReader {
public:
    ConfigReader();
    ServiceConfig load(const std::string& config_path);

private:
    log_t log_;
    static FlightSqlConfig parseFlightSqlConfig(const YAML::Node& config);
    static MysqlConfig parseMysqlConfig(const YAML::Node& config);
    static PostgresConfig parsePostgresConfig(const YAML::Node& config);
    static ConnectionRetryConfig parseConnectionRetryConfig(const YAML::Node& config);
};

}  // namespace config
