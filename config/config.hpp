// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#pragma once

#include <yaml-cpp/yaml.h>
#include "utility/logger.hpp"

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

struct ConnectionManagerConfig {
    uint16_t port = 8085;
};

struct ServiceConfig {
    FlightSqlConfig flight_sql;
    MysqlConfig mysql;
    PostgresConfig postgres;
    ConnectionManagerConfig connection_manager;
    std::string connection_config_path;
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
    static ConnectionManagerConfig parseConnectionManagerConfig(const YAML::Node& config);
};

}  // namespace config
