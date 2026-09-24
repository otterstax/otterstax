// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#pragma once

#include <yaml-cpp/yaml.h>
#include "utility/logger.hpp"
#include "connections/connection_config.hpp"

#include <core/result_wrapper.hpp>

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

// Reads the whole config.yaml. A missing file is the default ServiceConfig; a
// file that is not YAML is a conversion_failure; a value that cannot be read
// as its type or a connection entry that is incomplete is an invalid_parameter
// naming the field or the entry. No exception leaves load: the yaml-cpp calls
// are the only throwing sites and each is converted where it is made.
// `resource` owns the error messages.
class ConfigReader {
public:
    explicit ConfigReader(std::pmr::memory_resource* resource);
    core::result_wrapper_t<ServiceConfig> load(const std::string& config_path);

private:
    std::pmr::memory_resource* resource_;
    log_t log_;
    static core::result_wrapper_t<FlightSqlConfig> parseFlightSqlConfig(const YAML::Node& config,
                                                                        std::pmr::memory_resource* resource);
    static core::result_wrapper_t<MysqlConfig> parseMysqlConfig(const YAML::Node& config,
                                                                std::pmr::memory_resource* resource);
    static core::result_wrapper_t<PostgresConfig> parsePostgresConfig(const YAML::Node& config,
                                                                      std::pmr::memory_resource* resource);
    static core::result_wrapper_t<ConnectionRetryConfig> parseConnectionRetryConfig(const YAML::Node& config,
                                                                                    std::pmr::memory_resource* resource);
};

}  // namespace config
