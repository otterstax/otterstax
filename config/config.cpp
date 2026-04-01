// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#include "config.hpp"

#include <fstream>
#include <stdexcept>


#include <yaml-cpp/yaml.h>

namespace config {

    ConfigReader::ConfigReader() : log_(get_logger(logger_tag::Config)) {}

ServiceConfig ConfigReader::load(const std::string& config_path) {
    std::ifstream file(config_path);
    if (!file.good()) {
        log_->warn("Configuration file '{}' not found. Using default values.", config_path);
        ServiceConfig default_config;
        default_config.connection_config_path = ".connections/connection_config.yaml";
        return default_config;
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

    if (config["flight_sql"]) {
        server_config.flight_sql = parseFlightSqlConfig(config["flight_sql"]);
    }

    if (config["mysql"]) {
        server_config.mysql = parseMysqlConfig(config["mysql"]);
    }

    if (config["postgres"]) {
        server_config.postgres = parsePostgresConfig(config["postgres"]);
    }

    if (config["connection_manager"]) {
        server_config.connection_manager = parseConnectionManagerConfig(config["connection_manager"]);
    }

    if (config["general"] && config["general"]["connection_config_path"]) {
        server_config.connection_config_path = config["general"]["connection_config_path"].as<std::string>();
    } else {
        server_config.connection_config_path = ".connections/connection_config.yaml";
    }

    log_->info("Configuration loaded from '{}'", config_path);
    log_->debug("Flight SQL: {}:{} ", server_config.flight_sql.host, server_config.flight_sql.port);
    log_->debug("MySQL port: {}", server_config.mysql.port);
    log_->debug("Postgres port: {}", server_config.postgres.port);
    log_->debug("Connection Manager port: {}", server_config.connection_manager.port);
    log_->debug("Connection config path: {}", server_config.connection_config_path);

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

ConnectionManagerConfig ConfigReader::parseConnectionManagerConfig(const YAML::Node& config) {
    ConnectionManagerConfig cm_config;

    if (config["port"]) {
        cm_config.port = static_cast<uint16_t>(config["port"].as<int>());
    }

    return cm_config;
}

}  // namespace config
