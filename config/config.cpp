// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax


#include "config.hpp"
#include "connections/connection_config_reader.hpp"
#include "yaml_scalar.hpp"
#include "security/network_destinations.hpp"

#include "utility/tracy_profiler.hpp"

#include <fstream>
#include <algorithm>
#include <filesystem>
#include <utility>
#include <boost/asio/ip/address.hpp>


#include <yaml-cpp/yaml.h>

namespace config {

namespace {
    core::result_wrapper_t<std::string> listen_address(const YAML::Node& node, std::string_view field,
                                                      std::pmr::memory_resource* resource) {
        auto host = yaml_scalar<std::string>(node, field, resource);
        if (host.has_error()) return host;
        boost::system::error_code error;
        boost::asio::ip::make_address(host.value(), error);
        if (error) {
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"field '" + std::string(field) + "': must be an IPv4 or IPv6 address", resource}};
        }
        return host;
    }

    core::error_t invalid_field(std::string_view field, std::string_view reason,
                                std::pmr::memory_resource* resource) {
        return core::error_t{core::error_code_t::invalid_parameter,
                             std::pmr::string{"field '" + std::string(field) + "': " + std::string(reason), resource}};
    }

    core::result_wrapper_t<uint16_t> service_port(const YAML::Node& node, std::string_view field,
                                                 std::pmr::memory_resource* resource) {
        auto port = yaml_scalar<int64_t>(node, field, resource);
        if (port.has_error()) {
            return port.convert_error<uint16_t>();
        }
        if (port.value() < 1 || port.value() > 65535) {
            return invalid_field(field, "must be an integer in 1..65535", resource);
        }
        return static_cast<uint16_t>(port.value());
    }

    core::result_wrapper_t<otterstax::security::access_config>
    parse_security(const YAML::Node& node, std::pmr::memory_resource* resource) {
        using namespace otterstax::security;
        access_config result;
        auto mode = yaml_scalar<std::string>(node["mode"], "service.security.mode", resource);
        if (mode.has_error()) return mode.convert_error<access_config>();
        if (mode.value() == "trusted_private") result.mode = access_mode::trusted_private;
        else if (mode.value() == "keycloak") result.mode = access_mode::keycloak;
        else return invalid_field("service.security.mode", "must be trusted_private or keycloak", resource);

        const auto read_text = [&](const YAML::Node& section, const char* key, std::string_view prefix,
                                   std::string& target) -> core::error_t {
            if (!section[key]) return core::error_t::no_error();
            auto value = yaml_scalar<std::string>(section[key], std::string{prefix} + key, resource);
            if (value.has_error()) return value.error();
            target = std::move(value.value());
            return core::error_t::no_error();
        };
        for (auto [key, target] : {std::pair{"certificate_file", &result.certificate_file},
                                   std::pair{"private_key_file", &result.private_key_file}}) {
            auto error = read_text(node, key, "service.security.", *target);
            if (error.contains_error()) return error;
        }
        if (const auto provider = node["keycloak"]) {
            if (!provider.IsMap()) return invalid_field("service.security.keycloak", "must be a mapping", resource);
            for (auto [key, target] : {std::pair{"issuer", &result.keycloak.issuer},
                                       std::pair{"client_id", &result.keycloak.client_id},
                                       std::pair{"client_secret_env", &result.keycloak.client_secret_env},
                                       std::pair{"ca_file", &result.keycloak.ca_file}}) {
                auto error = read_text(provider, key, "service.security.keycloak.", *target);
                if (error.contains_error()) return error;
            }
            for (const auto* key : {"request_timeout_ms", "max_token_bytes", "max_response_bytes", "max_in_flight"}) {
                if (!provider[key]) continue;
                const auto field = std::string{"service.security.keycloak."} + key;
                auto value = yaml_scalar<int64_t>(provider[key], field, resource);
                if (value.has_error()) return value.convert_error<access_config>();
                if (value.value() <= 0) return invalid_field(field, "must be positive", resource);
                const std::string_view name{key};
                if (name == "request_timeout_ms") result.keycloak.request_timeout = std::chrono::milliseconds{value.value()};
                else if (name == "max_token_bytes") result.keycloak.max_token_bytes = value.value();
                else if (name == "max_response_bytes") result.keycloak.max_response_bytes = value.value();
                else result.keycloak.max_in_flight = value.value();
            }
        }
        if (const auto files = node["files"]) {
            if (!files.IsMap()) return invalid_field("service.security.files", "must be a mapping", resource);
            for (auto [key, target] : {std::pair{"read_roots", &result.files.read_roots},
                                      std::pair{"write_roots", &result.files.write_roots}}) {
                const auto roots = files[key];
                if (!roots) continue;
                const auto field = std::string{"service.security.files."} + key;
                if (!roots.IsSequence()) return invalid_field(field, "must be a sequence of absolute directories", resource);
                for (const auto& root : roots) {
                    auto text = yaml_scalar<std::string>(root, field, resource);
                    if (text.has_error()) return text.convert_error<access_config>();
                    const std::filesystem::path path{text.value()};
                    if (!path.is_absolute() || text.value().find('\0') != std::string::npos ||
                        std::any_of(path.begin(), path.end(), [](const auto& part) { return part == ".."; })) {
                        return invalid_field(field, "must contain absolute paths without parent traversal or NUL", resource);
                    }
                    target->push_back(std::move(text.value()));
                }
            }
            if (files["max_file_bytes"]) {
                auto value = yaml_scalar<int64_t>(files["max_file_bytes"], "service.security.files.max_file_bytes", resource);
                if (value.has_error()) return value.convert_error<access_config>();
                if (value.value() <= 0) return invalid_field("service.security.files.max_file_bytes", "must be positive", resource);
                result.files.max_file_bytes = value.value();
            }
        }
        if (const auto brokers = node["kafka_brokers"]) {
            constexpr auto field = "service.security.kafka_brokers";
            if (!brokers.IsSequence()) return invalid_field(field, "must be a sequence of exact host:port destinations", resource);
            for (const auto& broker : brokers) {
                auto value = yaml_scalar<std::string>(broker, field, resource);
                if (value.has_error()) return value.convert_error<access_config>();
                auto canonical = canonical_destination(value.value());
                if (!canonical) return invalid_field(field, "requires hostname:port or [IPv6]:port without wildcards or URL syntax", resource);
                result.kafka_brokers.push_back(std::move(*canonical));
            }
        }
        if (result.certificate_file.empty() != result.private_key_file.empty()) {
            return invalid_field("service.security", "TLS requires certificate_file and private_key_file", resource);
        }
        if (result.mode == access_mode::keycloak &&
            (result.certificate_file.empty() || result.keycloak.issuer.empty() || result.keycloak.client_id.empty() ||
             result.keycloak.client_secret_env.empty())) {
            return invalid_field("service.security", "keycloak requires TLS files, issuer, client_id and client_secret_env", resource);
        }
        return result;
    }
}

    ConfigReader::ConfigReader(std::pmr::memory_resource* resource)
        : resource_(resource)
        , log_(get_logger(logger_tag::Config)) {}

core::result_wrapper_t<ServiceConfig> ConfigReader::load(const std::string& config_path) {
    OTX_ZONE_N("config::ConfigReader::load");
    // Every failure is logged once here, under the config logger, before it is
    // handed to the caller as the result.
    const auto failed = [&](core::error_t error) -> core::result_wrapper_t<ServiceConfig> {
        log_->error("Failed to load configuration file '{}': {}", config_path, error.what.c_str());
        return std::move(error);
    };

    std::error_code path_error;
    const auto entry = std::filesystem::symlink_status(config_path, path_error);
    if (entry.type() == std::filesystem::file_type::not_found &&
        (!path_error || path_error == std::errc::no_such_file_or_directory)) {
        log_->warn("Configuration file '{}' not found. Using default values (no connections).", config_path);
        return ServiceConfig{};
    }
    if (path_error || !std::filesystem::is_regular_file(config_path, path_error)) {
        return failed(core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"configuration path is not a readable regular file: " + config_path,
                                                     resource_}});
    }
    std::ifstream file(config_path);
    if (!file.is_open()) {
        return failed(core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"cannot open configuration file: " + config_path, resource_}});
    }

    // Parse the stream we opened, without reopening the path between checks.
    YAML::Node config;
    try {
        config = YAML::Load(file);
    } catch (const YAML::Exception& e) {
        return failed(core::error_t(core::error_code_t::conversion_failure,
                                    std::pmr::string{("malformed configuration file '" + config_path + "': " + e.what()).c_str(),
                                                     resource_}));
    }
    if (file.bad()) {
        return failed(core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"cannot read configuration file: " + config_path, resource_}});
    }

    ServiceConfig server_config;
    if (config.IsNull()) {
        return server_config;
    }
    if (!config.IsMap()) {
        return failed(invalid_field("configuration", "must be a mapping", resource_));
    }

    // Wire-server settings live under the top-level `service:` key so they never
    // collide with the backend sections under `connections:` (both use the names
    // mysql/postgres). `service.mysql`/`service.postgres` are wire ports;
    // `connections.mysql`/`connections.postgresql` are remote backends.
    if (const auto service = config["service"]) {
        if (!service.IsMap()) {
            return failed(invalid_field("service", "must be a mapping", resource_));
        }
        for (const auto* section : {"flight_sql", "mysql", "postgres", "spark_connect", "connection_retry", "security"}) {
            if (const auto node = service[section]; node && !node.IsMap()) {
                return failed(invalid_field(std::string{"service."} + section, "must be a mapping", resource_));
            }
        }
        if (service["security"]) {
            auto security = parse_security(service["security"], resource_);
            if (security.has_error()) return failed(security.error());
            server_config.security = std::move(security.value());
        }
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
        if (service["spark_connect"]) {
            auto spark_connect = parseSparkConnectConfig(service["spark_connect"], resource_);
            if (spark_connect.has_error()) {
                return failed(spark_connect.error());
            }
            server_config.spark_connect = std::move(spark_connect.value());
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
    log_->debug("Spark Connect: {}:{}", server_config.spark_connect.host, server_config.spark_connect.port);
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
        auto port = service_port(config["port"], "service.flight_sql.port", resource);
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

    if (config["host"]) {
        auto host = listen_address(config["host"], "service.mysql.host", resource);
        if (host.has_error()) return host.convert_error<MysqlConfig>();
        mysql_config.host = std::move(host.value());
    }

    if (config["port"]) {
        auto port = service_port(config["port"], "service.mysql.port", resource);
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

    if (config["host"]) {
        auto host = listen_address(config["host"], "service.postgres.host", resource);
        if (host.has_error()) return host.convert_error<PostgresConfig>();
        postgres_config.host = std::move(host.value());
    }

    if (config["port"]) {
        auto port = service_port(config["port"], "service.postgres.port", resource);
        if (port.has_error()) {
            return port.convert_error<PostgresConfig>();
        }
        postgres_config.port = static_cast<uint16_t>(port.value());
    }

    return postgres_config;
}

core::result_wrapper_t<SparkConnectConfig> ConfigReader::parseSparkConnectConfig(const YAML::Node& config,
                                                                                 std::pmr::memory_resource* resource) {
    SparkConnectConfig spark_config;

    if (config["host"]) {
        auto host = yaml_scalar<std::string>(config["host"], "service.spark_connect.host", resource);
        if (host.has_error()) {
            return host.convert_error<SparkConnectConfig>();
        }
        spark_config.host = std::move(host.value());
    }

    if (config["port"]) {
        auto port = service_port(config["port"], "service.spark_connect.port", resource);
        if (port.has_error()) {
            return port.convert_error<SparkConnectConfig>();
        }
        spark_config.port = static_cast<uint16_t>(port.value());
    }

    return spark_config;
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
        if (retry.max_attempts < 1) {
            return invalid_field("service.connection_retry.max_attempts", "must be at least 1", resource);
        }
    }

    if (config["delay_ms"]) {
        auto delay_ms = yaml_scalar<int>(config["delay_ms"], "service.connection_retry.delay_ms", resource);
        if (delay_ms.has_error()) {
            return delay_ms.convert_error<ConnectionRetryConfig>();
        }
        retry.delay_ms = delay_ms.value();
        if (retry.delay_ms < 0) {
            return invalid_field("service.connection_retry.delay_ms", "must not be negative", resource);
        }
    }

    return retry;
}

}  // namespace config
