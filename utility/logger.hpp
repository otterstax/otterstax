// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <array>
#include <filesystem>
#include <string>
#include <string_view>

namespace logger_tag {
    inline constexpr std::string_view CATALOG_MANAGER = "CatalogManager";
    inline constexpr std::string_view CONNECTOR = "Connector";
    inline constexpr std::string_view CONNECTOR_MANAGER = "ConnectorManager";
    inline constexpr std::string_view OTTERBRIX_MANAGER = "OtterbrixManager";
    inline constexpr std::string_view SQL_CONNECTION_MANAGER = "SqlConnectionManager";
    inline constexpr std::string_view FRONTEND_SERVER = "FrontendServer";
    inline constexpr std::string_view FLIGHTSQL_SERVER = "FlightSQLServer";
    inline constexpr std::string_view MYSQL_CONNECTION = "MysqlConnection";
    inline constexpr std::string_view POSTGRES_CONNECTION = "PostgresConnection";
    inline constexpr std::string_view SCHEDULER = "Scheduler";
} // namespace logger_tag

inline log_t get_logger(std::string_view tag) { return get_logger(std::string(tag)); }

// from otterbrix, we need to make a new named logger, not the default one
inline log_t initialize_logger(std::string name, std::string prefix) {
    if (auto log_ptr = get_logger(name); log_ptr.is_valid()) {
        // prevent creating two loggers with same name
        return log_ptr;
    }

    std::filesystem::create_directories(prefix);
    if (prefix.back() != '/') {
        prefix += '/';
    }

    using namespace std::chrono;
    system_clock::time_point tp = system_clock::now();
    system_clock::duration dtn = tp.time_since_epoch();

    auto file_name = fmt::format("{}{}-{}.txt", prefix, name, duration_cast<seconds>(dtn).count());
    ///spdlog::init_thread_pool(8192, 1);
    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(file_name, true);
    std::vector<spdlog::sink_ptr> sinks{stdout_sink, file_sink};
    auto logger = std::make_shared<spdlog::logger>( ///async_logger
        std::move(name),
        sinks.begin(),
        sinks.end()
        /*,spdlog::thread_pool(),*/
        /*spdlog::async_overflow_policy::block*/);

    spdlog::flush_every(std::chrono::seconds(1)); //todo: hack
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] [pid %P tid %t] %v");
    logger->flush_on(spdlog::level::debug);
    spdlog::register_logger(logger);
    return logger;
}

inline void initialize_all_loggers(const std::string& prefix) {
    static constexpr std::array<std::string_view, 10> all_loggers = {
        logger_tag::CATALOG_MANAGER,
        logger_tag::CONNECTOR,
        logger_tag::CONNECTOR_MANAGER,
        logger_tag::OTTERBRIX_MANAGER,
        logger_tag::SQL_CONNECTION_MANAGER,
        logger_tag::FRONTEND_SERVER,
        logger_tag::FLIGHTSQL_SERVER,
        logger_tag::MYSQL_CONNECTION,
        logger_tag::POSTGRES_CONNECTION,
        logger_tag::SCHEDULER,
    };

    for (auto tag : all_loggers) {
        initialize_logger(std::string(tag), prefix);
    }
}
