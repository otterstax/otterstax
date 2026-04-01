// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <arrow/util/logging.h>
#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <spdlog/spdlog.h>

#include "component_manager/component_manager.hpp"
#include "connectors/http_server/connection_server.hpp"
#include "connectors/mysql/connector.hpp"
#include "frontend/flight_sql_server/server.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "otterbrix/config.hpp"
#include "config/config.hpp"

namespace po = boost::program_options;

int main(int argc, char* argv[]) {

    // Logging
    arrow::util::ArrowLog::StartArrowLog("server", arrow::util::ArrowLogLevel::ARROW_DEBUG);

    // Create component manager
    ComponentManager cmanager(make_create_config("/tmp/test_collection_sql/base"));

    auto log = get_logger(logger_tag::Main);
    log->info("Starting server...");
    // Load configuration from YAML file
    std::string config_path = "config.yaml";
    
    // Allow overriding config path via command line
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        ("config", po::value<std::string>(&config_path)->default_value(config_path),
         "Path to configuration file");

    // Parse arguments
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        log->error("Error parsing arguments: {}", e.what());
        std::ostringstream oss;
        oss << desc;
        log->error("{}", oss.str());
        return 1;
    }

    // Show help message if requested
    if (vm.count("help")) {
        std::ostringstream oss;
        oss << desc;
        log->info("{}", oss.str());
        return 0;
    }

    // Load server configuration from YAML file
    config::ServiceConfig server_config;
    try {
        config::ConfigReader reader;
        server_config = reader.load(config_path);
    } catch (const std::exception& e) {
        log->error("Failed to load configuration: {}", e.what());
        return 1;
    }



    // Configure the Flight SQL server
    Config config{
        .host = server_config.flight_sql.host,
        .port = server_config.flight_sql.port,
        .resource = cmanager.getResource(),
        .catalog_address = cmanager.catalog_address(),
        .scheduler_address = cmanager.scheduler_address(),
    };

    SimpleFlightSQLServer server(config);

    // Start the HTTP server in a separate thread
    std::jthread server_thread([mysql_conn_manager = std::move(cmanager.db_connection_manager()),
                                pg_conn_manager = std::move(cmanager.pg_connection_manager()),
                                http_port = server_config.connection_manager.port]() {
        asio::io_context ctx;
        http_server::Server server(ctx, http_port, mysql_conn_manager, pg_conn_manager);
        auto log = get_logger(logger_tag::Main);
        log->info("HTTP Server running on port {}...", http_port);
        ctx.run();
    });

    // Configure MySQL server
    frontend::frontend_server_config mysql_config{
        .resource = cmanager.getResource(),
        .port = server_config.mysql.port,
        .scheduler = cmanager.scheduler_address(),
    };

    // Start MySQL server
    log->info("MySQL Server running on port {}...", mysql_config.port);
    frontend::mysql::mysql_server mysql(mysql_config);
    mysql.start();

    // Configure Postgres server
    frontend::frontend_server_config postgres_config{
        .resource = cmanager.getResource(),
        .port = server_config.postgres.port,
        .scheduler = cmanager.scheduler_address(),
    };

    // Start Postgres server
    log->info("Postgres Server running on port {}...", postgres_config.port);
    frontend::postgres::postgres_server postgres(postgres_config);
    postgres.start();

    // Start the Flight SQL server
    arrow::Status status = server.Start();
    if (!status.ok()) {
        log->error("Failed to start FlightSQL server: {}", status.ToString());
        server_thread.join();
        return -1;
    }

    return 0;
}

