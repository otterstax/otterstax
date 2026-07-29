// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <arrow/util/logging.h>
#include <boost/program_options.hpp>
#include <spdlog/spdlog.h>

#include "component_manager/component_manager.hpp"
#include "connectors/mysql/connector.hpp"
#include "frontend/flight_sql_server/server.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "otterbrix/config.hpp"
#include "config/config.hpp"
#include "utility/tracy_profiler.hpp"

namespace po = boost::program_options;

int main(int argc, char* argv[]) {

    // Logging
    arrow::util::ArrowLog::StartArrowLog("server", arrow::util::ArrowLogLevel::ARROW_DEBUG);

    // Create component manager
    OTX_MESSAGE_L("startup: creating component manager");
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

    // Load server configuration from the single YAML config file. This carries
    // both the wire-server settings and, under `connections:`, every remote
    // backend and s3 alias — the single source of truth for connections. There
    // is no runtime add/remove API.
    config::ServiceConfig server_config;
    try {
        config::ConfigReader reader;
        server_config = reader.load(config_path);
    } catch (const std::exception& e) {
        log->error("Failed to load configuration: {}", e.what());
        return 1;
    }

    // Register the connections read from the config file with the connector
    // managers (opens the backend connections / stores the s3 aliases).
    cmanager.register_connections(server_config.connections, server_config.connection_retry);

    // Configure the Flight SQL server
    Config config{
        .host = server_config.flight_sql.host,
        .port = server_config.flight_sql.port,
        .resource = cmanager.getResource(),
        .catalog_address = cmanager.catalog_address(),
        .scheduler_address = cmanager.scheduler_address(),
    };

    SimpleFlightSQLServer server(config);

    // Configure MySQL server
    frontend::frontend_server_config mysql_config{
        .resource = cmanager.getResource(),
        .port = server_config.mysql.port,
        .scheduler = cmanager.scheduler_address(),
    };

    // Start MySQL server
    log->info("MySQL Server running on port {}...", mysql_config.port);
    OTX_MESSAGE_L("startup: mysql server starting");
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
    OTX_MESSAGE_L("startup: postgres server starting");
    frontend::postgres::postgres_server postgres(postgres_config);
    postgres.start();

    // Start the Flight SQL server. Serve() blocks until SIGTERM is received
    // (registered via SetShutdownOnSignals inside Start()).
    OTX_MESSAGE_L("startup: flightsql server starting");
    arrow::Status status = server.Start();

    // Serve() returned — graceful shutdown sequence.
    // Stop the wire-protocol frontends explicitly before their destructors run,
    // giving Tracy a clean window to flush the profile.
    {
        OTX_ZONE_N("server::shutdown");
        OTX_MESSAGE_L("shutdown: initiated");

        log->info("Shutdown initiated — stopping all servers...");
        mysql.stop();
        OTX_MESSAGE_L("shutdown: mysql server stopped");

        postgres.stop();
        OTX_MESSAGE_L("shutdown: postgres server stopped");

        log->info("Graceful shutdown complete.");
        OTX_MESSAGE_L("shutdown: complete");
    }

    if (!status.ok()) {
        log->error("FlightSQL server error: {}", status.ToString());
        return -1;
    }
    return 0;
}

