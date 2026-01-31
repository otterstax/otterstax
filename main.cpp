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
#include "connectors/mysql_connector.hpp"
#include "frontend/flight_sql_server/server.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "otterbrix/config.hpp"

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    // Default values
    std::string flight_host = "0.0.0.0";
    uint16_t flight_port = 8815;
    uint16_t mysql_port = 8816;
    uint16_t postgres_port = 8817;
    uint16_t http_port = 8085;

    // Define command-line options
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Show help message")
    ("host-flight",
    po::value<std::string>(&flight_host)->default_value(flight_host),
    "FlightSQL server host")
    ("port-flight",
    po::value<uint16_t>(&flight_port)->default_value(flight_port),
    "FlightSQL server port")
    ("port-mysql",
    po::value<uint16_t>(&mysql_port)->default_value(mysql_port),
    "MySQL server port")
    ("port-postgres",
    po::value<uint16_t>(&postgres_port)->default_value(postgres_port),
    "PostgreSQL server port")
    ("port-http",
    po::value<uint16_t>(&http_port)->default_value(http_port),
    "Connection manager HTTP server port");

    // Parse arguments
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        spdlog::error("Error parsing arguments: {}", e.what());
        std::ostringstream oss;
        oss << desc;
        spdlog::error("{}", oss.str());
        return 1;
    }

    // Show help message if requested
    if (vm.count("help")) {
        std::ostringstream oss;
        oss << desc;
        spdlog::info("{}", oss.str());
        return 0;
    }

    // Logging
    arrow::util::ArrowLog::StartArrowLog("server", arrow::util::ArrowLogLevel::ARROW_DEBUG);

    // Create component manager
    ComponentManager cmanager(make_create_config("/tmp/test_collection_sql/base"));

    // Configure the Flight SQL server
    Config config{
        .host = flight_host,
        .port = flight_port,
        .resource = cmanager.getResource(),
        .catalog_address = cmanager.catalog_address(),
        .scheduler_address = cmanager.scheduler_address(),
    };

    SimpleFlightSQLServer server(config);

    // Start the HTTP server in a separate thread
    std::jthread server_thread([db_connection_manager = std::move(cmanager.db_connection_manager()), http_port]() {
        asio::io_context ctx;
        http_server::Server server(ctx, http_port, db_connection_manager);
        spdlog::info("HTTP Server running on port {}...", http_port);
        ctx.run();
    });

    // Configure MySQL server
    frontend::frontend_server_config mysql_config{
        .resource = cmanager.getResource(),
        .port = mysql_port,
        .scheduler = cmanager.scheduler_address(),
    };

    // Start MySQL server
    spdlog::info("MySQL Server running on port {}...", mysql_config.port);
    frontend::mysql::mysql_server mysql(mysql_config);
    mysql.start();

    // Configure Postgres server
    frontend::frontend_server_config postgres_config{
        .resource = cmanager.getResource(),
        .port = postgres_port,
        .scheduler = cmanager.scheduler_address(),
    };

    // Start Postgres server
    spdlog::info("Postgres Server running on port {}...", postgres_config.port);
    frontend::postgres::postgres_server postgres(postgres_config);
    postgres.start();

    // Start the Flight SQL server
    arrow::Status status = server.Start();
    if (!status.ok()) {
        spdlog::error("Failed to start FlightSQL server: {}", status.ToString());
        server_thread.join();
        return -1;
    }

    return 0;
}
