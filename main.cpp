// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <arrow/util/logging.h>
#include <boost/asio.hpp>
#include <boost/program_options.hpp>

#include "component_manager/component_manager.hpp"
#include "connectors/http_server/connection_server.hpp"
#include "connectors/mysql_connector.hpp"
#include "frontend/flight_sql_server/server.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "otterbrix/config.hpp"

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    // Default values
    std::string flight_host = "0.0.0.0";
    uint16_t flight_port = 8815;
    uint16_t mysql_port = 8816;
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
    ("port-http",
    po::value<uint16_t>(&http_port)->default_value(http_port),
    "Connection manager HTTP server port");

    // Parse arguments
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "Error parsing arguments: " << e.what() << "\n";
        std::cerr << desc << "\n";
        return 1;
    }

    // Show help message if requested
    if (vm.count("help")) {
        std::cout << desc << "\n";
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
        std::cout << "HTTP Server running on port " << http_port << "..." << std::endl;
        ctx.run();
    });

    // Configure MySQL server
    mysql_front::mysql_server_config mysql_config{
        .resource = cmanager.getResource(),
        .port = mysql_port,
        .scheduler = cmanager.scheduler_address(),
    };

    // Start mysql server
    std::cout << "MySQL Server running on port " << mysql_config.port << "..." << std::endl;
    mysql_front::mysql_server mysql(mysql_config);
    mysql.start();

    // Start the Flight SQL server
    arrow::Status status = server.Start();
    if (!status.ok()) {
        std::cerr << "Failed to start FlightSQL server: " << status.ToString() << std::endl;
        server_thread.join();
        return -1;
    }

    return 0;
}