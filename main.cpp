// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <memory>
#include "security/file_access.hpp"
#include <cstdlib>
#include <iostream>
#include <memory_resource>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <boost/program_options.hpp>
#include <spdlog/spdlog.h>

#include "component_manager/component_manager.hpp"
#include "connectors/mysql/connector.hpp"
#include "connectors/s3/s3_subsystem.hpp"
#include "frontend/flight_sql/server.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "frontend/spark_connect_server/spark_connect_server.hpp"
#include "otterbrix/config.hpp"
#include "config/config.hpp"
#include "utility/logger.hpp"
#include "utility/data_directory_lock.hpp"
#include "utility/tracy_profiler.hpp"

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    // Load configuration from YAML file
    std::string config_path = "config.yaml";
    const char* configured_data_dir = std::getenv("OTTERSTAX_DATA_DIR");
    std::string data_dir = configured_data_dir ? configured_data_dir : "./data";
    
    // Allow overriding config path via command line
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Show help message")
        ("config", po::value<std::string>(&config_path)->default_value(config_path),
         "Path to configuration file")
        ("data-dir", po::value<std::string>(&data_dir)->default_value(data_dir),
         "Engine and log directory (default: OTTERSTAX_DATA_DIR or ./data)");

    // Parse arguments
    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "Error parsing arguments: " << e.what() << '\n' << desc << '\n';
        return 1;
    }

    // Show help message if requested
    if (vm.count("help")) {
        std::cout << desc << '\n';
        return 0;
    }
    if (data_dir.empty()) {
        std::cerr << "The data directory must not be empty\n";
        return 1;
    }
    std::unique_ptr<otterstax::data_directory_lock> data_lock;
    struct logger_lifetime {
        ~logger_lifetime() { spdlog::shutdown(); }
    } logger_shutdown;
    try {
        data_lock = std::make_unique<otterstax::data_directory_lock>(data_dir);
        initialize_all_loggers(data_dir);
    } catch (const std::exception& error) {
        std::cerr << "Cannot initialize the data directory: " << error.what() << '\n';
        return 1;
    }
    auto log = get_logger(logger_tag::Main);
    log->info("Starting server...");

    // Load server configuration from the single YAML config file. This carries
    // both the wire-server settings and, under `connections:`, every remote
    // backend and s3 alias — the single source of truth for connections. There
    // is no runtime add/remove API. An invalid file aborts startup here, before
    // the engine, any actor, connection or frontend exists — nothing is torn
    // down on this path.
    std::pmr::synchronized_pool_resource startup_resource(std::pmr::new_delete_resource());
    config::ConfigReader reader(&startup_resource);
    auto loaded = reader.load(config_path);
    if (loaded.has_error()) {
        log->error("Failed to load configuration: {}", loaded.error().what.c_str());
        return 1;
    }
    config::ServiceConfig server_config = std::move(loaded.value());
    std::shared_ptr<const otterstax::security::server_access> access;
    std::shared_ptr<const otterstax::security::file_access> files;
    try {
        access = otterstax::security::make_server_access(server_config.security);
        std::vector<std::filesystem::path> protected_paths{data_dir, config_path, server_config.security.private_key_file,
                                                          server_config.security.certificate_file, server_config.security.keycloak.ca_file};
        for (const auto& s3 : server_config.connections.s3) protected_paths.emplace_back(s3.ca_file);
        files = std::make_shared<otterstax::security::file_access>(
            server_config.security.files,
            server_config.security.mode == otterstax::security::access_mode::trusted_private,
            protected_paths);
    } catch (const std::exception& error) {
        log->error("Cannot initialize client security: {}", error.what());
        return 1;
    }

    // ComponentManager spawns the s3 connector actor, which initialises Arrow's
    // S3 subsystem; the process owes Arrow a FinalizeS3 before exit on every
    // path below, including the aborted-startup returns. Declared before the
    // manager so it finalizes after the whole actor graph is gone.
    conn::s3::subsystem_finalizer_t finalize_s3(get_logger(logger_tag::S3_MANAGER));

    // Create component manager
    OTX_MESSAGE_L("startup: creating component manager");
    ComponentManager cmanager(make_create_config(data_dir), server_config.connections, files, server_config.security.kafka_brokers);

    if (auto recovered = cmanager.recover(); recovered.contains_error()) {
        log->error("Cannot recover persisted Kafka objects: {}", recovered.what.c_str());
        return 1;
    }

    // Register the connections read from the config file with the connector
    // managers (opens the backend connections / stores the s3 aliases). A
    // backend that is down is skipped with a logged error; a descriptor the
    // backend cannot accept is a configuration error and the server does not
    // start with it.
    if (auto registered = cmanager.register_connections(server_config.connections, server_config.connection_retry);
        registered.contains_error()) {
        log->error("Invalid connection configuration: {}", registered.what.c_str());
        return 1;
    }

    // Configure the Flight SQL server. The custom Flight SQL wire protocol
    // (asio-grpc over gRPC): the engine adapter blocks its calling gRPC
    // thread for the length of a query, so the thread count is what bounds
    // the server's in-flight queries — one query thread per Worker thread,
    // the same sizing ComponentManager uses for the Worker pool.
    flight::server::config_t flight_config{
        .host = server_config.flight_sql.host,
        .port = server_config.flight_sql.port,
        .resource = cmanager.getResource(),
        .scheduler_address = cmanager.scheduler_address(),
        .catalog_address = cmanager.catalog_address(),
        .threads = std::max<std::size_t>(2, std::thread::hardware_concurrency()),
        .access = access,
    };

    flight::server::flight_sql_server flight(flight_config);

    // Configure MySQL server
    frontend::frontend_server_config mysql_config{
        .resource = cmanager.getResource(),
        .port = server_config.mysql.port,
        .scheduler = cmanager.scheduler_address(),
        .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
        .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
        .host = server_config.mysql.host,
        .access = access,
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
        .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
        .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
        .host = server_config.postgres.host,
        .access = access,
    };

    // Start Postgres server
    log->info("Postgres Server running on port {}...", postgres_config.port);
    OTX_MESSAGE_L("startup: postgres server starting");
    frontend::postgres::postgres_server postgres(postgres_config);
    postgres.start();

    // Configure Spark Connect server
    frontend::spark::SparkConnectServerConfig spark_config{
        .host = server_config.spark_connect.host,
        .port = server_config.spark_connect.port,
        .resource = cmanager.getResource(),
        .scheduler_address = cmanager.scheduler_address(),
        .aliases = &cmanager.aliases(),
        .access = access,
    };

    log->info("Spark Connect Server running on port {}...", spark_config.port);
    OTX_MESSAGE_L("startup: spark connect server starting");
    frontend::spark::spark_connect_server spark_server(spark_config);
    if (!spark_server.start()) {
        log->error("Spark Connect server failed to start on {}:{}", spark_config.host, spark_config.port);
        return 1;
    }

    // Start the Flight SQL server and block on it until SIGTERM/SIGINT —
    // the signal stops the grpc server and the GrpcContext, run() returns
    // and the graceful shutdown sequence below runs.
    OTX_MESSAGE_L("startup: flightsql server starting");
    if (!flight.start()) {
        log->error("FlightSQL server failed to start on {}:{}", flight_config.host, flight_config.port);
        mysql.stop();
        postgres.stop();
        spark_server.stop();
        return -1;
    }
    flight.run();

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

        spark_server.stop();
        OTX_MESSAGE_L("shutdown: spark connect server stopped");

        log->info("Graceful shutdown complete.");
        OTX_MESSAGE_L("shutdown: complete");
    }

    return 0;
}

