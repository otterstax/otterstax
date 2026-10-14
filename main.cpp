// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <sstream>
#include <string>
#include <thread>

#include <boost/program_options.hpp>
#include <spdlog/spdlog.h>

#include "backends/backends.hpp"
#include "backends/remote_backends.hpp"
#include "config/config.hpp"
#include "connectors/file/manager.hpp"
#include "connectors/s3/manager.hpp"
#include "connectors/s3/s3_subsystem.hpp"
#include "frontend/common/protocol_config.hpp"
#include "frontend/flight_sql/server.hpp"
#include "frontend/mysql_server/mysql_server.hpp"
#include "frontend/postgres_server/postgres_server.hpp"
#include "frontend/spark_connect_server/spark_connect_server.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/s3/s3_manager.hpp"
#include "otterbrix/federation/name_resolution.hpp"
#include "otterbrix/parser/parser.hpp"
#include "runtime/engine.hpp"
#include "runtime/runtime.hpp"
#include "session/session_factory.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

namespace po = boost::program_options;

// main owns every part of the process directly; the declaration order of the
// locals below is the order they are built in, and its reverse — RAII, on an
// aborted startup too — the order they are destroyed in:
//
//   subsystem_finalizer_t — Arrow's FinalizeS3, after everything else is gone
//   runtime_t         — the memory resource and every thread pool (service.runtime)
//   remote_backends_t — the MySQL / PostgreSQL / ClickHouse connections, their
//                       backends, the startup discovery and the federation
//                       registry, complete before the engine opens
//   engine_t          — the embedded otterbrix engine on the runtime's engine pools
//                       (service.engine), opened with the federation's primitives;
//                       destroying it is its final CHECKPOINT
//   s3 connector, file manager, S3 orchestrator, Kafka manager — the actors a
//                       query session sends to besides the engine; spawned after
//                       the engine, so destroyed before it, Kafka (its pollers and
//                       streams send to the engine) first
//   session_factory_t — the query sessions on the runtime's sessions pool; its
//                       destructor drains them, stops the pool and frees what
//                       the pool left
//   the four servers  — Flight SQL, MySQL, PostgreSQL, Spark Connect; each stops
//                       in its destructor, closing its connections' sessions
//
// Shutdown: servers (stopped, their sessions closed) → session factory
// (sessions drain, sessions pool stops) → Kafka, S3, file, s3 connector →
// engine (final CHECKPOINT, engine pools stop) → remote backends (registry
// reset, backends, connections) → runtime (the remaining pools, then the
// memory) → FinalizeS3.
int main(int argc, char* argv[]) {
    // The loggers write under service.engine.log_dir, which the config file
    // names: they are created once it is read. What goes wrong before that —
    // the arguments, the file itself — is written to stderr.
    std::string config_path = "config.yaml";

    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Show help message")(
        "config",
        po::value<std::string>(&config_path)->default_value(config_path),
        "Path to configuration file");

    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "Error parsing arguments: " << e.what() << "\n" << desc << std::endl;
        return 1;
    }

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    // The single YAML config file: the wire-server settings, service.engine /
    // service.runtime, and under `connections:` every remote backend and s3
    // alias — the single source of truth for connections. An invalid file
    // aborts startup here, before any module exists.
    std::pmr::synchronized_pool_resource startup_resource(std::pmr::new_delete_resource());
    auto loaded = config::load_config(&startup_resource, config_path);
    if (loaded.has_error()) {
        std::cerr << "Failed to load configuration '" << config_path << "': " << loaded.error().what.c_str()
                  << std::endl;
        return 1;
    }
    config::ServiceConfig server_config = std::move(loaded.value());

    // Every logger, and the engine's log over Main's sinks, under log_dir.
    const std::string& log_dir = config::log_directory(server_config.engine);
    try {
        initialize_all_loggers(log_dir);
    } catch (const std::exception& e) {
        std::cerr << "Cannot create the log files under '" << log_dir << "': " << e.what() << std::endl;
        return 1;
    }
    auto log = get_logger(logger_tag::Main);
    log->info("Starting server...");
    if (std::filesystem::exists(config_path)) {
        log->info("Configuration loaded from '{}'; logs under '{}'", config_path, log_dir);
    } else {
        log->warn("Configuration file '{}' not found: default values, no connections; logs under '{}'",
                  config_path,
                  log_dir);
    }

    // The s3 connector actor spawned below initialises Arrow's S3 subsystem; the process owes Arrow a FinalizeS3 before exit on every path
    // below, aborted startups included. Declared before every module so it
    // finalizes after all of them are gone.
    conn::s3::subsystem_finalizer_t finalize_s3(get_logger(logger_tag::S3_MANAGER));

    // The backends pool is sized against the remote connections: a statement
    // holds one of its threads for the whole remote call.
    config::RuntimeConfig runtime_config = server_config.runtime;
    runtime_config.backends.threads =
        otterstax::backends::backends_threads(server_config.runtime.backends.threads, server_config.connections);

    OTX_MESSAGE_L("startup: runtime");
    otterstax::runtime::runtime_t runtime(runtime_config);

    // The remote connections, opened and discovered before the engine: the
    // engine opens with the federation's name-resolution hooks, which read the
    // registry this module installs. A backend that is down is skipped with a
    // logged error; a descriptor the backend cannot accept is a configuration
    // error and the server does not start with it.
    OTX_MESSAGE_L("startup: remote backends");
    auto remote = std::make_unique<otterstax::backends::remote_backends_t>(
        runtime.resource(),
        otterstax::backends::remote_backends_options_t{
            .connector_threads = otterstax::backends::connector_io_threads(server_config.runtime.connector_io.threads,
                                                                           server_config.connections),
            .statements = runtime.backends_scheduler()});
    if (auto opened = remote->open(server_config.connections, server_config.connection_retry);
        opened.contains_error()) {
        log->error("Invalid connection configuration: {}", opened.what.c_str());
        return 1;
    }

    OTX_MESSAGE_L("startup: engine");
    auto opened = otterstax::runtime::engine_t::open(runtime, server_config.engine, otterstax::federation::make_primitives());
    if (opened.has_error()) {
        log->error("Failed to open the engine: {}", opened.error().what.c_str());
        return 1;
    }
    std::unique_ptr<otterstax::runtime::engine_t> engine = std::move(opened.value());

    OTX_MESSAGE_L("startup: backends");
    // Every actor that reaches the engine sends to its dispatcher directly.
    // Declared after the engine, so destroyed before it; the Kafka manager,
    // declared last, first.
    auto s3_connector = actor_zeta::spawn<conn::s3::ConnectorManager>(runtime.resource());
    auto file = actor_zeta::spawn<conn::file::FileManager>(runtime.resource(), engine->dispatcher_address());
    auto s3 = actor_zeta::spawn<db::S3Manager>(runtime.resource(), s3_connector->address(), file->address());
    // start_pollers: CREATE SOURCE launches an ingestion poller (needs a broker).
    auto kafka = actor_zeta::spawn<otterstax::kafka::KafkaManager>(runtime.resource(),
                                                                   engine->dispatcher_address(),
                                                                   /*start_pollers=*/true);
    if (auto registered = otterstax::backends::register_s3_aliases(runtime.resource(),
                                                                   s3_connector->address(),
                                                                   server_config.connections);
        registered.contains_error()) {
        log->error("Invalid connection configuration: {}", registered.what.c_str());
        return 1;
    }
    // The remote tables the startup discovery found, recorded in the engine's
    // federation metadata before any client can name one.
    if (auto published = otterstax::backends::publish_federation_metadata(runtime.resource(), *engine, *remote);
        published.contains_error()) {
        log->error("Failed to record the federation metadata: {}", published.what.c_str());
        return 1;
    }
    // The persisted Kafka objects relaunch once the engine and every backend
    // are up, before any client is accepted: a poller started earlier would
    // race a half-initialised engine.
    kafka->recover();

    auto services = otterstax::session::make_session_services(runtime.resource(),
                                                              engine->dispatcher_address(),
                                                              s3->address(),
                                                              file->address(),
                                                              kafka->address());
    if (services.has_error()) {
        log->error("Failed to assemble the session services: {}", services.error().what.c_str());
        return 1;
    }

    OTX_MESSAGE_L("startup: frontends");
    // One query session per wire connection, per Flight SQL RPC or prepared
    // statement, per Spark session. Declared before the servers: they own the
    // last session handles and are destroyed first.
    auto sessions =
        std::make_unique<otterstax::session::session_factory_t>(runtime, &make_parser, std::move(services.value()));
    // Each server's threads come from service.runtime (0 = auto,
    // runtime::resolve_threads). A wire connection waits
    // frontend::CONNECTION_TIMEOUT_SEC for the next client bytes, and the accept
    // chain pauses frontend::ACCEPT_RETRY_DELAY_MS after building a connection
    // threw (frontend/common/protocol_config.hpp).
    auto flight = std::make_unique<flight::server::flight_sql_server>(flight::server::config_t{
        .host = server_config.flight_sql.host,
        .port = server_config.flight_sql.port,
        .resource = runtime.resource(),
        .sessions = sessions.get(),
        .threads = otterstax::runtime::resolve_threads(server_config.runtime.flight_sql_io.threads),
    });
    auto mysql = std::make_unique<frontend::mysql::mysql_server>(frontend::frontend_server_config{
        .resource = runtime.resource(),
        .port = server_config.mysql.port,
        .sessions = sessions.get(),
        .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
        .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
        .pool_size = otterstax::runtime::resolve_threads(server_config.runtime.mysql_io.threads),
    });
    auto postgres = std::make_unique<frontend::postgres::postgres_server>(frontend::frontend_server_config{
        .resource = runtime.resource(),
        .port = server_config.postgres.port,
        .sessions = sessions.get(),
        .read_timeout = std::chrono::seconds(frontend::CONNECTION_TIMEOUT_SEC),
        .accept_retry_delay = std::chrono::milliseconds(frontend::ACCEPT_RETRY_DELAY_MS),
        .pool_size = otterstax::runtime::resolve_threads(server_config.runtime.postgres_io.threads),
    });
    auto spark = std::make_unique<frontend::spark::spark_connect_server>(frontend::spark::SparkConnectServerConfig{
        .host = server_config.spark_connect.host,
        .port = server_config.spark_connect.port,
        .resource = runtime.resource(),
        .sessions = sessions.get(),
        .session_timeout = std::chrono::seconds(server_config.spark_connect.session_timeout_s),
        .io_threads = otterstax::runtime::resolve_threads(server_config.runtime.spark_connect_io.threads),
    });

    mysql->start();
    postgres->start();
    spark->start();
    log->info("MySQL Server running on port {}...", server_config.mysql.port);
    log->info("Postgres Server running on port {}...", server_config.postgres.port);
    log->info("Spark Connect Server running on port {}...", server_config.spark_connect.port);
    OTX_MESSAGE_L("startup: flightsql server starting");
    if (!flight->start()) {
        log->error("FlightSQL server failed to start on {}:{}",
                   server_config.flight_sql.host,
                   server_config.flight_sql.port);
        return 1;
    }
    // The Flight SQL server blocks the main thread until SIGTERM/SIGINT; the
    // signal stops the grpc server and its context, run() returns and the
    // shutdown below runs.
    flight->run();

    {
        // The order RAII would take anyway (the reverse of the declarations),
        // spelled out for the markers.
        OTX_ZONE_N("server::shutdown");
        OTX_MESSAGE_L("shutdown: initiated");
        log->info("Shutdown initiated — stopping all servers...");
        spark.reset();
        postgres.reset();
        mysql.reset();
        flight.reset();
        OTX_MESSAGE_L("shutdown: frontends stopped");
        sessions.reset();
        OTX_MESSAGE_L("shutdown: sessions stopped");
        kafka.reset();
        s3.reset();
        file.reset();
        s3_connector.reset();
        OTX_MESSAGE_L("shutdown: backends stopped");
        engine.reset();
        OTX_MESSAGE_L("shutdown: engine stopped");
        remote.reset();
        OTX_MESSAGE_L("shutdown: remote backends stopped");
        log->info("Graceful shutdown complete.");
    }

    return 0;
}
