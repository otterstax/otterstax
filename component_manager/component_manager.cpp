// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "component_manager.hpp"
#include "connection_retry.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include "connectors/api_connections/connection_config.hpp"
#include "connectors/api_connections/pg_connection_config.hpp"
#include "connectors/api_connections/ch_connection_config.hpp"
#include "connectors/s3/s3_connect_params.hpp"
#include "utility/session.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

constexpr size_t MAX_THROUGHPUT =
    1000; // MAX_THROUGHPUT is the maximum number of messages an actor will process before yielding to the scheduler. Setting it to a high value (or to std::numeric_limits<size_t>::max()) means actors will yield only when they have no more messages to process, which can improve performance for actors that process many messages in bursts but can lead to starvation of other actors if one actor receives a continuous stream of messages.

ComponentManager::ComponentManager(const configuration::config& config)
    : engine_(new db::otterbrix_engine_t(config))
    , resource_(engine_->dispatcher()->resource()) {
    OTX_ZONE_N("ComponentManager::init");

    initialize_all_loggers(config.log.path.c_str());

    assert(resource_ != nullptr && "memory resource must not be null");

    {
        OTX_ZONE_N("ComponentManager::spawn_catalog");
        // OtterbrixManager must exist before CatalogManager: the catalog registers
        // external table schemas in the engine through the OtterbrixManager actor.
        otterbrix_manager_ = actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(engine_));
        assert(otterbrix_manager_ != nullptr && "otterbrix manager must not be null");

        kafka_manager_ = actor_zeta::spawn<otterstax::kafka::KafkaManager>(resource_,
                                                                           engine_->engine_dispatcher_address(),
                                                                           /*start_pollers=*/true);
        assert(kafka_manager_ != nullptr && "kafka manager must not be null");

        catalog_manager_ = actor_zeta::spawn<mysql::CatalogManager>(resource_, otterbrix_manager_->address());
        assert(catalog_manager_ != nullptr && "catalog manager must not be null");

        db_connector_manager_ = std::make_unique<mysql::ConnectorManager>(
            resource_, catalog_manager_->address(), &mysql::make_mysql_connector);

        pg_connector_manager_ =
            std::make_unique<pg::ConnectorManager>(resource_, catalog_manager_->address(), &pg::make_pg_connector);

        ch_connector_manager_ =
            std::make_unique<ch::ConnectorManager>(resource_, catalog_manager_->address(), &ch::make_ch_connector);
    }

    {
        OTX_ZONE_N("ComponentManager::spawn_managers");
        // Each integration actor starts the io pool of the connector manager it
        // wraps, so the pools are running before register_connections opens the
        // first backend. Nothing else starts them.
        sql_connection_manager_ = actor_zeta::spawn<db::MySQLManager>(resource_, db_connector_manager_.get());
        assert(sql_connection_manager_ != nullptr && "sql connection manager must not be null");

        pg_connection_manager_ = actor_zeta::spawn<db::PostgressManager>(resource_, pg_connector_manager_.get());
        assert(pg_connection_manager_ != nullptr && "pg connection manager must not be null");

        ch_connection_manager_actor_ = actor_zeta::spawn<db::ClickhouseManager>(resource_, ch_connector_manager_.get());
        assert(ch_connection_manager_actor_ != nullptr && "ch connection manager must not be null");

        // The integration actors are the sole drivers of their connector
        // managers; the catalog reaches a backend only through them (schema
        // discovery is a message), which is why it learns their addresses
        // rather than the managers themselves. cyclic dependency: the managers
        // were built with the catalog's address above.
        catalog_manager_->set_backend_managers(sql_connection_manager_->address(),
                                               pg_connection_manager_->address(),
                                               ch_connection_manager_actor_->address());
    }

    {
        OTX_ZONE_N("ComponentManager::spawn_s3");
        s3_manager_ = actor_zeta::spawn<conn::s3::ConnectorManager>(resource_);
        assert(s3_manager_ != nullptr && "s3 manager must not be null");

        file_manager_ = actor_zeta::spawn<conn::file::FileManager>(resource_, otterbrix_manager_->address());
        assert(file_manager_ != nullptr && "file manager must not be null");

        // Integration S3 manager orchestrates the raw s3 + file connectors; the
        // Scheduler routes s3 CREATE EXTERNAL TABLE / COPY statements to it.
        s3_integration_manager_ =
            actor_zeta::spawn<db::S3Manager>(resource_, s3_manager_->address(), file_manager_->address());
        assert(s3_integration_manager_ != nullptr && "s3 integration manager must not be null");
    }

    {
        OTX_ZONE_N("ComponentManager::spawn_scheduler");
        // Work-sharing thread pool for the Worker actors: one Worker per pool
        // thread; queries shard onto workers by session hash (id % worker_count).
        const std::size_t worker_count = std::max<std::size_t>(2, std::thread::hardware_concurrency());
        az_scheduler_ = std::make_unique<actor_zeta::scheduler::sharing_scheduler>(worker_count, MAX_THROUGHPUT);
        az_scheduler_->start();

        scheduler_ = actor_zeta::spawn<Scheduler>(resource_,
                                                  az_scheduler_.get(),
                                                  worker_count,
                                                  &make_parser,
                                                  sql_connection_manager_->address(),
                                                  pg_connection_manager_->address(),
                                                  ch_connection_manager_actor_->address(),
                                                  otterbrix_manager_->address(),
                                                  catalog_manager_->address(),
                                                  s3_integration_manager_->address(),
                                                  file_manager_->address(),
                                                  kafka_manager_->address());
        assert(scheduler_ != nullptr && "scheduler must not be null");
    }

    // relaunch persisted Kafka when the full actor graph and worker pool are up
    // doing this in the KafkaManager ctor races the half-initialised engine
    // (the poller starts before the scheduler/executor pool)
    kafka_manager_->recover();
}

ComponentManager::~ComponentManager() {
    // Stop the worker pool BEFORE any actor is destroyed: once stop() returns no
    // pool thread will resume a Worker, so the Scheduler (event loop + workers) can
    // be torn down safely during member destruction (supervisor lifecycle rule).
    if (az_scheduler_) {
        az_scheduler_->stop();
    }
}

std::pmr::memory_resource* ComponentManager::getResource() {
    assert(resource_);
    return resource_;
}

actor_zeta::address_t ComponentManager::scheduler_address() const { return scheduler_->address(); }

actor_zeta::address_t ComponentManager::catalog_address() const { return catalog_manager_->address(); }

core::error_t ComponentManager::register_connections(const config::ConnectionsConfig& connections,
                                                     const config::ConnectionRetryConfig& retry) {
    OTX_ZONE_N("ComponentManager::register_connections");
    auto log = get_logger(logger_tag::Main);

    // Backends may accept connections a moment after their container is reported
    // healthy (a startup race we hit with ClickHouse in the demo). Registration
    // is one-shot at startup — there is no runtime retry API — so opening a
    // backend retries per the configured policy (service.connection_retry).
    const int max_attempts = std::max(1, retry.max_attempts);
    const auto retry_delay = std::chrono::milliseconds(std::max(0, retry.delay_ms));
    auto sleep_for = [](std::chrono::milliseconds delay) { std::this_thread::sleep_for(delay); };
    // Re-homes the helper's verdict onto the engine resource, which outlives the
    // caller's handling of it.
    auto rejected = [this](const core::result_wrapper_t<bool>& opened) {
        return core::error_t(opened.error().type, std::pmr::string{opened.error().what.c_str(), resource_});
    };

    for (const auto& c : connections.mysql) {
        auto opened = otterstax::startup::open_with_retry(
            log,
            "MySQL",
            c.alias,
            max_attempts,
            retry_delay,
            [&] {
                return db_connector_manager_->addConnection(conn::api_server::ConnectionParams{
                    .alias = c.alias,
                    .host = c.host,
                    .port = c.port,
                    .username = c.username,
                    .password = c.password,
                    .database = c.database,
                    .table = c.table,
                });
            },
            sleep_for);
        if (opened.has_error()) {
            return rejected(opened);
        }
    }

    for (const auto& c : connections.postgresql) {
        auto opened = otterstax::startup::open_with_retry(
            log,
            "PostgreSQL",
            c.alias,
            max_attempts,
            retry_delay,
            [&] {
                return pg_connector_manager_->addConnection(conn::api_server::PgConnectionParams{
                    .alias = c.alias,
                    .host = c.host,
                    .port = c.port,
                    .username = c.username,
                    .password = c.password,
                    .database = c.database,
                    .schema = c.schema,
                    .table = c.table,
                });
            },
            sleep_for);
        if (opened.has_error()) {
            return rejected(opened);
        }
    }

    for (const auto& c : connections.clickhouse) {
        auto opened = otterstax::startup::open_with_retry(
            log,
            "ClickHouse",
            c.alias,
            max_attempts,
            retry_delay,
            [&] {
                return ch_connector_manager_->addConnection(conn::api_server::ChConnectionParams{
                    .alias = c.alias,
                    .host = c.host,
                    .port = c.port,
                    .username = c.username,
                    .password = c.password,
                    .database = c.database,
                    .table = c.table,
                });
            },
            sleep_for);
        if (opened.has_error()) {
            return rejected(opened);
        }
    }

    for (const auto& c : connections.s3) {
        // An s3 alias is only stored — no network is touched — so the only way
        // storing it can fail is the descriptor itself, which no retry fixes.
        conn::s3::connect_params params{
            .region = std::pmr::string{std::string_view{c.region}, resource_},
            .access_key = std::pmr::string{std::string_view{c.access_key}, resource_},
            .secret_key = std::pmr::string{std::string_view{c.secret_key}, resource_},
            .session_token = std::pmr::string{std::string_view{c.session_token}, resource_},
            .endpoint = std::pmr::string{std::string_view{c.endpoint}, resource_},
            .alias = std::pmr::string{std::string_view{c.alias}, resource_},
        };
        auto [needs_sched, stored] = actor_zeta::send(s3_manager_->address(),
                                                      &conn::s3::ConnectorManager::add_credentials,
                                                      session_id().hash(),
                                                      std::move(params));
        // The s3 actor runs its handler to completion on the sending thread, so
        // the future is settled once send() returns; anything else means the
        // alias was not stored.
        if (stored.failed()) {
            log->error("Failed to register s3 alias '{}': actor rejected the request", c.alias);
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{("s3 alias '" + c.alias + "': actor rejected the request").c_str(), resource_});
        }
        if (!stored.is_ready()) {
            log->error("Failed to register s3 alias '{}': actor did not settle the request", c.alias);
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{("s3 alias '" + c.alias + "': actor did not settle the request").c_str(),
                                 resource_});
        }
        auto result = std::move(stored).take_ready();
        if (result.has_error()) {
            log->error("Failed to register s3 alias '{}': {}", c.alias, result.error().what.c_str());
            return rejected(result);
        }
        log->info("Registered s3 alias '{}' (endpoint='{}')", c.alias, c.endpoint);
    }
    return core::error_t::no_error();
}
