// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "component_manager.hpp"
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
    , resource_(engine_->dispatcher()->resource())
    , log_path_(config.log.path.c_str()) {
    OTX_ZONE_N("ComponentManager::init");

    initialize_all_loggers(log_path_);

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

        db_connector_manager_ = std::make_shared<mysql::ConnectorManager>(catalog_manager_->address());
        catalog_manager_->set_mysql_connector_manager(db_connector_manager_); // cyclic dependency

        pg_connector_manager_ = std::make_shared<pg::ConnectorManager>(catalog_manager_->address());
        catalog_manager_->set_pg_connector_manager(pg_connector_manager_);

        ch_connector_manager_ = std::make_shared<ch::ConnectorManager>(catalog_manager_->address());
        catalog_manager_->set_ch_connector_manager(ch_connector_manager_);
    }

    {
        OTX_ZONE_N("ComponentManager::spawn_managers");
        sql_connection_manager_ = actor_zeta::spawn<db::MySQLManager>(resource_, db_connector_manager_);
        assert(sql_connection_manager_ != nullptr && "sql connection manager must not be null");

        pg_connection_manager_ = actor_zeta::spawn<db::PostgressManager>(resource_, pg_connector_manager_);
        assert(pg_connection_manager_ != nullptr && "pg connection manager must not be null");

        ch_connection_manager_actor_ = actor_zeta::spawn<db::ClickhouseManager>(resource_, ch_connector_manager_);
        assert(ch_connection_manager_actor_ != nullptr && "ch connection manager must not be null");
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

    {
        OTX_ZONE_N("ComponentManager::start_connectors");
        db_connector_manager_->start();
        pg_connector_manager_->start();
        ch_connector_manager_->start();
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

std::string ComponentManager::getLogPath() { return log_path_; }

std::shared_ptr<mysql::ConnectorManager> ComponentManager::db_connection_manager() const {
    return db_connector_manager_;
}

std::shared_ptr<pg::ConnectorManager> ComponentManager::pg_connection_manager() const { return pg_connector_manager_; }

std::shared_ptr<ch::ConnectorManager> ComponentManager::ch_connection_manager() const { return ch_connector_manager_; }

actor_zeta::address_t ComponentManager::scheduler_address() const { return scheduler_->address(); }

actor_zeta::address_t ComponentManager::catalog_address() const { return catalog_manager_->address(); }

actor_zeta::address_t ComponentManager::otterbrix_manager_address() const { return otterbrix_manager_->address(); }

actor_zeta::address_t ComponentManager::sql_connection_manager_address() const {
    return sql_connection_manager_->address();
}

actor_zeta::address_t ComponentManager::pg_connection_manager_address() const {
    return pg_connection_manager_->address();
}

actor_zeta::address_t ComponentManager::file_manager_address() const { return file_manager_->address(); }

actor_zeta::address_t ComponentManager::s3_manager_address() const { return s3_manager_->address(); }

void ComponentManager::register_connections(const config::ConnectionsConfig& connections,
                                            const config::ConnectionRetryConfig& retry) {
    OTX_ZONE_N("ComponentManager::register_connections");
    auto log = get_logger(logger_tag::Main);

    // Backends may accept connections a moment after their container is reported
    // healthy (a startup race we hit with ClickHouse in the demo). Registration
    // is one-shot at startup — there is no runtime retry API — so retry per the
    // configured policy (service.connection_retry), mirroring the retry the old
    // HTTP add-connection scripts had.
    const int max_attempts = std::max(1, retry.max_attempts);
    const auto retry_delay = std::chrono::milliseconds(std::max(0, retry.delay_ms));
    auto with_retry = [&](const char* kind, const std::string& alias, auto&& add) {
        for (int attempt = 1; attempt <= max_attempts; ++attempt) {
            try {
                add();
                log->info("Registered {} connection '{}'", kind, alias);
                return;
            } catch (const std::exception& e) {
                if (attempt == max_attempts) {
                    log->error("Failed to register {} connection '{}' after {} attempt(s): {}",
                               kind, alias, max_attempts, e.what());
                    return;
                }
                log->warn("{} connection '{}' not ready ({}/{}): {} — retrying in {} ms...",
                          kind, alias, attempt, max_attempts, e.what(), retry_delay.count());
                std::this_thread::sleep_for(retry_delay);
            }
        }
    };

    // Descriptors are already validated at parse time (parse_connections throws
    // on an incomplete entry, aborting startup), so here we only open them.
    for (const auto& c : connections.mysql) {
        with_retry("MySQL", c.alias, [&] {
            db_connector_manager_->addConnection(conn::api_server::ConnectionParams{
                .alias = c.alias,
                .host = c.host,
                .port = c.port,
                .username = c.username,
                .password = c.password,
                .database = c.database,
                .table = c.table,
            });
        });
    }

    for (const auto& c : connections.postgresql) {
        with_retry("PostgreSQL", c.alias, [&] {
            pg_connector_manager_->addConnection(conn::api_server::PgConnectionParams{
                .alias = c.alias,
                .host = c.host,
                .port = c.port,
                .username = c.username,
                .password = c.password,
                .database = c.database,
                .schema = c.schema,
                .table = c.table,
            });
        });
    }

    for (const auto& c : connections.clickhouse) {
        with_retry("ClickHouse", c.alias, [&] {
            ch_connector_manager_->addConnection(conn::api_server::ChConnectionParams{
                .alias = c.alias,
                .host = c.host,
                .port = c.port,
                .username = c.username,
                .password = c.password,
                .database = c.database,
                .table = c.table,
            });
        });
    }

    for (const auto& c : connections.s3) {
        // s3 aliases are only stored (no eager network open), so a single attempt
        // is enough — retry would not help a stored-credential failure.
        try {
            conn::s3::connect_params params{
                .region = c.region,
                .access_key = c.access_key,
                .secret_key = c.secret_key,
                .session_token = c.session_token,
                .endpoint = c.endpoint,
                .alias = c.alias,
            };
            auto fut = actor_zeta::send(s3_manager_->address(),
                                        &conn::s3::ConnectorManager::add_credentials,
                                        session_id().hash(),
                                        std::move(params));
            auto result = std::move(fut.second).take_ready();
            if (result.has_error()) {
                log->error("Failed to register s3 alias '{}': {}", c.alias, result.error().what.c_str());
            } else {
                log->info("Registered s3 alias '{}' (endpoint='{}')", c.alias, c.endpoint);
            }
        } catch (const std::exception& e) {
            log->error("Failed to register s3 alias '{}': {}", c.alias, e.what());
        }
    }
}
