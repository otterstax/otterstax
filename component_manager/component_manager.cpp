// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "component_manager.hpp"
#include "security/file_access.hpp"
#include "connection_retry.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include "connectors/api_connections/ch_connection_config.hpp"
#include "connectors/api_connections/connection_config.hpp"
#include "connectors/api_connections/pg_connection_config.hpp"
#include "connectors/s3/s3_connect_params.hpp"
#include "utility/session.hpp"

#include <algorithm>
#include <chrono>
#include <thread>


namespace {
    otterstax::names::alias_registry_t make_alias_registry(const config::ConnectionsConfig& connections) {
        otterstax::names::alias_registry_t aliases;
        for (const auto& connection : connections.mysql) {
            aliases.add(connection.alias, {backend_type_t::MySQL, connection.database, {}});
        }
        for (const auto& connection : connections.postgresql) {
            aliases.add(connection.alias, {backend_type_t::PostgreSQL, connection.database, connection.schema});
        }
        for (const auto& connection : connections.clickhouse) {
            aliases.add(connection.alias, {backend_type_t::ClickHouse, connection.database, {}});
        }
        return aliases;
    }
} // namespace

ComponentManager::ComponentManager(const configuration::config& config, const config::ConnectionsConfig& connections,
                                   std::shared_ptr<const otterstax::security::file_access> files,
                                   std::vector<std::string> kafka_brokers)
    : engine_(new db::otterbrix_engine_t(config))
    , resource_(engine_->dispatcher()->resource())
    , aliases_(make_alias_registry(connections)) {
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
                                                                           /*start_pollers=*/true,
                                                                           std::make_shared<const otterstax::security::network_destinations>(kafka_brokers));
        assert(kafka_manager_ != nullptr && "kafka manager must not be null");

        catalog_manager_ = actor_zeta::spawn<mysql::CatalogManager>(resource_, otterbrix_manager_->address(), aliases_);
        assert(catalog_manager_ != nullptr && "catalog manager must not be null");

        db_connector_manager_ = std::make_unique<mysql::ConnectorManager>(resource_,
                                                                          catalog_manager_->address(),
                                                                          &mysql::make_mysql_connector);

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
        const auto staging = std::filesystem::path(config.disk.path) / "s3-staging";
        std::filesystem::create_directories(staging);
        if (std::filesystem::is_symlink(staging)) throw std::runtime_error("S3 staging directory must not be a symbolic link");
        std::filesystem::permissions(staging, std::filesystem::perms::owner_all);
        otterstax::security::file_access_config staging_config;
        if (files) staging_config.max_file_bytes = files->max_file_bytes();
        staging_config.read_roots = {std::filesystem::absolute(staging).string()};
        staging_config.write_roots = staging_config.read_roots;
        auto staging_files = std::make_shared<otterstax::security::file_access>(staging_config);
        s3_manager_ = actor_zeta::spawn<conn::s3::ConnectorManager>(resource_, staging_config.read_roots.front(), staging_files);
        assert(s3_manager_ != nullptr && "s3 manager must not be null");

        file_manager_ = actor_zeta::spawn<conn::file::FileManager>(resource_, otterbrix_manager_->address(), std::move(files));
        assert(file_manager_ != nullptr && "file manager must not be null");

        // Integration S3 manager orchestrates the raw s3 + file connectors; the
        // Scheduler routes s3 CREATE EXTERNAL TABLE / COPY statements to it.
        staging_file_manager_ = actor_zeta::spawn<conn::file::FileManager>(resource_, otterbrix_manager_->address(), staging_files);
        s3_integration_manager_ =
            actor_zeta::spawn<db::S3Manager>(resource_, s3_manager_->address(), staging_file_manager_->address(), staging_config.write_roots.front());
        assert(s3_integration_manager_ != nullptr && "s3 integration manager must not be null");
    }

    {
        OTX_ZONE_N("ComponentManager::spawn_scheduler");
        // Each Worker owns an event loop; queries shard by request ID.
        const std::size_t worker_count = std::max<std::size_t>(2, std::thread::hardware_concurrency());
        scheduler_ = actor_zeta::spawn<Scheduler>(resource_,
                                                  worker_count,
                                                  &make_parser,
                                                  aliases_,
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

}

core::error_t ComponentManager::recover() {
    return kafka_manager_->recover();
}

ComponentManager::~ComponentManager() = default;

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
            .scheme = std::pmr::string{std::string_view{c.scheme}, resource_},
            .ca_file = std::pmr::string{std::string_view{c.ca_file}, resource_},
        };
        auto [needs_sched, stored] = actor_zeta::send(s3_manager_->address(),
                                                      &conn::s3::ConnectorManager::add_credentials,
                                                      next_request_id(),
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
                std::pmr::string{("s3 alias '" + c.alias + "': actor did not settle the request").c_str(), resource_});
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
