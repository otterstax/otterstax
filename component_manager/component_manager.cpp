// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "component_manager.hpp"
#include "connection_retry.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include "connectors/clickhouse/driver.hpp"
#include "connectors/file/driver.hpp"
#include "connectors/mysql/driver.hpp"
#include "connectors/postgresql/driver.hpp"
#include "connectors/s3/driver.hpp"
#include "utility/parse_port.hpp"
#include "utility/session.hpp"
#include "utility/settled_future.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

constexpr size_t MAX_THROUGHPUT =
    1000; // MAX_THROUGHPUT is the maximum number of messages an actor will process before yielding to the scheduler. Setting it to a high value (or to std::numeric_limits<size_t>::max()) means actors will yield only when they have no more messages to process, which can improve performance for actors that process many messages in bursts but can lead to starvation of other actors if one actor receives a continuous stream of messages.

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

    conn::pool_config_t to_pool_config(const config::PoolDesc& pool) {
        return conn::pool_config_t{
            .min_size = pool.min_size,
            .max_size = pool.max_size,
            .idle_timeout = std::chrono::milliseconds(pool.idle_timeout_ms),
            .check_after_idle = std::chrono::milliseconds(pool.check_after_idle_ms),
            .queue_timeout = std::chrono::milliseconds(pool.queue_timeout_ms),
            .connect_backoff = std::chrono::milliseconds(pool.connect_backoff_ms),
        };
    }

    // Opens the alias's pool, then has the catalog discover its tables (`scope`); a connection
    // whose schema could not be registered is unusable, so its pool is dropped again. The catalog
    // answers on this thread: its handler runs to completion, awaiting the pool's discovery statements.
    core::result_wrapper_t<std::string> open_backend(std::pmr::memory_resource* resource,
                                                     log_t& log,
                                                     conn::connection_pool_t& pool,
                                                     actor_zeta::address_t catalog,
                                                     const std::string& alias,
                                                     std::unique_ptr<conn::IDriver> prototype,
                                                     const config::PoolDesc& pool_desc,
                                                     qualified_name_t scope,
                                                     catalog_ext::ConnectionType type) {
        if (auto err = pool.add(alias, std::move(prototype), to_pool_config(pool_desc)); err.contains_error()) {
            return std::move(err);
        }
        auto [needs_sched, registered] =
            actor_zeta::send(catalog, &mysql::CatalogManager::add_connection_schema, std::move(scope), type);
        if (auto err = otterstax::take_settled_error(std::move(registered), resource); err.contains_error()) {
            log->error("Add connection {} failed: schema registration: {}", alias, err.what.c_str());
            pool.drop(alias);
            return std::move(err);
        }
        return alias;
    }

    // An empty port is the backend's default; the config layer already refused a malformed one.
    core::result_wrapper_t<uint16_t>
    port_or(const std::string& port, uint16_t fallback, std::pmr::memory_resource* resource) {
        if (port.empty()) {
            return fallback;
        }
        return otterstax::parse_port(port, resource);
    }
} // namespace

ComponentManager::ComponentManager(const configuration::config& config,
                                   const config::ConnectionsConfig& connections,
                                   std::pmr::memory_resource* data_resource)
    : engine_(new db::otterbrix_engine_t(config))
    , resource_(engine_->dispatcher()->resource())
    , data_resource_(data_resource)
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
                                                                           /*start_pollers=*/true);
        assert(kafka_manager_ != nullptr && "kafka manager must not be null");

        catalog_manager_ = actor_zeta::spawn<mysql::CatalogManager>(resource_, otterbrix_manager_->address(), aliases_);
        assert(catalog_manager_ != nullptr && "catalog manager must not be null");

        // The pools stay empty until register_connections; the catalog discovers through its own
        // backends over them. Allocated on the data resource, not the engine's synchronized pool:
        // every statement allocates its request state, ticket and SQL there from many threads.
        backends_ = std::make_unique<db::backend_registry_t>(data_resource_);
        catalog_backends_ =
            std::make_unique<db::backend_set_t>(resource_, *backends_, catalog_manager_->wait_owner());
        catalog_manager_->set_backends({&catalog_backends_->mysql,
                                        &catalog_backends_->postgresql,
                                        &catalog_backends_->clickhouse});
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
                                                  aliases_,
                                                  *backends_,
                                                  otterbrix_manager_->address(),
                                                  catalog_manager_->address(),
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
                                                     const config::ConnectionRetryConfig& retry,
                                                     const config::PoolDesc& file_transfers) {
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
            [&]() -> core::result_wrapper_t<std::string> {
                auto port = port_or(c.port, 3306, resource_);
                if (port.has_error()) {
                    return port.convert_error<std::string>();
                }
                mysql::connect_params params{
                    .statement_timeout = std::chrono::milliseconds(c.statement_timeout_ms),
                };
                params.server.server_address.emplace_host_and_port(c.host, port.value());
                params.server.username = c.username;
                params.server.password = c.password;
                params.server.database = c.database;
                // Empty collection → the catalog discovers every table of the
                // configured database (the configured table is not a filter here).
                return open_backend(resource_,
                                    log,
                                    backends_->mysql,
                                    catalog_manager_->address(),
                                    c.alias,
                                    mysql::make_driver(data_resource_, std::move(params), c.alias),
                                    c.pool,
                                    qualified_name_t(c.alias, c.database, "", ""),
                                    catalog_ext::ConnectionType::MySQL);
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
            [&]() -> core::result_wrapper_t<std::string> {
                auto port = port_or(c.port, 5432, resource_);
                if (port.has_error()) {
                    return port.convert_error<std::string>();
                }
                pg::connect_params params{
                    .host = c.host,
                    .port = port.value(),
                    .username = c.username,
                    .password = c.password,
                    .database = c.database,
                    .statement_timeout = std::chrono::milliseconds(c.statement_timeout_ms),
                };
                return open_backend(resource_,
                                    log,
                                    backends_->postgresql,
                                    catalog_manager_->address(),
                                    c.alias,
                                    pg::make_driver(data_resource_, std::move(params), c.alias),
                                    c.pool,
                                    qualified_name_t(c.alias, c.database, c.schema.empty() ? "public" : c.schema, c.table),
                                    catalog_ext::ConnectionType::PostgreSQL);
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
            [&]() -> core::result_wrapper_t<std::string> {
                auto port = port_or(c.port, 9000, resource_);
                if (port.has_error()) {
                    return port.convert_error<std::string>();
                }
                ch::connect_params params{
                    .host = c.host,
                    .port = port.value(),
                    .username = c.username,
                    .password = c.password,
                    .database = c.database,
                    .table = c.table,
                };
                // ClickHouse has no schema level: database.table.
                return open_backend(resource_,
                                    log,
                                    backends_->clickhouse,
                                    catalog_manager_->address(),
                                    c.alias,
                                    ch::make_driver(data_resource_, std::move(params), c.alias),
                                    c.pool,
                                    qualified_name_t(c.alias, c.database, "", c.table),
                                    catalog_ext::ConnectionType::ClickHouse);
            },
            sleep_for);
        if (opened.has_error()) {
            return rejected(opened);
        }
    }

    // Local files need no connection: the pool only bounds how many transfers run at once.
    if (auto err = backends_->transfers.set_files(std::make_shared<conn::file::driver_t>(data_resource_),
                                                  to_pool_config(file_transfers));
        err.contains_error()) {
        log->error("Failed to open the local-file transfer pool: {}", err.what.c_str());
        return err;
    }

    for (const auto& c : connections.s3) {
        // Building an alias's filesystem touches no network, so the only way it can
        // fail is the descriptor itself, which no retry fixes.
        conn::s3::connect_params params{
            .region = std::pmr::string{std::string_view{c.region}, resource_},
            .access_key = std::pmr::string{std::string_view{c.access_key}, resource_},
            .secret_key = std::pmr::string{std::string_view{c.secret_key}, resource_},
            .session_token = std::pmr::string{std::string_view{c.session_token}, resource_},
            .endpoint = std::pmr::string{std::string_view{c.endpoint}, resource_},
            .alias = std::pmr::string{std::string_view{c.alias}, resource_},
        };
        auto driver = conn::s3::driver_t::make(data_resource_, params, std::chrono::milliseconds(c.connect_timeout_ms));
        if (driver.has_error()) {
            log->error("Failed to register s3 alias '{}': {}", c.alias, driver.error().what.c_str());
            return driver.error();
        }
        if (auto err = backends_->transfers.add_s3(c.alias, std::move(driver.value()), to_pool_config(c.pool));
            err.contains_error()) {
            log->error("Failed to register s3 alias '{}': {}", c.alias, err.what.c_str());
            return err;
        }
        log->info("Registered s3 alias '{}' (endpoint='{}')", c.alias, c.endpoint);
    }
    return core::error_t::no_error();
}
