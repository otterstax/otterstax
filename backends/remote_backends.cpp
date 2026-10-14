// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "backends/remote_backends.hpp"

#include "backends/connection_retry.hpp"
#include "catalog/remote_metadata.hpp"
#include "connectors/api_connections/ch_connection_config.hpp"
#include "connectors/api_connections/connection_config.hpp"
#include "connectors/api_connections/pg_connection_config.hpp"
#include "otterbrix/federation/provider.hpp"
#include "runtime/runtime.hpp"
#include "utility/logger.hpp"
#include "utility/parse_port.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <utility>

namespace otterstax::backends {

    namespace {

        // A non-owning shared_ptr: the registry refers to the backend, whose
        // lifetime the module owns (the registry is reset before the backend goes).
        template<typename T>
        federation::backend_ptr borrowed(T& backend) {
            return federation::backend_ptr{std::shared_ptr<void>{}, static_cast<federation::backend_t*>(&backend)};
        }

        // The connections config.yaml opens to one backend kind.
        template<typename Descriptors>
        std::size_t opened_connections(const Descriptors& entries) noexcept {
            std::size_t total = 0;
            for (const auto& entry : entries) {
                total += static_cast<std::size_t>(std::max(1, entry.pool_size));
            }
            return total;
        }

        // A descriptor the connector can never accept: a port that is not a
        // decimal integer in 1..65535 (the rule the connectors apply, checked
        // again here because a descriptor may come from code, not from the
        // config reader) or a pool_size below 1. invalid_parameter naming the
        // connection; no error otherwise.
        core::error_t descriptor_error(std::pmr::memory_resource* resource,
                                       const char* kind,
                                       const std::string& alias,
                                       const std::string& port,
                                       int pool_size) {
            if (!port.empty()) {
                if (auto parsed = otterstax::parse_port(port, resource); parsed.has_error()) {
                    return core::error_t(core::error_code_t::invalid_parameter,
                                         std::pmr::string{("Invalid " + std::string{kind} + " connection '" + alias +
                                                           "': " + std::string{parsed.error().what.c_str()})
                                                              .c_str(),
                                                          resource});
                }
            }
            if (pool_size < 1) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{("Invalid " + std::string{kind} + " connection '" + alias +
                                                       "': pool_size must be at least 1 (got " +
                                                       std::to_string(pool_size) + ")")
                                                          .c_str(),
                                                      resource});
            }
            return core::error_t::no_error();
        }

    } // namespace

    connector_io_threads_t connector_io_threads(std::size_t configured,
                                                const config::ConnectionsConfig& connections) noexcept {
        if (configured != 0) {
            return connector_io_threads_t{configured, configured, configured};
        }
        // At least one each: a manager's io pool needs a thread.
        return connector_io_threads_t{std::max<std::size_t>(1, opened_connections(connections.mysql)),
                                      std::max<std::size_t>(1, opened_connections(connections.postgresql)),
                                      std::max<std::size_t>(1, opened_connections(connections.clickhouse))};
    }

    std::size_t backends_threads(std::size_t configured, const config::ConnectionsConfig& connections) {
        const std::size_t total = opened_connections(connections.mysql) + opened_connections(connections.postgresql) +
                                  opened_connections(connections.clickhouse);
        if (configured == 0) {
            return std::max(otterstax::runtime::resolve_threads(0), total);
        }
        if (configured < total) {
            get_logger(logger_tag::BACKENDS)
                ->warn("service.runtime.backends.threads is {} but config.yaml opens {} remote connection(s): a "
                       "statement holds a backends thread for its whole remote call, so the surplus connections stay "
                       "idle and one slow connection can take every thread",
                       configured,
                       total);
        }
        return configured;
    }

    remote_backends_t::remote_backends_t(std::pmr::memory_resource* resource, remote_backends_options_t options)
        : resource_(resource)
        , mysql_connectors_(std::make_unique<mysql::ConnectorManager>(resource_,
                                                                     options.connectors.mysql,
                                                                     options.connector_threads.mysql))
        , pg_connectors_(std::make_unique<pg::ConnectorManager>(resource_,
                                                               options.connectors.postgres,
                                                               options.connector_threads.postgres))
        , ch_connectors_(std::make_unique<ch::ConnectorManager>(resource_,
                                                               options.connectors.clickhouse,
                                                               options.connector_threads.clickhouse))
        // Each integration backend starts the io pool of the connector manager it
        // wraps, so the pools run before open() connects a backend.
        , mysql_(std::make_unique<db::MySQLManager>(resource_, mysql_connectors_.get(), options.statements))
        , postgres_(std::make_unique<db::PostgressManager>(resource_, pg_connectors_.get(), options.statements))
        , clickhouse_(std::make_unique<db::ClickhouseManager>(resource_, ch_connectors_.get(), options.statements)) {
        OTX_ZONE_N("remote_backends_t::remote_backends_t");
        assert(resource_ != nullptr && "memory resource must not be null");
        assert(options.statements != nullptr && "the backends pool runs the federation's statements");
    }

    remote_backends_t::~remote_backends_t() {
        OTX_ZONE_N("remote_backends_t::~remote_backends_t");
        // Nothing may reach a backend once it starts going: the registry holds
        // (non-owning) references to the backends below.
        federation::reset_registry();
    }

    core::error_t remote_backends_t::open(const config::ConnectionsConfig& connections,
                                          const config::ConnectionRetryConfig& retry) {
        OTX_ZONE_N("remote_backends_t::open");
        auto log = get_logger(logger_tag::BACKENDS);

        // Backends may accept connections a moment after their container is
        // reported healthy. Registration is one-shot at startup — there is no
        // runtime retry API — so opening a backend retries per the configured
        // policy (service.connection_retry).
        const auto retry_delay = std::chrono::milliseconds(std::max(0, retry.delay_ms));
        auto sleep_for = [](std::chrono::milliseconds delay) { std::this_thread::sleep_for(delay); };

        // One attempt: open the connection, discover its tables; a connection
        // whose discovery failed is not kept (the next attempt opens it again).
        // Each step says what its failure is: the connection and the discovery
        // are retried, an answer the metadata cannot record is not.
        auto attempt = [this](auto& manager,
                              federation::backend_t& backend,
                              auto open_connection,
                              const std::string& uid,
                              backend_type_t type,
                              qualified_name_t scope) -> std::optional<open_failure_t> {
            auto opened = open_connection();
            if (opened.has_error()) {
                return open_failure_t{open_failure_kind_t::unreachable,
                                      core::error_t(opened.error().type,
                                                    std::pmr::string{opened.error().what.c_str(), resource_})};
            }
            auto reply = backend.discover(scope);
            if (reply.has_error()) {
                manager.removeConnection(uid);
                return open_failure_t{
                    open_failure_kind_t::unreachable,
                    core::error_t(reply.error().type,
                                  std::pmr::string{("schema discovery: " + std::string{reply.error().what.c_str()}).c_str(),
                                                   resource_})};
            }
            // What the metadata cannot record (a column without a name, two columns
            // folding to one, a type the spec cannot carry) leaves the connection
            // out, as a discovery error does — whatever code the refusal carries.
            if (auto recordable = catalog::make_discovery_upsert(resource_, type, scope, reply.value());
                recordable.has_error()) {
                manager.removeConnection(uid);
                return open_failure_t{open_failure_kind_t::unusable,
                                      core::error_t(recordable.error().type,
                                                    std::pmr::string{recordable.error().what.c_str(), resource_})};
            }
            discovered_.push_back(discovered_connection_t{type, std::move(scope), std::move(reply.value())});
            return std::nullopt;
        };

        // The configuration is checked before any connection is opened: a
        // descriptor the connector can never accept is the one error that
        // aborts startup.
        auto misconfigured = [this, &log](const char* kind, const auto& c) -> core::error_t {
            auto error = descriptor_error(resource_, kind, c.alias, c.port, c.pool_size);
            if (error.contains_error()) {
                log->error("{}", error.what.c_str());
            }
            return error;
        };
        for (const auto& c : connections.mysql) {
            if (auto error = misconfigured("MySQL", c); error.contains_error()) {
                return error;
            }
        }
        for (const auto& c : connections.postgresql) {
            if (auto error = misconfigured("PostgreSQL", c); error.contains_error()) {
                return error;
            }
        }
        for (const auto& c : connections.clickhouse) {
            if (auto error = misconfigured("ClickHouse", c); error.contains_error()) {
                return error;
            }
        }

        for (const auto& c : connections.mysql) {
            servers_.push_back(configured_server_t{c.alias, backend_type_t::MySQL});
            static_cast<void>(open_with_retry(
                log,
                "MySQL",
                c.alias,
                retry.max_attempts,
                retry_delay,
                [&] {
                    return attempt(
                        *mysql_connectors_,
                        *mysql_,
                        [&] {
                            return mysql_connectors_->addConnection(conn::api_server::ConnectionParams{
                                .alias = c.alias,
                                .host = c.host,
                                .port = c.port,
                                .username = c.username,
                                .password = c.password,
                                .database = c.database,
                                .table = c.table,
                                .pool_size = static_cast<std::size_t>(c.pool_size),
                            });
                        },
                        c.alias,
                        backend_type_t::MySQL,
                        qualified_name_t{core::uid_t{c.alias},
                                         core::dbname_t{c.database},
                                         core::schema_t{},
                                         core::relname_t{c.table}});
                },
                sleep_for));
        }

        for (const auto& c : connections.postgresql) {
            servers_.push_back(configured_server_t{c.alias, backend_type_t::PostgreSQL});
            static_cast<void>(open_with_retry(
                log,
                "PostgreSQL",
                c.alias,
                retry.max_attempts,
                retry_delay,
                [&] {
                    return attempt(
                        *pg_connectors_,
                        *postgres_,
                        [&] {
                            return pg_connectors_->addConnection(conn::api_server::PgConnectionParams{
                                .alias = c.alias,
                                .host = c.host,
                                .port = c.port,
                                .username = c.username,
                                .password = c.password,
                                .database = c.database,
                                .schema = c.schema,
                                .table = c.table,
                                .pool_size = static_cast<std::size_t>(c.pool_size),
                            });
                        },
                        c.alias,
                        backend_type_t::PostgreSQL,
                        qualified_name_t{core::uid_t{c.alias},
                                         core::dbname_t{c.database},
                                         core::schema_t{c.schema.empty() ? std::string{"public"} : c.schema},
                                         core::relname_t{c.table}});
                },
                sleep_for));
        }

        for (const auto& c : connections.clickhouse) {
            servers_.push_back(configured_server_t{c.alias, backend_type_t::ClickHouse});
            static_cast<void>(open_with_retry(
                log,
                "ClickHouse",
                c.alias,
                retry.max_attempts,
                retry_delay,
                [&] {
                    return attempt(
                        *ch_connectors_,
                        *clickhouse_,
                        [&] {
                            return ch_connectors_->addConnection(conn::api_server::ChConnectionParams{
                                .alias = c.alias,
                                .host = c.host,
                                .port = c.port,
                                .username = c.username,
                                .password = c.password,
                                .database = c.database,
                                .table = c.table,
                                .pool_size = static_cast<std::size_t>(c.pool_size),
                            });
                        },
                        c.alias,
                        backend_type_t::ClickHouse,
                        qualified_name_t{core::uid_t{c.alias},
                                         core::dbname_t{c.database},
                                         core::schema_t{},
                                         core::relname_t{c.table}});
                },
                sleep_for));
        }

        // The registry the federation's hooks read: every configured connection,
        // installed before the engine opens and immutable while it runs.
        std::vector<federation::server_t> servers;
        servers.reserve(servers_.size());
        for (const auto& server : servers_) {
            federation::backend_ptr backend;
            switch (server.type) {
                case backend_type_t::MySQL:
                    backend = borrowed(*mysql_);
                    break;
                case backend_type_t::PostgreSQL:
                    backend = borrowed(*postgres_);
                    break;
                case backend_type_t::ClickHouse:
                    backend = borrowed(*clickhouse_);
                    break;
            }
            servers.push_back(federation::server_t{server.uid, server.type, std::move(backend)});
        }
        federation::install_registry(std::make_shared<const federation::registry_t>(std::move(servers)));
        return core::error_t::no_error();
    }

} // namespace otterstax::backends
