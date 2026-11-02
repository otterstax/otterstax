// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "backends/remote_backends.hpp"

#include "backends/connection_retry.hpp"
#include "otterbrix/federation/backend_agent.hpp"
#include "runtime/runtime.hpp"
#include "utility/logger.hpp"
#include "utility/parse_port.hpp"
#include "utility/tracy_profiler.hpp"

#include <actor-zeta/spawn.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <utility>

namespace otterstax::backends {

    namespace {

        // The connections config.yaml opens to one backend kind.
        template<typename Descriptors>
        std::size_t opened_connections(const Descriptors& entries) noexcept {
            std::size_t total = 0;
            for (const auto& entry : entries) {
                total += static_cast<std::size_t>(std::max(1, entry.pool_size));
            }
            return total;
        }

        core::error_t invalid(std::pmr::memory_resource* resource,
                              const char* kind,
                              const std::string& alias,
                              const std::string& what) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{("Invalid " + std::string{kind} + " connection '" + alias + "': " + what).c_str(),
                                 resource});
        }

        // The port of a descriptor: 0 for none (the driver's default), else a decimal integer in
        // 1..65535 (the rule of the config reader, checked again because a descriptor may come from
        // code), else invalid_parameter naming the connection.
        core::result_wrapper_t<std::uint16_t> port_of(std::pmr::memory_resource* resource,
                                                      const char* kind,
                                                      const std::string& alias,
                                                      const std::string& port) {
            if (port.empty()) {
                return std::uint16_t{0};
            }
            auto parsed = otterstax::parse_port(port, resource);
            if (parsed.has_error()) {
                return invalid(resource, kind, alias, std::string{parsed.error().what.c_str()});
            }
            return parsed.value();
        }

        // A descriptor the connector can never accept: a malformed port or a pool_size below 1.
        template<typename Descriptor>
        core::error_t descriptor_error(std::pmr::memory_resource* resource, const char* kind, const Descriptor& c) {
            if (auto port = port_of(resource, kind, c.alias, c.port); port.has_error()) {
                return port.error();
            }
            if (c.pool_size < 1) {
                return invalid(resource,
                               kind,
                               c.alias,
                               "pool_size must be at least 1 (got " + std::to_string(c.pool_size) + ")");
            }
            return core::error_t::no_error();
        }

        // The connector of one agent of `c`, built over the agent's io_context (MySQL).
        void attach(std::pmr::memory_resource* resource,
                    const connector_factories_t& factories,
                    federation::backend_agent_t& agent,
                    const config::MysqlConnectionDesc& c) {
            boost::mysql::connect_params params;
            const auto port = port_of(resource, "MySQL", c.alias, c.port).value(); // checked before
            if (port != 0) {
                params.server_address.emplace_host_and_port(c.host, port);
            } else {
                params.server_address.emplace_host_and_port(c.host);
            }
            params.username = c.username;
            params.password = c.password;
            params.database = c.database;
            agent.attach(factories.mysql(resource, agent.io(), std::move(params), c.alias));
        }

        void attach(std::pmr::memory_resource* resource,
                    const connector_factories_t& factories,
                    federation::backend_agent_t& agent,
                    const config::PgConnectionDesc& c) {
            pg::connect_params params;
            params.host = c.host;
            if (const auto port = port_of(resource, "PostgreSQL", c.alias, c.port).value(); port != 0) {
                params.port = port;
            }
            params.username = c.username;
            params.password = c.password;
            params.database = c.database;
            params.schema = c.schema.empty() ? "public" : c.schema;
            params.table = c.table;
            agent.attach(factories.postgres(resource, std::move(params), c.alias));
        }

        void attach(std::pmr::memory_resource* resource,
                    const connector_factories_t& factories,
                    federation::backend_agent_t& agent,
                    const config::ChConnectionDesc& c) {
            ch::connect_params params;
            params.host = c.host;
            if (const auto port = port_of(resource, "ClickHouse", c.alias, c.port).value(); port != 0) {
                params.port = port;
            }
            params.username = c.username;
            params.password = c.password;
            params.database = c.database;
            params.table = c.table;
            agent.attach(factories.clickhouse(resource, std::move(params), c.alias));
        }

        std::string schema_of(const config::MysqlConnectionDesc&) { return {}; }
        std::string schema_of(const config::PgConnectionDesc& c) { return c.schema; }
        std::string schema_of(const config::ChConnectionDesc&) { return {}; }

    } // namespace

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
        , options_(options)
        , registration_session_(components::session::session_id_t::generate_uid()) {
        OTX_ZONE_N("remote_backends_t::remote_backends_t");
        assert(resource_ != nullptr && "memory resource must not be null");
        assert(options_.runtime != nullptr && "the agents run on the runtime's backends pool");
    }

    remote_backends_t::~remote_backends_t() {
        OTX_ZONE_N("remote_backends_t::~remote_backends_t");
        // The engine is gone: no statement reaches an actor any more. The running statements are
        // aborted, the pool joins its threads (no agent job runs or waits after that: an actor-zeta
        // actor is destroyed only off a pool that runs it, AZ-teardown), then the actors and their
        // agents go.
        for (auto& actor : actors_) {
            actor->cancel_agents();
        }
        options_.runtime->stop_backends();
        actors_.clear();
    }

    core::error_t remote_backends_t::open(const config::ConnectionsConfig& connections,
                                          const config::ConnectionRetryConfig& retry) {
        OTX_ZONE_N("remote_backends_t::open");
        auto log = get_logger(logger_tag::BACKENDS);

        // The configuration is checked before any connection is opened: a descriptor the
        // connector can never accept is the one error that aborts startup.
        auto check = [this, &log](const char* kind, const auto& entries) -> core::error_t {
            for (const auto& c : entries) {
                if (auto error = descriptor_error(resource_, kind, c); error.contains_error()) {
                    log->error("{}", error.what.c_str());
                    return error;
                }
            }
            return core::error_t::no_error();
        };
        if (auto error = check("MySQL", connections.mysql); error.contains_error()) {
            return error;
        }
        if (auto error = check("PostgreSQL", connections.postgresql); error.contains_error()) {
            return error;
        }
        if (auto error = check("ClickHouse", connections.clickhouse); error.contains_error()) {
            return error;
        }

        // Backends may accept connections a moment after their container is reported healthy:
        // opening one retries per service.connection_retry.
        const auto retry_delay = std::chrono::milliseconds(std::max(0, retry.delay_ms));
        auto sleep_for = [](std::chrono::milliseconds delay) { std::this_thread::sleep_for(delay); };
        auto* pool = options_.runtime->backends_scheduler();

        auto open_connection = [&](const char* kind_text, backend_type_t kind, const auto& c) -> core::error_t {
            auto make_agents = [&] {
                std::vector<federation::backend_agent_ptr> agents;
                for (int i = 0; i < c.pool_size; ++i) {
                    agents.push_back(actor_zeta::spawn<federation::backend_agent_t>(resource_, c.alias));
                    attach(resource_, options_.connectors, *agents.back(), c);
                }
                return agents;
            };
            const qualified_name_t scope{core::connection_name_t{c.alias},
                                         core::dbname_t{c.database},
                                         core::schema_t{schema_of(c)},
                                         core::relname_t{c.table}};
            std::vector<federation::backend_agent_ptr> agents;
            federation::discovered_tables_t tables{resource_};
            // One attempt: open every connection of the pool, then discover the tables on the
            // first one, on this thread (the agents serve nobody yet).
            const bool opened = open_with_retry(
                log,
                kind_text,
                c.alias,
                retry.max_attempts,
                retry_delay,
                [&]() -> std::optional<open_failure_t> {
                    agents = make_agents();
                    for (auto& agent : agents) {
                        if (auto failed = agent->connect(); failed.contains_error()) {
                            return open_failure_t{open_failure_kind_t::unreachable,
                                                  core::error_t(failed.type, std::pmr::string{failed.what.c_str(), resource_})};
                        }
                    }
                    auto reply = agents.front()->discover_now(scope);
                    if (reply.has_error()) {
                        return open_failure_t{
                            open_failure_kind_t::unreachable,
                            core::error_t(reply.error().type,
                                          std::pmr::string{("schema discovery: " + std::string{reply.error().what.c_str()}).c_str(),
                                                           resource_})};
                    }
                    tables = std::move(reply.value());
                    return std::nullopt;
                },
                sleep_for);
            if (!opened) {
                // Kept in the map with agents that connect on their first statement: a statement of
                // the connection fails there while the backend stays away.
                agents = make_agents();
            }
            auto actor = actor_zeta::spawn<federation::backend_actor_t>(resource_,
                                                                        federation::connection_t{c.alias, kind},
                                                                        registration_session_,
                                                                        std::move(agents),
                                                                        pool);
            if (actor->construction_error().contains_error()) {
                return core::error_t(actor->construction_error().type,
                                     std::pmr::string{actor->construction_error().what, resource_});
            }
            discovered_.push_back(catalog::connection_discovery_t{
                catalog::configured_connection_t{c.alias, kind, c.database, schema_of(c), actor.get()},
                opened,
                std::move(tables)});
            actors_.push_back(std::move(actor));
            return core::error_t::no_error();
        };

        for (const auto& c : connections.mysql) {
            if (auto failed = open_connection("MySQL", backend_type_t::MySQL, c); failed.contains_error()) {
                return failed;
            }
        }
        for (const auto& c : connections.postgresql) {
            if (auto failed = open_connection("PostgreSQL", backend_type_t::PostgreSQL, c); failed.contains_error()) {
                return failed;
            }
        }
        for (const auto& c : connections.clickhouse) {
            if (auto failed = open_connection("ClickHouse", backend_type_t::ClickHouse, c); failed.contains_error()) {
                return failed;
            }
        }
        addresses_.clear();
        for (const auto& actor : actors_) {
            addresses_.push_back(actor->backend_address());
        }
        return core::error_t::no_error();
    }

    std::vector<catalog::configured_connection_t> remote_backends_t::connections() const {
        std::vector<catalog::configured_connection_t> out;
        out.reserve(discovered_.size());
        for (const auto& discovery : discovered_) {
            out.push_back(discovery.connection);
        }
        return out;
    }

    std::vector<std::string> remote_backends_t::aliases() const {
        std::vector<std::string> out;
        out.reserve(discovered_.size());
        for (const auto& discovery : discovered_) {
            out.push_back(discovery.connection.name);
        }
        return out;
    }

    federation::backend_actor_t* remote_backends_t::actor(std::string_view alias) const noexcept {
        for (const auto& discovery : discovered_) {
            if (discovery.connection.name == alias) {
                return discovery.connection.actor;
            }
        }
        return nullptr;
    }

    components::planner::primitives_t remote_backends_t::primitives() const noexcept {
        return components::planner::primitives_t{addresses_};
    }

} // namespace otterstax::backends
