// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/registration.hpp"
#include "config/config.hpp"
#include "connectors/clickhouse/connector.hpp"
#include "connectors/mysql/connector.hpp"
#include "connectors/postgresql/connector.hpp"
#include "otterbrix/federation/backend_actor.hpp"

#include <actor-zeta.hpp>
#include <components/planner/host_hooks.hpp>
#include <components/session/session.hpp>
#include <core/result_wrapper.hpp>

#include <cstddef>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

namespace otterstax::runtime {
    class runtime_t;
} // namespace otterstax::runtime

namespace otterstax::backends {

    // How a backend connection is opened: the server's real connectors, or a
    // test's mocks.
    struct connector_factories_t {
        mysql::connector_factory mysql = &mysql::make_mysql_connector;
        pg::connector_factory postgres = &pg::make_pg_connector;
        ch::connector_factory clickhouse = &ch::make_ch_connector;
    };

    // The backends pool (runtime_t::backends_scheduler()) for
    // service.runtime.backends.threads. A statement holds one of its threads for
    // the whole remote call — an agent runs it there — so with fewer threads
    // than connections (the sum of pool_size over every mysql / postgresql /
    // clickhouse entry) the surplus agents idle, and one slow alias can take
    // every thread from the others. 0 (auto) is max(2, hardware threads) and at
    // least the connections; a configured value is taken as is, logged as a
    // warning (BACKENDS) when it is below them.
    [[nodiscard]] std::size_t backends_threads(std::size_t configured, const config::ConnectionsConfig& connections);

    struct remote_backends_options_t {
        connector_factories_t connectors{};
        // The runtime whose backends pool the agents run on. Required; the
        // module stops that pool in its destructor (runtime_t::stop_backends),
        // after it cancelled the agents and before it destroys them.
        runtime::runtime_t* runtime = nullptr;
    };

    // The remote backends module — built after runtime_t and BEFORE engine_t, because the engine
    // opens with the backend actor's address of every connection (primitives()):
    //
    //   runtime_t → remote_backends_t → engine_t → (S3 / file / Kafka actors,
    //   session factory, servers — main.cpp's locals)
    //
    //  - open(): for every remote connection of `connections:`, pool_size agents
    //    (federation::backend_agent_t, one physical connection each) opened and
    //    the connection's tables discovered by the first of them on the calling
    //    thread, before any agent serves (retried per service.connection_retry;
    //    one that stays unreachable is logged and keeps agents that connect on
    //    their first statement; one whose server cannot be written to is left
    //    out at once), and the connection's backend actor
    //    (federation::backend_actor_t) spawned over its agents;
    //  - after the engine opened: backends::register_remote_tables
    //    (backends.hpp) registers the discovery in the engine's catalog.
    //
    // Destroyed after the engine: every agent cancelled (a running statement
    // aborted, later ones refused), the backends pool stopped, then the actors
    // and their agents. Not thread-safe: the assembling thread builds it and
    // opens it before the engine exists.
    class remote_backends_t final {
    public:
        remote_backends_t(std::pmr::memory_resource* resource, remote_backends_options_t options);
        remote_backends_t(const remote_backends_t&) = delete;
        remote_backends_t& operator=(const remote_backends_t&) = delete;
        ~remote_backends_t();

        // Opens and discovers the remote connections and spawns their backend
        // actors. A descriptor the connector can never accept (a malformed port,
        // a pool_size below 1) is a configuration error: every descriptor is
        // checked before the first connection opens, and the first bad one is
        // returned (invalid_parameter) so startup aborts; so is a loop thread
        // the OS refuses an actor. Nothing an attempt answers aborts: an
        // unreachable backend is retried, then left undiscovered with its actor
        // (open_failure_kind_t, connection_retry.hpp) — whatever error code it
        // carries; a backend whose server answers it cannot be written to (a
        // MySQL server under sql_mode NO_BACKSLASH_ESCAPES) is unusable: not
        // retried and left out — no actor, no address in primitives(), not in
        // connections().
        [[nodiscard]] core::error_t open(const config::ConnectionsConfig& connections,
                                         const config::ConnectionRetryConfig& retry);

        // What open() found: every configured connection (in config order:
        // mysql, postgresql, clickhouse) with its actor and discovery,
        // `discovered` false for one that did not answer, and a null actor for
        // one left out.
        [[nodiscard]] const std::vector<catalog::connection_discovery_t>& discovered() const noexcept {
            return discovered_;
        }
        // Every configured connection that has its actor (the session's
        // registration of a table it missed, the database-name guard); one
        // left out is not among them.
        [[nodiscard]] std::vector<catalog::configured_connection_t> connections() const;
        [[nodiscard]] std::vector<std::string> aliases() const;
        // The backend actor of a configured connection (null for another name).
        [[nodiscard]] federation::backend_actor_t* actor(std::string_view alias) const noexcept;
        // The session the registration runs on: DDL of it reaches no backend.
        [[nodiscard]] const components::session::session_id_t& registration_session() const noexcept {
            return registration_session_;
        }

        // The map open_engine takes: every configured connection, its actor's
        // address. It points into the actors: valid while this module lives.
        [[nodiscard]] components::planner::primitives_t primitives() const noexcept;

    private:
        std::pmr::memory_resource* resource_;
        remote_backends_options_t options_;
        components::session::session_id_t registration_session_;
        std::vector<catalog::connection_discovery_t> discovered_;
        std::vector<federation::backend_actor_ptr> actors_;
        std::vector<components::planner::backend_address_t> addresses_;
    };

} // namespace otterstax::backends
