// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/discovery.hpp"
#include "config/config.hpp"
#include "connectors/clickhouse/manager.hpp"
#include "connectors/mysql/manager.hpp"
#include "connectors/postgresql/manager.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"

#include <actor-zeta.hpp>
#include <core/result_wrapper.hpp>

#include <cstddef>
#include <memory>
#include <memory_resource>
#include <string>
#include <vector>

namespace otterstax::backends {

    // How a backend connection is opened: the server's real connectors, or a
    // test's mocks.
    struct connector_factories_t {
        mysql::connector_factory mysql = &mysql::make_mysql_connector;
        pg::connector_factory postgres = &pg::make_pg_connector;
        ch::connector_factory clickhouse = &ch::make_ch_connector;
    };

    // The io threads of each connector manager's asio pool (config::PoolConfig).
    struct connector_io_threads_t {
        std::size_t mysql = 1;
        std::size_t postgres = 1;
        std::size_t clickhouse = 1;
    };

    // The io pools for service.runtime.connector_io.threads: a non-zero
    // `configured` for each manager; 0 (auto) gives each manager one thread per
    // connection config.yaml opens to its backend kind — the sum of the entries'
    // pool_size — and at least one. A statement holds an io thread while it runs
    // and a connection runs one statement at a time, so fewer threads would queue
    // statements that already hold a connection, and more could never all be busy.
    [[nodiscard]] connector_io_threads_t connector_io_threads(std::size_t configured,
                                                              const config::ConnectionsConfig& connections) noexcept;

    // The backends pool (runtime_t::backends_scheduler()) for
    // service.runtime.backends.threads. A statement holds one of its threads for
    // the whole remote call — the connection checkout and the wait for the
    // backend's answer — so with fewer threads than connections (the sum of
    // pool_size over every mysql / postgresql / clickhouse entry) the surplus
    // connections idle, and one slow alias can take every thread from the
    // others. 0 (auto) is max(2, hardware threads) and at least the
    // connections; a configured value is taken as is, logged as a warning
    // (BACKENDS) when it is below them.
    [[nodiscard]] std::size_t backends_threads(std::size_t configured, const config::ConnectionsConfig& connections);

    struct remote_backends_options_t {
        connector_factories_t connectors{};
        // The io pools the three connector managers own: main sizes them with
        // connector_io_threads; one thread each by default.
        connector_io_threads_t connector_threads{};
        // The pool the backends run the federation's statements on (the
        // connection checkout and the wait for the answer, off the engine's
        // executors): runtime_t::backends_scheduler(), shared by the three
        // backends (integration/statement_executor.hpp). Required; it must run
        // until this module is destroyed.
        actor_zeta::scheduler::sharing_scheduler* statements = nullptr;
    };

    // What the startup discovery of one connection answered: the rows the
    // engine's federation metadata gets once it is open.
    struct discovered_connection_t {
        backend_type_t type;
        qualified_name_t scope;
        federation::discovered_tables_t tables;
    };

    // A connection of config.yaml as the federation registry lists it.
    struct configured_server_t {
        std::string uid;
        backend_type_t type;
    };

    // The remote backends module — built after runtime_t and BEFORE engine_t,
    // because the engine opens with the federation's primitives, whose registry
    // (uid → backend) must be complete by then:
    //
    //   runtime_t → remote_backends_t → engine_t → (S3 / file / Kafka actors,
    //   session factory, servers — main.cpp's locals)
    //
    //  - the three connector managers (MySQL / PostgreSQL / ClickHouse), sole
    //    owners of their connections, and the integration backends that drive
    //    them (db::MySQLManager / PostgressManager / ClickhouseManager, the
    //    federation's backend_t implementations);
    //  - open(): every remote connection of `connections:` opened and its tables
    //    discovered (retried per service.connection_retry; one that stays
    //    unreachable is logged and left out of the connector manager, its uid
    //    still in the registry: its statements then fail at the connection), and
    //    the federation registry installed — every configured connection.
    //
    // Destroyed after the engine: the registry is reset first (no hook or
    // operator can reach a backend after that), then the backends (the statement jobs they
    // posted drain), then the connector managers. Not thread-safe: the
    // assembling thread builds it and opens it before the engine exists.
    class remote_backends_t final {
    public:
        remote_backends_t(std::pmr::memory_resource* resource, remote_backends_options_t options = {});
        remote_backends_t(const remote_backends_t&) = delete;
        remote_backends_t& operator=(const remote_backends_t&) = delete;
        ~remote_backends_t();

        // Opens and discovers the remote connections and installs the registry.
        // A descriptor the connector can never accept (a malformed port, a
        // pool_size below 1) is a configuration error: every descriptor is
        // checked before the first connection opens, and the first bad one is
        // returned (invalid_parameter) so startup aborts. Nothing an attempt
        // answers aborts: an unreachable backend is retried, then left out; one
        // whose discovery the metadata cannot record is left out at once
        // (open_failure_kind_t, connection_retry.hpp) — whatever error code
        // either carries.
        [[nodiscard]] core::error_t open(const config::ConnectionsConfig& connections,
                                         const config::ConnectionRetryConfig& retry);

        // What open() found: the discovery of every connection that opened, and
        // every configured connection (opened or not).
        [[nodiscard]] const std::vector<discovered_connection_t>& discovered() const noexcept { return discovered_; }
        [[nodiscard]] const std::vector<configured_server_t>& servers() const noexcept { return servers_; }

        // The owned parts, for tests that drive one of them directly.
        [[nodiscard]] mysql::ConnectorManager& mysql_connectors() { return *mysql_connectors_; }
        [[nodiscard]] pg::ConnectorManager& postgres_connectors() { return *pg_connectors_; }
        [[nodiscard]] ch::ConnectorManager& clickhouse_connectors() { return *ch_connectors_; }
        [[nodiscard]] db::MySQLManager& mysql() { return *mysql_; }
        [[nodiscard]] db::PostgressManager& postgres() { return *postgres_; }
        [[nodiscard]] db::ClickhouseManager& clickhouse() { return *clickhouse_; }

    private:
        std::pmr::memory_resource* resource_;
        // Declaration order is construction order and the reverse of
        // destruction: the connector managers outlive the backends that point
        // into them.
        std::unique_ptr<mysql::ConnectorManager> mysql_connectors_;
        std::unique_ptr<pg::ConnectorManager> pg_connectors_;
        std::unique_ptr<ch::ConnectorManager> ch_connectors_;
        std::unique_ptr<db::MySQLManager> mysql_;
        std::unique_ptr<db::PostgressManager> postgres_;
        std::unique_ptr<db::ClickhouseManager> clickhouse_;
        std::vector<discovered_connection_t> discovered_;
        std::vector<configured_server_t> servers_;
    };

} // namespace otterstax::backends
