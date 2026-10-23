// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connectors/common/transfer.hpp"
#include "integration/clickhouse/clickhouse_backend.hpp"
#include "integration/postgresql/postgres_backend.hpp"
#include "integration/sql/mysql_backend.hpp"

#include <cassert>
#include <memory_resource>

namespace db {

    // What every backend is built on: one connection pool per backend type, the decode
    // metadata discovery fills, and the transfer pools of S3 aliases and local files.
    // Owned by ComponentManager (or a test fixture) and outliving every backend_set_t built
    // over it. Its pools are filled on the startup thread (register_connections) before the
    // first statement and only read afterwards. A type the deployment has no connection of
    // keeps an empty pool, whose statements answer do_not_exists.
    struct backend_registry_t {
        explicit backend_registry_t(std::pmr::memory_resource* resource)
            : mysql(resource)
            , postgresql(resource)
            , clickhouse(resource)
            , transfers(resource) {}

        backend_registry_t(const backend_registry_t&) = delete;
        backend_registry_t& operator=(const backend_registry_t&) = delete;

        conn::connection_pool_t mysql;
        conn::connection_pool_t postgresql;
        conn::connection_pool_t clickhouse;
        no_metadata_t mysql_metadata;
        pg_metadata_t pg_metadata;
        ch_metadata_t ch_metadata;
        // CREATE EXTERNAL TABLE / COPY ... TO: one pool per S3 alias and one for local files.
        conn::transfer_pools_t transfers;
    };

    // One backend of each type over a registry: what every Worker owns, and the catalog.
    // Plain objects; their coroutines run inside the owner's behavior. `owner` parks while
    // one of their statements is in flight (conn::wait_owner_t); null for an owner that polls.
    struct backend_set_t {
        backend_set_t(std::pmr::memory_resource* resource,
                      backend_registry_t& registry,
                      conn::wait_owner_t* owner = nullptr)
            : mysql(resource, &registry.mysql, &registry.mysql_metadata, owner)
            , postgresql(resource, &registry.postgresql, &registry.pg_metadata, owner)
            , clickhouse(resource, &registry.clickhouse, &registry.ch_metadata, owner)
            , resource_(resource) {}

        // actor-zeta allocates the coroutine frames of execute, execute_mixed and describe on it.
        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Runs the statement's remote slots on the backend `backend` names (the catalog's
        // classification): one backend's execute, or execute_mixed for a Mixed statement. A
        // local statement (Otterbrix) is handed back untouched.
        actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>>
        execute(session_hash_t id, ParsedQueryDataPtr data, backend_type_t backend) {
            switch (backend) {
                case backend_type_t::MySQL:
                    co_return co_await mysql.execute(id, std::move(data));
                case backend_type_t::PostgreSQL:
                    co_return co_await postgresql.execute(id, std::move(data));
                case backend_type_t::ClickHouse:
                    co_return co_await clickhouse.execute(id, std::move(data));
                case backend_type_t::Mixed:
                    co_return co_await execute_mixed(id, std::move(data));
                default:
                    co_return std::move(data);
            }
        }

        // Every backend that owns a slot of the statement describes its own stubs
        // (backend_t::describe), in the order execute runs them: MySQL, PostgreSQL,
        // ClickHouse. Each fills only the stubs node_backend_types gives it, so the
        // participants of a Mixed statement describe their shares in turn.
        actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>> describe(session_hash_t id,
                                                                                       ParsedQueryDataPtr data) {
            assert(data);
            if (owns_a_slot(*data, backend_type_t::MySQL)) {
                auto described = co_await mysql.describe(id, std::move(data));
                if (described.has_error()) {
                    co_return std::move(described);
                }
                data = std::move(described.value());
            }
            if (owns_a_slot(*data, backend_type_t::PostgreSQL)) {
                auto described = co_await postgresql.describe(id, std::move(data));
                if (described.has_error()) {
                    co_return std::move(described);
                }
                data = std::move(described.value());
            }
            if (owns_a_slot(*data, backend_type_t::ClickHouse)) {
                auto described = co_await clickhouse.describe(id, std::move(data));
                if (described.has_error()) {
                    co_return std::move(described);
                }
                data = std::move(described.value());
            }
            co_return std::move(data);
        }

        // A Mixed statement: every backend runs the slots node_backend_types gives it. Batch
        // by batch, innermost first — an outer slot may read an inner one's rows (an INSERT
        // whose SELECT ran on another backend becomes INSERT ... VALUES) — and within a
        // batch every backend's statements are sent before the first answer is awaited, so
        // MySQL, PostgreSQL and ClickHouse run side by side: the batch costs the slowest
        // backend's time, not the sum. Slots of one batch never read each other.
        actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>> execute_mixed(session_hash_t id,
                                                                                            ParsedQueryDataPtr data) {
            assert(data);
            assert(data->otterbrix_params);
            for (std::size_t batch = data->otterbrix_params->external_nodes.size(); batch-- > 0;) {
                // Every backend's slots are generated and sent before any answer is awaited;
                // a refusal by a later backend drops what the earlier ones sent (their
                // answers are discarded, as for a failed slot).
                auto my = mysql.send_batch(id, *data, batch);
                if (my.has_error()) {
                    co_return my.convert_error<ParsedQueryDataPtr>();
                }
                auto pg = postgresql.send_batch(id, *data, batch);
                if (pg.has_error()) {
                    co_return pg.convert_error<ParsedQueryDataPtr>();
                }
                auto ch = clickhouse.send_batch(id, *data, batch);
                if (ch.has_error()) {
                    co_return ch.convert_error<ParsedQueryDataPtr>();
                }
                if (auto answered = co_await mysql.answer_batch(*data, batch, std::move(my.value()));
                    answered.contains_error()) {
                    co_return std::move(answered);
                }
                if (auto answered = co_await postgresql.answer_batch(*data, batch, std::move(pg.value()));
                    answered.contains_error()) {
                    co_return std::move(answered);
                }
                if (auto answered = co_await clickhouse.answer_batch(*data, batch, std::move(ch.value()));
                    answered.contains_error()) {
                    co_return std::move(answered);
                }
            }
            co_return std::move(data);
        }

        MySQLBackend mysql;
        PostgresBackend postgresql;
        ClickhouseBackend clickhouse;

    private:
        // Whether `backend` runs a slot of the statement: the classification itself, or for
        // a Mixed statement any slot node_backend_types gives it.
        static bool owns_a_slot(const ParsedQueryData& data, backend_type_t backend) {
            if (data.backend_type != backend_type_t::Mixed) {
                return data.backend_type == backend;
            }
            for (const auto& node_backend : data.node_backend_types) {
                if (node_backend.second == backend) {
                    return true;
                }
            }
            return false;
        }

        std::pmr::memory_resource* resource_;
    };

} // namespace db
