// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "catalog/schema_store.hpp"
#include "connectors/clickhouse/manager.hpp"
#include "connectors/mysql/manager.hpp"
#include "connectors/postgresql/manager.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "otterbrix/translators/input/mysql_to_complex.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "scheduler/schema_utils.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/pipeline_error.hpp"
#include "utility/session.hpp"
#include "utility/table_info.hpp"

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include "arrow/flight/sql/server.h"
#include "utility/logger.hpp"
#include <actor-zeta.hpp>
#include <boost/mysql/connect_params.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <unordered_set>

namespace catalog_ext {

    enum class ConnectionType : uint8_t
    {
        MySQL = 0,
        PostgreSQL = 1,
        ClickHouse = 2
    };

    struct ConnectionInfo {
        std::string uuid;
        ConnectionType type;
        qualified_name_t collection_full_name;
    };

    // One table discovered on a remote backend: full qualified name plus the
    // STRUCT of column types produced by the input translators.
    struct discovered_table_t {
        qualified_name_t name;
        components::types::complex_logical_type schema;
    };

    using discovered_tables_t = std::pmr::vector<discovered_table_t>;

} // namespace catalog_ext

namespace mysql {
    class CatalogManager final : public actor_zeta::actor::actor_mixin<CatalogManager> {
    public:
        using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // `otterbrix_manager` is the address of db::OtterbrixManager — the
        // registration channel through which external table schemas are
        // mirrored into the engine pg_catalog.
        CatalogManager(std::pmr::memory_resource* res, actor_zeta::address_t otterbrix_manager);
        void set_mysql_connector_manager(std::shared_ptr<ConnectorManager> mysql_conn_manager);
        void set_pg_connector_manager(std::shared_ptr<pg::ConnectorManager> pg_conn_manager);
        void set_ch_connector_manager(std::shared_ptr<ch::ConnectorManager> ch_conn_manager);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Connection type management
        void registerConnection(const std::string& uuid,
                                catalog_ext::ConnectionType type,
                                const qualified_name_t& name);
        void unregisterConnection(const std::string& uuid);
        std::optional<catalog_ext::ConnectionType> getConnectionType(const std::string& uuid) const;
        bool hasConnection(const std::string& uuid) const;

        /// handler coroutines
        actor_zeta::unique_future<otterstax::result<ParsedQueryDataPtr>> get_catalog_schema(session_hash_t id,
                                                                                            ParsedQueryDataPtr data);

        actor_zeta::unique_future<otterstax::result<ParsedQueryDataPtr>> update_backend_type(session_hash_t id,
                                                                                             ParsedQueryDataPtr data);

        actor_zeta::unique_future<core::error_t> add_connection_schema(qualified_name_t name);

        actor_zeta::unique_future<void> remove_connection_schema(std::string uuid);

        actor_zeta::unique_future<void> get_tables(arrow::flight::sql::GetTables command,
                                                   shared_data<std::pmr::vector<table_info>> sdata);

        using dispatch_traits = actor_zeta::dispatch_traits<&CatalogManager::get_catalog_schema,
                                                            &CatalogManager::update_backend_type,
                                                            &CatalogManager::add_connection_schema,
                                                            &CatalogManager::remove_connection_schema,
                                                            &CatalogManager::get_tables>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

    private:
        std::pmr::memory_resource* resource_;
        log_t log_;
        // Actor-confined mirror of the external tables registered in the engine
        // catalog: full qualified name + discovered STRUCT schema, keyed by the
        // engine pg_class OID.
        otterstax::catalog::schema_store_t store_;
        actor_zeta::address_t otterbrix_manager_;
        // Connection uids whose engine database was already created via
        // register_external_database (one engine database per uid).
        std::pmr::unordered_set<std::pmr::string> registered_dbs_;
        std::shared_ptr<ConnectorManager> mysql_conn_manager_;
        std::shared_ptr<pg::ConnectorManager> pg_conn_manager_;
        std::shared_ptr<ch::ConnectorManager> ch_conn_manager_;
        std::mutex mutex_;
        actor_zeta::behavior_t current_behavior_;

        // Connection type registry
        mutable std::mutex connection_registry_mtx_;
        std::unordered_map<std::string, catalog_ext::ConnectionInfo> connection_registry_;

        otterstax::result<ParsedQueryDataPtr> update_backend_type_impl(ParsedQueryDataPtr&& data);
        // Shared pre-pass of update_backend_type/get_catalog_schema: normalizes
        // target names per connection type and lazily registers external tables
        // missing from the store (DDL targets are skipped — CREATE targets do
        // not exist yet, DROP needs no schema).
        actor_zeta::unique_future<core::error_t> ensure_external_targets_registered(ParsedQueryDataPtr& data);
        // Synchronous remote-schema discovery: probes the backend through the
        // connector managers and collects one (name, STRUCT) entry per table.
        // Does NOT touch the engine catalog or the local store.
        core::error_t discover_connection_schemas(const qualified_name_t& name, catalog_ext::discovered_tables_t& out);
    };
} // namespace mysql
