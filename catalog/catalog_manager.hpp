// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "connectors/clickhouse/manager.hpp"
#include "connectors/mysql/manager.hpp"
#include "connectors/postgresql/manager.hpp"
#include "connectors/clickhouse/manager.hpp"
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
#include <components/catalog/catalog.hpp>
#include <core/result_wrapper.hpp>

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
        collection_full_name_t collection_full_name;
    };

} // namespace catalog_ext

namespace mysql {
    class CatalogManager final : public actor_zeta::actor::actor_mixin<CatalogManager> {
    public:
        using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        CatalogManager(std::pmr::memory_resource* res);
        void set_mysql_connector_manager(std::shared_ptr<ConnectorManager> mysql_conn_manager);
        void set_pg_connector_manager(std::shared_ptr<pg::ConnectorManager> pg_conn_manager);
        void set_ch_connector_manager(std::shared_ptr<ch::ConnectorManager> ch_conn_manager);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Connection type management
        void registerConnection(const std::string& uuid,
                                catalog_ext::ConnectionType type,
                                const collection_full_name_t& name);
        void unregisterConnection(const std::string& uuid);
        std::optional<catalog_ext::ConnectionType> getConnectionType(const std::string& uuid) const;
        bool hasConnection(const std::string& uuid) const;

        /// handler coroutines
        actor_zeta::unique_future<otterstax::result<ParsedQueryDataPtr>> get_catalog_schema(session_hash_t id,
                                                                                            ParsedQueryDataPtr data);

        actor_zeta::unique_future<otterstax::result<ParsedQueryDataPtr>> update_backend_type(session_hash_t id,
                                                                                             ParsedQueryDataPtr data);

        actor_zeta::unique_future<core::error_t> add_connection_schema(collection_full_name_t name);

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
        components::catalog::catalog catalog_;
        std::shared_ptr<ConnectorManager> mysql_conn_manager_;
        std::shared_ptr<pg::ConnectorManager> pg_conn_manager_;
        std::shared_ptr<ch::ConnectorManager> ch_conn_manager_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "CatalogManager::mutex");
        actor_zeta::behavior_t current_behavior_;

        // Connection type registry
        mutable OTX_LOCKABLE_N(std::mutex, connection_registry_mtx_, "CatalogManager::connection_registry_mtx");
        std::unordered_map<std::string, catalog_ext::ConnectionInfo> connection_registry_;

        auto update_backend_type_impl(ParsedQueryDataPtr&& data) -> ParsedQueryDataPtr const;
        core::error_t add_connection_schema_sync(collection_full_name_t name);
    };
} // namespace mysql
