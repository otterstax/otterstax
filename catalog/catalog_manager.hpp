// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "connectors/mysql/manager.hpp"
#include "connectors/postgresql/manager.hpp"
#include "connectors/clickhouse/manager.hpp"
#include "connectors/file/manager.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/input/mysql_to_complex.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "routes/catalog_manager.hpp"
#include "routes/scheduler.hpp"
#include "scheduler/schema_utils.hpp"
#include "utility/cv_wrapper.hpp"
#include "utility/session.hpp"
#include "utility/table_info.hpp"
#include "utility/worker.hpp"

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include "arrow/flight/sql/server.h"
#include "utility/logger.hpp"
#include <actor-zeta.hpp>
#include <boost/mysql/connect_params.hpp>
#include <components/catalog/catalog.hpp>

namespace catalog_ext {

    enum class ConnectionType : uint8_t {
        MySQL = 0,
        PostgreSQL = 1,
        ClickHouse = 2,
        File = 3,
    };

    struct ConnectionInfo {
        std::string uuid;
        ConnectionType type;
        collection_full_name_t collection_full_name;
    };

    enum class catalog_result_t {
        EmptySchema,
        Schema,
        BackendType,
    };

} // namespace catalog_ext

namespace mysqlc {
    class CatalogManager final : public actor_zeta::cooperative_supervisor<CatalogManager> {
    public:
        CatalogManager(std::pmr::memory_resource* res);
        void set_mysql_connector_manager(std::shared_ptr<ConnectorManager> mysql_conn_manager);
        void set_pg_connector_manager(std::shared_ptr<pgc::ConnectorManager> pg_conn_manager);
        void set_ch_connector_manager(std::shared_ptr<chc::ConnectorManager> ch_conn_manager);
        void set_file_connector_manager(std::shared_ptr<filec::ConnectorManager> file_conn_manager);

        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;

        // Connection type management
        void registerConnection(const std::string& uuid, catalog_ext::ConnectionType type,
                                const collection_full_name_t& name);
        void unregisterConnection(const std::string& uuid);
        std::optional<catalog_ext::ConnectionType> getConnectionType(const std::string& uuid) const;
        bool hasConnection(const std::string& uuid) const;

    protected:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        // Behaviors
        actor_zeta::behavior_t get_catalog_schema_;
        actor_zeta::behavior_t update_backend_type_;
        actor_zeta::behavior_t add_connection_schema_;
        actor_zeta::behavior_t remove_connection_schema_;
        actor_zeta::behavior_t get_tables_;

        log_t log_;
        components::catalog::catalog catalog_;
        std::shared_ptr<ConnectorManager> mysql_conn_manager_;
        std::shared_ptr<pgc::ConnectorManager> pg_conn_manager_;
        std::shared_ptr<chc::ConnectorManager> ch_conn_manager_;
        std::shared_ptr<filec::ConnectorManager> file_conn_manager_;
        std::mutex input_mtx_;
        TaskManager<std::packaged_task<void()>> worker_;

        // Connection type registry
        mutable std::mutex connection_registry_mtx_;
        std::unordered_map<std::string, catalog_ext::ConnectionInfo> connection_registry_;

        /// async method
        auto get_catalog_schema(session_hash_t id, ParsedQueryDataPtr&& data) -> void;
        auto update_backend_type(session_hash_t id, ParsedQueryDataPtr&& data) -> void;
        auto update_backend_type_impl(ParsedQueryDataPtr&& data) -> ParsedQueryDataPtr const;
        auto add_connection_schema(collection_full_name_t name) -> catalog::catalog_error;
        auto remove_connection_schema(const std::string& uuid) -> void;
        auto get_tables(const arrow::flight::sql::GetTables& command, shared_data<std::pmr::vector<table_info>> sdata)
            -> void;
        auto send_result(session_hash_t id, ParsedQueryDataPtr&& data, catalog::catalog_error err, catalog_ext::catalog_result_t result_type) -> void;
    };
} // namespace mysqlc