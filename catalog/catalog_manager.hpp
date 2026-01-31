// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "connectors/mysql_manager.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/translators/input/mysql_to_complex.hpp"
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

namespace mysqlc {
    class CatalogManager final : public actor_zeta::cooperative_supervisor<CatalogManager> {
    public:
        CatalogManager(std::pmr::memory_resource* res);
        void set_connector_manager(std::shared_ptr<ConnectorManager> conn_manager);

        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;

    protected:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        // Behaviors
        actor_zeta::behavior_t get_catalog_schema_;
        actor_zeta::behavior_t add_connection_schema_;
        actor_zeta::behavior_t remove_connection_schema_;
        actor_zeta::behavior_t get_tables_;

        log_t log_;
        components::catalog::catalog catalog_;
        std::shared_ptr<ConnectorManager> conn_manager_;
        std::mutex input_mtx_;
        TaskManager<std::function<void()>> worker_;

        /// async method
        auto get_catalog_schema(session_hash_t id, ParsedQueryDataPtr&& data) -> void;
        auto add_connection_schema(collection_full_name_t name) -> catalog::catalog_error;
        auto remove_connection_schema(const std::string& uuid) -> void;
        auto get_tables(const arrow::flight::sql::GetTables& command, shared_data<std::pmr::vector<table_info>> sdata)
            -> void;
        auto send_result(session_hash_t id, ParsedQueryDataPtr&& data, catalog::catalog_error err) -> void;
    };
} // namespace mysqlc