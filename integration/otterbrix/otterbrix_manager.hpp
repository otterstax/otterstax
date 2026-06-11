// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/parser.hpp"
#include "types/otterbrix.hpp"
#include "utility/pipeline_error.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"
#include <actor-zeta.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>

#include <memory_resource>
#include <mutex>
#include <string>
#include <vector>

namespace db {
    class OtterbrixManager final : public actor_zeta::actor::actor_mixin<OtterbrixManager> {
    public:
        using is_cooperative_actor_type = void; // Required by actor_zeta::send() concept
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        OtterbrixManager(std::pmr::memory_resource* res, std::unique_ptr<IDataManager> data_manager);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        /// handler coroutines
        actor_zeta::unique_future<components::cursor::cursor_t_ptr> execute(session_hash_t id,
                                                                            OtterbrixStatementPtr params);

        actor_zeta::unique_future<otterstax::result<std::pair<components::cursor::cursor_t_ptr, ParsedQueryDataPtr>>>
        get_schema(session_hash_t id,
                   std::pmr::map<qualified_name_t, size_t> dependencies,
                   ParsedQueryDataPtr data);

        // Registration channel: mirrors external (remote-backend) tables into the
        // engine pg_catalog so the planner can resolve them by OID.
        // Creates the engine database "<db_name>" (one per connection uid).
        actor_zeta::unique_future<otterstax::result<bool>> register_external_database(std::string db_name);

        // Creates collection `encoded_collection` in database `encoded_db` and
        // reads back its pg_class OID. `name` is the original qualified name,
        // used for diagnostics only.
        actor_zeta::unique_future<otterstax::result<components::catalog::oid_t>>
        register_external_table(qualified_name_t name,
                                std::string encoded_db,
                                std::string encoded_collection,
                                std::vector<components::table::column_definition_t> columns);

        // Drops the engine database "<db_name>" together with all collections.
        actor_zeta::unique_future<otterstax::result<bool>> drop_external_database(std::string db_name);

        using dispatch_traits = actor_zeta::dispatch_traits<&OtterbrixManager::execute,
                                                            &OtterbrixManager::get_schema,
                                                            &OtterbrixManager::register_external_database,
                                                            &OtterbrixManager::register_external_table,
                                                            &OtterbrixManager::drop_external_database>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

    private:
        std::pmr::memory_resource* resource_;
        std::unique_ptr<IDataManager> data_manager_;
        log_t log_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "OtterbrixManager::mutex");
        actor_zeta::behavior_t current_behavior_;
    };
} // namespace db
