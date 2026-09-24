// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/parser.hpp"
#include "types/otterbrix.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"
#include <actor-zeta.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>
#include <core/result_wrapper.hpp>

#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory_resource>
#include <mutex>
#include <string>
#include <vector>

namespace db {

    // Per-uid manifest of the external tables mirrored into the uid's engine
    // database: the collection `<uid>.<external_manifest_collection>`, one row
    // (external_manifest_column STRING = encoded collection name) per mirror.
    // It exists because the engine's pg_class is not readable through SQL, so
    // it is the only record of which mirrors a previous run left behind; a
    // registration writes its row BEFORE creating the collection and a drop
    // deletes it AFTER dropping, so the manifest is always a superset of the
    // mirrors that exist. No encoded mirror name can collide with it: an
    // encoded name always carries two ':' separators.
    inline constexpr const char* external_manifest_collection = "__otterstax_tables";
    inline constexpr const char* external_manifest_column = "collection";

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

        actor_zeta::unique_future<core::result_wrapper_t<std::pair<components::cursor::cursor_t_ptr, ParsedQueryDataPtr>>>
        get_schema(session_hash_t id,
                   std::pmr::map<qualified_name_t, size_t> dependencies,
                   ParsedQueryDataPtr data);

        // Registration channel: mirrors external (remote-backend) tables into the
        // engine pg_catalog so the planner can resolve them by OID.
        // Makes the engine database "<db_name>" (one per connection uid) and
        // its manifest exist. Answers `true` when the database was created and
        // `false` when the engine already held it: the catalog's
        // check_database_ownership keeps user DDL off a uid's name, so an
        // existing database of that name is this connection's mirror from a
        // previous run over the same data dir, to be reconciled, not refused.
        // The one exception is a data dir written before that guard existed,
        // where a user database of the uid's name is taken for the mirror.
        actor_zeta::unique_future<core::result_wrapper_t<bool>> register_external_database(std::string db_name);

        // Makes the engine collection for the external table `name` — the
        // connection uid is the engine database, the remaining qualifiers fold
        // into the encoded collection name — carry exactly `columns`, and
        // answers its pg_class OID: an absent collection is created; a present
        // one with the same column names and types is reused under its
        // restored OID; a present one with another schema is dropped and
        // recreated (logged). The manifest row is written first.
        actor_zeta::unique_future<core::result_wrapper_t<components::catalog::oid_t>>
        register_external_table(qualified_name_t name, std::vector<components::table::column_definition_t> columns);

        // Drops the engine collection register_external_table made for `name`
        // and its manifest row, so the catalog can undo a registration it could
        // not mirror: an external table present in the engine catalog but
        // absent from the catalog's store would resolve nowhere.
        actor_zeta::unique_future<core::result_wrapper_t<bool>> drop_external_table(qualified_name_t name);

        // Drops every mirror the uid's manifest lists that `live` — the tables
        // the current discovery returned — does not name, manifest row
        // included; a manifest row whose collection is already gone is deleted
        // alone. Answers the number of manifest rows removed.
        actor_zeta::unique_future<core::result_wrapper_t<size_t>>
        drop_stale_external_tables(std::string db_name, std::pmr::vector<qualified_name_t> live);

        actor_zeta::unique_future<core::result_wrapper_t<bool>> create_table(session_hash_t id,
                                                                             std::string database,
                                                                             std::string table,
                                                                             components::vector::data_chunk_t chunk);

        using dispatch_traits = actor_zeta::dispatch_traits<&OtterbrixManager::execute,
                                                            &OtterbrixManager::get_schema,
                                                            &OtterbrixManager::create_table,
                                                            &OtterbrixManager::register_external_database,
                                                            &OtterbrixManager::register_external_table,
                                                            &OtterbrixManager::drop_external_table,
                                                            &OtterbrixManager::drop_stale_external_tables>;


        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

    private:
        std::pmr::memory_resource* resource_;
        std::unique_ptr<IDataManager> data_manager_;
        log_t log_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "OtterbrixManager::mutex");
        actor_zeta::behavior_t current_behavior_;

        // Manifest of `db_name`: the manifest collection created when absent,
        // reused when the engine already holds it.
        core::error_t ensure_manifest(const std::string& db_name);
        // Encoded collection names the manifest of `db_name` lists.
        core::result_wrapper_t<std::pmr::vector<std::pmr::string>> read_manifest(const std::string& db_name);
        // The manifest row of `collection` written exactly once: any row it
        // already has is deleted first.
        core::error_t write_manifest_row(const std::string& db_name, const std::string& collection);
        core::error_t delete_manifest_row(const std::string& db_name, const std::string& collection);
        // DROP TABLE of one engine collection; the engine's verdict as is.
        core::error_t drop_collection(const std::string& db_name, const std::string& collection);
    };
} // namespace db
