// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/log/log.hpp>

#include "catalog/discovery.hpp"
#include "catalog/schema_store.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "utility/session.hpp"
#include "utility/table_info.hpp"
#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser and arrow
#undef DAY
#undef SECOND

#include "utility/logger.hpp"
#include <actor-zeta.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace catalog_ext {

    // Every entry the catalog mirrors is a base table of a remote backend; this
    // is the FlightSQL `table_type` it is listed under and the only value the
    // GetTables `table_types` filter can match.
    inline constexpr std::string_view table_type_name = "TABLE";

    // The GetTables metadata command: the Flight SQL CommandGetTables filters
    // decoupled from any wire-protocol library. `catalog` is an exact match on
    // the database part; the two *_filter_pattern fields are SQL LIKE patterns
    // (% and _); a non-empty `table_types` lists anything only if it names
    // `table_type_name`.
    struct get_tables_command_t {
        std::optional<std::string> catalog;
        std::optional<std::string> db_schema_filter_pattern;
        std::optional<std::string> table_name_filter_pattern;
        std::vector<std::string> table_types;
        bool include_schema = false;
    };

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

        // The three backend integration actors (db::MySQLManager,
        // db::PostgressManager, db::ClickhouseManager). Each one is the sole
        // driver of its connector manager, so schema discovery is a message to
        // it (`discover`), never a call into the connector manager from here.
        // A backend that is not part of the deployment keeps an empty address.
        void set_backend_managers(actor_zeta::address_t mysql_manager,
                                  actor_zeta::address_t pg_manager,
                                  actor_zeta::address_t ch_manager);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        /// handler coroutines
        actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>>
        get_catalog_schema(session_hash_t id, ParsedQueryDataPtr data);

        actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>>
        update_backend_type(session_hash_t id, ParsedQueryDataPtr data);

        // Discovers the table(s) `name` designates on the backend of `type`
        // (empty collection → every table of the database/schema), registers
        // them in the engine and mirrors them in the store. Sent by
        // ConnectorManager::addConnection, which knows its backend; a uid that
        // is already registered under another type is an invalid_parameter
        // error. Lazily re-run from the pre-pass with the registered type.
        actor_zeta::unique_future<core::error_t> add_connection_schema(qualified_name_t name,
                                                                       catalog_ext::ConnectionType type);

        // Guard for database-level DDL: the engine database named after a
        // registered connection uid holds that connection's mirrored tables,
        // and the kafka object database belongs to the KafkaManager. A user
        // CREATE DATABASE / DROP DATABASE on such a name would desynchronise
        // the mirror (dead OIDs in the store; a user database taken for the
        // mirror on the next start, which reuses a database of the uid's
        // name), so the Worker asks here before the statement reaches the
        // engine.
        // Matching is case-insensitive: an unquoted identifier reaches the
        // engine lower-cased while the mirror database keeps the uid's exact
        // spelling. Answers invalid_parameter for an owned name.
        actor_zeta::unique_future<core::error_t> check_database_ownership(std::string dbname);

        // The FlightSQL GetTables listing travels back as a value through the
        // future; the frontend awaits it on its own asio executor via
        // frontend/common/asio_future_bridge.hpp — no shared state, no blocking.
        // `catalog` is an exact match on the database part; the two
        // *_filter_pattern fields are SQL LIKE patterns (% and _) on the schema
        // and table parts; a non-empty `table_types` must name
        // catalog_ext::table_type_name for anything to be listed.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<table_info>>>
        get_tables(catalog_ext::get_tables_command_t command);

        using dispatch_traits = actor_zeta::dispatch_traits<&CatalogManager::get_catalog_schema,
                                                            &CatalogManager::update_backend_type,
                                                            &CatalogManager::add_connection_schema,
                                                            &CatalogManager::check_database_ownership,
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
        actor_zeta::address_t mysql_manager_;
        actor_zeta::address_t pg_manager_;
        actor_zeta::address_t ch_manager_;
        OTX_LOCKABLE_N(std::mutex, mutex_, "CatalogManager::mutex");
        actor_zeta::behavior_t current_behavior_;

        // Backend type per connection uid whose tables were mirrored into the
        // engine — one engine database per uid, made to exist by the uid's
        // first registration (a uid absent here has not completed one).
        // Actor-confined: written and read only from handler bodies, which
        // enqueue_impl serialises under mutex_.
        std::pmr::unordered_map<std::pmr::string, catalog_ext::ConnectionType> connection_registry_;

        void registerConnection(const std::string& uuid, catalog_ext::ConnectionType type);
        std::optional<catalog_ext::ConnectionType> getConnectionType(const std::string& uuid) const;

        core::result_wrapper_t<ParsedQueryDataPtr> update_backend_type_impl(ParsedQueryDataPtr&& data);
        // Shared pre-pass of update_backend_type/get_catalog_schema: normalizes
        // target names per connection type and lazily registers external tables
        // missing from the store (DDL targets are skipped — CREATE targets do
        // not exist yet, DROP needs no schema). Takes a non-owning reference:
        // the pass MUTATES the parsed data (name normalization, store fills).
        actor_zeta::unique_future<core::error_t> ensure_external_targets_registered(ParsedQueryData& data);
        // Body of add_connection_schema once the backend type is settled:
        // discovery through the backend actor, engine registration, store
        // mirror, registry entry.
        actor_zeta::unique_future<core::error_t> register_tables(const qualified_name_t& name,
                                                                 catalog_ext::ConnectionType type);
        // Asks the backend actor of `conn_type` for the table(s) `name`
        // designates. Unified contract for all three backends: empty
        // `name.collection` → discover every table of the configured
        // database/schema; non-empty → discover that single table. Does NOT
        // touch the engine catalog, the local store or the connection
        // registry. Any per-table failure makes the whole discovery fail.
        actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
        discover_connection_schemas(const qualified_name_t& name, catalog_ext::ConnectionType conn_type);
    };
} // namespace mysql
