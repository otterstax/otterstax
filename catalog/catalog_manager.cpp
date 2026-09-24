// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog_manager.hpp"

#include "integration/clickhouse/connection_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/identifier_types.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/table/column_definition.hpp>

#include <algorithm>
#include <cctype>
#include <string_view>
#include <thread>

using namespace components;

namespace {

    // result_wrapper_t only exposes its error by const reference, and a plain
    // error_t copy re-allocates the message on the default resource. Rebuild
    // the error on the actor's own resource instead.
    core::error_t copy_error(std::pmr::memory_resource* resource, const core::error_t& err) {
        return core::error_t(err.type, std::pmr::string{err.what.c_str(), resource});
    }

    core::error_t make_error(std::pmr::memory_resource* resource, core::error_code_t code, const std::string& what) {
        return core::error_t(code, std::pmr::string{what.c_str(), resource});
    }

    // True when the external entry's node needs no registered schema: CREATE
    // targets a table that does not exist yet, DROP removes one, and a subquery
    // stub (schema_node_t, node_type::unused) names no relation to register —
    // its schema is the backend's, which ClickhouseManager::describe writes
    // into the stub at prepare. Of the drop kinds only DROP TABLE and
    // DROP INDEX carry an alias-qualified name (the parser's
    // carries_table_reference); any other kind inside external_nodes is a
    // contract violation, not a target to skip.
    core::result_wrapper_t<bool> is_schema_exempt(std::pmr::memory_resource* resource,
                                                  const logical_plan::node_t& node) {
        switch (node.type()) {
            case logical_plan::node_type::create_collection_t:
            case logical_plan::node_type::create_index_t:
            case logical_plan::node_type::unused:
                return true;
            case logical_plan::node_type::drop_t: {
                const auto kind = static_cast<const logical_plan::node_drop_t&>(node).kind();
                if (kind == logical_plan::drop_target_kind::collection ||
                    kind == logical_plan::drop_target_kind::index) {
                    return true;
                }
                return make_error(resource,
                                  core::error_code_t::invalid_parameter,
                                  "External DROP target of kind " + std::to_string(static_cast<int>(kind)) +
                                      " carries no alias-qualified table name");
            }
            default:
                return false;
        }
    }

    // ASCII case-insensitive equality: identifiers reach the engine lower-cased
    // unless quoted, while a connection uid keeps its configured spelling.
    bool iequals(std::string_view lhs, std::string_view rhs) {
        return lhs.size() == rhs.size() &&
               std::equal(lhs.begin(), lhs.end(), rhs.begin(), [](unsigned char a, unsigned char b) {
                   return std::tolower(a) == std::tolower(b);
               });
    }

    // SQL LIKE over the whole value: `%` matches any run, `_` one character,
    // everything else literally (no escape character — FlightSQL patterns
    // define none). Case-sensitive, like the names it filters.
    bool like_match(std::string_view pattern, std::string_view value) {
        size_t p = 0;
        size_t v = 0;
        size_t star_p = std::string_view::npos;
        size_t star_v = 0;
        while (v < value.size()) {
            if (p < pattern.size() && pattern[p] == '%') {
                star_p = p++;
                star_v = v;
            } else if (p < pattern.size() && (pattern[p] == '_' || pattern[p] == value[v])) {
                ++p;
                ++v;
            } else if (star_p != std::string_view::npos) {
                p = star_p + 1;
                v = ++star_v;
            } else {
                return false;
            }
        }
        while (p < pattern.size() && pattern[p] == '%') {
            ++p;
        }
        return p == pattern.size();
    }

    const char* backend_name(catalog_ext::ConnectionType type) {
        switch (type) {
            case catalog_ext::ConnectionType::MySQL:
                return "MySQL";
            case catalog_ext::ConnectionType::PostgreSQL:
                return "PostgreSQL";
            case catalog_ext::ConnectionType::ClickHouse:
                return "ClickHouse";
        }
        return "unknown";
    }

} // namespace

namespace mysql {
    CatalogManager::CatalogManager(std::pmr::memory_resource* res, actor_zeta::address_t otterbrix_manager)
        : resource_(res)
        , log_(get_logger(logger_tag::CATALOG_MANAGER))
        , store_(res)
        , otterbrix_manager_(std::move(otterbrix_manager))
        , mysql_manager_(actor_zeta::address_t::empty_address())
        , pg_manager_(actor_zeta::address_t::empty_address())
        , ch_manager_(actor_zeta::address_t::empty_address())
        , connection_registry_(res) {
        assert(log_.is_valid());
        assert(res != nullptr);
        log_->info("CatalogManager initialized successfully");
    }

    void CatalogManager::set_backend_managers(actor_zeta::address_t mysql_manager,
                                              actor_zeta::address_t pg_manager,
                                              actor_zeta::address_t ch_manager) {
        mysql_manager_ = std::move(mysql_manager);
        pg_manager_ = std::move(pg_manager);
        ch_manager_ = std::move(ch_manager);
    }

    void CatalogManager::registerConnection(const std::string& uuid, catalog_ext::ConnectionType type) {
        connection_registry_.insert_or_assign(std::pmr::string{uuid.c_str(), resource()}, type);
        log_->debug("Registered connection: {} with type: {}", uuid, static_cast<int>(type));
    }

    std::optional<catalog_ext::ConnectionType> CatalogManager::getConnectionType(const std::string& uuid) const {
        auto it = connection_registry_.find(std::pmr::string{uuid.c_str(), resource()});
        if (it != connection_registry_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    CatalogManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        OTX_ZONE_N("CatalogManager::enqueue_impl");
        std::lock_guard guard(mutex_);
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                std::this_thread::yield();
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t CatalogManager::behavior(actor_zeta::mailbox::message* msg) {
        OTX_ZONE_N("CatalogManager::behavior");
        auto cmd = msg->command();
        if (cmd == actor_zeta::msg_id<CatalogManager, &CatalogManager::get_catalog_schema>) {
            co_await actor_zeta::dispatch(this, &CatalogManager::get_catalog_schema, msg);
        } else if (cmd == actor_zeta::msg_id<CatalogManager, &CatalogManager::update_backend_type>) {
            co_await actor_zeta::dispatch(this, &CatalogManager::update_backend_type, msg);
        } else if (cmd == actor_zeta::msg_id<CatalogManager, &CatalogManager::add_connection_schema>) {
            co_await actor_zeta::dispatch(this, &CatalogManager::add_connection_schema, msg);
        } else if (cmd == actor_zeta::msg_id<CatalogManager, &CatalogManager::check_database_ownership>) {
            co_await actor_zeta::dispatch(this, &CatalogManager::check_database_ownership, msg);
        } else if (cmd == actor_zeta::msg_id<CatalogManager, &CatalogManager::get_tables>) {
            co_await actor_zeta::dispatch(this, &CatalogManager::get_tables, msg);
        }
    }

    core::result_wrapper_t<ParsedQueryDataPtr> CatalogManager::update_backend_type_impl(ParsedQueryDataPtr&& data) {
        OTX_ZONE_N("catalog::backend_type_detection");
        assert(data != nullptr);
        log_->debug("update_backend_type_impl: start updating backend type for query with external nodes count {}",
                    static_cast<int>(data->otterbrix_params->external_nodes.size()));

        // Classification runs exactly once per parsed statement: the Worker
        // stores the classified data and never sends it back, so a statement
        // that already carries a backend type is a caller contract violation.
        if (data->backend_type != backend_type_t::Unknown) {
            log_->error("update_backend_type_impl: backend type is already set to {}",
                        static_cast<int>(data->backend_type));
            return make_error(resource(),
                              core::error_code_t::invalid_parameter,
                              "Backend type is already classified as " +
                                  std::to_string(static_cast<int>(data->backend_type)));
        }

        bool has_mysql = false;
        bool has_pg = false;
        bool has_ch = false;

        for (auto& batch : data->otterbrix_params->external_nodes) {
            for (auto& entry : batch) {
                auto& target = entry.target;
                const auto& name = target.name;

                // Determine backend type for this node
                auto conn_type_opt = getConnectionType(name.unique_identifier);
                if (conn_type_opt.has_value()) {
                    auto conn_type = conn_type_opt.value();
                    if (conn_type == catalog_ext::ConnectionType::MySQL) {
                        has_mysql = true;
                        data->node_backend_types[name.unique_identifier] = backend_type_t::MySQL;
                    } else if (conn_type == catalog_ext::ConnectionType::PostgreSQL) {
                        has_pg = true;
                        data->node_backend_types[name.unique_identifier] = backend_type_t::PostgreSQL;
                    } else if (conn_type == catalog_ext::ConnectionType::ClickHouse) {
                        has_ch = true;
                        data->node_backend_types[name.unique_identifier] = backend_type_t::ClickHouse;
                    }
                }

                auto exempt = is_schema_exempt(resource(), **entry.node);
                if (exempt.has_error()) {
                    log_->error("update_backend_type_impl: {}", exempt.error().what.c_str());
                    return copy_error(resource(), exempt.error());
                }
                if (exempt.value()) {
                    continue;
                }

                // Stamp the engine OID resolved at registration time.
                auto oid = store_.find(name);
                if (oid == components::catalog::INVALID_OID) {
                    if (!name.unique_identifier.empty()) {
                        log_->error("update_backend_type_impl: no registered schema for external table {}",
                                    name.to_string());
                        return make_error(resource(),
                                          core::error_code_t::table_not_exists,
                                          "External table is not registered: " + name.to_string());
                    }
                    // No connection uid — local Otterbrix table, resolved by the engine itself.
                } else {
                    target.oid = oid;
                    (*entry.node)->set_table_oid(oid);
                }
            }
        }

        // Set backend type based on connections found
        int backend_count = (has_mysql ? 1 : 0) + (has_pg ? 1 : 0) + (has_ch ? 1 : 0);
        if (backend_count > 1) {
            data->backend_type = backend_type_t::Mixed;
        } else if (has_ch) {
            data->backend_type = backend_type_t::ClickHouse;
        } else if (has_pg) {
            data->backend_type = backend_type_t::PostgreSQL;
        } else if (has_mysql) {
            data->backend_type = backend_type_t::MySQL;
        } else {
            // No external connections found — check if this is a join node whose
            // children will be executed independently and merged by Otterbrix.
            auto node_type = data->otterbrix_params->node->type();
            if (node_type == logical_plan::node_type::join_t || node_type == logical_plan::node_type::intersect_t ||
                node_type == logical_plan::node_type::union_t) {
                log_->debug("update_backend_type_impl: no external connections found, but node is join/set op — using "
                            "Otterbrix");
                data->backend_type = backend_type_t::Otterbrix;
            } else {
                log_->error(
                    "update_backend_type_impl: Can't determine backend type: no connections found for external nodes");
                return make_error(resource(),
                                  core::error_code_t::schema_error,
                                  "Cannot determine backend type: no registered connection for the external nodes");
            }
        }
        log_->debug("update_backend_type_impl: determined backend_type = {}", static_cast<int>(data->backend_type));
        return std::move(data);
    }

    actor_zeta::unique_future<core::error_t> CatalogManager::ensure_external_targets_registered(ParsedQueryData& data) {
        OTX_ZONE_N("catalog::ensure_external_targets_registered");
        // Normalize names and lazily register external tables the engine does
        // not know yet (e.g. created at runtime by a previous DDL statement).
        // Must run BEFORE update_backend_type_impl: OID stamping there requires
        // every non-DDL external table to be present in the store.
        for (auto& batch : data.otterbrix_params->external_nodes) {
            for (auto& entry : batch) {
                auto& target = entry.target;
                if (target.name.unique_identifier.empty()) {
                    continue;
                }
                auto exempt = is_schema_exempt(resource(), **entry.node);
                if (exempt.has_error()) {
                    log_->error("ensure_external_targets_registered: {}", exempt.error().what.c_str());
                    co_return copy_error(resource(), exempt.error());
                }
                if (exempt.value()) {
                    continue;
                }
                auto conn_type_opt = getConnectionType(target.name.unique_identifier);
                if (conn_type_opt.has_value() && conn_type_opt.value() != catalog_ext::ConnectionType::PostgreSQL) {
                    // ignore schema qualifier if not postgres
                    target.name.schema = "";
                }
                if (store_.find(target.name) == components::catalog::INVALID_OID) {
                    // The registry is the only source of a uid's backend here:
                    // a uid it does not know was never registered by a
                    // connector manager, so there is no backend to probe.
                    if (!conn_type_opt.has_value()) {
                        log_->error("ensure_external_targets_registered: no registered connection for uid {}",
                                    target.name.unique_identifier);
                        co_return make_error(resource(),
                                             core::error_code_t::do_not_exists,
                                             "No registered connection for uuid: " + target.name.unique_identifier);
                    }
                    auto err = co_await register_tables(target.name, conn_type_opt.value());
                    if (err.contains_error()) {
                        co_return std::move(err);
                    }
                }
            }
        }
        co_return core::error_t::no_error();
    }

    actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>>
    CatalogManager::update_backend_type(session_hash_t id, ParsedQueryDataPtr data) {
        OTX_ZONE_N("catalog::update_backend_type");
        auto err = co_await ensure_external_targets_registered(*data);
        if (err.contains_error()) {
            log_->error("update_backend_type: {}", err.what.c_str());
            co_return std::move(err);
        }
        auto impl_result = update_backend_type_impl(std::move(data));
        if (impl_result.has_error()) {
            log_->error("update_backend_type: {}", impl_result.error().what.c_str());
            co_return std::move(impl_result);
        }
        auto updated_data = std::move(impl_result.value());
        log_->debug("update_backend_type: determined backend_type = {}", static_cast<int>(updated_data->backend_type));
        co_return std::move(updated_data);
    }

    actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>>
    CatalogManager::get_catalog_schema(session_hash_t id, ParsedQueryDataPtr data) {
        OTX_ZONE_N("catalog::get_catalog_schema");
        auto err = co_await ensure_external_targets_registered(*data);
        if (err.contains_error()) {
            co_return std::move(err);
        }

        auto impl_result = update_backend_type_impl(std::move(data));
        if (impl_result.has_error()) {
            log_->error("get_catalog_schema: {}", impl_result.error().what.c_str());
            co_return std::move(impl_result);
        }
        auto updated_data = std::move(impl_result.value());

        log_->debug(
            "get_catalog_schema: start getting catalog schema for query with external nodes count {}, backend type {}",
            static_cast<int>(updated_data->otterbrix_params->external_nodes.size()),
            static_cast<int>(updated_data->backend_type));

        // The transformer wraps table-referencing statements in a
        // node_sequence_t whose data-producing node is the LAST child;
        // planner-emitted sequences order children differently but never
        // reach this path. Unwrap before the aggregate check below.
        const logical_plan::node_t* schema_root = updated_data->otterbrix_params->node.get();
        if (schema_root->type() == logical_plan::node_type::sequence_t) {
            if (schema_root->children().empty()) {
                log_->error("get_catalog_schema: sequence node has no children, cannot resolve schema");
                co_return make_error(resource(),
                                     core::error_code_t::schema_error,
                                     "Sequence node has no children, cannot resolve schema");
            }
            schema_root = schema_root->children().back().get();
        }
        if (schema_root->type() != logical_plan::node_type::aggregate_t) {
            // node is not aggregate nor join - result is empty schema
            log_->debug("prepare_schema: node is not aggregate, returning empty schema");
            co_return std::move(updated_data);
        }

        for (auto& batch : updated_data->otterbrix_params->external_nodes) {
            for (auto& entry : batch) {
                auto* node = entry.node;
                if ((*node)->type() == logical_plan::node_type::aggregate_t) {
                    const auto& target = entry.target;

                    const auto* struct_schema = store_.schema_by_oid(target.oid);
                    if (struct_schema == nullptr) {
                        log_->error("get_catalog_schema: no schema registered for external table {}",
                                    target.name.to_string());
                        co_return make_error(resource(),
                                             core::error_code_t::schema_error,
                                             "No schema registered for external table: " + target.name.to_string());
                    }

                    const auto& agg = static_cast<logical_plan::node_aggregate_t&>(*(*node));
                    std::pmr::vector<types::complex_logical_type> schema_types(struct_schema->child_types().begin(),
                                                                               struct_schema->child_types().end(),
                                                                               resource());
                    auto initial_schema =
                        schema_utils::aggregate_filter_schema(agg,
                                                              updated_data->otterbrix_params->params_node.get(),
                                                              schema_types);

                    auto node_schema = schema_utils::make_node_schema(target.name,
                                                                      std::move(initial_schema),
                                                                      components::logical_plan::node_aggregate_t(agg));
                    *node = node_schema;
                }
            }
        }

        log_->debug("get_catalog_schema: determined backend_type = {}", static_cast<int>(updated_data->backend_type));
        co_return std::move(updated_data);
    }

    actor_zeta::unique_future<core::error_t> CatalogManager::add_connection_schema(qualified_name_t name,
                                                                                   catalog_ext::ConnectionType type) {
        OTX_ZONE_N("catalog::add_connection_schema");
        // One uid belongs to exactly one backend: the registry entry made by a
        // previous registration is authoritative, and a message naming another
        // backend for the same uid is a wiring error, not a re-registration.
        auto registered = getConnectionType(name.unique_identifier);
        if (registered.has_value() && registered.value() != type) {
            log_->error("add_connection_schema: uid {} is registered as {}, not {}",
                        name.unique_identifier,
                        backend_name(registered.value()),
                        backend_name(type));
            co_return make_error(resource(),
                                 core::error_code_t::invalid_parameter,
                                 "Connection '" + name.unique_identifier + "' is registered as " +
                                     backend_name(registered.value()) + ", not " + backend_name(type));
        }
        co_return co_await register_tables(name, type);
    }

    actor_zeta::unique_future<core::error_t> CatalogManager::register_tables(const qualified_name_t& name,
                                                                             catalog_ext::ConnectionType type) {
        OTX_ZONE_N("catalog::register_tables");
        const std::string uuid = name.unique_identifier;

        // Step 1: probe the remote backend and collect per-table STRUCT schemas.
        auto discovered = co_await discover_connection_schemas(name, type);
        if (discovered.has_error()) {
            co_return copy_error(resource(), discovered.error());
        }
        auto& tables = discovered.value();

        // A mirrored column is defined by its name, so a discovered column without
        // one fails the whole registration before the engine is touched. Its type
        // may carry no alias at all, and alias() has no null guard for such a type.
        for (const auto& table : tables) {
            const auto& fields = table.schema.child_types();
            for (size_t i = 0; i < fields.size(); ++i) {
                if (!fields[i].has_alias()) {
                    const std::string what = "Discovered table '" + table.name.to_string() +
                                             "' has a column without a name at position " + std::to_string(i);
                    log_->error("add_connection_schema: {}", what);
                    co_return make_error(resource(), core::error_code_t::schema_error, what);
                }
            }
        }

        // Step 2: the uid's first registration in this process makes the
        // per-connection engine database exist. The registry says whether it
        // is the first: a uid enters it only once its tables are mirrored, so a
        // failed attempt repeats this step. A database the engine already held
        // is the mirror a previous run left on this data dir; every mirror in
        // it that this discovery did not return is dropped before the
        // discovered ones are reconciled table by table.
        if (!getConnectionType(uuid).has_value()) {
            auto [db_sched, db_future] =
                actor_zeta::send(otterbrix_manager_, &db::OtterbrixManager::register_external_database, uuid);
            auto db_result = co_await std::move(db_future);
            if (db_result.has_error()) {
                log_->error("add_connection_schema: failed to create engine database for uid {}: {}",
                            uuid,
                            db_result.error().what.c_str());
                co_return make_error(resource(),
                                     core::error_code_t::schema_error,
                                     "Failed to create engine database for uid '" + uuid +
                                         "': " + db_result.error().what.c_str());
            }
            const bool created = db_result.value();
            if (!created) {
                std::pmr::vector<qualified_name_t> live(resource());
                live.reserve(tables.size());
                for (const auto& table : tables) {
                    live.push_back(table.name);
                }
                auto [stale_sched, stale_future] = actor_zeta::send(
                    otterbrix_manager_, &db::OtterbrixManager::drop_stale_external_tables, uuid, std::move(live));
                auto stale_result = co_await std::move(stale_future);
                if (stale_result.has_error()) {
                    log_->error("add_connection_schema: failed to drop the stale mirrors of uid {}: {}",
                                uuid,
                                stale_result.error().what.c_str());
                    co_return make_error(resource(),
                                         core::error_code_t::schema_error,
                                         "Failed to drop the stale mirrors of uid '" + uuid +
                                             "': " + stale_result.error().what.c_str());
                }
                log_->info("add_connection_schema: uid {} reuses its engine database; {} stale mirror(s) dropped",
                           uuid,
                           stale_result.value());
            }
        }

        // Step 3: register each discovered table in the engine catalog and
        // mirror it locally. The engine side reconciles a mirror a previous run
        // left (register_external_table); the OID it answers is the one the
        // engine holds, created or restored.
        for (auto& table : tables) {
            if (store_.find(table.name) != components::catalog::INVALID_OID) {
                log_->info("add_connection_schema: table {} already registered, skipping", table.name.to_string());
                continue;
            }

            // std::vector: the engine's make_node_create_collection takes the
            // column definitions by std::vector.
            const auto& fields = table.schema.child_types();
            std::vector<components::table::column_definition_t> columns;
            columns.reserve(fields.size());
            for (const auto& field : fields) {
                columns.emplace_back(field.alias(), field);
            }

            auto [tbl_sched, tbl_future] = actor_zeta::send(otterbrix_manager_,
                                                            &db::OtterbrixManager::register_external_table,
                                                            table.name,
                                                            std::move(columns));
            auto tbl_result = co_await std::move(tbl_future);
            if (tbl_result.has_error()) {
                log_->error("add_connection_schema: failed to register external table {}: {}",
                            table.name.to_string(),
                            tbl_result.error().what.c_str());
                co_return make_error(resource(),
                                     core::error_code_t::schema_error,
                                     "Failed to register external table '" + table.name.to_string() +
                                         "': " + tbl_result.error().what.c_str());
            }

            auto oid = tbl_result.value();
            if (auto err = store_.put(oid, table.name, std::move(table.schema)); err.contains_error()) {
                log_->error("add_connection_schema: failed to store schema for table {}: {}",
                            table.name.to_string(),
                            err.what.c_str());
                // The engine already holds the collection; without its mirror
                // in the store it would resolve nowhere, so the registration is
                // undone before the failure is reported. A failed undo is
                // reported on top of the original error, never in its place.
                auto [drop_sched, drop_future] =
                    actor_zeta::send(otterbrix_manager_, &db::OtterbrixManager::drop_external_table, table.name);
                auto drop_result = co_await std::move(drop_future);
                if (drop_result.has_error()) {
                    log_->error("add_connection_schema: failed to undo the engine registration of {}: {}",
                                table.name.to_string(),
                                drop_result.error().what.c_str());
                    err.what.append("; undo failed: ");
                    err.what.append(drop_result.error().what);
                }
                co_return std::move(err);
            }
            log_->info("add_connection_schema: registered {} with oid {}", table.name.to_string(), oid);
        }

        // The uid becomes routable only once its tables are mirrored: a failure
        // above leaves the registry unchanged, so the next statement naming the
        // uid retries discovery instead of resolving against a missing schema.
        registerConnection(uuid, type);
        co_return core::error_t::no_error();
    }

    actor_zeta::unique_future<core::error_t> CatalogManager::check_database_ownership(std::string dbname) {
        OTX_ZONE_N("catalog::check_database_ownership");
        // The registry is the authority: a uid is entered there only once its
        // tables are mirrored, which is exactly the state a user CREATE/DROP
        // DATABASE would corrupt.
        std::string_view owner;
        if (iequals(dbname, otterstax::kafka::KAFKA_DATABASE_NAME)) {
            owner = otterstax::kafka::KAFKA_DATABASE_NAME;
        } else {
            for (const auto& entry : connection_registry_) {
                if (iequals(dbname, entry.first)) {
                    owner = entry.first;
                    break;
                }
            }
        }
        if (owner.empty()) {
            co_return core::error_t::no_error();
        }
        log_->error("check_database_ownership: database '{}' is owned by connection '{}'", dbname, owner);
        std::pmr::string what{resource()};
        what.append("database '");
        what.append(dbname);
        what.append("' is owned by connection '");
        what.append(owner);
        what.push_back('\'');
        co_return core::error_t(core::error_code_t::invalid_parameter, std::move(what));
    }

    // Discovery is a message to the backend actor that owns the connector
    // manager; the catalog awaits its reply and never touches a connector
    // itself. The reply is settled by the time send() returns (the backend
    // actors run their handlers to completion on the sending thread), so the
    // co_await below never suspends across another catalog message.
    actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
    CatalogManager::discover_connection_schemas(const qualified_name_t& name, catalog_ext::ConnectionType conn_type) {
        OTX_ZONE_N("catalog::discover_connection_schemas");
        const actor_zeta::address_t* backend = nullptr;
        switch (conn_type) {
            case catalog_ext::ConnectionType::MySQL:
                backend = &mysql_manager_;
                break;
            case catalog_ext::ConnectionType::PostgreSQL:
                backend = &pg_manager_;
                break;
            case catalog_ext::ConnectionType::ClickHouse:
                backend = &ch_manager_;
                break;
        }
        if (backend == nullptr || !*backend) {
            log_->error("discover_connection_schemas: no {} backend manager registered (uid {})",
                        backend_name(conn_type),
                        name.unique_identifier);
            co_return make_error(resource(),
                                 core::error_code_t::do_not_exists,
                                 std::string{"No "} + backend_name(conn_type) +
                                     " backend manager is registered for uid: " + name.unique_identifier);
        }

        switch (conn_type) {
            case catalog_ext::ConnectionType::MySQL: {
                auto [needs_sched, future] = actor_zeta::send(*backend, &db::MySQLManager::discover, name);
                co_return co_await std::move(future);
            }
            case catalog_ext::ConnectionType::PostgreSQL: {
                auto [needs_sched, future] = actor_zeta::send(*backend, &db::PostgressManager::discover, name);
                co_return co_await std::move(future);
            }
            case catalog_ext::ConnectionType::ClickHouse: {
                auto [needs_sched, future] = actor_zeta::send(*backend, &db::ClickhouseManager::discover, name);
                co_return co_await std::move(future);
            }
        }
        co_return make_error(resource(),
                             core::error_code_t::invalid_parameter,
                             "Unknown backend type for uid: " + name.unique_identifier);
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<table_info>>>
    CatalogManager::get_tables(arrow::flight::sql::GetTables command) {
        OTX_ZONE_N("catalog::get_tables");
        std::pmr::vector<table_info> data(resource());

        // Every mirrored entry is a base table; a type filter that does not
        // name that type matches nothing.
        if (!command.table_types.empty() &&
            std::find(command.table_types.begin(), command.table_types.end(), catalog_ext::table_type_name) ==
                command.table_types.end()) {
            co_return std::move(data);
        }

        store_.for_each([&](const qualified_name_t& name,
                            components::catalog::oid_t /*oid*/,
                            const types::complex_logical_type& schema) {
            // FlightSQL filter mapping: command.catalog is an exact match on
            // the database part; the schema and table patterns are LIKE.
            if (command.catalog && name.database != command.catalog.value()) {
                return;
            }
            if (command.db_schema_filter_pattern &&
                !like_match(command.db_schema_filter_pattern.value(), name.schema)) {
                return;
            }
            if (command.table_name_filter_pattern &&
                !like_match(command.table_name_filter_pattern.value(), name.collection)) {
                return;
            }

            data.emplace_back(name);
            if (command.include_schema) {
                data.back().schema = types::complex_logical_type::create_struct(
                    "",
                    std::pmr::vector<types::complex_logical_type>(schema.child_types().begin(),
                                                                  schema.child_types().end(),
                                                                  resource()));
            }
        });

        co_return std::move(data);
    }

} // namespace mysql
