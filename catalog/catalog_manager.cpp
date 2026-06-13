// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog_manager.hpp"

#include "integration/otterbrix/otterbrix_manager.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/identifier_types.hpp>
#include <components/table/column_definition.hpp>

#include <thread>

using namespace components;

namespace {

    // Single-quoted SQL string literal for the handwritten discovery list
    // queries (information_schema / system.tables): embedded quotes doubled.
    // Lives here because these are the only handwritten queries left — every
    // per-table probe goes through sql_gen::generate_query.
    std::string escape_sql_literal(const std::string& value) {
        std::string out;
        out.reserve(value.size() + 2);
        out.push_back('\'');
        for (char c : value) {
            if (c == '\'') {
                out.push_back('\'');
            }
            out.push_back(c);
        }
        out.push_back('\'');
        return out;
    }

    // Backend-dialect schema probe (SELECT * FROM <table> WHERE 1 = 0) built
    // through the regular plan-driven generator — the single quoting point.
    // The always-false predicate is two bound parameters, so no fake column
    // identifier appears in the generated SQL.
    std::string make_schema_probe_query(std::pmr::memory_resource* resource,
                                        const qualified_name_t& name,
                                        backend_type_t backend) {
        logical_plan::parameter_node_t param(resource);
        auto node = logical_plan::make_node_aggregate(resource,
                                                      core::uid_t{name.unique_identifier},
                                                      core::dbname_t{name.database},
                                                      core::relname_t{name.collection});
        node->append_child(logical_plan::make_node_match(
            resource,
            core::dbname_t{name.database},
            core::relname_t{name.collection},
            expressions::make_compare_expression(resource,
                                                 expressions::compare_type::eq,
                                                 param.add_parameter(types::logical_value_t(resource, 1)),
                                                 param.add_parameter(types::logical_value_t(resource, 0)))));

        otterstax::names::resolved_target_t probe_target{components::catalog::INVALID_OID, name, {}};
        std::pmr::vector<external_entry_t> empty_batch{resource};
        return sql_gen::generate_query(node, &param.parameters(), backend, probe_target, empty_batch);
    }

    // §2.1: per-table discovery failures are collected and folded into one
    // hard error naming the failure count and the first few tables.
    core::error_t make_discovery_error(std::pmr::memory_resource* resource,
                                       const std::pmr::vector<std::pmr::string>& failed_tables) {
        constexpr size_t max_named = 3;
        std::pmr::string msg{resource};
        msg.append("Schema discovery failed for ");
        msg.append(std::to_string(failed_tables.size()).c_str());
        msg.append(" table(s): ");
        for (size_t i = 0; i < failed_tables.size() && i < max_named; ++i) {
            if (i != 0) {
                msg.append(", ");
            }
            msg.append(failed_tables[i]);
        }
        if (failed_tables.size() > max_named) {
            msg.append(", ...");
        }
        return core::error_t(core::error_code_t::schema_error, std::move(msg));
    }

} // namespace

namespace mysql {
    CatalogManager::CatalogManager(std::pmr::memory_resource* res, actor_zeta::address_t otterbrix_manager)
        : resource_(res)
        , log_(get_logger(logger_tag::CATALOG_MANAGER))
        , store_(res)
        , otterbrix_manager_(std::move(otterbrix_manager))
        , registered_dbs_(res)
        , mysql_conn_manager_(nullptr)
        , pg_conn_manager_(nullptr)
        , ch_conn_manager_(nullptr) {
        assert(log_.is_valid());
        assert(res != nullptr);
        log_->info("CatalogManager initialized successfully");
    }

    void CatalogManager::set_mysql_connector_manager(std::shared_ptr<ConnectorManager> mysql_conn_manager) {
        mysql_conn_manager_ = std::move(mysql_conn_manager);
    }

    void CatalogManager::set_pg_connector_manager(std::shared_ptr<pg::ConnectorManager> pg_conn_manager) {
        pg_conn_manager_ = std::move(pg_conn_manager);
    }

    void CatalogManager::set_ch_connector_manager(std::shared_ptr<ch::ConnectorManager> ch_conn_manager) {
        ch_conn_manager_ = std::move(ch_conn_manager);
    }

    void CatalogManager::registerConnection(const std::string& uuid,
                                            catalog_ext::ConnectionType type,
                                            const qualified_name_t& name) {
        std::lock_guard lock(connection_registry_mtx_);
        connection_registry_[uuid] = catalog_ext::ConnectionInfo{uuid, type, name};
        log_->debug("Registered connection: {} with type: {}", uuid, static_cast<int>(type));
    }

    void CatalogManager::unregisterConnection(const std::string& uuid) {
        std::lock_guard lock(connection_registry_mtx_);
        connection_registry_.erase(uuid);
        log_->debug("Unregistered connection: {}", uuid);
    }

    std::optional<catalog_ext::ConnectionType> CatalogManager::getConnectionType(const std::string& uuid) const {
        std::lock_guard lock(connection_registry_mtx_);
        auto it = connection_registry_.find(uuid);
        if (it != connection_registry_.end()) {
            return it->second.type;
        }
        return std::nullopt;
    }

    bool CatalogManager::hasConnection(const std::string& uuid) const {
        std::lock_guard lock(connection_registry_mtx_);
        return connection_registry_.contains(uuid);
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
        } else if (cmd == actor_zeta::msg_id<CatalogManager, &CatalogManager::remove_connection_schema>) {
            co_await actor_zeta::dispatch(this, &CatalogManager::remove_connection_schema, msg);
        } else if (cmd == actor_zeta::msg_id<CatalogManager, &CatalogManager::get_tables>) {
            co_await actor_zeta::dispatch(this, &CatalogManager::get_tables, msg);
        }
    }

    core::result_wrapper_t<ParsedQueryDataPtr> CatalogManager::update_backend_type_impl(ParsedQueryDataPtr&& data) {
        OTX_ZONE_N("catalog::backend_type_detection");
        assert(data != nullptr);
        log_->debug("update_backend_type_impl: start updating backend type for query with external nodes count {}",
                    static_cast<int>(data->otterbrix_params->external_nodes.size()));

        if (data->backend_type != backend_type_t::Unknown) {
            log_->error("update_backend_type_impl: Backend type is already set to {}, cannot update",
                        static_cast<int>(data->backend_type));
            return std::move(data); // Return original data without setting backend type
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

                // DDL targets are exempt from OID stamping: CREATE targets a
                // table that does not exist yet, DROP removes one — neither
                // needs a registered schema to be routed. Subquery stubs
                // (schema_node_t, node_type::unused) are placeholders whose
                // schema is computed from the subquery plan — not remote
                // tables either.
                auto node_type = (*entry.node)->type();
                if (node_type == logical_plan::node_type::create_collection_t ||
                    node_type == logical_plan::node_type::drop_collection_t ||
                    node_type == logical_plan::node_type::create_index_t ||
                    node_type == logical_plan::node_type::drop_index_t ||
                    node_type == logical_plan::node_type::unused) {
                    continue;
                }

                // Stamp the engine OID resolved at registration time.
                auto oid = store_.find(name);
                if (oid == components::catalog::INVALID_OID) {
                    if (!name.unique_identifier.empty()) {
                        log_->error("update_backend_type_impl: no registered schema for external table {}",
                                    name.to_string());
                        return core::error_t(
                            core::error_code_t::table_not_exists,
                            std::pmr::string{("External table is not registered: " + name.to_string()).c_str(),
                                             resource()});
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
                return std::move(data);
            }
        }
        log_->debug("update_backend_type_impl: determined backend_type = {}", static_cast<int>(data->backend_type));
        return std::move(data);
    }

    actor_zeta::unique_future<core::error_t>
    CatalogManager::ensure_external_targets_registered(ParsedQueryData& data) {
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
                auto node_type = (*entry.node)->type();
                if (node_type == logical_plan::node_type::create_collection_t ||
                    node_type == logical_plan::node_type::drop_collection_t ||
                    node_type == logical_plan::node_type::create_index_t ||
                    node_type == logical_plan::node_type::drop_index_t ||
                    node_type == logical_plan::node_type::unused) {
                    // CREATE targets do not exist yet; DROP needs no schema;
                    // subquery stubs (schema_node_t) are computed, not remote.
                    continue;
                }
                auto conn_type_opt = getConnectionType(target.name.unique_identifier);
                if (conn_type_opt.has_value() && conn_type_opt.value() != catalog_ext::ConnectionType::PostgreSQL) {
                    // ignore schema qualifier if not postgres
                    target.name.schema = "";
                }
                if (store_.find(target.name) == components::catalog::INVALID_OID) {
                    auto err = co_await add_connection_schema(target.name);
                    if (err.contains_error()) {
                        co_return err;
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
            log_->error("update_backend_type: {}", impl_result.error().what);
            co_return std::move(impl_result);
        }
        auto updated_data = std::move(impl_result.value());
        if (updated_data->backend_type == backend_type_t::Unknown) {
            log_->error("update_backend_type: Backend type is unknown after update_backend_type_impl, cannot proceed");
            co_return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"Backend type is unknown after update_backend_type_impl, cannot proceed",
                                 resource()});
        }

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
            log_->error("get_catalog_schema: {}", impl_result.error().what);
            co_return std::move(impl_result);
        }
        auto updated_data = std::move(impl_result.value());
        if (updated_data->backend_type == backend_type_t::Unknown) {
            log_->error("get_catalog_schema: Backend type is unknown after update_backend_type_impl, cannot proceed");
            co_return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"Backend type is unknown after update_backend_type_impl, cannot proceed",
                                 resource()});
        }

        log_->debug(
            "get_catalog_schema: start getting catalog schema for query with external nodes count {}, backend type {}",
            static_cast<int>(updated_data->otterbrix_params->external_nodes.size()),
            static_cast<int>(updated_data->backend_type));

        // a13 transformer output wraps table-referencing statements in a
        // node_sequence_t whose data-producing node is the LAST child;
        // planner-emitted sequences order children differently but never
        // reach this path. Unwrap before the aggregate check below.
        const logical_plan::node_t* schema_root = updated_data->otterbrix_params->node.get();
        if (schema_root->type() == logical_plan::node_type::sequence_t) {
            if (schema_root->children().empty()) {
                log_->error("get_catalog_schema: sequence node has no children, cannot resolve schema");
                co_return core::error_t(
                    core::error_code_t::schema_error,
                    std::pmr::string{"Sequence node has no children, cannot resolve schema", resource()});
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
                        co_return core::error_t(
                            core::error_code_t::schema_error,
                            std::pmr::string{
                                ("No schema registered for external table: " + target.name.to_string()).c_str(),
                                resource()});
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

    actor_zeta::unique_future<core::error_t> CatalogManager::add_connection_schema(qualified_name_t name) {
        OTX_ZONE_N("catalog::add_connection_schema");
        const std::string uuid = name.unique_identifier;

        // Determine connection type by checking which ConnectorManager has this connection.
        catalog_ext::ConnectionType conn_type;
        if (mysql_conn_manager_ && mysql_conn_manager_->hasConnection(uuid)) {
            conn_type = catalog_ext::ConnectionType::MySQL;
            log_->debug("add_connection_schema: detected MySQL connection for uuid: {}", uuid);
        } else if (pg_conn_manager_ && pg_conn_manager_->hasConnection(uuid)) {
            conn_type = catalog_ext::ConnectionType::PostgreSQL;
            log_->debug("add_connection_schema: detected PostgreSQL connection for uuid: {}", uuid);
        } else if (ch_conn_manager_ && ch_conn_manager_->hasConnection(uuid)) {
            conn_type = catalog_ext::ConnectionType::ClickHouse;
            log_->debug("add_connection_schema: detected ClickHouse connection for uuid: {}", uuid);
        } else {
            log_->error("add_connection_schema: no connector manager has connection with uuid: {}", uuid);
            co_return core::error_t(
                core::error_code_t::missing_field,
                std::pmr::string{("No connector manager found for uuid: " + uuid).c_str(), resource()});
        }

        // Step 1: probe the remote backend and collect per-table STRUCT schemas.
        catalog_ext::discovered_tables_t tables(resource());
        if (auto err = co_await discover_connection_schemas(name, conn_type, tables); err.contains_error()) {
            co_return err;
        }

        registerConnection(uuid, conn_type, name);

        // Step 2: make sure the per-connection engine database exists (one per uid).
        std::pmr::string uid_key{uuid.c_str(), resource()};
        if (!registered_dbs_.contains(uid_key)) {
            auto [db_sched, db_future] =
                actor_zeta::send(otterbrix_manager_, &db::OtterbrixManager::register_external_database, uuid);
            auto db_result = co_await std::move(db_future);
            if (db_result.has_error()) {
                log_->error("add_connection_schema: failed to create engine database for uid {}: {}",
                            uuid,
                            db_result.error().what);
                co_return core::error_t(
                    core::error_code_t::schema_error,
                    std::pmr::string{("Failed to create engine database for uid '" + uuid +
                                      "': " + db_result.error().what.c_str())
                                         .c_str(),
                                     resource()});
            }
            registered_dbs_.insert(uid_key);
        }

        // Step 3: register each discovered table in the engine catalog and mirror it locally.
        for (auto& table : tables) {
            if (store_.find(table.name) != components::catalog::INVALID_OID) {
                log_->info("add_connection_schema: table {} already registered, skipping", table.name.to_string());
                continue;
            }

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
                            tbl_result.error().what);
                co_return core::error_t(
                    core::error_code_t::schema_error,
                    std::pmr::string{("Failed to register external table '" + table.name.to_string() +
                                      "': " + tbl_result.error().what.c_str())
                                         .c_str(),
                                     resource()});
            }

            auto oid = tbl_result.value();
            if (auto err = store_.put(oid, table.name, std::move(table.schema)); err.contains_error()) {
                log_->error("add_connection_schema: failed to store schema for table {}: {}",
                            table.name.to_string(),
                            err.what);
                co_return err;
            }
            log_->info("add_connection_schema: registered {} with oid {}", table.name.to_string(), oid);
        }

        co_return core::error_t::no_error();
    }

    // Discovery only — engine registration and the connection-type registry
    // update happen in add_connection_schema. Coroutine: every connector
    // future is consumed at the top level of this body (one query in flight
    // per connection at a time); result handlers never issue queries
    // themselves. Unified contract: empty `name.collection` → discover every
    // table of the configured database/schema; non-empty → that single table.
    actor_zeta::unique_future<core::error_t>
    CatalogManager::discover_connection_schemas(const qualified_name_t& name,
                                                catalog_ext::ConnectionType conn_type,
                                                catalog_ext::discovered_tables_t& out) {
        OTX_ZONE_N("catalog::discover_connection_schemas");
        const std::string& uuid = name.unique_identifier;

        if (conn_type == catalog_ext::ConnectionType::MySQL) {
            // MySQL: query schema using boost::mysql
            if (name.collection.empty()) {
                // Whole-database discovery via information_schema.
                if (name.database.empty()) {
                    log_->error("discover_connection_schemas: no MySQL database configured for uuid {}", uuid);
                    co_return core::error_t(
                        core::error_code_t::missing_field,
                        std::pmr::string{
                            ("Cannot discover MySQL schema: no database configured for uuid: " + uuid).c_str(),
                            resource()});
                }

                // Phase 1: list table names — one query, consumed here; the
                // handler only collects names and never issues queries itself.
                std::string list_tables_query =
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = " +
                    escape_sql_literal(name.database) + " AND table_type = 'BASE TABLE';";
                log_->debug("discover_connection_schemas: empty table, querying information_schema: \"{}\"",
                            list_tables_query);

                std::pmr::vector<std::pmr::string> table_names(resource());
                auto list_handler = [&table_names](const boost::mysql::results& result) -> otterstax::asio_error_t {
                    for (auto row : result.rows()) {
                        auto view = row.at(0).as_string();
                        table_names.emplace_back(view.data(), view.size());
                    }
                    return otterstax::asio_error_t{};
                };

                try {
                    auto future = mysql_conn_manager_->executeQuery(uuid, list_tables_query, list_handler);
                    if (auto err = std::move(future.get()).release(); err.contains_error()) {
                        co_return err;
                    }
                } catch (const std::exception& e) {
                    log_->error("discover_connection_schemas: failed to query table list from MySQL database {}",
                                name.database);
                    co_return core::error_t(
                        core::error_code_t::missing_field,
                        std::pmr::string{(std::string("Failed to list MySQL tables: ") + e.what()).c_str(),
                                         resource()});
                }
                log_->info("discover_connection_schemas: found {} tables in MySQL database {}",
                           table_names.size(),
                           name.database);

                // Phase 2: probe each table sequentially at coroutine top
                // level — the connection is free between queries. Any failed
                // table fails the whole discovery (§2.1).
                std::pmr::vector<std::pmr::string> failed_tables(resource());
                for (const auto& tn : table_names) {
                    std::string table_name{tn.c_str(), tn.size()};
                    log_->debug("discover_connection_schemas: processing MySQL table {}", table_name);

                    qualified_name_t table_name_obj(uuid, name.database, "", table_name);
                    auto schema_handler =
                        [this, table_name_obj, &out](const boost::mysql::results& result) -> otterstax::asio_error_t {
                        auto schema_struct = tsl::mysql_to_struct(resource(), result.meta());
                        out.push_back(catalog_ext::discovered_table_t{table_name_obj, std::move(schema_struct)});
                        log_->info("discover_connection_schemas: schema discovered for: {}",
                                   table_name_obj.to_string());
                        return otterstax::asio_error_t{};
                    };

                    std::string schema_query = make_schema_probe_query(resource(), table_name_obj, backend_type_t::MySQL);
                    try {
                        auto future = mysql_conn_manager_->executeQuery(uuid, schema_query, schema_handler);
                        if (auto err = std::move(future.get()).release(); err.contains_error()) {
                            log_->error("discover_connection_schemas: failed to fetch schema for {}.{}: {}",
                                        name.database,
                                        table_name,
                                        err.what.c_str());
                            failed_tables.emplace_back((name.database + "." + table_name).c_str());
                        }
                    } catch (const std::exception& e) {
                        log_->error("discover_connection_schemas: failed to query schema for {}.{}: {}",
                                    name.database,
                                    table_name,
                                    e.what());
                        failed_tables.emplace_back((name.database + "." + table_name).c_str());
                    }
                }
                if (!failed_tables.empty()) {
                    co_return make_discovery_error(resource(), failed_tables);
                }
                co_return core::error_t::no_error();
            }

            // Single-table probe.
            auto schema_handler = [this, &name, &out](const boost::mysql::results& result) -> otterstax::asio_error_t {
                auto schema_struct = tsl::mysql_to_struct(resource(), result.meta());
                out.push_back(catalog_ext::discovered_table_t{name, std::move(schema_struct)});
                log_->info("discover_connection_schemas: schema discovered for: {}", name.to_string());
                return otterstax::asio_error_t{};
            };

            std::string query = make_schema_probe_query(resource(), name, backend_type_t::MySQL);
            log_->debug("discover_connection_schemas: Generated MySQL Query: \"{}\"", query);

            try {
                auto future = mysql_conn_manager_->executeQuery(uuid, query, schema_handler);
                co_return std::move(future.get()).release();
            } catch (const std::exception& e) {
                log_->error("discover_connection_schemas: failed to query MySQL schema for {}", name.to_string());
                co_return core::error_t(
                    core::error_code_t::missing_field,
                    std::pmr::string{(std::string("MySQL schema query failed: ") + e.what()).c_str(), resource()});
            }
        } else if (conn_type == catalog_ext::ConnectionType::PostgreSQL) {
            // PostgreSQL: query schema using libpq
            // Get the actual schema and table from connection params
            // The 'name' parameter may have unique_identifier in schema field (for catalog lookups)
            // We need the real PostgreSQL schema (e.g., "public") for query generation
            auto conn_params = pg_conn_manager_->conn_params(uuid);
            qualified_name_t pg_name;
            if (conn_params) {
                // Use connection params for the correct database/schema; the
                // requested collection (when given) selects the single table.
                pg_name = qualified_name_t(uuid,
                                           conn_params->database,
                                           conn_params->schema.empty() ? "public" : conn_params->schema,
                                           name.collection.empty() ? conn_params->table : name.collection);
                log_->debug("discover_connection_schemas: using conn_params - schema={}, table={}",
                            pg_name.schema,
                            pg_name.collection);
            } else {
                pg_name = qualified_name_t(name.unique_identifier,
                                           name.database,
                                           name.schema.empty() ? "public" : name.schema,
                                           name.collection);
                log_->debug("discover_connection_schemas: no conn_params, using name - schema={}, table={}",
                            pg_name.schema,
                            pg_name.collection);
            }

            pg_conn_manager_->fetch_enum_types(uuid);
            auto pg_enum_oids = pg_conn_manager_->enums_for(uuid);

            // If table is empty, fetch all tables from the schema
            if (pg_name.collection.empty()) {
                // Phase 1: list table names — one query, consumed here; the
                // handler only collects names and never issues queries itself.
                std::string list_tables_query =
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = " +
                    escape_sql_literal(pg_name.schema) + " AND table_type = 'BASE TABLE';";
                log_->debug("discover_connection_schemas: empty table, querying information_schema: \"{}\"",
                            list_tables_query);

                std::pmr::vector<std::pmr::string> table_names(resource());
                auto list_handler = [&table_names](PGresult* result) -> otterstax::asio_error_t {
                    int num_tables = PQntuples(result);
                    for (int i = 0; i < num_tables; ++i) {
                        table_names.emplace_back(PQgetvalue(result, i, 0));
                    }
                    return otterstax::asio_error_t{};
                };

                try {
                    auto future = pg_conn_manager_->executeQuery(uuid, list_tables_query, list_handler);
                    if (auto err = std::move(future.get()).release(); err.contains_error()) {
                        co_return err;
                    }
                } catch (const std::exception& e) {
                    log_->error("discover_connection_schemas: failed to query table list from schema {}",
                                pg_name.schema);
                    co_return core::error_t(
                        core::error_code_t::missing_field,
                        std::pmr::string{(std::string("Failed to list tables: ") + e.what()).c_str(), resource()});
                }
                log_->info("discover_connection_schemas: found {} tables in schema {}",
                           table_names.size(),
                           pg_name.schema);

                // Phase 2: probe each table sequentially at coroutine top
                // level — the connection is free between queries. Any failed
                // table fails the whole discovery (§2.1).
                std::pmr::vector<std::pmr::string> failed_tables(resource());
                for (const auto& table_name : table_names) {
                    log_->debug("discover_connection_schemas: processing table {}", table_name);

                    qualified_name_t full_table_name = pg_name;
                    full_table_name.collection = std::string{table_name.c_str(), table_name.size()};

                    auto schema_handler = [this, full_table_name, pg_enum_oids, &out](
                                              PGresult* schema_result) -> otterstax::asio_error_t {
                        auto schema_struct = tsl::pg_to_struct(resource(), schema_result, pg_enum_oids);
                        out.push_back(catalog_ext::discovered_table_t{full_table_name, std::move(schema_struct)});
                        log_->info("discover_connection_schemas: schema discovered for: {}.{}",
                                   full_table_name.schema,
                                   full_table_name.collection);
                        return otterstax::asio_error_t{};
                    };

                    std::string schema_query =
                        make_schema_probe_query(resource(), full_table_name, backend_type_t::PostgreSQL);
                    try {
                        auto future = pg_conn_manager_->executeQuery(uuid, schema_query, schema_handler);
                        if (auto err = std::move(future.get()).release(); err.contains_error()) {
                            log_->error("discover_connection_schemas: failed to fetch schema for {}.{}: {}",
                                        full_table_name.schema,
                                        full_table_name.collection,
                                        err.what.c_str());
                            failed_tables.emplace_back(
                                (full_table_name.schema + "." + full_table_name.collection).c_str());
                        }
                    } catch (const std::exception& e) {
                        log_->error("discover_connection_schemas: failed to query schema for {}.{}: {}",
                                    full_table_name.schema,
                                    full_table_name.collection,
                                    e.what());
                        failed_tables.emplace_back(
                            (full_table_name.schema + "." + full_table_name.collection).c_str());
                    }
                }
                if (!failed_tables.empty()) {
                    co_return make_discovery_error(resource(), failed_tables);
                }
                co_return core::error_t::no_error();
            } else {
                // Fetch schema for a single specific table
                auto schema_handler =
                    [this, pg_name, pg_enum_oids, &out](PGresult* schema_result) -> otterstax::asio_error_t {
                    auto schema_struct = tsl::pg_to_struct(resource(), schema_result, pg_enum_oids);
                    out.push_back(catalog_ext::discovered_table_t{pg_name, std::move(schema_struct)});
                    log_->info("discover_connection_schemas: schema discovered for: {}.{}",
                               pg_name.schema,
                               pg_name.collection);
                    return otterstax::asio_error_t{};
                };

                std::string schema_query = make_schema_probe_query(resource(), pg_name, backend_type_t::PostgreSQL);
                log_->debug("discover_connection_schemas: querying single table schema: \"{}\"", schema_query);

                try {
                    auto future = pg_conn_manager_->executeQuery(uuid, schema_query, schema_handler);
                    co_return std::move(future.get()).release();
                } catch (const std::exception& e) {
                    log_->error("discover_connection_schemas: failed to query schema for {}.{}",
                                pg_name.schema,
                                pg_name.collection);
                    co_return core::error_t(
                        core::error_code_t::missing_field,
                        std::pmr::string{(std::string("Failed to fetch table schema: ") + e.what()).c_str(),
                                         resource()});
                }
            }
        } else {
            // ClickHouse: query schema using clickhouse-cpp native protocol
            auto conn_params = ch_conn_manager_->conn_params(uuid);
            std::string ch_database = conn_params ? conn_params->database : "default";

            if (!name.collection.empty()) {
                // Single-table probe.
                std::string table_name = name.collection;
                ch_conn_manager_->fetch_named_types(uuid, ch_database, table_name);
                qualified_name_t table_name_obj(uuid, ch_database, "", table_name);

                auto schema_handler =
                    [this, table_name_obj, ch_database, table_name, &out](
                        const std::vector<clickhouse::Block>& schema_blocks) -> otterstax::asio_error_t {
                    const clickhouse::Block& schema_block =
                        schema_blocks.empty() ? clickhouse::Block{} : schema_blocks[0];
                    auto schema_struct = tsl::ch_to_struct(resource(), schema_block);
                    out.push_back(catalog_ext::discovered_table_t{table_name_obj, std::move(schema_struct)});
                    log_->info("discover_connection_schemas: schema discovered for: {}.{}", ch_database, table_name);
                    return otterstax::asio_error_t{};
                };

                std::string schema_query =
                    make_schema_probe_query(resource(), table_name_obj, backend_type_t::ClickHouse);
                log_->debug("discover_connection_schemas: querying single ClickHouse table schema: \"{}\"",
                            schema_query);
                try {
                    auto future = ch_conn_manager_->executeQuery(uuid, schema_query, schema_handler);
                    co_return std::move(future.get()).release();
                } catch (const std::exception& e) {
                    log_->error("discover_connection_schemas: failed to query schema for {}.{}",
                                ch_database,
                                table_name);
                    co_return core::error_t(
                        core::error_code_t::missing_field,
                        std::pmr::string{(std::string("Failed to fetch ClickHouse table schema: ") + e.what()).c_str(),
                                         resource()});
                }
            }

            // Phase 1: list table names — the handler only collects names and
            // never issues queries itself.
            std::string list_tables_query =
                "SELECT name FROM system.tables WHERE database = " + escape_sql_literal(ch_database);
            log_->debug("discover_connection_schemas: querying ClickHouse tables: \"{}\"", list_tables_query);

            std::pmr::vector<std::pmr::string> table_names(resource());
            auto list_handler = [this, &table_names](
                                    const std::vector<clickhouse::Block>& blocks) -> otterstax::asio_error_t {
                for (const auto& block : blocks) {
                    if (block.GetRowCount() == 0)
                        continue;

                    auto name_col = block[0]->As<clickhouse::ColumnString>();
                    if (!name_col) {
                        return core::error_t(
                            core::error_code_t::missing_field,
                            std::pmr::string{"Failed to read table names from ClickHouse", resource()});
                    }

                    for (size_t i = 0; i < block.GetRowCount(); ++i) {
                        auto view = name_col->At(i);
                        table_names.emplace_back(view.data(), view.size());
                    }
                }
                return otterstax::asio_error_t{};
            };

            try {
                auto future = ch_conn_manager_->executeQuery(uuid, list_tables_query, list_handler);
                if (auto err = std::move(future.get()).release(); err.contains_error()) {
                    co_return err;
                }
            } catch (const std::exception& e) {
                log_->error("discover_connection_schemas: failed to query table list from ClickHouse database {}",
                            ch_database);
                co_return core::error_t(
                    core::error_code_t::missing_field,
                    std::pmr::string{(std::string("Failed to list ClickHouse tables: ") + e.what()).c_str(),
                                     resource()});
            }
            log_->info("discover_connection_schemas: found {} tables in ClickHouse database {}",
                       table_names.size(),
                       ch_database);

            // Phase 2: probe each table sequentially at coroutine top level —
            // fetch_named_types and the schema probe both run while the
            // connection is otherwise idle. Any failed table fails the whole
            // discovery (§2.1).
            std::pmr::vector<std::pmr::string> failed_tables(resource());
            for (const auto& tn : table_names) {
                std::string table_name{tn.c_str(), tn.size()};
                log_->debug("discover_connection_schemas: processing ClickHouse table {}", table_name);

                ch_conn_manager_->fetch_named_types(uuid, ch_database, table_name);
                qualified_name_t table_name_obj(uuid, ch_database, "", table_name);

                auto schema_handler =
                    [this, table_name_obj, ch_database, table_name, &out](
                        const std::vector<clickhouse::Block>& schema_blocks) -> otterstax::asio_error_t {
                    const clickhouse::Block& schema_block =
                        schema_blocks.empty() ? clickhouse::Block{} : schema_blocks[0];
                    auto schema_struct = tsl::ch_to_struct(resource(), schema_block);
                    out.push_back(catalog_ext::discovered_table_t{table_name_obj, std::move(schema_struct)});
                    log_->info("discover_connection_schemas: schema discovered for: {}.{}", ch_database, table_name);
                    return otterstax::asio_error_t{};
                };

                std::string schema_query =
                    make_schema_probe_query(resource(), table_name_obj, backend_type_t::ClickHouse);
                try {
                    auto future = ch_conn_manager_->executeQuery(uuid, schema_query, schema_handler);
                    if (auto err = std::move(future.get()).release(); err.contains_error()) {
                        log_->error("discover_connection_schemas: failed to fetch schema for {}.{}: {}",
                                    ch_database,
                                    table_name,
                                    err.what.c_str());
                        failed_tables.emplace_back((ch_database + "." + table_name).c_str());
                    }
                } catch (const std::exception& e) {
                    log_->error("discover_connection_schemas: failed to query schema for {}.{}: {}",
                                ch_database,
                                table_name,
                                e.what());
                    failed_tables.emplace_back((ch_database + "." + table_name).c_str());
                }
            }
            if (!failed_tables.empty()) {
                co_return make_discovery_error(resource(), failed_tables);
            }
            co_return core::error_t::no_error();
        }
    }

    actor_zeta::unique_future<void> CatalogManager::remove_connection_schema(std::string uuid) {
        OTX_ZONE_N("catalog::remove_connection_schema");
        std::pmr::string uid_key{uuid.c_str(), resource()};
        if (registered_dbs_.erase(uid_key) > 0) {
            auto [drop_sched, drop_future] =
                actor_zeta::send(otterbrix_manager_, &db::OtterbrixManager::drop_external_database, uuid);
            auto drop_result = co_await std::move(drop_future);
            if (drop_result.has_error()) {
                log_->error("remove_connection_schema: failed to drop engine database {}: {}",
                            uuid,
                            drop_result.error().what);
            }
        }

        for (auto oid : store_.oids_by_uid(uuid)) {
            store_.erase(oid);
        }
        co_return;
    }

    actor_zeta::unique_future<void> CatalogManager::get_tables(arrow::flight::sql::GetTables command,
                                                               shared_data<std::pmr::vector<table_info>> sdata) {
        OTX_ZONE_N("catalog::get_tables");
        std::pmr::vector<table_info> data(resource());

        store_.for_each([&](const qualified_name_t& name,
                            components::catalog::oid_t /*oid*/,
                            const types::complex_logical_type& schema) {
            // FlightSQL filter mapping: db_schema_filter_pattern matches the
            // schema part, command.catalog matches the database part.
            if (command.db_schema_filter_pattern && name.schema != command.db_schema_filter_pattern.value()) {
                return;
            }
            if (command.catalog && name.database != command.catalog.value()) {
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

        sdata->set_result(std::move(data));
        co_return;
    }

} // namespace mysql
