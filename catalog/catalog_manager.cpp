// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog_manager.hpp"

#include "integration/otterbrix/otterbrix_manager.hpp"
#include "utility/external_name.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/identifier_types.hpp>
#include <components/table/column_definition.hpp>

#include <thread>

using namespace components;
using otterstax::error_code_t;
using otterstax::error_tag_t;
using otterstax::pipeline_error;

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

    otterstax::result<ParsedQueryDataPtr> CatalogManager::update_backend_type_impl(ParsedQueryDataPtr&& data) {
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

        auto& nodes = data->otterbrix_params->external_nodes;
        auto& targets = data->otterbrix_params->external_targets;
        if (targets.size() != nodes.size()) {
            log_->error("update_backend_type_impl: external_targets/external_nodes batch count mismatch: {} vs {}",
                        targets.size(),
                        nodes.size());
            return pipeline_error(error_code_t::internal_error,
                                  error_tag_t::catalog_manager,
                                  "external_targets/external_nodes batch count mismatch");
        }

        for (size_t b = 0; b < nodes.size(); ++b) {
            if (targets[b].size() != nodes[b].size()) {
                log_->error("update_backend_type_impl: external_targets/external_nodes size mismatch in batch {}", b);
                return pipeline_error(error_code_t::internal_error,
                                      error_tag_t::catalog_manager,
                                      "external_targets/external_nodes size mismatch");
            }
            for (size_t i = 0; i < nodes[b].size(); ++i) {
                auto& target = targets[b][i];
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
                // needs a registered schema to be routed.
                auto node_type = (*nodes[b][i])->type();
                if (node_type == logical_plan::node_type::create_collection_t ||
                    node_type == logical_plan::node_type::drop_collection_t ||
                    node_type == logical_plan::node_type::create_index_t ||
                    node_type == logical_plan::node_type::drop_index_t) {
                    continue;
                }

                // Stamp the engine OID resolved at registration time.
                auto oid = store_.find(name);
                if (oid == components::catalog::INVALID_OID) {
                    if (!name.unique_identifier.empty()) {
                        log_->error("update_backend_type_impl: no registered schema for external table {}",
                                    name.to_string());
                        return pipeline_error(error_code_t::catalog_error,
                                              error_tag_t::catalog_manager,
                                              "External table is not registered: " + name.to_string());
                    }
                    // No connection uid — local Otterbrix table, resolved by the engine itself.
                } else {
                    target.oid = oid;
                    (*nodes[b][i])->set_table_oid(oid);
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
    CatalogManager::ensure_external_targets_registered(ParsedQueryDataPtr& data) {
        // Normalize names and lazily register external tables the engine does
        // not know yet (e.g. created at runtime by a previous DDL statement).
        // Must run BEFORE update_backend_type_impl: OID stamping there requires
        // every non-DDL external table to be present in the store.
        auto& nodes = data->otterbrix_params->external_nodes;
        auto& targets = data->otterbrix_params->external_targets;
        for (size_t b = 0; b < nodes.size() && b < targets.size(); ++b) {
            for (size_t i = 0; i < nodes[b].size() && i < targets[b].size(); ++i) {
                auto& target = targets[b][i];
                if (target.name.unique_identifier.empty()) {
                    continue;
                }
                auto node_type = (*nodes[b][i])->type();
                if (node_type == logical_plan::node_type::create_collection_t ||
                    node_type == logical_plan::node_type::drop_collection_t ||
                    node_type == logical_plan::node_type::create_index_t ||
                    node_type == logical_plan::node_type::drop_index_t) {
                    // CREATE targets do not exist yet; DROP needs no schema.
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

    actor_zeta::unique_future<otterstax::result<ParsedQueryDataPtr>>
    CatalogManager::update_backend_type(session_hash_t id, ParsedQueryDataPtr data) {
        OTX_ZONE_N("catalog::update_backend_type");
        auto err = co_await ensure_external_targets_registered(data);
        if (err.contains_error()) {
            log_->error("update_backend_type: {}", err.what.c_str());
            co_return pipeline_error(error_code_t::catalog_error,
                                     error_tag_t::catalog_manager,
                                     std::string{err.what.c_str()});
        }
        auto impl_result = update_backend_type_impl(std::move(data));
        if (impl_result.has_error()) {
            log_->error("update_backend_type: {}", impl_result.error().what);
            co_return std::move(impl_result);
        }
        auto updated_data = impl_result.take_value();
        if (updated_data->backend_type == backend_type_t::Unknown) {
            log_->error("update_backend_type: Backend type is unknown after update_backend_type_impl, cannot proceed");
            co_return pipeline_error(error_code_t::backend_unknown,
                                     error_tag_t::catalog_manager,
                                     "Backend type is unknown after update_backend_type_impl, cannot proceed");
        }

        log_->debug("update_backend_type: determined backend_type = {}", static_cast<int>(updated_data->backend_type));
        co_return std::move(updated_data);
    }

    actor_zeta::unique_future<otterstax::result<ParsedQueryDataPtr>>
    CatalogManager::get_catalog_schema(session_hash_t id, ParsedQueryDataPtr data) {
        OTX_ZONE_N("catalog::get_catalog_schema");
        auto err = co_await ensure_external_targets_registered(data);
        if (err.contains_error()) {
            co_return pipeline_error(error_code_t::catalog_error,
                                     error_tag_t::catalog_manager,
                                     std::string{err.what.c_str()});
        }

        auto impl_result = update_backend_type_impl(std::move(data));
        if (impl_result.has_error()) {
            log_->error("get_catalog_schema: {}", impl_result.error().what);
            co_return std::move(impl_result);
        }
        auto updated_data = impl_result.take_value();
        if (updated_data->backend_type == backend_type_t::Unknown) {
            log_->error("get_catalog_schema: Backend type is unknown after update_backend_type_impl, cannot proceed");
            co_return pipeline_error(error_code_t::backend_unknown,
                                     error_tag_t::catalog_manager,
                                     "Backend type is unknown after update_backend_type_impl, cannot proceed");
        }

        log_->debug(
            "get_catalog_schema: start getting catalog schema for query with external nodes count {}, backend type {}",
            static_cast<int>(updated_data->otterbrix_params->external_nodes.size()),
            static_cast<int>(updated_data->backend_type));

        if (updated_data->otterbrix_params->node->type() != logical_plan::node_type::aggregate_t) {
            // node is not aggregate nor join - result is empty schema
            log_->debug("prepare_schema: node is not aggregate, returning empty schema");
            co_return std::move(updated_data);
        }

        auto& nodes = updated_data->otterbrix_params->external_nodes;
        auto& targets = updated_data->otterbrix_params->external_targets;
        for (size_t b = 0; b < nodes.size(); ++b) {
            for (size_t i = 0; i < nodes[b].size(); ++i) {
                auto& node = nodes[b][i];
                if ((*node)->type() == logical_plan::node_type::aggregate_t) {
                    const auto& target = targets[b][i];

                    const auto* struct_schema = store_.schema_by_oid(target.oid);
                    if (struct_schema == nullptr) {
                        log_->error("get_catalog_schema: no schema registered for external table {}",
                                    target.name.to_string());
                        co_return pipeline_error(error_code_t::catalog_error,
                                                 error_tag_t::catalog_manager,
                                                 "No schema registered for external table: " +
                                                     target.name.to_string());
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

        // Step 1 (sync): probe the remote backend and collect per-table STRUCT schemas.
        catalog_ext::discovered_tables_t tables(resource());
        if (auto err = discover_connection_schemas(name, tables); err.contains_error()) {
            co_return err;
        }

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
                    std::pmr::string{
                        ("Failed to create engine database for uid '" + uuid + "': " + db_result.error().what).c_str(),
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
                                                            uuid,
                                                            otterstax::encode_external_collection(table.name),
                                                            std::move(columns));
            auto tbl_result = co_await std::move(tbl_future);
            if (tbl_result.has_error()) {
                log_->error("add_connection_schema: failed to register external table {}: {}",
                            table.name.to_string(),
                            tbl_result.error().what);
                co_return core::error_t(
                    core::error_code_t::schema_error,
                    std::pmr::string{("Failed to register external table '" + table.name.to_string() +
                                      "': " + tbl_result.error().what)
                                         .c_str(),
                                     resource()});
            }

            auto oid = tbl_result.take_value();
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

    // Discovery only — engine registration happens in add_connection_schema.
    core::error_t CatalogManager::discover_connection_schemas(const qualified_name_t& name,
                                                              catalog_ext::discovered_tables_t& out) {
        OTX_ZONE_N("catalog::discover_connection_schemas");
        const std::string& uuid = name.unique_identifier;

        // Determine connection type by checking which ConnectorManager has this connection
        catalog_ext::ConnectionType conn_type;
        bool is_mysql = mysql_conn_manager_ && mysql_conn_manager_->hasConnection(uuid);
        bool is_pg = pg_conn_manager_ && pg_conn_manager_->hasConnection(uuid);
        bool is_ch = ch_conn_manager_ && ch_conn_manager_->hasConnection(uuid);

        if (is_mysql) {
            conn_type = catalog_ext::ConnectionType::MySQL;
            log_->debug("add_connection_schema: detected MySQL connection for uuid: {}", uuid);
        } else if (is_pg) {
            conn_type = catalog_ext::ConnectionType::PostgreSQL;
            log_->debug("add_connection_schema: detected PostgreSQL connection for uuid: {}", uuid);
        } else if (is_ch) {
            conn_type = catalog_ext::ConnectionType::ClickHouse;
            log_->debug("add_connection_schema: detected ClickHouse connection for uuid: {}", uuid);
        } else {
            log_->error("add_connection_schema: no connector manager has connection with uuid: {}", uuid);
            return core::error_t(
                core::error_code_t::missing_field,
                std::pmr::string{("No connector manager found for uuid: " + uuid).c_str(), resource()});
        }

        registerConnection(uuid, conn_type, name);

        if (is_mysql) {
            // MySQL: query schema using boost::mysql
            auto schema_handler = [this, &name, &out](const boost::mysql::results& result) -> otterstax::asio_error_t {
                auto schema_struct = tsl::mysql_to_struct(resource(), result.meta());
                out.push_back(catalog_ext::discovered_table_t{name, std::move(schema_struct)});
                log_->info("add_connection_schema: schema discovered for: {}", name.to_string());
                return otterstax::asio_error_t{};
            };

            logical_plan::parameter_node_t param(resource());
            auto node = logical_plan::make_node_aggregate(resource(),
                                                          core::uid_t{name.unique_identifier},
                                                          core::dbname_t{name.database},
                                                          core::relname_t{name.collection});
            node->append_child(logical_plan::make_node_match(
                resource(),
                core::dbname_t{name.database},
                core::relname_t{name.collection},
                expressions::make_compare_expression(resource(),
                                                     expressions::compare_type::eq,
                                                     expressions::key_t(resource(), "1"),
                                                     param.add_parameter(types::logical_value_t(resource(), 0)))));

            otterstax::names::resolved_target_t probe_target{components::catalog::INVALID_OID, name, {}};
            std::pmr::vector<otterstax::names::resolved_target_t> empty_targets{resource()};
            std::string query =
                sql_gen::generate_query(node, &param.parameters(), backend_type_t::MySQL, probe_target, empty_targets);
            log_->debug("add_connection_schema: Generated MySQL Query: \"{}\"", query);

            try {
                auto future = mysql_conn_manager_->executeQuery(uuid, query, schema_handler);
                return std::move(future.get()).release();
            } catch (const std::exception& e) {
                log_->error("add_connection_schema: failed to query MySQL schema for {}", name.to_string());
                return core::error_t(
                    core::error_code_t::missing_field,
                    std::pmr::string{(std::string("MySQL schema query failed: ") + e.what()).c_str(), resource()});
            }
        } else if (is_pg) {
            // PostgreSQL: query schema using libpq
            // Get the actual schema and table from connection params
            // The 'name' parameter may have unique_identifier in schema field (for catalog lookups)
            // We need the real PostgreSQL schema (e.g., "public") for query generation
            auto conn_params = pg_conn_manager_->conn_params(uuid);
            qualified_name_t pg_name;
            if (conn_params) {
                // Use connection params for correct schema.table format
                pg_name = qualified_name_t(uuid,
                                           conn_params->database,
                                           conn_params->schema.empty() ? "public" : conn_params->schema,
                                           conn_params->table);
                log_->debug("add_connection_schema: using conn_params - schema={}, table={}",
                            pg_name.schema,
                            pg_name.collection);
            } else {
                // Fallback: use name as-is (may be from parsed query with correct schema)
                pg_name = qualified_name_t(name.unique_identifier,
                                           name.database,
                                           name.schema.empty() ? "public" : name.schema,
                                           name.collection);
                log_->debug("add_connection_schema: no conn_params, using name - schema={}, table={}",
                            pg_name.schema,
                            pg_name.collection);
            }

            pg_conn_manager_->fetch_enum_types(uuid);
            auto pg_enum_oids = pg_conn_manager_->enums_for(uuid);

            // If table is empty, fetch all tables from the schema
            if (pg_name.collection.empty()) {
                // First, get list of all tables in the schema
                std::string list_tables_query =
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + pg_name.schema +
                    "' AND table_type = 'BASE TABLE';";
                log_->debug("add_connection_schema: empty table, querying information_schema: \"{}\"",
                            list_tables_query);

                // Handler to process list of tables and then fetch each table's schema
                auto list_handler = [this, uuid, pg_name, pg_enum_oids, &out](
                                        PGresult* result) -> otterstax::asio_error_t {
                    int num_tables = PQntuples(result);
                    log_->info("add_connection_schema: found {} tables in schema {}", num_tables, pg_name.schema);

                    // For each table, fetch its schema
                    for (int i = 0; i < num_tables; ++i) {
                        std::string table_name = PQgetvalue(result, i, 0);
                        log_->debug("add_connection_schema: processing table {}", table_name);

                        qualified_name_t full_table_name = pg_name;
                        full_table_name.collection = table_name;

                        // Create a handler for this specific table's schema
                        auto schema_handler = [this, full_table_name, pg_enum_oids, &out](
                                                  PGresult* schema_result) -> otterstax::asio_error_t {
                            auto schema_struct = tsl::pg_to_struct(resource(), schema_result, pg_enum_oids);
                            out.push_back(catalog_ext::discovered_table_t{full_table_name, std::move(schema_struct)});
                            log_->info("add_connection_schema: schema discovered for: {}.{}",
                                       full_table_name.schema,
                                       full_table_name.collection);
                            return otterstax::asio_error_t{};
                        };

                        // Query schema for this table
                        std::string schema_query = "SELECT * FROM " + full_table_name.schema + "." +
                                                   full_table_name.collection + " WHERE 1 = 0;";
                        try {
                            auto future = pg_conn_manager_->executeQuery(uuid, schema_query, schema_handler);
                            auto err = future.get();
                            if (err.contains_error()) {
                                log_->error("add_connection_schema: failed to fetch schema for {}.{}",
                                            full_table_name.schema,
                                            full_table_name.collection);
                            }
                        } catch (const std::exception& e) {
                            log_->error("add_connection_schema: failed to query schema for {}.{}: {}",
                                        full_table_name.schema,
                                        full_table_name.collection,
                                        e.what());
                        }
                    }
                    return otterstax::asio_error_t{};
                };

                try {
                    auto future = pg_conn_manager_->executeQuery(uuid, list_tables_query, list_handler);
                    return std::move(future.get()).release();
                } catch (const std::exception& e) {
                    log_->error("add_connection_schema: failed to query table list from schema {}", pg_name.schema);
                    return core::error_t(
                        core::error_code_t::missing_field,
                        std::pmr::string{(std::string("Failed to list tables: ") + e.what()).c_str(), resource()});
                }
            } else {
                // Fetch schema for a single specific table
                auto schema_handler =
                    [this, pg_name, pg_enum_oids, &out](PGresult* schema_result) -> otterstax::asio_error_t {
                    auto schema_struct = tsl::pg_to_struct(resource(), schema_result, pg_enum_oids);
                    out.push_back(catalog_ext::discovered_table_t{pg_name, std::move(schema_struct)});
                    log_->info("add_connection_schema: schema discovered for: {}.{}",
                               pg_name.schema,
                               pg_name.collection);
                    return otterstax::asio_error_t{};
                };

                std::string schema_query =
                    "SELECT * FROM " + pg_name.schema + "." + pg_name.collection + " WHERE 1 = 0;";
                log_->debug("add_connection_schema: querying single table schema: \"{}\"", schema_query);

                try {
                    auto future = pg_conn_manager_->executeQuery(uuid, schema_query, schema_handler);
                    return std::move(future.get()).release();
                } catch (const std::exception& e) {
                    log_->error("add_connection_schema: failed to query schema for {}.{}",
                                pg_name.schema,
                                pg_name.collection);
                    return core::error_t(
                        core::error_code_t::missing_field,
                        std::pmr::string{(std::string("Failed to fetch table schema: ") + e.what()).c_str(),
                                         resource()});
                }
            }
        } else if (is_ch) {
            // ClickHouse: query schema using clickhouse-cpp native protocol
            auto conn_params = ch_conn_manager_->conn_params(uuid);
            std::string ch_database = conn_params ? conn_params->database : "default";

            // Phase 1: list tables in the database
            std::string list_tables_query = "SELECT name FROM system.tables WHERE database = '" + ch_database + "'";
            log_->debug("add_connection_schema: querying ClickHouse tables: \"{}\"", list_tables_query);

            auto list_handler = [this, uuid, ch_database, &out](
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
                        std::string table_name(name_col->At(i));
                        log_->debug("add_connection_schema: processing ClickHouse table {}", table_name);

                        ch_conn_manager_->fetch_named_types(uuid, ch_database, table_name);
                        qualified_name_t table_name_obj(uuid, ch_database, "", table_name);

                        auto schema_handler =
                            [this, table_name_obj, ch_database, table_name, &out](
                                const std::vector<clickhouse::Block>& schema_blocks) -> otterstax::asio_error_t {
                            const clickhouse::Block& schema_block =
                                schema_blocks.empty() ? clickhouse::Block{} : schema_blocks[0];
                            auto schema_struct = tsl::ch_to_struct(resource(), schema_block);
                            out.push_back(catalog_ext::discovered_table_t{table_name_obj, std::move(schema_struct)});
                            log_->info("add_connection_schema: schema discovered for: {}.{}",
                                       ch_database,
                                       table_name);
                            return otterstax::asio_error_t{};
                        };

                        std::string schema_query = "SELECT * FROM " + ch_database + "." + table_name + " LIMIT 0";
                        try {
                            auto future = ch_conn_manager_->executeQuery(uuid, schema_query, schema_handler);
                            auto err = future.get();
                            if (err.contains_error()) {
                                log_->error("add_connection_schema: failed to fetch schema for {}.{}",
                                            ch_database,
                                            table_name);
                            }
                        } catch (const std::exception& e) {
                            log_->error("add_connection_schema: failed to query schema for {}.{}: {}",
                                        ch_database,
                                        table_name,
                                        e.what());
                        }
                    }
                }
                return otterstax::asio_error_t{};
            };

            try {
                auto future = ch_conn_manager_->executeQuery(uuid, list_tables_query, list_handler);
                return std::move(future.get()).release();
            } catch (const std::exception& e) {
                log_->error("add_connection_schema: failed to query table list from ClickHouse database {}",
                            ch_database);
                return core::error_t(
                    core::error_code_t::missing_field,
                    std::pmr::string{(std::string("Failed to list ClickHouse tables: ") + e.what()).c_str(),
                                     resource()});
            }
        }
        log_->error("add_connection_schema: connection type for uuid {} not found during schema addition", uuid);
        return core::error_t(core::error_code_t::missing_field,
                             std::pmr::string{("Connection type not found for uuid: " + uuid).c_str(), resource()});
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
