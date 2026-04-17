// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog_manager.hpp"

#include <components/table/column_definition.hpp>
#include <thread>

using namespace components;
using otterstax::error_code_t;
using otterstax::error_tag_t;
using otterstax::pipeline_error;

namespace {
    // Convert a STRUCT complex_logical_type to a catalog::schema
    catalog::schema schema_from_struct(std::pmr::memory_resource* resource,
                                       const types::complex_logical_type& struct_type) {
        auto& ext = catalog::to_struct(struct_type);
        auto& fields = ext.child_types();

        std::vector<table::column_definition_t> columns;
        std::vector<types::field_description> descriptions;
        columns.reserve(fields.size());
        descriptions.reserve(fields.size());

        for (uint64_t i = 0; i < fields.size(); ++i) {
            columns.emplace_back(fields[i].alias(), fields[i]);
            descriptions.emplace_back(i + 1);
        }

        return catalog::schema(resource, columns, descriptions);
    }
} // namespace

namespace mysqlc {
    CatalogManager::CatalogManager(std::pmr::memory_resource* res)
        : resource_(res)
        , catalog_(resource())
        , mysql_conn_manager_(nullptr)
        , pg_conn_manager_(nullptr)
        , ch_conn_manager_(nullptr)
        , log_(get_logger(logger_tag::CATALOG_MANAGER)) {
        assert(log_.is_valid());
        assert(res != nullptr);
        log_->info("CatalogManager initialized successfully");
    }

    void CatalogManager::set_mysql_connector_manager(std::shared_ptr<ConnectorManager> mysql_conn_manager) {
        mysql_conn_manager_ = std::move(mysql_conn_manager);
    }

    void CatalogManager::set_pg_connector_manager(std::shared_ptr<pgc::ConnectorManager> pg_conn_manager) {
        pg_conn_manager_ = std::move(pg_conn_manager);
    }

    void CatalogManager::set_ch_connector_manager(std::shared_ptr<chc::ConnectorManager> ch_conn_manager) {
        ch_conn_manager_ = std::move(ch_conn_manager);
    }

    void CatalogManager::registerConnection(const std::string& uuid,
                                            catalog_ext::ConnectionType type,
                                            const collection_full_name_t& name) {
        std::lock_guard<std::mutex> lock(connection_registry_mtx_);
        connection_registry_[uuid] = catalog_ext::ConnectionInfo{uuid, type, name};
        log_->debug("Registered connection: {} with type: {}", uuid, static_cast<int>(type));
    }

    void CatalogManager::unregisterConnection(const std::string& uuid) {
        std::lock_guard<std::mutex> lock(connection_registry_mtx_);
        connection_registry_.erase(uuid);
        log_->debug("Unregistered connection: {}", uuid);
    }

    std::optional<catalog_ext::ConnectionType> CatalogManager::getConnectionType(const std::string& uuid) const {
        std::lock_guard<std::mutex> lock(connection_registry_mtx_);
        auto it = connection_registry_.find(uuid);
        if (it != connection_registry_.end()) {
            return it->second.type;
        }
        return std::nullopt;
    }

    bool CatalogManager::hasConnection(const std::string& uuid) const {
        std::lock_guard<std::mutex> lock(connection_registry_mtx_);
        return connection_registry_.contains(uuid);
    }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    CatalogManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        std::lock_guard<std::mutex> guard(mutex_);
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

    auto CatalogManager::update_backend_type_impl(ParsedQueryDataPtr&& data) -> ParsedQueryDataPtr const {
        assert(data != nullptr);
        log_->debug("update_backend_type_impl: start updating backend type for query with external nodes count {}",
                    static_cast<int>(data->otterbrix_params->external_nodes.size()));

        if (data->backend_type != backend_type_t::Unknown) {
            log_->error("update_backend_type_impl: Backend type is already set to {}, cannot update",
                        static_cast<int>(data->backend_type));
            return data; // Return original data without setting backend type
        }

        bool has_mysql = false;
        bool has_pg = false;
        bool has_ch = false;

        for (auto& batch : data->otterbrix_params->external_nodes) {
            for (size_t i = 0; i < batch.size(); ++i) {
                const auto& name = (*batch[i])->collection_full_name();

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
                return data;
            }
        }
        log_->debug("update_backend_type_impl: determined backend_type = {}", static_cast<int>(data->backend_type));
        return data;
    }

    actor_zeta::unique_future<otterstax::result<ParsedQueryDataPtr>>
    CatalogManager::update_backend_type(session_hash_t id, ParsedQueryDataPtr data) {
        auto updated_data = update_backend_type_impl(std::move(data));
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
        auto updated_data = update_backend_type_impl(std::move(data));
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

        log_->debug("get_catalog_schema: determined backend_type = {}", static_cast<int>(updated_data->backend_type));

        if (updated_data->otterbrix_params->node->type() != logical_plan::node_type::aggregate_t) {
            // node is not aggregate nor join - result is empty schema
            log_->debug("prepare_schema: node is not aggregate, returning empty schema");
            co_return std::move(updated_data);
        }

        for (auto& batch : updated_data->otterbrix_params->external_nodes) {
            for (auto& node : batch) {
                if ((*node)->type() == logical_plan::node_type::aggregate_t) {
                    auto name = (*node)->collection_full_name();
                    if (updated_data->backend_type != backend_type_t::PostgreSQL) {
                        // ignore schema if not postgres
                        name.schema = "";
                    }
                    catalog::table_id name_id(resource(), name);

                    if (!catalog_.table_exists(name_id)) {
                        if (auto err = add_connection_schema_sync(name); err.contains_error()) {
                            co_return pipeline_error(error_code_t::catalog_error,
                                                     error_tag_t::catalog_manager,
                                                     std::string{err.what.c_str()});
                        }
                    }

                    const auto& agg = static_cast<logical_plan::node_aggregate_t&>(*(*node));
                    auto schema_types = catalog_.get_table_schema(name_id).types();
                    auto initial_schema =
                        schema_utils::aggregate_filter_schema(agg,
                                                              updated_data->otterbrix_params->params_node.get(),
                                                              schema_types);

                    auto node_schema = schema_utils::make_node_schema(name,
                                                                      std::move(initial_schema),
                                                                      components::logical_plan::node_aggregate_t(agg));
                    *node = node_schema;
                }
            }
        }

        log_->debug("get_catalog_schema: determined backend_type = {}", static_cast<int>(updated_data->backend_type));
        co_return std::move(updated_data);
    }

    actor_zeta::unique_future<core::error_t>
    CatalogManager::add_connection_schema(collection_full_name_t name) {
        co_return add_connection_schema_sync(std::move(name));
    }

    // Synchronous implementation — called from both the coroutine handler and internally from get_catalog_schema
    core::error_t CatalogManager::add_connection_schema_sync(collection_full_name_t name) {
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
            return core::error_t(core::error_code_t::missing_field,
                                 std::pmr::string{("No connector manager found for uuid: " + uuid).c_str(),
                                                  resource()});
        }

        registerConnection(uuid, conn_type, name);

        catalog::table_id id(resource(), name);

        if (is_mysql) {
            // MySQL: query schema using boost::mysql
            auto schema_handler = [this, &id](const boost::mysql::results& result) -> otterstax::asio_error_t {
                auto schema_struct = tsl::mysql_to_struct(result.meta());
                auto schema = schema_from_struct(resource(), schema_struct);

                if (catalog_.table_exists(id)) {
                    return core::error_t(core::error_code_t::already_exists,
                                         std::pmr::string{"Connection already exists", resource()});
                }

                catalog_.create_namespace(id.get_namespace());
                auto err = catalog_.create_table(id, catalog::table_metadata(resource(), std::move(schema)));
                log_->info("add_connection_schema: {} for: {}",
                           (err.contains_error()) ? "failed to add schema" : "schema added",
                           id.to_string());
                return err;
            };

            logical_plan::parameter_node_t param(resource());
            auto node = logical_plan::make_node_aggregate(resource(), name);
            node->append_child(logical_plan::make_node_match(
                resource(),
                name,
                expressions::make_compare_expression(resource(),
                                                     expressions::compare_type::eq,
                                                     expressions::key_t(resource(), "1"),
                                                     param.add_parameter(types::logical_value_t(resource(), 0)))));

            std::string query = sql_gen::generate_query(node, &param.parameters(), backend_type_t::MySQL);
            log_->debug("add_connection_schema: Generated MySQL Query: \"{}\"", query);

            try {
                auto future = mysql_conn_manager_->executeQuery(uuid, query, schema_handler);
                return std::move(future.get()).release();
            } catch (const std::exception& e) {
                log_->error("add_connection_schema: failed to query MySQL schema for {}", id.to_string());
                return core::error_t(core::error_code_t::missing_field,
                                     std::pmr::string{(std::string("MySQL schema query failed: ") + e.what()).c_str(),
                                                      resource()});
            }
        } else if (is_pg) {
            // PostgreSQL: query schema using libpq
            // Get the actual schema and table from connection params
            // The 'name' parameter may have unique_identifier in schema field (for catalog lookups)
            // We need the real PostgreSQL schema (e.g., "public") for query generation
            auto conn_params = pg_conn_manager_->conn_params(uuid);
            collection_full_name_t pg_name;
            if (conn_params) {
                // Use connection params for correct schema.table format
                pg_name = collection_full_name_t(uuid,
                                                 conn_params->database,
                                                 conn_params->schema.empty() ? "public" : conn_params->schema,
                                                 conn_params->table);
                log_->debug("add_connection_schema: using conn_params - schema={}, table={}",
                            pg_name.schema,
                            pg_name.collection);
            } else {
                // Fallback: use name as-is (may be from parsed query with correct schema)
                pg_name = collection_full_name_t(name.unique_identifier,
                                                 name.database,
                                                 name.schema.empty() ? "public" : name.schema,
                                                 name.collection);
                log_->debug("add_connection_schema: no conn_params, using name - schema={}, table={}",
                            pg_name.schema,
                            pg_name.collection);
            }

            // If table is empty, fetch all tables from the schema
            if (pg_name.collection.empty()) {
                // First, get list of all tables in the schema
                std::string list_tables_query =
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + pg_name.schema +
                    "' AND table_type = 'BASE TABLE';";
                log_->debug("add_connection_schema: empty table, querying information_schema: \"{}\"",
                            list_tables_query);

                // Handler to process list of tables and then fetch each table's schema
                auto list_handler = [this, uuid, pg_name](PGresult* result) -> otterstax::asio_error_t {
                    int num_tables = PQntuples(result);
                    log_->info("add_connection_schema: found {} tables in schema {}", num_tables, pg_name.schema);

                    // For each table, fetch its schema
                    for (int i = 0; i < num_tables; ++i) {
                        std::string table_name = PQgetvalue(result, i, 0);
                        log_->debug("add_connection_schema: processing table {}", table_name);

                        collection_full_name_t full_table_name = pg_name;
                        full_table_name.collection = table_name;
                        catalog::table_id table_id(resource(), full_table_name);

                        // Create a handler for this specific table's schema
                        auto schema_handler =
                            [this, table_id, full_table_name](PGresult* schema_result) -> otterstax::asio_error_t {
                            auto schema_struct = tsl::pg_to_struct(schema_result);
                            auto schema = schema_from_struct(resource(), schema_struct);

                            if (catalog_.table_exists(table_id)) {
                                log_->info("add_connection_schema: table {}.{} already exists, skipping",
                                           full_table_name.schema,
                                           full_table_name.collection);
                                return otterstax::asio_error_t{};
                            }

                            catalog_.create_namespace(table_id.get_namespace());
                            auto err =
                                catalog_.create_table(table_id, catalog::table_metadata(resource(), std::move(schema)));
                            log_->info("add_connection_schema: {} for: {}.{}",
                                       (err.contains_error()) ? "failed to add schema" : "schema added",
                                       full_table_name.schema,
                                       full_table_name.collection);
                            return err;
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
                    return core::error_t(core::error_code_t::missing_field,
                                         std::pmr::string{(std::string("Failed to list tables: ") + e.what()).c_str(),
                                                          resource()});
                }
            } else {
                // Fetch schema for a single specific table
                auto schema_handler = [this, pg_name](PGresult* schema_result) -> otterstax::asio_error_t {
                    auto schema_struct = tsl::pg_to_struct(schema_result);
                    auto schema = schema_from_struct(resource(), schema_struct);

                    catalog::table_id table_id(resource(), pg_name);
                    if (catalog_.table_exists(table_id)) {
                        log_->info("add_connection_schema: table {}.{} already exists, skipping",
                                   pg_name.schema,
                                   pg_name.collection);
                        return otterstax::asio_error_t{};
                    }

                    catalog_.create_namespace(table_id.get_namespace());
                    auto err = catalog_.create_table(table_id, catalog::table_metadata(resource(), std::move(schema)));
                    log_->info("add_connection_schema: {} for: {}.{}",
                               (err.contains_error()) ? "failed to add schema" : "schema added",
                               pg_name.schema,
                               pg_name.collection);
                    return err;
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

            auto list_handler =
                [this, uuid, ch_database](const std::vector<clickhouse::Block>& blocks) -> otterstax::asio_error_t {
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

                        collection_full_name_t table_name_obj(uuid, ch_database, "", table_name);
                        catalog::table_id table_id(resource(), table_name_obj);

                        auto schema_handler =
                            [this, table_id, ch_database, table_name](
                                const std::vector<clickhouse::Block>& schema_blocks) -> otterstax::asio_error_t {
                            const clickhouse::Block& schema_block =
                                schema_blocks.empty() ? clickhouse::Block{} : schema_blocks[0];
                            auto schema_struct = tsl::ch_to_struct(schema_block);
                            auto schema = schema_from_struct(resource(), schema_struct);

                            if (catalog_.table_exists(table_id)) {
                                log_->info("add_connection_schema: table {}.{} already exists, skipping",
                                           ch_database,
                                           table_name);
                                return otterstax::asio_error_t{};
                            }

                            catalog_.create_namespace(table_id.get_namespace());
                            auto err =
                                catalog_.create_table(table_id, catalog::table_metadata(resource(), std::move(schema)));
                            log_->info("add_connection_schema: {} for: {}.{}",
                                       (err.contains_error()) ? "failed to add schema" : "schema added",
                                       ch_database,
                                       table_name);
                            return err;
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
        catalog_.drop_namespace({uuid.c_str()});
        co_return;
    }

    actor_zeta::unique_future<void> CatalogManager::get_tables(arrow::flight::sql::GetTables command,
                                                               shared_data<std::pmr::vector<table_info>> sdata) {
        std::pmr::vector<table_info> data(resource());
        std::pmr::vector<catalog::table_id> ids(resource());

        for (const auto& uuid : catalog_.list_namespaces()) {
            const auto& root = uuid.front();
            if (command.db_schema_filter_pattern && command.catalog) {
                // schema & catalog are known - fast track
                auto tables = catalog_.list_tables(
                    {root, command.db_schema_filter_pattern.value().data(), command.catalog.value().data()});
                ids.reserve(tables.size());
                for (auto&& table_id : tables) {
                    ids.emplace_back(std::move(table_id));
                }
            } else {
                // command contains either schema or catalog (or none)
                std::pmr::vector<catalog::table_namespace_t> db_ns(resource()); // vector of namespaces to be visited
                // get schemas (all or filtered)
                for (const auto& sch_ns :
                     (command.db_schema_filter_pattern)
                         ? catalog_.list_namespaces({root, command.db_schema_filter_pattern.value().data()})
                         : catalog_.list_namespaces({root})) {
                    if (command.db_schema_filter_pattern) {
                        // already db namespace
                        db_ns.assign(1, sch_ns);
                    } else if (command.catalog) {
                        // extend root schema with catalog name
                        db_ns.assign(1, sch_ns);
                        db_ns.back().emplace_back(command.catalog.value().data());
                    } else {
                        // need to list all child namespaces otherwise
                        db_ns = catalog_.list_namespaces(sch_ns);
                    }

                    for (const auto& db : db_ns) {
                        auto tables = catalog_.list_tables(db);
                        ids.reserve(ids.size() + tables.size());
                        for (auto&& table_id : tables) {
                            ids.emplace_back(std::move(table_id));
                        }
                    }
                }
            }
        }

        if (ids.empty()) {
            sdata->release_empty();
            co_return;
        }

        for (auto&& id : ids) {
            data.emplace_back(id.collection_full_name());
            if (command.include_schema) {
                data.back().schema =
                    types::complex_logical_type::create_struct("", catalog_.get_table_schema(id).types());
            }
        }

        sdata->set_result(std::move(data));
        co_return;
    }

} // namespace mysqlc
