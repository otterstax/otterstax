// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "catalog_manager.hpp"

using namespace components;

namespace mysqlc {
    CatalogManager::CatalogManager(std::pmr::memory_resource* res)
        : actor_zeta::cooperative_supervisor<CatalogManager>(res)
        , get_catalog_schema_(
              actor_zeta::make_behavior(resource(),
                                        catalog_manager::handler_id(catalog_manager::route::get_catalog_schema),
                                        this,
                                        &CatalogManager::get_catalog_schema))
        , add_connection_schema_(
              actor_zeta::make_behavior(resource(),
                                        catalog_manager::handler_id(catalog_manager::route::add_connection_schema),
                                        this,
                                        &CatalogManager::add_connection_schema))
        , remove_connection_schema_(
              actor_zeta::make_behavior(resource(),
                                        catalog_manager::handler_id(catalog_manager::route::remove_connection_schema),
                                        this,
                                        &CatalogManager::remove_connection_schema))
        , get_tables_(actor_zeta::make_behavior(resource(),
                                                catalog_manager::handler_id(catalog_manager::route::get_tables),
                                                this,
                                                &CatalogManager::get_tables))
        , catalog_(resource())
        , conn_manager_(nullptr)
        , log_(get_logger(logger_tag::CATALOG_MANAGER)) {
        assert(log_.is_valid());
        assert(res != nullptr);
        worker_.start();
    }

    void CatalogManager::set_connector_manager(std::shared_ptr<ConnectorManager> conn_manager) {
        conn_manager_ = std::move(conn_manager);
    }

    actor_zeta::behavior_t CatalogManager::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case catalog_manager::handler_id(catalog_manager::route::get_catalog_schema): {
                    get_catalog_schema_(msg);
                    break;
                }
                case catalog_manager::handler_id(catalog_manager::route::add_connection_schema): {
                    add_connection_schema_(msg);
                    break;
                }
                case catalog_manager::handler_id(catalog_manager::route::remove_connection_schema): {
                    remove_connection_schema_(msg);
                    break;
                }
                case catalog_manager::handler_id(catalog_manager::route::get_tables): {
                    get_tables_(msg);
                    break;
                }
            }
        });
    }

    auto CatalogManager::make_type() const noexcept -> const char* const { return "CatalogManager"; }

    auto CatalogManager::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
        assert("CatalogManager::executor_impl");
        return nullptr;
    }

    auto CatalogManager::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        std::unique_lock<std::mutex> _(input_mtx_);
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    auto CatalogManager::get_catalog_schema(session_hash_t id, ParsedQueryDataPtr&& data) -> void {
        for (auto& batch : data->otterbrix_params->external_nodes) {
            for (size_t i = 0; i < batch.size(); ++i) {
                if ((*batch[i])->type() == logical_plan::node_type::aggregate_t) {
                    const auto& name = (*batch[i])->collection_full_name();
                    collection_full_name_t uid_as_schema(name.database, name.unique_identifier, name.collection);
                    catalog::table_id uid_as_schema_id(resource(), uid_as_schema);

                    if (!catalog_.table_exists(uid_as_schema_id)) {
                        if (auto err = add_connection_schema(uid_as_schema); err) {
                            send_result(id, std::move(data), err);
                            return;
                        }
                    }

                    const auto& agg = static_cast<logical_plan::node_aggregate_t&>(*(*batch[i]));
                    auto initial_schema = catalog_.get_table_schema(uid_as_schema_id).schema_struct();
                    initial_schema = schema_utils::aggregate_filter_schema(agg,
                                                                           data->otterbrix_params->params_node.get(),
                                                                           catalog::schema(resource(), initial_schema));

                    auto node_schema = schema_utils::make_node_schema(name,
                                                                      std::move(initial_schema),
                                                                      components::logical_plan::node_aggregate_t(agg));
                    *batch[i] = node_schema;
                }
            }
        }

        send_result(id, std::move(data), catalog::catalog_error{});
    }

    auto CatalogManager::add_connection_schema(collection_full_name_t name) -> catalog::catalog_error {
        if (!conn_manager_) {
            log_->warn("add_connection_schema: mysql_manager is null, unable to query schema");
            return catalog::catalog_error(catalog::catalog_mistake_t::FIELD_MISSING, "Unable to query schema");
        }

        catalog::table_id id(resource(), name);
        auto schema_handler = [this, &id](const boost::mysql::results& result) -> catalog::catalog_error {
            auto schema_struct = tsl::mysql_to_struct(result.meta());
            auto schema = catalog::schema(resource(), std::move(schema_struct));

            if (catalog_.table_exists(id)) {
                return catalog::catalog_error(catalog::catalog_mistake_t::ALREADY_EXISTS, "Connection alreqdy exists");
            }

            catalog_.create_namespace(id.get_namespace());
            auto err = catalog_.create_table(id, catalog::table_metadata(resource(), std::move(schema)));
            log_->info("add_connection_schema: {} for: {}",
                       (err) ? "failed to add schema" : "schema added",
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
                                                 expressions::side_t::undefined,
                                                 expressions::key_t("1"),
                                                 param.add_parameter(types::logical_value_t(0)))));

        std::string query = sql_gen::generate_query(node, &param.parameters());
        log_->debug("add_connection_schema: Generated SQL Query: \"{}\"", query);

        try {
            auto future = conn_manager_->executeQuery(name.schema, query, schema_handler);
            return future.get();
        } catch (const std::exception& e) {
            log_->error("add_connection_schema: failed to query schema for {}", id.to_string());
            return catalog::catalog_error(catalog::catalog_mistake_t::FIELD_MISSING,
                                          std::string("Schema query failed: ") + e.what());
        }
    }

    auto CatalogManager::remove_connection_schema(const std::string& uuid) -> void {
        catalog_.drop_namespace({uuid.c_str()});
    }

    auto CatalogManager::get_tables(const arrow::flight::sql::GetTables& command,
                                    shared_data<std::pmr::vector<table_info>> sdata) -> void {
        std::pmr::vector<table_info> data(resource());
        std::pmr::vector<catalog::table_id> ids(resource());

        if (command.db_schema_filter_pattern && command.catalog) {
            // schema & catalog are known - fast track
            auto tables =
                catalog_.list_tables({command.db_schema_filter_pattern.value().data(), command.catalog.value().data()});
            ids.reserve(tables.size());
            for (auto&& table_id : tables) {
                ids.emplace_back(std::move(table_id));
            }
        } else {
            // command contains either schema or catalog (or none)
            std::pmr::vector<catalog::table_namespace_t> db_ns(resource()); // vector of namespaces to be visited
            // get schemas (all or filtered)
            for (const auto& sch_ns : (command.db_schema_filter_pattern)
                                          ? catalog_.list_namespaces({command.db_schema_filter_pattern.value().data()})
                                          : catalog_.list_namespaces()) {
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

        if (ids.empty()) {
            sdata->release_empty();
            return;
        }

        for (auto&& id : ids) {
            data.emplace_back(id.collection_full_name());
            if (command.include_schema) {
                data.back().schema = catalog_.get_table_schema(id).schema_struct();
            }
        }

        sdata->result = std::move(data);
        sdata->release();
    }

    auto CatalogManager::send_result(session_hash_t id, ParsedQueryDataPtr&& data, catalog::catalog_error err) -> void {
        auto send_task = [this, id, &data, err = std::move(err)]() mutable {
            actor_zeta::send(current_message()->sender(),
                             address(),
                             scheduler::handler_id(scheduler::route::get_catalog_schema_finish),
                             id,
                             std::move(data),
                             std::move(err));
        };
        if (!worker_.addTask(std::move(send_task))) {
            log_->error("get_catalog_schema failed to add task to worker");
        } else {
            log_->trace("get_catalog_schema added task to worker");
        }
    }

} // namespace mysqlc