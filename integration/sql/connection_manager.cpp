// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "integration/prepare_probe.hpp"
#include "transaction_policy.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "otterbrix/translators/input/chunk_windows.hpp"
#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/wait_barrier.hpp"

#include <optional>
#include <thread>
#include <unordered_map>
#include <set>

using namespace db;

namespace {

    core::error_t make_error(std::pmr::memory_resource* resource, core::error_code_t code, const std::string& what) {
        return core::error_t(code, std::pmr::string{what.c_str(), resource});
    }

    // result_wrapper_t only exposes its error by const reference; rebuild it on
    // this actor's resource instead of copying it onto the default one.
    core::error_t copy_error(std::pmr::memory_resource* resource, const core::error_t& err) {
        return core::error_t(err.type, std::pmr::string{err.what.c_str(), resource});
    }

} // namespace

MySQLManager::MySQLManager(std::pmr::memory_resource* res, mysql::ConnectorManager* connector_manager)
    : resource_(res)
    , connector_manager_(connector_manager)
    , log_(get_logger(logger_tag::SQL_CONNECTION_MANAGER)) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(connector_manager_ != nullptr);
    log_->info("MySQLManager initialized successfully");
    // The wrapped connector manager's io pool is started here, once: it is the
    // only place that knows the pool is about to receive queries.
    if (connector_manager_->status() == thread_pool_status::CREATED) connector_manager_->start();
}

MySQLManager::MySQLManager(std::pmr::memory_resource* res, std::unique_ptr<mysql::ConnectorManager> connector_manager)
    : MySQLManager(res, connector_manager.get()) {
    owned_connector_manager_ = std::move(connector_manager);
}

std::pair<bool, actor_zeta::detail::enqueue_result> MySQLManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    return mailbox_.enqueue(std::move(msg));
}

actor_zeta::behavior_t MySQLManager::behavior(actor_zeta::mailbox::message* msg) {
    OTX_ZONE_N("MySQLManager::behavior");
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<MySQLManager, &MySQLManager::begin_transaction>) {
        co_await actor_zeta::dispatch(this, &MySQLManager::begin_transaction, msg);
    } else if (cmd == actor_zeta::msg_id<MySQLManager, &MySQLManager::finish_transaction>) {
        co_await actor_zeta::dispatch(this, &MySQLManager::finish_transaction, msg);
    } else if (cmd == actor_zeta::msg_id<MySQLManager, &MySQLManager::execute>) {
        co_await actor_zeta::dispatch(this, &MySQLManager::execute, msg);
    } else if (cmd == actor_zeta::msg_id<MySQLManager, &MySQLManager::discover>) {
        co_await actor_zeta::dispatch(this, &MySQLManager::discover, msg);
    } else if (cmd == actor_zeta::msg_id<MySQLManager, &MySQLManager::describe>) {
        co_await actor_zeta::dispatch(this, &MySQLManager::describe, msg);
    }
}

// What the slot holds decides the text: a raw-SQL subquery stub is its text with
// the qualifiers rewritten for MySQL, the catalog's aggregate stub and a plan
// node are generated. execute runs this statement and describe probes its
// header, so both see one text.
core::result_wrapper_t<std::string>
MySQLManager::generate_slot_statement(const components::logical_plan::node_ptr& node,
                                      const otterstax::names::resolved_target_t& target,
                                      const std::pmr::vector<external_entry_t>& batch,
                                      const components::logical_plan::storage_parameters* parameters) {
    OTX_ZONE_N("MySQLManager::generate_slot_statement");
    if (node->type() == logical_plan::node_type::unused) {
        auto& stub = static_cast<schema_utils::schema_node_t&>(*node);
        if (stub.has_raw_sql()) {
            return sql_gen::replace_qualifiers(stub.raw_sql(), stub.qualifiers(), backend_type_t::MySQL, resource());
        }
        return sql_gen::generate_query(stub.agg_node(), parameters, backend_type_t::MySQL, target, batch, resource());
    }
    return sql_gen::generate_query(node, parameters, backend_type_t::MySQL, target, batch, resource());
}

// Every connector future is consumed at the top level of this body (one query
// in flight per connection at a time); the result handlers only collect and
// never issue queries themselves. The handlers capture frame locals by
// reference: they run on the io thread while this thread waits in get(), and
// the future's hand-off orders the handler's writes before the reads below.
actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
MySQLManager::discover(qualified_name_t scope) {
    OTX_ZONE_N("MySQLManager::discover");
    const std::string& uuid = scope.unique_identifier;
    catalog_ext::discovered_tables_t out(resource());

    auto probe = [this, &out, &uuid](const qualified_name_t& table) -> core::error_t {
        auto schema_handler = [this, table, &out](const boost::mysql::results& result) -> otterstax::asio_error_t {
            // Read by the very table execute types its rows with, so a mirrored
            // column's type is the executed column's type by construction. A
            // column type that table has no arm for is the same
            // conversion_failure execute answers for it, naming the column —
            // never a column mirrored into the catalog without a type.
            auto schema_struct = tsl::mysql_to_struct(resource(), result);
            if (schema_struct.has_error()) {
                return make_error(resource(), schema_struct.error().type, schema_struct.error().what.c_str());
            }
            out.push_back(catalog_ext::discovered_table_t{table, std::move(schema_struct.value())});
            log_->info("discover: schema discovered for: {}", table.to_string());
            return otterstax::asio_error_t{};
        };
        auto probe_query = otterstax::catalog::make_schema_probe_query(resource(), table, backend_type_t::MySQL);
        if (probe_query.has_error()) {
            return copy_error(resource(), probe_query.error());
        }
        log_->debug("discover: generated MySQL probe: \"{}\"", probe_query.value());
        return connector_manager_->executeQuery(uuid, probe_query.value(), schema_handler).get();
    };

    if (!scope.collection.empty()) {
        if (auto err = probe(scope); err.contains_error()) {
            co_return std::move(err);
        }
        co_return std::move(out);
    }

    // Whole-database discovery via information_schema.
    if (scope.database.empty()) {
        log_->error("discover: no MySQL database configured for uuid {}", uuid);
        co_return make_error(resource(),
                             core::error_code_t::missing_field,
                             "Cannot discover MySQL schema: no database configured for uuid: " + uuid);
    }

    auto list_tables_query =
        otterstax::catalog::make_list_tables_query(resource(), backend_type_t::MySQL, scope.database);
    log_->debug("discover: querying information_schema: \"{}\"", list_tables_query.c_str());

    std::pmr::vector<std::pmr::string> table_names(resource());
    auto list_handler = [&table_names](const boost::mysql::results& result) -> otterstax::asio_error_t {
        for (auto row : result.rows()) {
            auto view = row.at(0).as_string();
            table_names.emplace_back(view.data(), view.size());
        }
        return otterstax::asio_error_t{};
    };
    if (auto err = connector_manager_->executeQuery(uuid, list_tables_query, list_handler).get();
        err.contains_error()) {
        co_return std::move(err);
    }
    log_->info("discover: found {} tables in MySQL database {}", table_names.size(), scope.database);

    // Probe each table sequentially — the connection is free between queries.
    std::pmr::vector<std::pmr::string> failed_tables(resource());
    for (const auto& tn : table_names) {
        std::string table_name{tn.c_str(), tn.size()};
        qualified_name_t table(uuid, scope.database, "", table_name);
        if (auto err = probe(table); err.contains_error()) {
            log_->error("discover: failed to fetch schema for {}.{}: {}",
                        scope.database,
                        table_name,
                        err.what.c_str());
            failed_tables.emplace_back((scope.database + "." + table_name).c_str());
        }
    }
    if (!failed_tables.empty()) {
        co_return otterstax::catalog::make_discovery_error(resource(), failed_tables);
    }
    co_return std::move(out);
}

actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>> MySQLManager::execute(session_hash_t id,
                                                                                            ParsedQueryDataPtr data) {
    OTX_ZONE_N("MySQLManager::execute");
    assert(data);
    assert(data->otterbrix_params);
    log_->debug("execute started, id hash: {}", id);

    if (auto error = validate_transaction_tables(*data); error.contains_error()) co_return error;

    const auto* parameters = &data->otterbrix_params->params_node->parameters();

    log_->debug("execute Total execute queries: {}", data->otterbrix_params->external_nodes_count);
    log_->debug("execute Execute batches: {}", data->otterbrix_params->external_nodes.size());
    size_t counter = 0;
    auto& batches = data->otterbrix_params->external_nodes;
    // Batches are processed back to front (innermost dependencies first).
    for (size_t batch = batches.size(); batch-- > 0;) {
        OTX_ZONE_N("MySQLManager::batch");
        auto& batch_nodes = batches[batch];
        log_->debug("execute Current batch size: {}", batch_nodes.size());
        std::pmr::vector<std::string> generated_queries(resource());
        generated_queries.reserve(batch_nodes.size());
        // A converter that cannot produce a chunk records why in its slot; the
        // slots are reserved up front so the io-thread handlers keep stable
        // pointers, and they are read only after the waiter drained every
        // future. Declared before the waiter, so they are destroyed after its
        // drain: a co_return that leaves queries in flight — the first failure
        // wait() reports, or an error while the batch is still being dispatched —
        // still has converters writing here.
        std::pmr::vector<std::optional<core::error_t>> conversion_errors(resource());
        conversion_errors.reserve(batch_nodes.size());
        // unique_ptr because data_chunk_t has no default constructor.
        otterstax::QueryHandleWaiter<std::unique_ptr<components::vector::data_chunk_t>> wait_guard{resource()};
        std::pmr::unordered_map<std::string, size_t> pending_connections(resource());
        // Order inside a batch does not matter. For a mixed backend, nodes that
        // belong to another backend are skipped, so the slots this manager filled
        // are tracked by index.
        std::pmr::vector<size_t> processed_indices(resource());
        for (size_t i = 0; i < batch_nodes.size(); i++) {
            OTX_ZONE_N("MySQLManager::node_dispatch");
            log_->trace("Execute query: {}", ++counter);

            auto& node = *batch_nodes[i].node;
            const auto& target = batch_nodes[i].target;
            const auto& uid = target.name.unique_identifier;
            log_->trace("UID: {}", uid);

            // A data_t slot was already fetched by another backend's manager.
            if (node->type() == logical_plan::node_type::data_t) {
                log_->debug("execute: Skipping already processed node with UID: {}", uid);
                continue;
            }

            if (data->backend_type == backend_type_t::Mixed) {
                auto it_backend = data->node_backend_types.find(uid);
                if (it_backend != data->node_backend_types.end() && it_backend->second != backend_type_t::MySQL) {
                    log_->debug("execute: Skipping non-MySQL node with UID: {}", uid);
                    continue;
                }
            }

            // A node attributed to this backend with no live connection must fail
            // THIS query: silently skipping leaves the node symbolic, and the engine
            // later dies with a misleading "database does not exist" (mid-flight
            // deregistration race). Nothing downstream can handle the node either.
            if (!connector_manager_->hasConnection(uid)) {
                log_->error("execute: no MySQL connection registered for UID: {}", uid);
                co_return core::error_t(
                    core::error_code_t::do_not_exists,
                    std::pmr::string{("no MySQL connection registered for '" + uid + "'").c_str(), resource()});
            }

            processed_indices.push_back(i);

            auto statement = generate_slot_statement(node, target, batch_nodes, parameters);
            if (statement.has_error()) {
                log_->error("execute: SQL generation failed: {}", statement.error().what.c_str());
                co_return statement.convert_error<ParsedQueryDataPtr>();
            }
            generated_queries.emplace_back(std::move(statement.value()));
            log_->debug("execute Generated SQL Query: \"{}\"", generated_queries.back());
            // Converts a driver result set into the engine's chunk shape.
            conversion_errors.emplace_back();
            auto* conversion_error = &conversion_errors.back();
            auto data_converter =
                [this, conversion_error](const boost::mysql::results& result) -> std::unique_ptr<data_chunk_t> {
                auto converted = tsl::mysql_to_chunk(this->resource(), result);
                if (converted.has_error()) {
                    conversion_error->emplace(converted.error().type,
                                              std::pmr::string{converted.error().what.c_str(), this->resource()});
                    return nullptr;
                }
                return std::make_unique<data_chunk_t>(std::move(converted.value()));
            };
            // One driver connection per alias: finish its previous operation
            // before checking/reusing it. Different aliases may stay in flight.
            if (auto previous = pending_connections.find(uid); previous != pending_connections.end()) {
                wait_guard.futures[previous->second].wait();
            }
            pending_connections[uid] = wait_guard.futures.size();
            wait_guard.futures.push_back(
                connector_manager_->executeQuery(uid, generated_queries.back(), data_converter,
                    data->otterbrix_params->context.in_transaction
                        ? std::optional{data->otterbrix_params->context.engine_session} : std::nullopt));
        }

        if (processed_indices.empty()) {
            log_->debug("execute: No MySQL nodes in this batch");
            continue;
        }

        // wait() returns the first failure as a value; `results` then holds only
        // the successes before it, so returning here keeps the positional
        // results[j] loop below in bounds.
        if (auto barrier = wait_guard.wait(); barrier.has_error()) {
            log_->error("execute: backend query failed: {}", barrier.error().what.c_str());
            co_return barrier.convert_error<ParsedQueryDataPtr>();
        }
        log_->debug("execute Run Query Success! results count: {}", wait_guard.results.size());
        assert(generated_queries.size() == processed_indices.size());
        for (size_t j = 0; j < processed_indices.size(); j++) {
            OTX_ZONE_N("MySQLManager::to_chunk");
            const size_t i = processed_indices[j];
            auto& chunk_ptr = wait_guard.results[j];
            // The converter always allocates a chunk; a connector that resolved
            // the query without one has broken that contract, and a plan slot
            // cannot be filled from nothing.
            if (!chunk_ptr) {
                const auto& uid = batch_nodes[i].target.name.unique_identifier;
                if (conversion_errors[j].has_value()) {
                    log_->error("execute result[{}]: backend '{}' result conversion failed: {}",
                                j,
                                uid,
                                conversion_errors[j]->what.c_str());
                    co_return std::move(*conversion_errors[j]);
                }
                log_->error("execute result[{}]: backend '{}' returned no result chunk", j, uid);
                co_return core::error_t(
                    core::error_code_t::other_error,
                    std::pmr::string{("MySQL backend '" + uid + "' returned no result chunk").c_str(), resource()});
            }
            log_->debug("execute result[{}]: chunk size={}", j, chunk_ptr->size());
            auto tmp = std::move(*chunk_ptr);
            // The slot being replaced is still typed: if it IS the statement's
            // own DML node, the whole statement ran on the backend and `tmp`'s
            // cardinality is its affected-row count. Record it now — one line
            // below it becomes a data_t, and from then on a fully-remote DML
            // and a fully-remote SELECT are indistinguishable.
            capture_remote_dml_count(*data->otterbrix_params, *batch_nodes[i].node, tmp);
            // The engine takes at most DEFAULT_VECTOR_CAPACITY rows per chunk:
            // a wider result set goes in as a run of such chunks (a count
            // carrier stays whole).
            auto data_node =
                logical_plan::make_node_raw_data(resource(), tsl::split_to_capacity(resource(), std::move(tmp)));
            *batch_nodes[i].node = data_node;
        }
    }
    log_->debug("execute finished");
    co_return std::move(data);
}

// Like execute, this body never co_awaits: every connector future is consumed
// with get() on this thread, so the zone spans the handler. The header handler
// captures frame locals by reference — it runs on the io thread while this
// thread waits in get(), and the future's hand-off orders its writes before the
// reads below.
actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>>
MySQLManager::describe(session_hash_t id, ParsedQueryDataPtr data) {
    OTX_ZONE_N("MySQLManager::describe");
    assert(data);
    assert(data->otterbrix_params);
    log_->debug("describe started, id hash: {}", id);

    // An extension root (CREATE EXTERNAL TABLE, kafka DDL) is `unused` like a
    // stub and the Worker keeps it away from the schema path; one arriving here
    // is a routing error, and reading it as a stub would be undefined.
    if (data->extension_kind != extension_kind_t::none) {
        co_return make_error(resource(),
                             core::error_code_t::invalid_parameter,
                             "describe: an extension statement has no MySQL result schema");
    }
    if (auto error = validate_transaction_tables(*data); error.contains_error()) co_return error;
    // The binder takes every parameter of a parameterized statement out of the
    // plan — the `$n` placeholders and the literals alike — and returns them at
    // finalize, so before Bind there is no statement to generate, hence nothing
    // to ask the backend. The schema of such a statement is known once its
    // parameters are bound; a type read off the plan instead would be the guess
    // this handler exists to replace.
    if (const size_t unbound = data->otterbrix_params->parameters_count; unbound > 0) {
        log_->debug("describe: statement carries {} unbound parameter(s), no probe", unbound);
        co_return make_error(resource(),
                             core::error_code_t::unimplemented_yet,
                             "the result schema of a MySQL statement with " + std::to_string(unbound) +
                                 " unbound parameter(s) is known only once they are bound");
    }

    const auto* parameters = &data->otterbrix_params->params_node->parameters();
    auto& batches = data->otterbrix_params->external_nodes;
    for (size_t batch = batches.size(); batch-- > 0;) {
        auto& batch_nodes = batches[batch];
        for (size_t i = 0; i < batch_nodes.size(); i++) {
            auto& node = *batch_nodes[i].node;
            const auto& target = batch_nodes[i].target;
            const auto& uid = target.name.unique_identifier;

            // Only a stub carries a schema the backend can fill: the catalog's,
            // made of an aggregate slot, or the parser's, holding the raw SQL of
            // a lifted-out sub-query. Any other slot (a DDL/DML target, a
            // data_t) has none.
            if (node->type() != logical_plan::node_type::unused) {
                continue;
            }
            auto& stub = static_cast<schema_utils::schema_node_t&>(*node);
            if (data->backend_type == backend_type_t::Mixed) {
                auto it_backend = data->node_backend_types.find(uid);
                if (it_backend != data->node_backend_types.end() && it_backend->second != backend_type_t::MySQL) {
                    continue;
                }
            }
            if (!connector_manager_->hasConnection(uid)) {
                log_->error("describe: no MySQL connection registered for UID: {}", uid);
                co_return make_error(resource(),
                                     core::error_code_t::do_not_exists,
                                     "no MySQL connection registered for '" + uid + "'");
            }

            auto statement = generate_slot_statement(node, target, batch_nodes, parameters);
            if (statement.has_error()) {
                log_->error("describe: SQL generation failed: {}", statement.error().what.c_str());
                co_return statement.convert_error<ParsedQueryDataPtr>();
            }
            // The wrap, its cost and what it cannot carry: prepare_probe.hpp.
            const std::string probe = make_prepare_probe(statement.value(), backend_type_t::MySQL);
            log_->debug("describe: probing \"{}\"", probe);

            // The answer is read by the very function execute reads its rows
            // with, so the prepared column types are the executed chunk's by
            // construction — the same one table tsl::mysql_to_struct answers
            // discovery from. A SELECT's column definitions
            // precede its rows, and the connection reads them in
            // metadata_mode::full (names included), so mysql_to_chunk types a
            // zero-row result set from them; an answer with no columns at all is
            // a bare OK packet, which describes nothing and is reported below.
            std::optional<components::types::complex_logical_type> header;
            auto header_handler = [this, &header](const boost::mysql::results& result) -> otterstax::asio_error_t {
                auto converted = tsl::mysql_to_chunk(this->resource(), result);
                if (converted.has_error()) {
                    // A column type the translator cannot represent is the same
                    // conversion_failure execute answers for it, code and message
                    // kept — never a column prepared as NA.
                    return make_error(resource(), converted.error().type, converted.error().what.c_str());
                }
                if (converted.value().column_count() == 0) {
                    return otterstax::asio_error_t{};
                }
                header.emplace(components::types::complex_logical_type::create_struct("", converted.value().types()));
                return otterstax::asio_error_t{};
            };
            if (auto err = connector_manager_->executeQuery(uid, probe, header_handler,
                    data->otterbrix_params->context.in_transaction
                        ? std::optional{data->otterbrix_params->context.engine_session} : std::nullopt).get(); err.contains_error()) {
                log_->error("describe: probe failed for UID {}: {}", uid, err.what.c_str());
                co_return std::move(err);
            }
            if (!header.has_value()) {
                log_->error("describe: probe answered without column metadata: \"{}\"", probe);
                co_return make_error(resource(),
                                     core::error_code_t::schema_error,
                                     "MySQL answered the prepare probe without column metadata: " + probe);
            }
            log_->debug("describe: {} column(s) described for UID {}", header->child_types().size(), uid);
            // Filled in place rather than rebuilt: a raw-SQL stub must keep the
            // text and qualifiers execute generates its statement from, and the
            // slot and the plan hold the one node either way.
            stub.set_schema(std::move(*header));
        }
    }
    log_->debug("describe finished");
    co_return std::move(data);
}

actor_zeta::unique_future<core::result_wrapper_t<MySQLManager::transaction_actor>>
MySQLManager::begin_transaction(session_id session, std::string alias) {
    auto manager = connector_manager_->fork_transaction(session, alias);
    if (manager.has_error()) co_return manager.convert_error<transaction_actor>();
    auto child = actor_zeta::spawn<MySQLManager>(resource(), std::move(manager.value()));
    co_return child;
}

actor_zeta::unique_future<core::error_t> MySQLManager::finish_transaction(session_id session, bool commit) {
    co_return connector_manager_->finish_transaction(session, commit);
}

core::error_t MySQLManager::validate_transaction_tables(const ParsedQueryData& data) {
    const auto& statement = *data.otterbrix_params;
    if (!statement.context.in_transaction) return core::error_t::no_error();
    const auto session = statement.context.engine_session;
    std::set<std::tuple<std::string, std::string, std::string>> tables;
    for (const auto& batch : statement.external_nodes) {
        for (const auto& entry : batch) {
            if ((*entry.node)->type() == logical_plan::node_type::unused &&
                static_cast<const schema_utils::schema_node_t&>(**entry.node).has_raw_sql()) {
                return make_error(resource(), core::error_code_t::unimplemented_yet,
                    "MySQL transaction subqueries require complete table dependency information");
            }
            for (const auto* name : {&entry.target.name, &entry.target.from_name}) {
                if (name->collection.empty()) continue;
                if (name->unique_identifier.empty() || name->database.empty()) {
                    return make_error(resource(), core::error_code_t::unimplemented_yet,
                        "MySQL transactions require fully resolved remote table names");
                }
                tables.emplace(name->unique_identifier, name->database, name->collection);
            }
        }
    }
    for (const auto& [alias, database, table] : tables) {
        const auto scope = "TABLE_SCHEMA=" + mysql_transaction::text_literal(database) +
                           " AND TABLE_NAME=" + mysql_transaction::text_literal(table);
        auto engine_handler = [this](const boost::mysql::results& result) -> otterstax::asio_error_t {
            if (result.rows().size() != 1 || result.rows()[0].size() != 2 ||
                !result.rows()[0][0].is_string() || result.rows()[0][0].as_string() != "InnoDB" ||
                !result.rows()[0][1].is_string() || result.rows()[0][1].as_string() != "BASE TABLE") {
                return make_error(resource(), core::error_code_t::unimplemented_yet,
                    "MySQL transactions require InnoDB base tables");
            }
            return {};
        };
        const auto metadata = "SELECT ENGINE,TABLE_TYPE FROM information_schema.TABLES WHERE " + scope;
        if (auto error = connector_manager_->executeQuery(alias, metadata, engine_handler, session).get();
            error.contains_error()) return error;
        // Acquire a transaction-duration metadata lock without reading rows,
        // then recheck the engine under that lock before any user DML runs.
        const auto probe = "SELECT * FROM " + mysql_transaction::identifier(database) + "." +
                           mysql_transaction::identifier(table) + " LIMIT 0";
        auto ignore = [](const boost::mysql::results&) { return otterstax::asio_error_t{}; };
        if (auto error = connector_manager_->executeQuery(alias, probe, ignore, session).get();
            error.contains_error()) return error;
        if (auto error = connector_manager_->executeQuery(alias, metadata, engine_handler, session).get();
            error.contains_error()) return error;

        // Without TRIGGER permission an empty information_schema result would
        // also mean hidden triggers. Require a direct grant; roles and partial
        // revokes whose effective scope is unproven are refused.
        auto grants_handler = [this, &database, &table](const boost::mysql::results& result) -> otterstax::asio_error_t {
            bool permitted = false;
            for (const auto& row : result.rows()) {
                if (row.size() != 1 || !row[0].is_string()) continue;
                auto value = row[0].as_string();
                std::string_view grant{value.data(), value.size()};
                if (grant.starts_with("REVOKE ")) {
                    return make_error(resource(), core::error_code_t::unimplemented_yet,
                        "MySQL transaction trigger visibility cannot be verified with partial revokes");
                }
                permitted = permitted || mysql_transaction::grants_trigger(grant, database, table);
            }
            if (!permitted) {
                return make_error(resource(), core::error_code_t::unimplemented_yet,
                    "MySQL transactions require a direct TRIGGER grant to verify table dependencies");
            }
            return {};
        };
        if (auto error = connector_manager_->executeQuery(alias, "SHOW GRANTS FOR CURRENT_USER", grants_handler, session).get();
            error.contains_error()) return error;
        const auto triggers = "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=" +
            mysql_transaction::text_literal(database) + " AND EVENT_OBJECT_TABLE=" +
            mysql_transaction::text_literal(table) + " LIMIT 1";
        auto trigger_handler = [this](const boost::mysql::results& result) -> otterstax::asio_error_t {
            if (!result.rows().empty()) {
                return make_error(resource(), core::error_code_t::unimplemented_yet,
                    "MySQL transactions cannot verify the storage dependencies of table triggers");
            }
            return {};
        };
        if (auto error = connector_manager_->executeQuery(alias, triggers, trigger_handler, session).get();
            error.contains_error()) return error;
    }
    return core::error_t::no_error();
}
