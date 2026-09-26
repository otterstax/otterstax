// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "integration/prepare_probe.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "otterbrix/translators/input/affected_rows_carrier.hpp"
#include "otterbrix/translators/input/ch_to_chunk.hpp"
#include "otterbrix/translators/input/chunk_windows.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/wait_barrier.hpp"

#include <optional>
#include <thread>

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

ClickhouseManager::ClickhouseManager(std::pmr::memory_resource* res, ch::ConnectorManager* connector_manager)
    : resource_(res)
    , connector_manager_(connector_manager)
    , log_(get_logger(logger_tag::CH_CONNECTION_MANAGER))
    , named_types_(res) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(connector_manager_ != nullptr);
    // The wrapped connector manager's io pool is started here, once: it is the
    // only place that knows the pool is about to receive queries.
    connector_manager_->start();
}

std::pair<bool, actor_zeta::detail::enqueue_result>
ClickhouseManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    OTX_ZONE_N("ClickhouseManager::enqueue_impl");
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

actor_zeta::behavior_t ClickhouseManager::behavior(actor_zeta::mailbox::message* msg) {
    OTX_ZONE_N("ClickhouseManager::behavior");
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<ClickhouseManager, &ClickhouseManager::execute>) {
        co_await actor_zeta::dispatch(this, &ClickhouseManager::execute, msg);
    } else if (cmd == actor_zeta::msg_id<ClickhouseManager, &ClickhouseManager::discover>) {
        co_await actor_zeta::dispatch(this, &ClickhouseManager::discover, msg);
    } else if (cmd == actor_zeta::msg_id<ClickhouseManager, &ClickhouseManager::describe>) {
        co_await actor_zeta::dispatch(this, &ClickhouseManager::describe, msg);
    }
}

// What the slot holds decides the text: a raw-SQL subquery stub is its text with
// the qualifiers rewritten for ClickHouse, the catalog's aggregate stub and a
// plan node are generated. The named types that apply to the result are the
// target table's — a raw subquery's first qualifier names that table.
core::result_wrapper_t<ClickhouseManager::slot_statement_t>
ClickhouseManager::generate_slot_statement(const logical_plan::node_ptr& node,
                                           const otterstax::names::resolved_target_t& target,
                                           const std::pmr::vector<external_entry_t>& batch,
                                           const logical_plan::storage_parameters* parameters) {
    OTX_ZONE_N("ClickhouseManager::generate_slot_statement");
    slot_statement_t statement;
    statement.table = target.name.collection;
    if (node->type() == logical_plan::node_type::unused) {
        auto& stub = static_cast<schema_utils::schema_node_t&>(*node);
        if (stub.has_raw_sql()) {
            auto rendered =
                sql_gen::replace_qualifiers(stub.raw_sql(), stub.qualifiers(), backend_type_t::ClickHouse, resource());
            if (rendered.has_error()) {
                return rendered.convert_error<slot_statement_t>();
            }
            statement.sql = std::move(rendered.value());
            if (!stub.qualifiers().empty()) {
                statement.table = stub.qualifiers().front().name.collection;
            }
            return std::move(statement);
        }
        auto generated = sql_gen::generate_query(stub.agg_node(),
                                                 parameters,
                                                 backend_type_t::ClickHouse,
                                                 target,
                                                 batch,
                                                 resource());
        if (generated.has_error()) {
            return generated.convert_error<slot_statement_t>();
        }
        statement.sql = std::move(generated.value());
        return std::move(statement);
    }
    auto generated = sql_gen::generate_query(node, parameters, backend_type_t::ClickHouse, target, batch, resource());
    if (generated.has_error()) {
        return generated.convert_error<slot_statement_t>();
    }
    statement.sql = std::move(generated.value());
    return std::move(statement);
}

core::result_wrapper_t<ClickhouseManager::named_types_t>
ClickhouseManager::fetch_named_types(const std::string& uuid, const std::string& database, const std::string& table) {
    OTX_ZONE_N("ClickhouseManager::fetch_named_types");
    auto query = otterstax::catalog::make_named_types_query(resource(), database, table);

    // Filled on the io thread, read here after get(): the handler only sees
    // this frame, never the actor's cache. A block without rows describes no
    // column; a block with rows that is not the (name, type) pair
    // system.columns answers with is a malformed reply, not an empty one.
    named_types_t fresh;
    auto handler = [this, &fresh](const ch::select_result_t& columns) -> otterstax::asio_error_t {
        for (const auto& block : columns.blocks) {
            if (block.GetRowCount() == 0) {
                continue;
            }
            if (block.GetColumnCount() < 2) {
                return make_error(resource(),
                                  core::error_code_t::schema_error,
                                  "system.columns answered with " + std::to_string(block.GetColumnCount()) +
                                      " column(s) instead of (name, type)");
            }
            auto name_col = block[0]->As<clickhouse::ColumnString>();
            auto type_col = block[1]->As<clickhouse::ColumnString>();
            if (!name_col || !type_col) {
                return make_error(resource(),
                                  core::error_code_t::schema_error,
                                  "system.columns answered with non-string name/type columns");
            }
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                fresh.insert_or_assign(std::string(name_col->At(i)), std::string(type_col->At(i)));
            }
        }
        return otterstax::asio_error_t{};
    };

    if (auto err = connector_manager_->executeQuery(uuid, query, handler).get(); err.contains_error()) {
        log_->error("fetch_named_types failed for {}.{}: {}", database, table, err.what.c_str());
        return std::move(err);
    }
    log_->debug("fetch_named_types: {} column type(s) for {}.{}.{}", fresh.size(), uuid, database, table);
    return std::move(fresh);
}

const ClickhouseManager::named_types_t* ClickhouseManager::named_types_for(const std::string& uuid,
                                                                           const std::string& table) const {
    auto u_it = named_types_.find(std::pmr::string{uuid.c_str(), resource()});
    if (u_it == named_types_.end()) {
        return nullptr;
    }
    auto t_it = u_it->second.find(std::pmr::string{table.c_str(), resource()});
    if (t_it == u_it->second.end()) {
        return nullptr;
    }
    return &t_it->second;
}

// Every connector future is consumed at the top level of this body (one query
// in flight per connection at a time); the result handlers only collect and
// never issue queries themselves. The handlers capture frame locals by
// reference: they run on the io thread while this thread waits in get(), and
// the future's hand-off orders the handler's writes before the reads below.
actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
ClickhouseManager::discover(qualified_name_t scope) {
    OTX_ZONE_N("ClickhouseManager::discover");
    const std::string& uuid = scope.unique_identifier;
    if (scope.database.empty()) {
        log_->error("discover: no ClickHouse database configured for uuid {}", uuid);
        co_return make_error(resource(),
                             core::error_code_t::missing_field,
                             "Cannot discover ClickHouse schema: no database configured for uuid: " + uuid);
    }
    const std::string& database = scope.database;

    catalog_ext::discovered_tables_t out(resource());
    // Named types first, then the `WHERE 1 = 0` probe, which answers with the
    // header block (columns, no rows); no block or a block without columns
    // means the probe did not describe the table, and registering a
    // column-less table would hide that until the first query against it.
    auto probe = [this, &out, &uuid, &database](const std::string& table_name) -> core::error_t {
        auto fetched = fetch_named_types(uuid, database, table_name);
        if (fetched.has_error()) {
            return copy_error(resource(), fetched.error());
        }
        named_types_t overrides = std::move(fetched.value());
        named_types_[std::pmr::string{uuid.c_str(), resource()}].insert_or_assign(
            std::pmr::string{table_name.c_str(), resource()},
            overrides);

        qualified_name_t table(uuid, database, "", table_name);
        auto schema_handler = [this, table, overrides, &out](const ch::select_result_t& probe_result)
            -> otterstax::asio_error_t {
            const auto& schema_blocks = probe_result.blocks;
            if (schema_blocks.empty() || schema_blocks[0].GetColumnCount() == 0) {
                return make_error(resource(),
                                  core::error_code_t::schema_error,
                                  "ClickHouse schema probe returned no columns for " + table.database + "." +
                                      table.collection);
            }
            auto schema_struct = tsl::ch_to_struct(resource(), schema_blocks[0], overrides);
            out.push_back(catalog_ext::discovered_table_t{table, std::move(schema_struct)});
            log_->info("discover: schema discovered for: {}.{}", table.database, table.collection);
            return otterstax::asio_error_t{};
        };
        auto probe_query = otterstax::catalog::make_schema_probe_query(resource(), table, backend_type_t::ClickHouse);
        if (probe_query.has_error()) {
            return copy_error(resource(), probe_query.error());
        }
        log_->debug("discover: generated ClickHouse probe: \"{}\"", probe_query.value());
        return connector_manager_->executeQuery(uuid, probe_query.value(), schema_handler).get();
    };

    if (!scope.collection.empty()) {
        if (auto err = probe(scope.collection); err.contains_error()) {
            co_return std::move(err);
        }
        co_return std::move(out);
    }

    // Whole-database discovery via system.tables.
    auto list_tables_query =
        otterstax::catalog::make_list_tables_query(resource(), backend_type_t::ClickHouse, database);
    log_->debug("discover: querying ClickHouse tables: \"{}\"", list_tables_query.c_str());

    std::pmr::vector<std::pmr::string> table_names(resource());
    auto list_handler = [this, &table_names](const ch::select_result_t& listed) -> otterstax::asio_error_t {
        for (const auto& block : listed.blocks) {
            if (block.GetRowCount() == 0) {
                continue;
            }
            auto name_col = block[0]->As<clickhouse::ColumnString>();
            if (!name_col) {
                return make_error(resource(),
                                  core::error_code_t::missing_field,
                                  "Failed to read table names from ClickHouse");
            }
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                auto view = name_col->At(i);
                table_names.emplace_back(view.data(), view.size());
            }
        }
        return otterstax::asio_error_t{};
    };
    if (auto err = connector_manager_->executeQuery(uuid, list_tables_query, list_handler).get();
        err.contains_error()) {
        co_return std::move(err);
    }
    log_->info("discover: found {} tables in ClickHouse database {}", table_names.size(), database);

    // Probe each table sequentially — the connection is free between queries.
    std::pmr::vector<std::pmr::string> failed_tables(resource());
    for (const auto& tn : table_names) {
        std::string table_name{tn.c_str(), tn.size()};
        if (auto err = probe(table_name); err.contains_error()) {
            log_->error("discover: failed to fetch schema for {}.{}: {}", database, table_name, err.what.c_str());
            failed_tables.emplace_back((database + "." + table_name).c_str());
        }
    }
    if (!failed_tables.empty()) {
        co_return otterstax::catalog::make_discovery_error(resource(), failed_tables);
    }
    co_return std::move(out);
}

actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>>
ClickhouseManager::execute(session_hash_t id, ParsedQueryDataPtr data) {
    OTX_ZONE_N("ClickhouseManager::execute");
    assert(data);
    assert(data->otterbrix_params);
    log_->debug("execute started, id hash: {}", id);

    const auto* parameters = &data->otterbrix_params->params_node->parameters();

    log_->debug("execute Total execute queries: {}", data->otterbrix_params->external_nodes_count);
    log_->debug("execute Execute batches: {}", data->otterbrix_params->external_nodes.size());
    size_t counter = 0;
    auto& batches = data->otterbrix_params->external_nodes;
    // Batches are processed back to front (innermost dependencies first).
    for (size_t batch = batches.size(); batch-- > 0;) {
        OTX_ZONE_N("ClickhouseManager::batch");
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
        // Order inside a batch does not matter. For a mixed backend, nodes that
        // belong to another backend are skipped, so the slots this manager filled
        // are tracked by index.
        std::pmr::vector<size_t> processed_indices(resource());
        for (size_t i = 0; i < batch_nodes.size(); i++) {
            OTX_ZONE_N("ClickhouseManager::node_dispatch");
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
                if (it_backend != data->node_backend_types.end() &&
                    it_backend->second != backend_type_t::ClickHouse) {
                    log_->debug("execute: Skipping non-ClickHouse node with UID: {}", uid);
                    continue;
                }
            }

            // A node attributed to this backend with no live connection must fail
            // THIS query: silently skipping leaves the node symbolic, and the engine
            // later dies with a misleading "database does not exist" (mid-flight
            // deregistration race). Nothing downstream can handle the node either.
            if (!connector_manager_->hasConnection(uid)) {
                log_->error("execute: no ClickHouse connection registered for UID: {}", uid);
                co_return core::error_t(
                    core::error_code_t::do_not_exists,
                    std::pmr::string{("no ClickHouse connection registered for '" + uid + "'").c_str(), resource()});
            }

            processed_indices.push_back(i);

            auto statement = generate_slot_statement(node, target, batch_nodes, parameters);
            if (statement.has_error()) {
                log_->error("execute: SQL generation failed: {}", statement.error().what.c_str());
                co_return statement.convert_error<ParsedQueryDataPtr>();
            }
            generated_queries.emplace_back(std::move(statement.value().sql));
            log_->debug("execute Generated ClickHouse Query: \"{}\"", generated_queries.back());

            // Copied into the handler: the io thread must not read actor state.
            named_types_t named_overrides;
            if (const auto* discovered = named_types_for(uid, statement.value().table); discovered != nullptr) {
                named_overrides = *discovered;
            }
            // A DML statement streams no block back, so its result is the same
            // column-less count carrier the engine gives a local DML. The only
            // count ClickHouse reports is the written-row progress, and only an
            // INSERT's is its affected-row count (materialized views add their
            // rows to it; async_insert reports none). A lightweight DELETE and
            // ALTER ... UPDATE are mutations: they run outside the statement's
            // pipeline, and anything they ever attributed to it would be the
            // size of the rewritten parts, not the rows matched — so they
            // report 0 rather than a wrong number.
            const auto slot_type = node->type();
            conversion_errors.emplace_back();
            auto* conversion_error = &conversion_errors.back();
            auto data_converter = [this, slot_type, conversion_error, named_overrides = std::move(named_overrides)](
                                      const ch::select_result_t& result) -> std::unique_ptr<data_chunk_t> {
                switch (slot_type) {
                    case logical_plan::node_type::insert_t:
                        return std::make_unique<data_chunk_t>(
                            tsl::make_affected_rows_carrier(this->resource(), result.written_rows));
                    case logical_plan::node_type::update_t:
                    case logical_plan::node_type::delete_t:
                        return std::make_unique<data_chunk_t>(tsl::make_affected_rows_carrier(this->resource(), 0));
                    default: {
                        auto converted = tsl::ch_to_chunk(this->resource(), result.blocks, named_overrides);
                        if (converted.has_error()) {
                            conversion_error->emplace(
                                converted.error().type,
                                std::pmr::string{converted.error().what.c_str(), this->resource()});
                            return nullptr;
                        }
                        return std::make_unique<data_chunk_t>(std::move(converted.value()));
                    }
                }
            };

            wait_guard.futures.push_back(
                connector_manager_->executeQuery(uid, generated_queries.back(), data_converter));
        }

        if (processed_indices.empty()) {
            log_->debug("execute: No ClickHouse nodes in this batch");
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
            OTX_ZONE_N("ClickhouseManager::to_chunk");
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
                    std::pmr::string{("ClickHouse backend '" + uid + "' returned no result chunk").c_str(),
                                     resource()});
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
ClickhouseManager::describe(session_hash_t id, ParsedQueryDataPtr data) {
    OTX_ZONE_N("ClickhouseManager::describe");
    assert(data);
    assert(data->otterbrix_params);
    log_->debug("describe started, id hash: {}", id);

    // An extension root (CREATE EXTERNAL TABLE, kafka DDL) is `unused` like a
    // stub and the Worker keeps it away from the schema path; one arriving here
    // is a routing error, and reading it as a stub would be undefined.
    if (data->extension_kind != extension_kind_t::none) {
        co_return make_error(resource(),
                             core::error_code_t::invalid_parameter,
                             "describe: an extension statement has no ClickHouse result schema");
    }
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
                             "the result schema of a ClickHouse statement with " + std::to_string(unbound) +
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
                if (it_backend != data->node_backend_types.end() &&
                    it_backend->second != backend_type_t::ClickHouse) {
                    continue;
                }
            }
            if (!connector_manager_->hasConnection(uid)) {
                log_->error("describe: no ClickHouse connection registered for UID: {}", uid);
                co_return make_error(resource(),
                                     core::error_code_t::do_not_exists,
                                     "no ClickHouse connection registered for '" + uid + "'");
            }

            auto statement = generate_slot_statement(node, target, batch_nodes, parameters);
            if (statement.has_error()) {
                log_->error("describe: SQL generation failed: {}", statement.error().what.c_str());
                co_return statement.convert_error<ParsedQueryDataPtr>();
            }
            // The wrap, its cost and what it cannot carry: prepare_probe.hpp.
            const std::string probe = make_prepare_probe(statement.value().sql, backend_type_t::ClickHouse);
            log_->debug("describe: probing \"{}\"", probe);

            // Copied into the handler: the io thread must not read actor state.
            named_types_t named_overrides;
            if (const auto* discovered = named_types_for(uid, statement.value().table); discovered != nullptr) {
                named_overrides = *discovered;
            }
            // The header is the first block with columns; the column-less block
            // that ends the stream describes nothing.
            std::optional<types::complex_logical_type> header;
            auto header_handler = [this, &header, named_overrides = std::move(named_overrides)](
                                      const ch::select_result_t& result) -> otterstax::asio_error_t {
                for (const auto& block : result.blocks) {
                    if (block.GetColumnCount() == 0) {
                        continue;
                    }
                    header.emplace(tsl::ch_to_struct(this->resource(), block, named_overrides));
                    break;
                }
                return otterstax::asio_error_t{};
            };
            if (auto err = connector_manager_->executeQuery(uid, probe, header_handler).get(); err.contains_error()) {
                log_->error("describe: probe failed for UID {}: {}", uid, err.what.c_str());
                co_return std::move(err);
            }
            if (!header.has_value()) {
                log_->error("describe: probe answered without a header block: \"{}\"", probe);
                co_return make_error(resource(),
                                     core::error_code_t::schema_error,
                                     "ClickHouse answered the prepare probe without a header block: " + probe);
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
