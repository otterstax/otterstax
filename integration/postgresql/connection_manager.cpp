// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_manager.hpp"

#include "integration/prepare_probe.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "otterbrix/translators/input/chunk_windows.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"
#include "utility/wait_barrier.hpp"

#include <charconv>
#include <optional>
#include <string_view>
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

PostgressManager::PostgressManager(std::pmr::memory_resource* res, pg::ConnectorManager* connector_manager)
    : resource_(res)
    , connector_manager_(connector_manager)
    , log_(get_logger(logger_tag::PG_CONNECTION_MANAGER))
    , enums_(res) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(connector_manager_ != nullptr);
    // The wrapped connector manager's io pool is started here, once: it is the
    // only place that knows the pool is about to receive queries.
    connector_manager_->start();
}

std::pair<bool, actor_zeta::detail::enqueue_result>
PostgressManager::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
    OTX_ZONE_N("PostgressManager::enqueue_impl");
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

actor_zeta::behavior_t PostgressManager::behavior(actor_zeta::mailbox::message* msg) {
    OTX_ZONE_N("PostgressManager::behavior");
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<PostgressManager, &PostgressManager::execute>) {
        co_await actor_zeta::dispatch(this, &PostgressManager::execute, msg);
    } else if (cmd == actor_zeta::msg_id<PostgressManager, &PostgressManager::discover>) {
        co_await actor_zeta::dispatch(this, &PostgressManager::discover, msg);
    } else if (cmd == actor_zeta::msg_id<PostgressManager, &PostgressManager::describe>) {
        co_await actor_zeta::dispatch(this, &PostgressManager::describe, msg);
    }
}

// What the slot holds decides the text: a raw-SQL subquery stub is its text with
// the qualifiers rewritten for PostgreSQL, the catalog's aggregate stub and a
// plan node are generated. execute runs this statement and describe probes its
// header, so both see one text.
core::result_wrapper_t<std::string>
PostgressManager::generate_slot_statement(const components::logical_plan::node_ptr& node,
                                          const otterstax::names::resolved_target_t& target,
                                          const std::pmr::vector<external_entry_t>& batch,
                                          const components::logical_plan::storage_parameters* parameters) {
    OTX_ZONE_N("PostgressManager::generate_slot_statement");
    if (node->type() == logical_plan::node_type::unused) {
        auto& stub = static_cast<schema_utils::schema_node_t&>(*node);
        if (stub.has_raw_sql()) {
            return sql_gen::replace_qualifiers(stub.raw_sql(),
                                               stub.qualifiers(),
                                               backend_type_t::PostgreSQL,
                                               resource());
        }
        return sql_gen::generate_query(stub.agg_node(),
                                       parameters,
                                       backend_type_t::PostgreSQL,
                                       target,
                                       batch,
                                       resource());
    }
    return sql_gen::generate_query(node, parameters, backend_type_t::PostgreSQL, target, batch, resource());
}

core::result_wrapper_t<tsl::pg_enum_oid_map> PostgressManager::fetch_enum_types(const std::string& uuid) {
    OTX_ZONE_N("PostgressManager::fetch_enum_types");
    const std::string query =
        "SELECT t.oid, t.typname, e.enumlabel "
        "FROM pg_type t "
        "JOIN pg_enum e ON e.enumtypid = t.oid "
        "WHERE t.typtype = 'e' "
        "ORDER BY t.oid, e.enumsortorder;";

    // Filled on the io thread, read here after get(): the handler only sees
    // this frame, never the actor's cache.
    tsl::pg_enum_oid_map map;
    auto handler = [this, &map](PGresult* result) -> otterstax::asio_error_t {
        if (result == nullptr) {
            return make_error(resource(), core::error_code_t::io_error, "pg_enum query produced no result");
        }
        const int nrows = PQntuples(result);
        for (int i = 0; i < nrows; ++i) {
            const char* oid_str = PQgetvalue(result, i, 0);
            const char* typname = PQgetvalue(result, i, 1);
            const char* enumlabel = PQgetvalue(result, i, 2);
            if (oid_str == nullptr || typname == nullptr || enumlabel == nullptr) {
                return make_error(resource(),
                                  core::error_code_t::conversion_failure,
                                  "pg_enum row " + std::to_string(i) + " has a NULL oid, typname or label");
            }
            unsigned int oid = 0;
            const std::string_view oid_text{oid_str};
            auto [end, ec] = std::from_chars(oid_text.data(), oid_text.data() + oid_text.size(), oid);
            if (ec != std::errc{} || end != oid_text.data() + oid_text.size()) {
                return make_error(resource(),
                                  core::error_code_t::conversion_failure,
                                  "pg_enum row " + std::to_string(i) + " has a non-numeric oid '" + oid_str + "'");
            }
            auto& desc = map[oid];
            if (desc.typname.empty()) {
                desc.typname = typname;
            }
            desc.values.emplace_back(enumlabel);
        }
        return otterstax::asio_error_t{};
    };

    if (auto err = connector_manager_->executeQuery(uuid, query, handler).get(); err.contains_error()) {
        log_->error("fetch_enum_types: query failed for uuid={}: {}", uuid, err.what.c_str());
        return std::move(err);
    }
    log_->info("fetch_enum_types: discovered {} ENUM types for uuid={}", map.size(), uuid);
    return std::move(map);
}

// Every connector future is consumed at the top level of this body (one query
// in flight per connection at a time); the result handlers only collect and
// never issue queries themselves. The handlers capture frame locals by
// reference: they run on the io thread while this thread waits in get(), and
// the future's hand-off orders the handler's writes before the reads below.
actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
PostgressManager::discover(qualified_name_t scope) {
    OTX_ZONE_N("PostgressManager::discover");
    const std::string& uuid = scope.unique_identifier;
    if (scope.schema.empty()) {
        log_->error("discover: no PostgreSQL schema configured for uuid {}", uuid);
        co_return make_error(resource(),
                             core::error_code_t::missing_field,
                             "Cannot discover PostgreSQL schema: no schema configured for uuid: " + uuid);
    }

    // The ENUM map is refreshed on every discovery of the uid and cached only
    // once the query succeeded; the probes below decode enum columns with it.
    auto enums = fetch_enum_types(uuid);
    if (enums.has_error()) {
        co_return copy_error(resource(), enums.error());
    }
    const tsl::pg_enum_oid_map pg_enum_oids = std::move(enums.value());
    enums_.insert_or_assign(std::pmr::string{uuid.c_str(), resource()}, pg_enum_oids);

    catalog_ext::discovered_tables_t out(resource());
    auto probe = [this, &out, &uuid, &pg_enum_oids](const qualified_name_t& table) -> core::error_t {
        auto schema_handler = [this, table, pg_enum_oids, &out](PGresult* schema_result) -> otterstax::asio_error_t {
            auto schema_struct = tsl::pg_to_struct(resource(), schema_result, pg_enum_oids);
            out.push_back(catalog_ext::discovered_table_t{table, std::move(schema_struct)});
            log_->info("discover: schema discovered for: {}.{}", table.schema, table.collection);
            return otterstax::asio_error_t{};
        };
        auto probe_query =
            otterstax::catalog::make_schema_probe_query(resource(), table, backend_type_t::PostgreSQL);
        if (probe_query.has_error()) {
            return copy_error(resource(), probe_query.error());
        }
        log_->debug("discover: generated PostgreSQL probe: \"{}\"", probe_query.value());
        return connector_manager_->executeQuery(uuid, probe_query.value(), schema_handler).get();
    };

    if (!scope.collection.empty()) {
        if (auto err = probe(scope); err.contains_error()) {
            co_return std::move(err);
        }
        co_return std::move(out);
    }

    // Whole-schema discovery via information_schema.
    auto list_tables_query =
        otterstax::catalog::make_list_tables_query(resource(), backend_type_t::PostgreSQL, scope.schema);
    log_->debug("discover: querying information_schema: \"{}\"", list_tables_query.c_str());

    std::pmr::vector<std::pmr::string> table_names(resource());
    auto list_handler = [&table_names](PGresult* result) -> otterstax::asio_error_t {
        const int num_tables = PQntuples(result);
        for (int i = 0; i < num_tables; ++i) {
            table_names.emplace_back(PQgetvalue(result, i, 0));
        }
        return otterstax::asio_error_t{};
    };
    if (auto err = connector_manager_->executeQuery(uuid, list_tables_query, list_handler).get();
        err.contains_error()) {
        co_return std::move(err);
    }
    log_->info("discover: found {} tables in schema {}", table_names.size(), scope.schema);

    // Probe each table sequentially — the connection is free between queries.
    std::pmr::vector<std::pmr::string> failed_tables(resource());
    for (const auto& tn : table_names) {
        qualified_name_t table = scope;
        table.collection = std::string{tn.c_str(), tn.size()};
        if (auto err = probe(table); err.contains_error()) {
            log_->error("discover: failed to fetch schema for {}.{}: {}",
                        table.schema,
                        table.collection,
                        err.what.c_str());
            failed_tables.emplace_back((table.schema + "." + table.collection).c_str());
        }
    }
    if (!failed_tables.empty()) {
        co_return otterstax::catalog::make_discovery_error(resource(), failed_tables);
    }
    co_return std::move(out);
}

actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>>
PostgressManager::execute(session_hash_t id, ParsedQueryDataPtr data) {
    OTX_ZONE_N("PostgressManager::execute");
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
        OTX_ZONE_N("PostgressManager::batch");
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
            OTX_ZONE_N("PostgressManager::node_dispatch");
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
                    it_backend->second != backend_type_t::PostgreSQL) {
                    log_->debug("execute: Skipping non-PostgreSQL node with UID: {}", uid);
                    continue;
                }
            }

            // A node attributed to this backend with no live connection must fail
            // THIS query: silently skipping leaves the node symbolic, and the engine
            // later dies with a misleading "database does not exist" (mid-flight
            // deregistration race). Nothing downstream can handle the node either.
            if (!connector_manager_->hasConnection(uid)) {
                log_->error("execute: no PostgreSQL connection registered for UID: {}", uid);
                co_return core::error_t(
                    core::error_code_t::do_not_exists,
                    std::pmr::string{("no PostgreSQL connection registered for '" + uid + "'").c_str(), resource()});
            }

            processed_indices.push_back(i);

            auto statement = generate_slot_statement(node, target, batch_nodes, parameters);
            if (statement.has_error()) {
                log_->error("execute: SQL generation failed: {}", statement.error().what.c_str());
                co_return statement.convert_error<ParsedQueryDataPtr>();
            }
            generated_queries.emplace_back(std::move(statement.value()));
            log_->debug("execute Generated PostgreSQL Query: \"{}\"", generated_queries.back());
            // Discovery always leaves an entry for the uid it probed, so a
            // missing one means this uid never went through registration and
            // its enum columns could not be decoded.
            auto enum_it = enums_.find(std::pmr::string{uid.c_str(), resource()});
            if (enum_it == enums_.end()) {
                log_->error("execute: no ENUM metadata discovered for PostgreSQL connection '{}'", uid);
                co_return core::error_t(
                    core::error_code_t::do_not_exists,
                    std::pmr::string{("no ENUM metadata discovered for PostgreSQL connection '" + uid + "'").c_str(),
                                     resource()});
            }
            // Copied into the handler: the io thread must not read actor state.
            auto enum_oids = enum_it->second;
            conversion_errors.emplace_back();
            auto* conversion_error = &conversion_errors.back();
            auto data_converter =
                [this, enum_oids, conversion_error](PGresult* result) -> std::unique_ptr<data_chunk_t> {
                auto converted = tsl::pg_to_chunk(this->resource(), result, enum_oids);
                if (converted.has_error()) {
                    conversion_error->emplace(converted.error().type,
                                              std::pmr::string{converted.error().what.c_str(), this->resource()});
                    return nullptr;
                }
                return std::make_unique<data_chunk_t>(std::move(converted.value()));
            };
            wait_guard.futures.push_back(
                connector_manager_->executeQuery(uid, generated_queries.back(), data_converter));
        }

        if (processed_indices.empty()) {
            log_->debug("execute: No PostgreSQL nodes in this batch");
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
            OTX_ZONE_N("PostgressManager::to_chunk");
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
                    std::pmr::string{("PostgreSQL backend '" + uid + "' returned no result chunk").c_str(),
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
PostgressManager::describe(session_hash_t id, ParsedQueryDataPtr data) {
    OTX_ZONE_N("PostgressManager::describe");
    assert(data);
    assert(data->otterbrix_params);
    log_->debug("describe started, id hash: {}", id);

    // An extension root (CREATE EXTERNAL TABLE, kafka DDL) is `unused` like a
    // stub and the Worker keeps it away from the schema path; one arriving here
    // is a routing error, and reading it as a stub would be undefined.
    if (data->extension_kind != extension_kind_t::none) {
        co_return make_error(resource(),
                             core::error_code_t::invalid_parameter,
                             "describe: an extension statement has no PostgreSQL result schema");
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
                             "the result schema of a PostgreSQL statement with " + std::to_string(unbound) +
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
                    it_backend->second != backend_type_t::PostgreSQL) {
                    continue;
                }
            }
            if (!connector_manager_->hasConnection(uid)) {
                log_->error("describe: no PostgreSQL connection registered for UID: {}", uid);
                co_return make_error(resource(),
                                     core::error_code_t::do_not_exists,
                                     "no PostgreSQL connection registered for '" + uid + "'");
            }
            // Discovery always leaves an entry for the uid it probed, so a
            // missing one means this uid never went through registration and its
            // enum columns could not be decoded — the same rule execute applies.
            auto enum_it = enums_.find(std::pmr::string{uid.c_str(), resource()});
            if (enum_it == enums_.end()) {
                log_->error("describe: no ENUM metadata discovered for PostgreSQL connection '{}'", uid);
                co_return make_error(resource(),
                                     core::error_code_t::do_not_exists,
                                     "no ENUM metadata discovered for PostgreSQL connection '" + uid + "'");
            }

            auto statement = generate_slot_statement(node, target, batch_nodes, parameters);
            if (statement.has_error()) {
                log_->error("describe: SQL generation failed: {}", statement.error().what.c_str());
                co_return statement.convert_error<ParsedQueryDataPtr>();
            }
            // The wrap, its cost and what it cannot carry: prepare_probe.hpp.
            const std::string probe = make_prepare_probe(statement.value(), backend_type_t::PostgreSQL);
            log_->debug("describe: probing \"{}\"", probe);

            // Copied into the handler: the io thread must not read actor state.
            auto enum_oids = enum_it->second;
            std::optional<components::types::complex_logical_type> header;
            auto header_handler = [this, enum_oids, &header](PGresult* result) -> otterstax::asio_error_t {
                if (result == nullptr) {
                    return make_error(resource(),
                                      core::error_code_t::io_error,
                                      "the prepare probe produced no result");
                }
                // A row-producing statement is answered with its
                // RowDescription whether or not a row follows it; a result
                // without fields describes nothing and is reported below.
                if (PQnfields(result) == 0) {
                    return otterstax::asio_error_t{};
                }
                header.emplace(tsl::pg_to_struct(this->resource(), result, enum_oids));
                return otterstax::asio_error_t{};
            };
            if (auto err = connector_manager_->executeQuery(uid, probe, header_handler).get(); err.contains_error()) {
                log_->error("describe: probe failed for UID {}: {}", uid, err.what.c_str());
                co_return std::move(err);
            }
            if (!header.has_value()) {
                log_->error("describe: probe answered without columns: \"{}\"", probe);
                co_return make_error(resource(),
                                     core::error_code_t::schema_error,
                                     "PostgreSQL answered the prepare probe without columns: " + probe);
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
