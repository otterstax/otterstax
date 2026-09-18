// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "worker.hpp"

#include "catalog/catalog_manager.hpp"
#include "connectors/file/manager.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/otterbrix/otterbrix_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/s3/s3_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "otterbrix/operators/schema_probe.hpp"
#include "otterbrix/parser/grammar_extention/external_node.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "utility/logger.hpp"
#include "utility/timer.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_drop.hpp>

#include <cassert>
#include <deque>
#include <exception>
#include <string>
#include <string_view>
#include <utility>

using namespace components;
using core::error_code_t;

namespace {

    // Every error_t the Worker hands back is built on the Worker's resource.
    core::error_t make_error(std::pmr::memory_resource* resource, error_code_t code, std::string_view what) {
        return core::error_t{code, std::pmr::string{what, resource}};
    }

    // A callee's error keeps its code; only the text may gain a prefix.
    core::error_t
    forward_error(std::pmr::memory_resource* resource, const core::error_t& error, std::string_view prefix) {
        std::pmr::string what{prefix, resource};
        what.append(error.what);
        return core::error_t{error.type, std::move(what)};
    }

    constexpr std::string_view kReprepare = "prepared statement must be re-prepared";

    // A stored statement is single-use: executing it hands the plan to the
    // engine and finalizes the binder, so re-running the same entry is never
    // valid. The guard erases the entry when the enclosing coroutine returns —
    // on every path, success or error — and the frontend re-prepares.
    template<typename Map>
    class erase_on_exit_t {
    public:
        erase_on_exit_t(Map& map, session_hash_t id)
            : map_(map)
            , id_(id) {}
        erase_on_exit_t(const erase_on_exit_t&) = delete;
        erase_on_exit_t& operator=(const erase_on_exit_t&) = delete;
        ~erase_on_exit_t() { map_.erase(id_); }

    private:
        Map& map_;
        session_hash_t id_;
    };

    // Root carriers produced by the grammar extensions. Both are
    // node_type::unused (the engine never executes them), the same tag as a
    // schema_node_t stub, so the plan root's type cannot tell them apart; the
    // parser records the claiming extension on ParsedQueryData::extension_kind
    // and that is the only discriminator the split needs.
    struct extension_root_t {
        otterstax::external::external_node_t* external{nullptr};
        otterstax::kafka::kafka_node_t* kafka{nullptr};
    };

    extension_root_t extension_root(const ParsedQueryData& data) {
        extension_root_t root;
        auto* node = data.otterbrix_params->node.get();
        switch (data.extension_kind) {
            case extension_kind_t::external:
                root.external = static_cast<otterstax::external::external_node_t*>(node);
                break;
            case extension_kind_t::kafka:
                root.kafka = static_cast<otterstax::kafka::kafka_node_t*>(node);
                break;
            case extension_kind_t::none:
                break;
        }
        return root;
    }

    // The database a CREATE DATABASE / DROP DATABASE statement names; empty
    // for every other statement. Both keep the name on the DDL node itself —
    // catalog resolves live outside the plan tree — so a DROP DATABASE root
    // without one names nothing and is refused rather than passed on. The
    // sequence unwrap stays as the defensive read of a wrapped root.
    core::result_wrapper_t<std::pmr::string> database_ddl_target(std::pmr::memory_resource* resource,
                                                                 const ParsedQueryData& data) {
        const logical_plan::node_t* root = plan_statement_node(data.otterbrix_params->node.get());
        if (root == nullptr) {
            return std::pmr::string{resource};
        }
        if (root->type() == logical_plan::node_type::create_database_t) {
            const auto& create = static_cast<const logical_plan::node_create_database_t&>(*root);
            return std::pmr::string{std::string_view{create.dbname()}, resource};
        }
        if (root->type() != logical_plan::node_type::drop_t) {
            return std::pmr::string{resource};
        }
        const auto& drop = static_cast<const logical_plan::node_drop_t&>(*root);
        if (drop.kind() != logical_plan::drop_target_kind::database) {
            return std::pmr::string{resource};
        }
        if (drop.dbname().empty()) {
            return make_error(resource, error_code_t::invalid_parameter, "DROP DATABASE names no database");
        }
        return std::pmr::string{std::string_view{drop.dbname()}, resource};
    }

    // The relations a plan reads, as the dependency map the schema computation
    // resolves its aggregates against: one entry per DISTINCT aggregate key,
    // numbered in insertion order, because IDataManager::get_schema answers a
    // vector of that many probes and compute_otterbrix_schema indexes into it
    // by these numbers. Two aggregates over one relation share a key and so
    // share a slot — the number is the map's own size, never a node counter,
    // which for a self-join would run past the vector the map sizes.
    std::pmr::map<qualified_name_t, size_t> plan_dependencies(std::pmr::memory_resource* resource,
                                                              const logical_plan::node_ptr& root) {
        std::pmr::map<qualified_name_t, size_t> dependencies(resource);
        std::deque<logical_plan::node_ptr> nodes_traverse;
        nodes_traverse.emplace_back(root);
        while (!nodes_traverse.empty()) {
            auto& n = nodes_traverse.front();
            if (n->type() == logical_plan::node_type::aggregate_t) {
                const size_t index = dependencies.size();
                dependencies.emplace(schema_utils::agg_key(static_cast<const logical_plan::node_aggregate_t&>(*n)),
                                     index);
            }
            for (auto& child : n->children()) {
                nodes_traverse.emplace_back(child);
            }
            nodes_traverse.pop_front();
        }
        return dependencies;
    }

    // The RETURNING list of a DML statement and the relation whose columns
    // resolve it, or an empty answer for every other statement.
    //
    // RETURNING makes an INSERT / UPDATE / DELETE answer ROWS, so it has a
    // result schema, and which relation resolves the names is the engine
    // validator's rule (resolve_returning_columns,
    // services/dispatcher/validate_logical_plan.cpp): an INSERT resolves its
    // RETURNING against the target table ALONE — a source sub-select's columns
    // are never in scope — so INSERT ... SELECT is described from the target
    // just like INSERT ... VALUES. An UPDATE ... FROM / DELETE ... USING puts
    // the SOURCE's columns in scope as well, and which side a bare name belongs
    // to is not decidable from the plan here, so that one shape is left
    // undescribed rather than described wrongly.
    struct returning_clause_t {
        const std::pmr::vector<expressions::expression_ptr>* list{nullptr};
        const std::string* database{nullptr};
        const std::string* relation{nullptr};
    };

    // An UPDATE / DELETE whose plan carries a source to join against; its match
    // and limit children are the statement's own, not a source.
    bool joins_a_source(const logical_plan::node_t& statement) {
        for (const auto& child : statement.children()) {
            const auto type = child->type();
            if (type != logical_plan::node_type::match_t && type != logical_plan::node_type::limit_t) {
                return true;
            }
        }
        return false;
    }

    returning_clause_t dml_returning(const logical_plan::node_ptr& root) {
        const logical_plan::node_t* statement = plan_statement_node(root.get());
        if (statement == nullptr) {
            return {};
        }
        switch (statement->type()) {
            case logical_plan::node_type::insert_t: {
                const auto& insert = static_cast<const logical_plan::node_insert_t&>(*statement);
                if (insert.returning().empty()) {
                    return {};
                }
                return {&insert.returning(), &insert.dbname(), &insert.relname()};
            }
            case logical_plan::node_type::update_t: {
                const auto& update = static_cast<const logical_plan::node_update_t&>(*statement);
                if (update.returning().empty() || joins_a_source(update)) {
                    return {};
                }
                return {&update.returning(), &update.dbname(), &update.relname()};
            }
            case logical_plan::node_type::delete_t: {
                const auto& remove = static_cast<const logical_plan::node_delete_t&>(*statement);
                if (remove.returning().empty() || joins_a_source(remove)) {
                    return {};
                }
                return {&remove.returning(), &remove.dbname(), &remove.relname()};
            }
            default:
                return {};
        }
    }

    // Whether a RETURNING expression is a column of the answer at all — the same
    // set the planner projects (projects_column,
    // components/physical_plan_generator/impl/create_plan_select.cpp).
    bool projects_column(const expressions::expression_i& expr) {
        switch (expr.group()) {
            case expressions::expression_group::scalar:
            case expressions::expression_group::function:
            case expressions::expression_group::cast:
            case expressions::expression_group::compare:
                return true;
            default:
                return false;
        }
    }

    bool is_star(const expressions::expression_i& expr) {
        return expr.group() == expressions::expression_group::scalar &&
               static_cast<const expressions::scalar_expression_t&>(expr).type() ==
                   expressions::scalar_type::star_expand;
    }

    // The name the executed chunk's column carries for this output, by the
    // planner's own rule (projected_name, create_plan_select.cpp): the
    // expression's key — the alias where it was written with one — and for a
    // bare column reference the column it reads.
    std::string projected_name(const expressions::expression_i& expr) {
        if (expr.group() != expressions::expression_group::scalar) {
            return expr.key().as_string();
        }
        if (!expr.key().storage().empty()) {
            const auto& part = expr.key().storage().back();
            return std::string{part.data(), part.size()};
        }
        const auto& scalar = static_cast<const expressions::scalar_expression_t&>(expr);
        if (scalar.type() == expressions::scalar_type::get_field && !scalar.params().empty() &&
            std::holds_alternative<expressions::key_t>(scalar.params().front())) {
            const auto& field = std::get<expressions::key_t>(scalar.params().front());
            if (!field.storage().empty()) {
                const auto& part = field.storage().back();
                return std::string{part.data(), part.size()};
            }
        }
        return std::string{};
    }

    // The column a RETURNING expression READS, which is what types it; empty for
    // an expression that reads none (a call, a literal, a value the binder still
    // holds). An output alias renames the column but does not retype it, so the
    // name looked up here is the operand's, not the output's.
    std::string source_column(const expressions::expression_i& expr) {
        if (expr.group() != expressions::expression_group::scalar) {
            return std::string{};
        }
        const auto& scalar = static_cast<const expressions::scalar_expression_t&>(expr);
        if (!scalar.params().empty()) {
            if (!std::holds_alternative<expressions::key_t>(scalar.params().front())) {
                return std::string{};
            }
            const auto& field = std::get<expressions::key_t>(scalar.params().front());
            if (field.storage().empty()) {
                return std::string{};
            }
            const auto& part = field.storage().back();
            return std::string{part.data(), part.size()};
        }
        if (expr.key().storage().empty()) {
            return std::string{};
        }
        const auto& part = expr.key().storage().back();
        return std::string{part.data(), part.size()};
    }

    // The columns a RETURNING list projects, typed from the columns the target
    // relation's probe answered. `*` is the relation's own columns in its own
    // order — what the engine expands it to — and a named column carries that
    // column's type. What this cannot name it leaves NA, exactly as the SELECT
    // projection does: the kernel of a function call is not consulted here and a
    // value the binder still holds has no type before Bind; the frontend's check
    // on the executed result is what catches the difference. A list that
    // projects no column at all answers NA — no statement that returns rows is
    // ever described as a row of zero columns.
    types::complex_logical_type
    returning_schema(std::pmr::memory_resource* resource,
                     const std::pmr::vector<expressions::expression_ptr>& returning,
                     const std::pmr::vector<types::complex_logical_type>& columns) {
        OTX_ZONE_N("Worker::returning_schema");
        auto find_field_type = [&columns](const std::string& name) -> types::complex_logical_type {
            if (name.empty()) {
                return types::logical_type::NA;
            }
            for (const auto& column : columns) {
                if (column.has_alias() && column.alias() == name) {
                    return column;
                }
            }
            return types::logical_type::NA;
        };

        std::pmr::vector<types::complex_logical_type> projected(resource);
        projected.reserve(returning.size());
        for (const auto& expr : returning) {
            if (!expr || !projects_column(*expr)) {
                continue;
            }
            if (is_star(*expr)) {
                for (const auto& column : columns) {
                    projected.push_back(column);
                }
                continue;
            }
            auto type = find_field_type(source_column(*expr));
            type.set_alias(projected_name(*expr));
            projected.push_back(std::move(type));
        }
        if (projected.empty()) {
            return types::logical_type::NA;
        }
        return types::complex_logical_type::create_struct("", projected);
    }

} // namespace

Worker::Worker(std::pmr::memory_resource* res,
               std::size_t self_index,
               std::size_t worker_count,
               std::unique_ptr<IParser> parser,
               actor_zeta::address_t sql_connection_manager,
               actor_zeta::address_t pg_connection_manager,
               actor_zeta::address_t ch_connection_manager,
               actor_zeta::address_t otterbrix_manager,
               actor_zeta::address_t catalog_manager,
               actor_zeta::address_t s3_manager,
               actor_zeta::address_t file_manager,
               actor_zeta::address_t kafka_manager)
    : actor_zeta::basic_actor<Worker>(res)
    , resource_(res)
    , self_index_(self_index)
    , worker_count_(worker_count)
    , log_(get_logger(logger_tag::SCHEDULER))
    , parser_(std::move(parser))
    , sql_connection_manager_(sql_connection_manager)
    , pg_connection_manager_(pg_connection_manager)
    , ch_connection_manager_(ch_connection_manager)
    , otterbrix_manager_(otterbrix_manager)
    , catalog_manager_(catalog_manager)
    , s3_manager_(s3_manager)
    , file_manager_(file_manager)
    , kafka_manager_(kafka_manager)
    , metadata_map_(res) {
    assert(log_.is_valid());
    assert(res != nullptr);
    assert(parser_ != nullptr);
    assert(worker_count_ > 0);
    assert(self_index_ < worker_count_);
}

actor_zeta::behavior_t Worker::behavior(actor_zeta::mailbox::message* msg) {
    auto cmd = msg->command();
    if (cmd == actor_zeta::msg_id<Worker, &Worker::execute>) {
        co_await actor_zeta::dispatch(this, &Worker::execute, msg);
    } else if (cmd == actor_zeta::msg_id<Worker, &Worker::execute_statement>) {
        co_await actor_zeta::dispatch(this, &Worker::execute_statement, msg);
    } else if (cmd == actor_zeta::msg_id<Worker, &Worker::execute_prepared_statement>) {
        co_await actor_zeta::dispatch(this, &Worker::execute_prepared_statement, msg);
    } else if (cmd == actor_zeta::msg_id<Worker, &Worker::prepare_schema>) {
        co_await actor_zeta::dispatch(this, &Worker::prepare_schema, msg);
    } else if (cmd == actor_zeta::msg_id<Worker, &Worker::close_statement>) {
        co_await actor_zeta::dispatch(this, &Worker::close_statement, msg);
    }
}

// ─── Entry-point coroutines ───────────────────────────────────────────────────
// Nothing below throws: errors travel as core::error_t through the future, and
// the only call that may throw (IParser::parse) is fenced inside parse_sql.
//
// Tracy zones are per-thread and strictly nested, and a Worker coroutine resumes
// after co_await on whichever sharing_scheduler thread picks the actor up. A
// zone spanning a suspension point would therefore end on another thread (or
// out of order) and corrupt the trace, so each handler opens its zone in a block
// that closes before the first co_await: the zone measures the handler's own
// synchronous work, the awaited actors measure theirs.

actor_zeta::unique_future<Worker::session_result> Worker::execute(session_hash_t id, std::string sql) {
    assert(id % worker_count_ == self_index_);
    Timer timer("Worker::execute", log_);
    erase_on_exit_t erase_entry{metadata_map_, id};

    ParsedQueryDataPtr data;
    {
        OTX_ZONE_N("Worker::execute");
        log_->info("Worker::execute called with sql: {}", sql);

        auto parsed = parse_sql(sql);
        if (parsed.has_error()) {
            log_->error("Failed to parse SQL: {}", sql);
            co_return forward_error(resource(), parsed.error(), "");
        }
        data = std::move(parsed.value());
    }

    // CREATE EXTERNAL TABLE / COPY ... TO and Kafka DDL land on extension root
    // nodes that no backend and not the engine can execute; they are routed to
    // their managers before any classification.
    const auto root = extension_root(*data);
    if (root.external != nullptr) {
        log_->debug("Worker::execute: external-table statement, routing to file/s3 manager");
        co_return co_await handle_external_statement(id, *root.external);
    }
    if (root.kafka != nullptr) {
        log_->debug("Worker::execute: kafka DDL detected, routing to kafka_manager");
        otterstax::kafka::kafka_node_ptr node{root.kafka}; // shares ownership with the plan
        auto [ns_k, kafka_future] =
            actor_zeta::send(kafka_manager_, &otterstax::kafka::KafkaManager::execute, id, std::move(node));
        auto cursor = co_await std::move(kafka_future);
        if (!cursor) {
            co_return make_error(resource(), error_code_t::kernel_error, "Kafka DDL returned no cursor");
        }
        if (cursor->is_error()) {
            co_return forward_error(resource(), cursor->get_error(), "Kafka DDL failed: ");
        }
        co_return session_payload{resource()};
    }

    // Plain-SQL `INSERT INTO kafka.<obj>` is a write to a kafka object: publish
    // the rows to the topic via the KafkaManager producer instead of writing an
    // engine table.
    if (auto write = otterstax::kafka::kafka_write_target(data->otterbrix_params->node)) {
        // INSERT ... SELECT into a kafka object is a continuous ksqlDB "INSERT INTO
        // query" (persistent fan-in into an existing stream), NOT a one-shot
        // produce of a source-table snapshot. Route it to KafkaManager, which sets
        // up a dedicated continuous worker; hand it the raw SQL so it (and restart
        // recovery) can compile the SELECT itself.
        if (write->source_is_select) {
            log_->debug("Worker::execute: kafka INSERT INTO '{}' SELECT, routing to insert-query setup",
                        write->relname);
            auto [ns_ki, ki_future] = actor_zeta::send(kafka_manager_,
                                                       &otterstax::kafka::KafkaManager::add_stream_insert,
                                                       id,
                                                       std::move(write->relname),
                                                       std::string{sql});
            auto cursor = co_await std::move(ki_future);
            if (!cursor) {
                co_return make_error(resource(),
                                     error_code_t::kernel_error,
                                     "Kafka INSERT INTO ... SELECT returned no cursor");
            }
            if (cursor->is_error()) {
                co_return forward_error(resource(), cursor->get_error(), "Kafka INSERT INTO ... SELECT failed: ");
            }
            co_return session_payload{resource()};
        }

        log_->debug("Worker::execute: kafka write to '{}', routing to kafka_manager produce", write->relname);
        auto [ns_kw, kw_future] = actor_zeta::send(kafka_manager_,
                                                   &otterstax::kafka::KafkaManager::produce,
                                                   id,
                                                   std::move(write->relname),
                                                   std::move(write->source));
        auto cursor = co_await std::move(kw_future);
        if (!cursor) {
            co_return make_error(resource(), error_code_t::kernel_error, "Kafka write returned no cursor");
        }
        if (cursor->is_error()) {
            co_return forward_error(resource(), cursor->get_error(), "Kafka write failed: ");
        }
        co_return session_payload{resource()};
    }

    if (auto guard = co_await guard_database_ddl(*data); guard.contains_error()) {
        co_return std::move(guard);
    }

    // Classification is written on the DATA (`backend_type`); update_metadata
    // rebuilds the map entry wholesale from it. A statement without external
    // nodes never consults the catalog for a schema: it is the engine's.
    if (data->otterbrix_params->external_nodes_count > 0) {
        auto [ns_cat, catalog_future] =
            actor_zeta::send(catalog_manager_, &mysql::CatalogManager::update_backend_type, id, std::move(data));
        auto catalog_result = co_await std::move(catalog_future);
        if (catalog_result.has_error()) {
            co_return forward_error(resource(), catalog_result.error(), "");
        }
        update_metadata(id, std::move(catalog_result.value()));
    } else {
        data->backend_type = backend_type_t::Otterbrix;
        update_metadata(id, std::move(data));
    }

    auto it = metadata_map_.find(id);
    assert(it != metadata_map_.end() && "update_metadata stores the entry it was just given");
    const auto backend = it->second.backend_type;
    log_->debug("Worker::execute routing to backend_type: {}", static_cast<int>(backend));
    if (backend == backend_type_t::Unknown) {
        co_return make_error(resource(),
                             error_code_t::schema_error,
                             "backend_unknown: Unknown backend type, cannot execute");
    }

    co_return co_await run_pipeline(id, std::move(it->second.query_data_ptr), backend);
}

actor_zeta::unique_future<Worker::session_result> Worker::execute_statement(session_hash_t id) {
    assert(id % worker_count_ == self_index_);
    Timer timer("Worker::execute_statement", log_);
    erase_on_exit_t erase_entry{metadata_map_, id};

    ParsedQueryDataPtr data_ptr;
    backend_type_t backend = backend_type_t::Unknown;
    {
        OTX_ZONE_N("Worker::execute_statement");
        auto it = metadata_map_.find(id);
        if (it == metadata_map_.end() || !it->second.query_data_ptr) {
            co_return make_error(resource(), error_code_t::invalid_parameter, kReprepare);
        }
        backend = it->second.backend_type;
        data_ptr = std::move(it->second.query_data_ptr);
        // A statement with placeholders runs only after execute_prepared_statement
        // has bound every one of them; executed without that Bind, the plan would
        // reach the engine carrying unbound parameters.
        if (auto& binder = data_ptr->binder(); !binder.all_bound()) {
            co_return make_error(resource(),
                                 error_code_t::invalid_parameter,
                                 "prepared statement executed without binding its " +
                                     std::to_string(binder.parameter_count()) + " parameter(s)");
        }
    }

    // External-table statements (prepared via prepare_schema) carry an
    // external_node_t; route to the file/s3 managers, bypassing backend routing.
    if (const auto root = extension_root(*data_ptr); root.external != nullptr) {
        log_->debug("Worker::execute_statement: external-table statement, routing to file/s3 manager");
        co_return co_await handle_external_statement(id, *root.external);
    }

    if (auto guard = co_await guard_database_ddl(*data_ptr); guard.contains_error()) {
        co_return std::move(guard);
    }

    if (backend == backend_type_t::Unknown) {
        co_return make_error(resource(),
                             error_code_t::schema_error,
                             "backend_unknown: Backend type is unknown, cannot execute statement.");
    }

    co_return co_await run_pipeline(id, std::move(data_ptr), backend);
}

actor_zeta::unique_future<Worker::session_result>
Worker::execute_prepared_statement(session_hash_t id, std::pmr::vector<types::logical_value_t> parameters) {
    assert(id % worker_count_ == self_index_);
    Timer timer("Worker::execute_prepared_statement", log_);
    // A failed bind consumes the entry as well: the binder has been partially
    // written and cannot be rerun.
    erase_on_exit_t erase_entry{metadata_map_, id};

    {
        OTX_ZONE_N("Worker::execute_prepared_statement");
        auto it = metadata_map_.find(id);
        if (it == metadata_map_.end() || !it->second.query_data_ptr) {
            co_return make_error(resource(), error_code_t::invalid_parameter, kReprepare);
        }
        auto& data = *it->second.query_data_ptr;
        auto& binder = data.binder();
        for (size_t i = 0; i < parameters.size(); ++i) {
            binder.bind(i + 1, std::move(parameters[i]));
        }
        // finalize() is the only door onto the statement's catalog lookups, and a
        // parameterized statement passes through it HERE rather than at parse
        // time: the parser leaves it unfinalized while any `$n` is unbound (an
        // early finalize records the failure and breaks every later bind). The
        // plan it answers is dropped — every execution site builds its own from
        // (node, params), because a backend manager may have replaced the root —
        // but the lookups are a sibling of that root and travel no other way, so
        // they are taken over onto the statement (OtterbrixStatement::catalog_resolves).
        // Without them the engine runs this statement with no CHECK, PK or FK to
        // enforce. Taken once per prepare: the entry is single-use, so nothing
        // re-reads or re-appends them.
        auto bind_res = binder.finalize();
        if (bind_res.has_error()) {
            co_return forward_error(resource(), bind_res.error(), "Argument binding failed: ");
        }
        data.otterbrix_params->catalog_resolves = bind_res.value().catalog_resolves;
    }

    co_return co_await execute_statement(id);
}

actor_zeta::unique_future<Worker::session_result> Worker::prepare_schema(session_hash_t id, std::string sql) {
    assert(id % worker_count_ == self_index_);
    Timer timer("Worker::prepare_schema", log_);

    ParsedQueryDataPtr parsed_data;
    {
        OTX_ZONE_N("Worker::prepare_schema");
        log_->debug("[prepare_schema] Start, id hash: {}, sql: {}", id, sql);

        auto parsed = parse_sql(sql);
        if (parsed.has_error()) {
            co_return forward_error(resource(), parsed.error(), "");
        }
        parsed_data = std::move(parsed.value());

        // Extension roots are `unused`-tagged and must not reach the
        // schema_node_t path below. CREATE EXTERNAL TABLE / COPY ... TO has no
        // preparable result schema: stash an empty one, the work runs in DoGet
        // (execute_statement). Kafka DDL cannot be prepared at all.
        const auto root = extension_root(*parsed_data);
        if (root.external != nullptr) {
            log_->debug("Worker::prepare_schema: external-table statement, returning empty schema");
            parsed_data->backend_type = backend_type_t::Otterbrix;
            co_return finish_schema_value(id, cursor::make_cursor(resource()), std::move(parsed_data));
        }
        if (root.kafka != nullptr) {
            co_return make_error(resource(),
                                 error_code_t::unimplemented_yet,
                                 "Kafka DDL statements cannot be prepared");
        }
    }

    if (auto guard = co_await guard_database_ddl(*parsed_data); guard.contains_error()) {
        co_return std::move(guard);
    }

    // Classification lives on the DATA: finish_schema_value builds the map
    // entry from `backend_type`, so a purely local statement is marked
    // before the entry exists.
    if (parsed_data->otterbrix_params->external_nodes_count == 0) {
        parsed_data->backend_type = backend_type_t::Otterbrix;
        // Only a row-producing statement has a result schema. A DML answers an
        // affected count, which has none — unless it carries RETURNING, which
        // makes it answer ROWS, and those rows must be described: the clients
        // that live by the statement's description read NoData as "this
        // statement returns no rows", and lib/pq then hands the caller a row of
        // zero values rather than raising, so the rows are lost without an
        // error. NoData is never the answer for a statement that returns rows.
        //
        // Described from the PLAN, never by running it. Describe(portal)
        // describes a SELECT exactly by executing it, but a DML executed to be
        // described would WRITE — and write a second time for a client that
        // binds the portal again — which is why the frontend refuses that path
        // by statement kind. So the TARGET RELATION is probed for its columns
        // instead (`SELECT * FROM db.rel LIMIT 0`, which reads no row and writes
        // nothing) and the RETURNING list is resolved against them. The one
        // shape left undescribed is an UPDATE ... FROM / DELETE ... USING, whose
        // RETURNING may name the source as well (dml_returning).
        if (parsed_data->tag != T_SelectStmt) {
            const auto returning = dml_returning(parsed_data->otterbrix_params->node);
            if (returning.list == nullptr) {
                co_return finish_schema_value(id, cursor::make_cursor(resource()), std::move(parsed_data));
            }
            // The probe is a statement of its own: the aggregate `schema_probe`
            // builds for exactly this question, so no name is ever spelled into
            // SQL text. It carries no parameter and no external slot, so
            // OtterbrixManager::get_schema answers it from the engine's own plan
            // validation. The binder rides along only because a ParsedQueryData
            // owns one; nothing on this path reads it.
            auto probe =
                otterstax::schema_probe::make_table_probe(resource(), *returning.database, *returning.relation);
            auto probe_data = std::make_unique<ParsedQueryData>(
                std::make_unique<OtterbrixStatement>(std::pmr::vector<std::pmr::vector<external_entry_t>>{resource()},
                                                     std::move(probe.params),
                                                     std::move(probe.root),
                                                     0,
                                                     0),
                sql::transform::transform_result{resource(), core::error_t::no_error()},
                T_SelectStmt);
            std::pmr::map<qualified_name_t, size_t> no_dependencies(resource());
            auto [ns_probe, probe_future] = actor_zeta::send(otterbrix_manager_,
                                                             &db::OtterbrixManager::get_schema,
                                                             id,
                                                             std::move(no_dependencies),
                                                             std::move(probe_data));
            auto probed = co_await std::move(probe_future);
            if (probed.has_error()) {
                co_return forward_error(resource(), probed.error(), "RETURNING: ");
            }
            const auto& probe_cursor = probed.value().first;
            if (probe_cursor->is_error()) {
                co_return forward_error(resource(), probe_cursor->get_error(), "RETURNING: ");
            }
            if (probe_cursor->size() == 0 || probe_cursor->type_data()[0].type() != types::logical_type::STRUCT) {
                co_return make_error(resource(),
                                     error_code_t::schema_error,
                                     "RETURNING: the engine answered no columns for the statement's target relation");
            }
            const auto& fields = probe_cursor->type_data()[0].child_types();
            std::pmr::vector<types::complex_logical_type> columns(fields.begin(), fields.end(), resource());
            auto schema = returning_schema(resource(), *returning.list, columns);
            if (schema.type() != types::logical_type::STRUCT) {
                co_return make_error(resource(),
                                     error_code_t::schema_error,
                                     "RETURNING: the statement projects no column this prepare can name");
            }
            std::pmr::vector<types::complex_logical_type> schema_types(resource());
            schema_types.push_back(std::move(schema));
            co_return finish_schema_value(id,
                                          cursor::make_cursor(resource(), std::move(schema_types)),
                                          std::move(parsed_data));
        }
        // Without a placeholder the engine resolves the plan's output columns
        // itself, in plan-only EXPLAIN mode (resolve + validate, no scan), and
        // needs no dependency map; the plan is executed later from the same
        // nodes, every execution re-running the resolve pass.
        //
        // With one it cannot: the plan-only pass refuses a plan that still
        // carries `$n` ("unbound parameter in expression") and the binder holds
        // every parameter — the literals included — until finalize, so there is
        // nothing to validate a plan with before Bind. Such a statement is
        // described the way a statement with external slots is: the relations
        // it reads are probed for their columns (`SELECT * FROM db.rel LIMIT 0`,
        // one per relation, which no placeholder reaches) and the plan's own
        // projection is resolved against them. That is how the columns of `*`
        // are known before Bind — PostgreSQL expands the star out of its
        // catalog at parse time for the same reason — and it is what types a
        // named column. What it cannot name it still leaves NA — a function
        // call, whose kernel this computation does not consult, where an
        // arithmetic expression takes its first operand's type — and a root it
        // cannot read at all (a set operation) answers no schema, as it did
        // before; the frontends describe what came back rather than "this
        // statement returns no rows" and hold the executed result to it.
        // Built for every local SELECT, not only a parameterized one: the
        // engine path below ignores the map, and a set operation needs it
        // whether or not the statement carries a placeholder. Held in a named
        // local because the send moves `parsed_data` — the order in which a
        // call's arguments are evaluated is unspecified, so reading the plan
        // through it in the same argument list could read a moved-from pointer.
        auto dependencies = plan_dependencies(resource(), parsed_data->otterbrix_params->node);
        auto [ns_local, local_future] = actor_zeta::send(otterbrix_manager_,
                                                         &db::OtterbrixManager::get_schema,
                                                         id,
                                                         std::move(dependencies),
                                                         std::move(parsed_data));
        auto local_result = co_await std::move(local_future);
        if (local_result.has_error()) {
            co_return forward_error(resource(), local_result.error(), "");
        }
        auto [local_cursor, local_data] = std::move(local_result.value());
        co_return finish_schema_value(id, std::move(local_cursor), std::move(local_data));
    }

    auto [ns_cat, catalog_future] =
        actor_zeta::send(catalog_manager_, &mysql::CatalogManager::get_catalog_schema, id, std::move(parsed_data));
    auto catalog_result = co_await std::move(catalog_future);
    if (catalog_result.has_error()) {
        co_return forward_error(resource(), catalog_result.error(), "");
    }
    auto data = std::move(catalog_result.value());

    // The prepared schema of a statement's remote slots is the backend's, not
    // the plan's. Each backend owns the result type of every expression it
    // evaluates — ClickHouse's count() is UInt64 where the plan reads BIGINT and
    // `x + 1` over an Int32 column is Int64, MySQL widens `x + 1` over an INT
    // column to BIGINT, and a non-aggregate function aliased as a base column
    // (`length(name) AS name`) has no type on the plan at all — so a
    // plan-derived type names something the executed chunk does not carry, and
    // the stream then disagrees with the schema it was announced under. A
    // raw-SQL sub-query stub is worse off still: it carries the user's text,
    // which nothing on the plan describes, so its schema stays NA, and a JOIN
    // with it does not merely degrade — the schema merge answers "OtterBrix
    // collection is missing in catalog" and the prepare fails outright.
    //
    // So every backend that owns a slot of this statement describes its own, in
    // the order run_pipeline executes them: each manager probes the slot with
    // the statement execute would run and fills that stub's schema with what the
    // backend answers, decoded exactly as execute decodes its rows. A manager
    // fills the stubs its node_backend_types entry names and leaves every other
    // slot alone, which is what lets each participant of a Mixed statement
    // describe its own share; a statement whose slots carry no stub (a DML, a
    // JOIN root over plain tables) comes back with nothing probed. The sends
    // cost no thread hop — these managers run a handler to completion on the
    // sender's thread — so a described statement pays one backend round-trip per
    // stub.
    //
    // A parameterized statement is out of the described scope for now — this is
    // a narrower scope, not a fallback: the binder holds every parameter, the
    // literals included, until finalize, so before Bind there is no SQL to
    // probe with, and what a prepare should answer for `$n` is being decided
    // separately. Until that policy lands such a statement keeps the schema the
    // catalog put on the stub, exactly as it did before this routing.
    if (data->otterbrix_params->parameters_count == 0) {
        const auto owns_a_slot = [&data](backend_type_t backend) {
            if (data->backend_type != backend_type_t::Mixed) {
                return data->backend_type == backend;
            }
            for (const auto& node_backend : data->node_backend_types) {
                if (node_backend.second == backend) {
                    return true;
                }
            }
            return false;
        };
        if (owns_a_slot(backend_type_t::MySQL)) {
            auto [ns_my, my_future] =
                actor_zeta::send(sql_connection_manager_, &db::MySQLManager::describe, id, std::move(data));
            auto described = co_await std::move(my_future);
            if (described.has_error()) {
                co_return forward_error(resource(), described.error(), "");
            }
            data = std::move(described.value());
        }
        if (owns_a_slot(backend_type_t::PostgreSQL)) {
            auto [ns_pg, pg_future] =
                actor_zeta::send(pg_connection_manager_, &db::PostgressManager::describe, id, std::move(data));
            auto described = co_await std::move(pg_future);
            if (described.has_error()) {
                co_return forward_error(resource(), described.error(), "");
            }
            data = std::move(described.value());
        }
        if (owns_a_slot(backend_type_t::ClickHouse)) {
            auto [ns_ch, ch_future] =
                actor_zeta::send(ch_connection_manager_, &db::ClickhouseManager::describe, id, std::move(data));
            auto described = co_await std::move(ch_future);
            if (described.has_error()) {
                co_return forward_error(resource(), described.error(), "");
            }
            data = std::move(described.value());
        }
    }

    // Extension roots were filtered above, so an `unused` schema root here is
    // the catalog's schema_node_t stub standing in for the whole statement: a
    // SELECT over one remote table. The transformer wraps a table-referencing
    // statement in a node_sequence_t whose data-producing node is the LAST
    // child, and that child is what the catalog rewrote, so the stub is looked
    // for under the sequence as well — the catalog has already refused an
    // empty sequence.
    const logical_plan::node_t* schema_root = data->otterbrix_params->node.get();
    if (schema_root->type() == logical_plan::node_type::sequence_t) {
        schema_root = schema_root->children().back().get();
    }
    if (schema_root->type() == logical_plan::node_type::unused) {
        std::pmr::vector<types::complex_logical_type> schema_types(resource());
        schema_types.push_back(static_cast<const schema_utils::schema_node_t&>(*schema_root).schema());
        auto cursor = cursor::make_cursor(resource(), std::move(schema_types));
        co_return finish_schema_value(id, std::move(cursor), std::move(data));
    }

    // Named local: the send moves `data`, and argument evaluation order is
    // unspecified, so the plan must be read before the call rather than inside
    // its argument list.
    auto dependencies = plan_dependencies(resource(), data->otterbrix_params->node);
    auto [ns_sch, schema_future] = actor_zeta::send(otterbrix_manager_,
                                                    &db::OtterbrixManager::get_schema,
                                                    id,
                                                    std::move(dependencies),
                                                    std::move(data));
    auto schema_result = co_await std::move(schema_future);
    if (schema_result.has_error()) {
        co_return forward_error(resource(), schema_result.error(), "");
    }
    auto [cursor, data_back] = std::move(schema_result.value());
    co_return finish_schema_value(id, std::move(cursor), std::move(data_back));
}

actor_zeta::unique_future<Worker::session_result> Worker::close_statement(session_hash_t id) {
    OTX_ZONE_N("Worker::close_statement");
    assert(id % worker_count_ == self_index_);
    // Idempotent: the entry is gone afterwards whether or not it existed — a
    // consumed statement was already erased by its execute, and closing a
    // session that was never prepared changes nothing.
    metadata_map_.erase(id);
    co_return session_payload{resource()};
}

// ─── Backend dispatch + engine execution ─────────────────────────────────────

actor_zeta::unique_future<Worker::session_result>
Worker::run_pipeline(session_hash_t id, ParsedQueryDataPtr data_ptr, backend_type_t backend) {
    if (backend == backend_type_t::MySQL || backend == backend_type_t::Mixed) {
        auto [ns_sql, sql_future] =
            actor_zeta::send(sql_connection_manager_, &db::MySQLManager::execute, id, std::move(data_ptr));
        auto sql_result = co_await std::move(sql_future);
        if (sql_result.has_error()) {
            co_return forward_error(resource(), sql_result.error(), "");
        }
        data_ptr = std::move(sql_result.value());

        if (backend == backend_type_t::Mixed) {
            auto [ns_pg, pg_future] =
                actor_zeta::send(pg_connection_manager_, &db::PostgressManager::execute, id, std::move(data_ptr));
            auto pg_result = co_await std::move(pg_future);
            if (pg_result.has_error()) {
                co_return forward_error(resource(), pg_result.error(), "");
            }
            data_ptr = std::move(pg_result.value());

            auto [ns_ch, ch_future] =
                actor_zeta::send(ch_connection_manager_, &db::ClickhouseManager::execute, id, std::move(data_ptr));
            auto ch_result = co_await std::move(ch_future);
            if (ch_result.has_error()) {
                co_return forward_error(resource(), ch_result.error(), "");
            }
            data_ptr = std::move(ch_result.value());
        }
    } else if (backend == backend_type_t::PostgreSQL) {
        auto [ns_pg, pg_future] =
            actor_zeta::send(pg_connection_manager_, &db::PostgressManager::execute, id, std::move(data_ptr));
        auto pg_result = co_await std::move(pg_future);
        if (pg_result.has_error()) {
            co_return forward_error(resource(), pg_result.error(), "");
        }
        data_ptr = std::move(pg_result.value());
    } else if (backend == backend_type_t::ClickHouse) {
        auto [ns_ch, ch_future] =
            actor_zeta::send(ch_connection_manager_, &db::ClickhouseManager::execute, id, std::move(data_ptr));
        auto ch_result = co_await std::move(ch_future);
        if (ch_result.has_error()) {
            co_return forward_error(resource(), ch_result.error(), "");
        }
        data_ptr = std::move(ch_result.value());
    }

    auto [ns_ot, ot_future] = actor_zeta::send(otterbrix_manager_,
                                               &db::OtterbrixManager::execute,
                                               id,
                                               std::move(data_ptr->otterbrix_params));
    auto cursor = co_await std::move(ot_future);
    if (!cursor) {
        co_return make_error(resource(), error_code_t::kernel_error, "Otterbrix execution returned no cursor");
    }
    if (cursor->is_error()) {
        co_return forward_error(resource(), cursor->get_error(), "Otterbrix execution failed: ");
    }
    co_return take_payload(id, *cursor);
}

actor_zeta::unique_future<core::error_t> Worker::guard_database_ddl(const ParsedQueryData& data) {
    std::pmr::string dbname{resource()};
    {
        OTX_ZONE_N("Worker::guard_database_ddl");
        auto target = database_ddl_target(resource(), data);
        if (target.has_error()) {
            co_return forward_error(resource(), target.error(), "");
        }
        if (target.value().empty()) {
            co_return core::error_t::no_error();
        }
        dbname = std::move(target.value());
    }

    auto [ns_own, owner_future] = actor_zeta::send(catalog_manager_,
                                                   &mysql::CatalogManager::check_database_ownership,
                                                   std::string{dbname.c_str(), dbname.size()});
    auto owned = co_await std::move(owner_future);
    if (owned.contains_error()) {
        co_return forward_error(resource(), owned, "");
    }
    co_return core::error_t::no_error();
}

// ─── External-table dispatch (CREATE EXTERNAL TABLE / COPY ... TO) ──────────
// Both forms parse into an otterstax::external::external_node_t. The s3 / file
// managers do the actual ingestion or export — neither lands on the regular
// backend / otterbrix execute path. DDL and COPY produce no rows, so success
// returns an empty session_payload.

actor_zeta::unique_future<Worker::session_result>
Worker::handle_external_statement(session_hash_t id, const otterstax::external::external_node_t& ext) {
    OTX_ZONE_N("Worker::handle_external_statement");
    if (ext.op() == otterstax::external::external_op_t::create_external_table) {
        return load_external_table(id, ext);
    }
    return copy_to(id, ext);
}

actor_zeta::unique_future<Worker::session_result>
Worker::load_external_table(session_hash_t id, const otterstax::external::external_node_t& ext) {
    if (ext.is_s3()) {
        auto [ns_s3, fut] = actor_zeta::send(s3_manager_,
                                             &db::S3Manager::download,
                                             id,
                                             ext.s3_alias(),
                                             ext.object_path(),
                                             ext.database(),
                                             ext.table());
        auto r = co_await std::move(fut);
        if (r.has_error()) {
            co_return forward_error(resource(), r.error(), "CREATE EXTERNAL TABLE failed: ");
        }
        co_return session_payload{resource()};
    }

    conn::file::FileAddParams params;
    {
        OTX_ZONE_N("Worker::load_external_table");
        params.database = ext.database();
        params.table = ext.table();
        params.path = ext.location();
        params.format = ext.format().empty() ? std::string{"auto"} : ext.format();
    }
    auto [ns_file, fut] =
        actor_zeta::send(file_manager_, &conn::file::FileManager::add_file, id, std::move(params));
    auto r = co_await std::move(fut);
    if (r.has_error()) {
        co_return forward_error(resource(), r.error(), "CREATE EXTERNAL TABLE failed: ");
    }
    if (!r.value()) {
        co_return make_error(resource(),
                             error_code_t::io_error,
                             "CREATE EXTERNAL TABLE failed: add_file returned false");
    }
    co_return session_payload{resource()};
}

actor_zeta::unique_future<Worker::session_result>
Worker::copy_to(session_hash_t id, const otterstax::external::external_node_t& ext) {
    // The inner query is re-parsed here; its result is what gets exported.
    OtterbrixStatementPtr statement;
    conn::file::FileFormat fmt = conn::file::FileFormat::Unknown;
    {
        OTX_ZONE_N("Worker::copy_to");
        if (ext.inner_sql().empty()) {
            co_return make_error(resource(), error_code_t::sql_parse_error, "COPY ... TO: missing inner query");
        }
        auto inner = parse_sql(ext.inner_sql());
        if (inner.has_error()) {
            co_return forward_error(resource(), inner.error(), "COPY ... TO: ");
        }
        statement = std::move(inner.value()->otterbrix_params);

        if (!ext.is_s3()) {
            fmt = conn::file::resolve_format(ext.format(), ext.location());
            if (fmt == conn::file::FileFormat::Unknown) {
                co_return make_error(resource(),
                                     error_code_t::invalid_parameter,
                                     "COPY ... TO: cannot determine file format for '" + ext.location() + "'");
            }
        }
    }

    if (ext.is_s3()) {
        auto [ns_s3, fut] = actor_zeta::send(s3_manager_,
                                             &db::S3Manager::upload,
                                             id,
                                             ext.s3_alias(),
                                             ext.object_path(),
                                             std::move(statement));
        auto r = co_await std::move(fut);
        if (r.has_error()) {
            co_return forward_error(resource(), r.error(), "COPY ... TO failed: ");
        }
        co_return session_payload{resource()};
    }

    conn::file::FileMetadata meta;
    meta.statement = std::move(statement);
    meta.path = ext.location();
    meta.format = fmt;
    auto [ns_file, fut] = actor_zeta::send(file_manager_, &conn::file::FileManager::dump_file, id, std::move(meta));
    auto r = co_await std::move(fut);
    if (r.has_error()) {
        co_return forward_error(resource(), r.error(), "COPY ... TO failed: ");
    }
    co_return session_payload{resource()};
}

// ─── Helpers (per-worker, no locks) ─────────────────────────────────────────

core::result_wrapper_t<ParsedQueryDataPtr> Worker::parse_sql(const std::string& sql) {
    OTX_ZONE_N("Worker::parse_sql");
    // The engine's raw parser sits behind IParser::parse, and a test parser may
    // throw on purpose; the catch is confined to this call.
    try {
        auto parsed = parser_->parse(sql);
        if (parsed.has_error()) {
            return parsed;
        }
        if (!parsed.value() || !parsed.value()->otterbrix_params || !parsed.value()->otterbrix_params->node) {
            return make_error(resource(), error_code_t::sql_parse_error, "parser returned no plan");
        }
        return parsed;
    } catch (const std::exception& e) {
        log_->error("Worker::parse_sql: parser threw: {}", e.what());
        return make_error(resource(), error_code_t::sql_parse_error, e.what());
    } catch (...) {
        log_->error("Worker::parse_sql: parser threw a non-standard exception");
        return make_error(resource(), error_code_t::sql_parse_error, "parser threw a non-standard exception");
    }
}

Worker::session_result Worker::finish_schema_value(session_hash_t id,
                                                   cursor::cursor_t_ptr cursor,
                                                   ParsedQueryDataPtr data) {
    OTX_ZONE_N("Worker::finish_schema_value");
    if (cursor->is_error()) {
        return forward_error(resource(), cursor->get_error(), "");
    }
    types::complex_logical_type schema;
    if (cursor->size()) {
        schema = cursor->type_data()[0];
    }
    const size_t param_cnt = data->otterbrix_params->parameters_count;
    const NodeTag tag = data->tag;
    update_metadata(id, std::move(data), schema);
    std::pmr::vector<components::vector::data_chunk_t> chunks(resource());
    chunks.emplace_back(resource(), std::pmr::vector<components::types::complex_logical_type>{resource()}, 0);
    return session_payload{std::move(schema), std::move(chunks), param_cnt, tag};
}

Worker::session_result Worker::take_payload(session_hash_t id, cursor::cursor_t& cursor) {
    OTX_ZONE_N("Worker::take_payload");
    // The Worker is the only writer of its map and the entry was present when
    // the pipeline started; it is moved out here, not copied, and the caller's
    // guard erases it.
    auto it = metadata_map_.find(id);
    assert(it != metadata_map_.end() && "session metadata must outlive its pipeline");
    if (it == metadata_map_.end()) {
        return make_error(resource(), error_code_t::kernel_error, "session metadata vanished during execution");
    }
    auto& meta = it->second;
    return session_payload{
        std::move(meta.schema),
        std::exchange(cursor.chunks(), std::pmr::vector<components::vector::data_chunk_t>{resource()}),
        0,
        meta.tag};
}

void Worker::update_metadata(session_hash_t id,
                             ParsedQueryDataPtr metadata,
                             components::types::complex_logical_type schema) {
    NodeTag tag = metadata->tag;
    backend_type_t backend_type = metadata->backend_type;
    metadata_map_[id] = metadata_t{std::move(schema), std::move(metadata), tag, backend_type};
}
