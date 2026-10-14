// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "pushdown.hpp"

#include "otterbrix/federation/remote_storage.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <services/collection/context_storage.hpp>

#include <algorithm>
#include <cassert>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

using namespace components;

namespace otterstax::federation::rules {

    namespace {

        // What a generated statement names: the server and the table in the backend's spelling.
        struct remote_target_t {
            server_t server;
            qualified_name_t table;
            sql_gen::column_map_t columns;
            std::vector<std::string> all_columns;
            std::string name;
        };

        remote_target_t target_of(const remote_storage_t& storage) {
            return remote_target_t{storage.table().server,
                                   storage.generator_table(),
                                   storage.column_map(),
                                   storage.table().remote_columns,
                                   storage.name()};
        }

        // One table of a query over several tables of one server (a JOIN, a derived table): the
        // node that reads it in the replaced subtree and the table.
        struct relation_target_t {
            const logical_plan::node_t* node;
            remote_target_t target;
        };

        // The statement of `node` over `target` (and `source`, the second table; or `relations`, the
        // tables of a query over a JOIN or a derived table): with the parameter values when they are
        // known, with `?` for each while the rule decides.
        core::result_wrapper_t<std::string> generate(const logical_plan::node_ptr& node,
                                                     const remote_target_t& target,
                                                     const remote_target_t* source,
                                                     std::span<const relation_target_t> relations,
                                                     const logical_plan::storage_parameters* parameters,
                                                     bool placeholders,
                                                     std::pmr::memory_resource* resource) {
            std::vector<sql_gen::relation_t> tables;
            tables.reserve(relations.size());
            for (const auto& relation : relations) {
                tables.push_back(sql_gen::relation_t{relation.node,
                                                     relation.target.table,
                                                     &relation.target.columns,
                                                     relation.target.all_columns});
            }
            sql_gen::statement_target_t statement{relations.empty() ? target.table : qualified_name_t{},
                                                  source != nullptr ? source->table : qualified_name_t{},
                                                  relations.empty() ? &target.columns : nullptr,
                                                  source != nullptr ? &source->columns : nullptr,
                                                  relations.empty() ? std::span<const std::string>{target.all_columns}
                                                                    : std::span<const std::string>{},
                                                  tables};
            return sql_gen::generate_query(node,
                                           parameters,
                                           target.server.kind,
                                           statement,
                                           resource,
                                           sql_gen::generation_options_t{.placeholders = placeholders,
                                                                         .validated = true});
        }

        using next_t = core::result_wrapper_t<std::optional<vector::data_chunk_t>>;

        // ── a query block as one remote SELECT ──────────────────────────────────

        // An output column that reads a table column as it is: the column's discovered type and where
        // it is on the backend.
        struct plain_column_t {
            types::complex_logical_type type;
            column_origin_t origin;
        };
        using plain_columns_t = std::vector<std::optional<plain_column_t>>;

        struct select_payload_t final : logical_plan::extension_payload_t {
            select_payload_t(remote_target_t target,
                             std::vector<relation_target_t> relations,
                             logical_plan::node_ptr block,
                             std::vector<types::complex_logical_type> columns,
                             const plain_columns_t& plain,
                             std::string preview)
                : target(std::move(target))
                , relations(std::move(relations))
                , block(std::move(block))
                , columns(std::move(columns))
                , preview(std::move(preview)) {
                discovered.reserve(plain.size());
                origins.reserve(plain.size());
                for (const auto& column : plain) {
                    discovered.push_back(column ? std::optional<types::complex_logical_type>{column->type}
                                                : std::nullopt);
                    origins.push_back(column ? std::optional<column_origin_t>{column->origin} : std::nullopt);
                }
            }

            // The table of a block over one table; of a query over relations, the first table read
            // (the server, the dialect) beside every table in `relations`.
            remote_target_t target;
            std::vector<relation_target_t> relations;
            logical_plan::node_ptr block;
            std::vector<types::complex_logical_type> columns;
            // Per output column: the discovered type of the table column it projects as is, and that
            // column on the backend (scan_request_t::columns).
            std::vector<std::optional<types::complex_logical_type>> discovered;
            std::vector<std::optional<column_origin_t>> origins;
            // The statement with `?` for its parameters, for EXPLAIN.
            std::string preview;
        };

        // The source a pushed-down block becomes: open() generates the statement with the
        // statement's parameter values and starts it; source_next() hands the answer out, held to
        // the block's output types, in windows of at most DEFAULT_VECTOR_CAPACITY rows.
        class remote_query_t final : public operators::read_only_operator_t {
        public:
            // `sql`: the statement generated with the plan's parameter values, or empty when it could
            // not be generated then (open() generates it with the values it runs with).
            remote_query_t(std::pmr::memory_resource* resource,
                           log_t log,
                           boost::intrusive_ptr<const select_payload_t> payload,
                           std::string sql)
                : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
                , payload_(std::move(payload))
                , sql_(std::move(sql)) {}

            [[nodiscard]] operators::pipeline_role role() const noexcept override {
                return operators::pipeline_role::source;
            }

            [[nodiscard]] actor_zeta::unique_future<next_t> source_next(pipeline::context_t*) override {
                OTX_ZONE_N("federation::remote_query::source_next");
                // The engine opens every source before it pumps any.
                assert(rows_ && "a remote query is asked for rows only once it was opened");
                if (!conformed_) {
                    for (auto& chunk : rows_->chunks) {
                        if (auto err = conform_to_result(resource(),
                                                         sql_,
                                                         payload_->target.name,
                                                         payload_->columns,
                                                         payload_->discovered,
                                                         timezone_,
                                                         chunk);
                            err.contains_error()) {
                            return actor_zeta::make_ready_future<next_t>(resource(), next_t{std::move(err)});
                        }
                    }
                    conformed_ = true;
                }
                auto& chunks = rows_->chunks;
                while (chunk_index_ < chunks.size() && offset_ >= chunks[chunk_index_].size()) {
                    ++chunk_index_;
                    offset_ = 0;
                }
                if (chunk_index_ == chunks.size()) {
                    if (!handed_out_) {
                        // No row: one typed chunk without rows, so the statement's result keeps the
                        // block's columns (as an empty remote table scans).
                        handed_out_ = true;
                        std::pmr::vector<types::complex_logical_type> types(payload_->columns.begin(),
                                                                            payload_->columns.end(),
                                                                            resource());
                        vector::data_chunk_t empty(resource(), types, 1);
                        empty.set_cardinality(0);
                        return actor_zeta::make_ready_future<next_t>(resource(), next_t{std::move(empty)});
                    }
                    return actor_zeta::make_ready_future<next_t>(resource(), next_t{std::nullopt});
                }
                handed_out_ = true;
                const auto& source = chunks[chunk_index_];
                const auto count = std::min<std::uint64_t>(source.size() - offset_, vector::DEFAULT_VECTOR_CAPACITY);
                auto window = source.partial_copy(resource(), offset_, count);
                offset_ += count;
                return actor_zeta::make_ready_future<next_t>(resource(), next_t{std::move(window)});
            }

            void reset_pipeline_state() noexcept override {
                chunk_index_ = 0;
                offset_ = 0;
                handed_out_ = false;
            }

        private:
            actor_zeta::unique_future<core::error_t> open_impl(pipeline::context_t* ctx) override {
                OTX_ZONE_N("federation::remote_query::open");
                if (rows_) {
                    return actor_zeta::make_ready_future<core::error_t>(resource(), core::error_t::no_error());
                }
                rows_ = std::make_shared<scan_rows_t>();
                timezone_ = ctx->execution_context.timezone_offset;
                const auto& target = payload_->target;
                // The registry holds a backend for every configured connection.
                assert(target.server.backend);
                if (sql_.empty()) {
                    auto sql = generate(payload_->block,
                                        target,
                                        nullptr,
                                        payload_->relations,
                                        &ctx->parameters,
                                        false,
                                        resource());
                    if (sql.has_error()) {
                        return actor_zeta::make_ready_future<core::error_t>(resource(),
                                                                            core::error_on(resource(), sql.error()));
                    }
                    sql_ = std::move(sql.value());
                }
                return target.server.backend->scan(resource(),
                                                   scan_request_t{target.server.uid, sql_, payload_->origins},
                                                   rows_);
            }

            std::pmr::string explain_label_impl() const override {
                return std::pmr::string{("Foreign Query on " + payload_->target.name).c_str(), resource()};
            }

            std::pmr::vector<std::pmr::string> explain_details_impl() const override {
                std::pmr::vector<std::pmr::string> details{resource()};
                details.emplace_back(("Remote SQL: " + (sql_.empty() ? payload_->preview : sql_)).c_str());
                return details;
            }

            boost::intrusive_ptr<const select_payload_t> payload_;
            std::shared_ptr<scan_rows_t> rows_;
            std::string sql_;
            core::date::timezone_offset_t timezone_{};
            bool conformed_{false};
            bool handed_out_{false};
            std::size_t chunk_index_{0};
            std::uint64_t offset_{0};
        };

        // The rule decides with every parameter written as `?`, so it cannot know whether the dialect
        // has a literal for each value the statement is run with (a UUID, an INTERVAL, MySQL's NaN or
        // ROW). The operator factory runs with the values (context.parameters): it generates the
        // statement, and where a value has no literal (unimplemented_yet) it plans the replaced
        // subtree as the engine would have without the rule — over the storages' scan and sinks —
        // instead of sending a statement that cannot be written. Any other failure (a parameter not
        // bound yet: a plan-only describe) leaves the generation to the operator, at open. True: the
        // engine plans the subtree; otherwise `sql` is the statement, or empty to be generated at open.
        bool planned_by_the_engine(const logical_plan::node_ptr& node,
                                   const remote_target_t& target,
                                   const remote_target_t* source,
                                   std::span<const relation_target_t> relations,
                                   const services::context_storage_t& context,
                                   std::string& sql) {
            auto generated = generate(node, target, source, relations, context.parameters, false, context.resource);
            if (!generated.has_error()) {
                sql = std::move(generated.value());
                return false;
            }
            return generated.error().type == core::error_code_t::unimplemented_yet;
        }

        core::result_wrapper_t<boost::intrusive_ptr<operators::operator_t>>
        make_remote_query(const services::context_storage_t& context,
                          const compute::function_registry_t& functions,
                          const logical_plan::node_extension_t& node) {
            OTX_ZONE_N("federation::make_remote_query");
            boost::intrusive_ptr<const select_payload_t> payload{
                static_cast<const select_payload_t*>(node.payload())};
            std::string sql;
            if (planned_by_the_engine(payload->block, payload->target, nullptr, payload->relations, context, sql)) {
                return services::planner::create_plan(context,
                                                      functions,
                                                      payload->block,
                                                      logical_plan::limit_t::unlimit(),
                                                      context.parameters);
            }
            return boost::intrusive_ptr<operators::operator_t>{
                new remote_query_t(context.resource, context.log.clone(), std::move(payload), std::move(sql))};
        }

        // ── a write as one remote statement ─────────────────────────────────────

        struct statement_payload_t final : logical_plan::extension_payload_t {
            statement_payload_t(write_kind_t kind,
                                remote_target_t target,
                                std::optional<remote_target_t> source,
                                logical_plan::node_ptr statement,
                                std::string preview)
                : kind(kind)
                , target(std::move(target))
                , source(std::move(source))
                , statement(std::move(statement))
                , preview(std::move(preview)) {}

            write_kind_t kind;
            remote_target_t target;
            std::optional<remote_target_t> source;
            logical_plan::node_ptr statement;
            std::string preview;
        };

        // The INSERT / UPDATE / DELETE the backend runs whole: generated with the parameter values
        // when the plan runs, its count the rows the backend reports.
        class remote_statement_t final : public operators::read_write_operator_t {
        public:
            // `sql` as remote_query_t takes it: generated with the plan's values, or empty.
            remote_statement_t(std::pmr::memory_resource* resource,
                               log_t log,
                               boost::intrusive_ptr<const statement_payload_t> payload,
                               std::string sql)
                : operators::read_write_operator_t(resource, std::move(log), operators::operator_type::extension)
                , payload_(std::move(payload))
                , sql_(std::move(sql)) {}

            [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

            actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override {
                std::string sql;
                {
                    OTX_ZONE_N("federation::remote_statement::generate");
                    if (executed_) {
                        co_return;
                    }
                    executed_ = true;
                    const auto& target = payload_->target;
                    // The registry holds a backend for every configured connection.
                    assert(target.server.backend);
                    if (sql_.empty()) {
                        auto generated = generate(payload_->statement,
                                                  target,
                                                  payload_->source ? &*payload_->source : nullptr,
                                                  {},
                                                  &ctx->parameters,
                                                  false,
                                                  resource());
                        if (generated.has_error()) {
                            set_error(generated.error());
                            co_return;
                        }
                        sql_ = std::move(generated.value());
                    }
                    sql = sql_;
                }
                auto future = payload_->target.server.backend->write(
                    resource(),
                    write_request_t{payload_->target.server.uid, std::move(sql), payload_->kind});
                auto answer = co_await std::move(future);
                if (answer.has_error()) {
                    set_error(answer.error());
                    co_return;
                }
                written_ += answer.value();
                mark_executed();
                co_return;
            }

        private:
            std::pmr::string explain_label_impl() const override {
                const char* verb = payload_->kind == write_kind_t::insert   ? "Foreign Insert on "
                                   : payload_->kind == write_kind_t::update ? "Foreign Update on "
                                                                            : "Foreign Delete on ";
                return std::pmr::string{(verb + payload_->target.name).c_str(), resource()};
            }

            std::pmr::vector<std::pmr::string> explain_details_impl() const override {
                std::pmr::vector<std::pmr::string> details{resource()};
                details.emplace_back(("Remote SQL: " + payload_->preview).c_str());
                return details;
            }

            boost::intrusive_ptr<const statement_payload_t> payload_;
            std::string sql_;
            bool executed_{false};
        };

        core::result_wrapper_t<boost::intrusive_ptr<operators::operator_t>>
        make_remote_statement(const services::context_storage_t& context,
                              const compute::function_registry_t& functions,
                              const logical_plan::node_extension_t& node) {
            OTX_ZONE_N("federation::make_remote_statement");
            boost::intrusive_ptr<const statement_payload_t> payload{
                static_cast<const statement_payload_t*>(node.payload())};
            std::string sql;
            if (planned_by_the_engine(payload->statement,
                                      payload->target,
                                      payload->source ? &*payload->source : nullptr,
                                      {},
                                      context,
                                      sql)) {
                return services::planner::create_plan(context,
                                                      functions,
                                                      payload->statement,
                                                      logical_plan::limit_t::unlimit(),
                                                      context.parameters);
            }
            return boost::intrusive_ptr<operators::operator_t>{
                new remote_statement_t(context.resource, context.log.clone(), std::move(payload), std::move(sql))};
        }

        // ── recognising what can be pushed ──────────────────────────────────────

        remote_storage_t* own_storage(const logical_plan::node_t& node) {
            const auto* table = node.table_metadata();
            if (table == nullptr || table->storage == nullptr || table->storage->owner() != storage_owner()) {
                return nullptr;
            }
            return static_cast<remote_storage_t*>(table->storage);
        }

        bool is_clause(logical_plan::node_type type) noexcept {
            using logical_plan::node_type;
            switch (type) {
                case node_type::match_t:
                case node_type::group_t:
                case node_type::having_t:
                case node_type::sort_t:
                case node_type::limit_t:
                case node_type::select_t:
                    return true;
                default:
                    return false;
            }
        }

        bool same_server(const remote_storage_t& a, const remote_storage_t& b) {
            return fold_identifier(a.table().server.uid) == fold_identifier(b.table().server.uid);
        }

        // A query block over one remote table: its aggregate and nothing under it but clauses.
        bool is_query_block(const logical_plan::node_t& node) {
            if (node.type() != logical_plan::node_type::aggregate_t || own_storage(node) == nullptr) {
                return false;
            }
            return std::all_of(node.children().begin(), node.children().end(), [](const logical_plan::node_ptr& c) {
                return c && is_clause(c->type());
            });
        }

        // The plain column of `storage`'s table at `index`.
        std::optional<plain_column_t> table_column(const remote_storage_t& storage, std::size_t index) {
            const auto& table = storage.table();
            if (index >= table.columns.size() || index >= table.remote_columns.size()) {
                return std::nullopt;
            }
            return plain_column_t{table.columns[index], column_origin_t{table.remote, table.remote_columns[index]}};
        }

        // Of a JOIN tree read by a block: per column it outputs, the discovered type of the table
        // column it is (nullopt past a derived table, whose columns are computed).
        void column_origins(const logical_plan::node_ptr& node, plain_columns_t& out) {
            if (node->type() == logical_plan::node_type::join_t) {
                column_origins(node->children()[0], out);
                column_origins(node->children()[1], out);
                return;
            }
            if (auto* storage = own_storage(*node); storage != nullptr && node->children().empty()) {
                for (std::size_t i = 0; i < storage->table().columns.size(); ++i) {
                    out.push_back(table_column(*storage, i));
                }
                return;
            }
            if (node->children().size() == 1 && node->children().front() &&
                node->children().front()->type() == logical_plan::node_type::aggregate_t &&
                own_storage(*node) == nullptr) {
                column_origins(node->children().front(), out);
                return;
            }
            out.insert(out.end(), node->output_types().size(), std::nullopt);
        }

        // Per column of the SELECT list the generator writes for `block`: the discovered type of the
        // table column it projects as is, or nothing for a computed one. Over one table (`table`) a
        // column reference names its column by name; over relations (`table` null: a JOIN tree or a
        // derived table) by its position in what the FROM outputs. Empty when the list cannot be
        // matched to `outputs` columns (then no column is checked).
        plain_columns_t
        plain_columns(const logical_plan::node_t& block, const remote_storage_t* table, std::size_t outputs) {
            using expressions::expression_group;
            using expressions::scalar_expression_t;
            using expressions::scalar_type;
            const logical_plan::node_ptr* from = nullptr;
            const logical_plan::node_t* group = nullptr;
            const logical_plan::node_t* select = nullptr;
            for (const auto& child : block.children()) {
                if (child && child->type() == logical_plan::node_type::group_t) {
                    group = child.get();
                } else if (child && child->type() == logical_plan::node_type::select_t) {
                    select = child.get();
                } else if (child && !is_clause(child->type())) {
                    from = &child;
                }
            }
            // Every column the block reads, in order.
            plain_columns_t origins;
            if (table != nullptr) {
                for (std::size_t i = 0; i < table->table().columns.size(); ++i) {
                    origins.push_back(table_column(*table, i));
                }
            } else if (from != nullptr) {
                column_origins(*from, origins);
            } else {
                return {};
            }
            const logical_plan::node_t* projection = group != nullptr ? group : select;
            auto is_star = [](const expressions::expression_ptr& expr) {
                return expr && expr->group() == expression_group::scalar &&
                       static_cast<const scalar_expression_t&>(*expr).type() == scalar_type::star_expand &&
                       expr->key().is_null();
            };
            const bool star_alone = table != nullptr && group == nullptr && projection != nullptr &&
                                    projection->expressions().size() == 1 && is_star(projection->expressions().front());
            if (projection == nullptr || star_alone) {
                return origins.size() == outputs ? origins : plain_columns_t{};
            }
            // The column a plain reference names.
            auto column_of = [&origins, table](const expressions::key_t& key) -> std::optional<plain_column_t> {
                if (table == nullptr) {
                    const auto& path = key.path();
                    return path.size() == 1 && path.front() < origins.size() ? origins[path.front()] : std::nullopt;
                }
                if (key.storage().size() != 1) {
                    return std::nullopt;
                }
                const std::string name{key.storage().front().data(), key.storage().front().size()};
                const auto& columns = table->table().columns;
                auto it = std::find_if(columns.begin(), columns.end(), [&name](const auto& column) {
                    return column.has_alias() && column.alias() == name;
                });
                return it == columns.end() ? std::nullopt : origins[static_cast<std::size_t>(it - columns.begin())];
            };
            plain_columns_t out;
            for (const auto& expr : projection->expressions()) {
                if (!expr) {
                    return {};
                }
                if (expr->group() == expression_group::scalar) {
                    const auto& scalar = static_cast<const scalar_expression_t&>(*expr);
                    if (scalar.type() == scalar_type::group_field) {
                        continue; // a grouping-key marker, never projected
                    }
                    if (scalar.type() == scalar_type::star_expand) {
                        return {};
                    }
                }
                if (expr->key().storage().size() == 1) {
                    const std::string_view name{expr->key().storage().front().data(),
                                                expr->key().storage().front().size()};
                    if (name.starts_with("__having_") || name.starts_with("__group_key_")) {
                        continue; // an engine-internal output, never projected
                    }
                }
                const bool plain = expr->group() == expression_group::scalar &&
                                   static_cast<const scalar_expression_t&>(*expr).type() == scalar_type::get_field &&
                                   static_cast<const scalar_expression_t&>(*expr).params().empty();
                out.push_back(plain ? column_of(expr->key()) : std::nullopt);
            }
            if (out.size() != outputs) {
                return {};
            }
            return out;
        }

        logical_plan::node_ptr push_select(std::pmr::memory_resource* resource, const logical_plan::node_ptr& node) {
            if (!is_query_block(*node) || node->children().empty() || !node->has_output_types()) {
                return nullptr;
            }
            auto* storage = own_storage(*node);
            auto target = target_of(*storage);
            auto preview = generate(node, target, nullptr, {}, nullptr, true, resource);
            if (preview.has_error()) {
                return nullptr; // not expressible: the engine runs it over the storage's scan
            }
            std::vector<types::complex_logical_type> columns(node->output_types().begin(), node->output_types().end());
            auto discovered = plain_columns(*node, storage, columns.size());
            std::pmr::vector<types::complex_logical_type> declared(node->output_types().begin(),
                                                                   node->output_types().end(),
                                                                   resource);
            auto name = target.name;
            auto extension = logical_plan::node_ptr{new logical_plan::node_extension_t(
                resource,
                name,
                std::move(declared),
                &make_remote_query,
                logical_plan::extension_payload_ptr{
                    new select_payload_t{std::move(target),
                                         {},
                                         node,
                                         std::move(columns),
                                         discovered,
                                         std::move(preview.value())}})};
            extension->set_result_alias(node->result_alias());
            // The replaced block's output, stamped as the validator stamped it: what a caller reading
            // the planned tree's output types (a plan-only describe) finds where the block was.
            extension->set_output_types(
                std::pmr::vector<types::complex_logical_type>(node->output_types().begin(), node->output_types().end(), resource));
            return extension;
        }

        // The tables a subtree reads when the generator can write it as one query of one server:
        // query blocks (an aggregate and its clauses), JOINs, and remote tables, every table a remote
        // table of `server` (fold of its uid; the first table sets it). False for anything else — a
        // local table, a node of another kind (VALUES, a function source, a set operation, a host
        // node), a table of another server — which leaves the subtree to the engine.
        bool collect_relations(const logical_plan::node_ptr& node,
                               std::vector<relation_target_t>& relations,
                               std::string& server) {
            if (!node) {
                return false;
            }
            switch (node->type()) {
                case logical_plan::node_type::join_t:
                    return node->children().size() == 2 && collect_relations(node->children()[0], relations, server) &&
                           collect_relations(node->children()[1], relations, server);
                case logical_plan::node_type::aggregate_t: {
                    if (auto* storage = own_storage(*node)) {
                        const auto uid = fold_identifier(storage->table().server.uid);
                        if (!server.empty() && server != uid) {
                            return false;
                        }
                        server = uid;
                        if (!std::all_of(node->children().begin(),
                                         node->children().end(),
                                         [](const logical_plan::node_ptr& c) { return c && is_clause(c->type()); })) {
                            return false;
                        }
                        relations.push_back(relation_target_t{node.get(), target_of(*storage)});
                        return true;
                    }
                    if (node->table_metadata() != nullptr) {
                        return false; // a local table (or another storage's)
                    }
                    std::size_t sources = 0;
                    for (const auto& child : node->children()) {
                        if (!child) {
                            return false;
                        }
                        if (is_clause(child->type())) {
                            continue;
                        }
                        if (++sources > 1 || !collect_relations(child, relations, server)) {
                            return false;
                        }
                    }
                    return sources == 1;
                }
                default:
                    return false;
            }
        }

        // A query block over a JOIN tree or a derived table, or a JOIN alone, every table of which
        // is a table of one server: one remote SELECT (generated over `relations`) when the dialect
        // can say the whole of it — its JOINs, their ON conditions, the aggregates, clauses and
        // windows above them, the derived tables under them.
        logical_plan::node_ptr push_relations(std::pmr::memory_resource* resource, const logical_plan::node_ptr& node) {
            if ((node->type() != logical_plan::node_type::aggregate_t &&
                 node->type() != logical_plan::node_type::join_t) ||
                !node->has_output_types() || own_storage(*node) != nullptr) {
                return nullptr;
            }
            std::vector<relation_target_t> relations;
            std::string server;
            if (!collect_relations(node, relations, server) || relations.empty()) {
                return nullptr;
            }
            auto target = relations.front().target;
            auto preview = generate(node, target, nullptr, relations, nullptr, true, resource);
            if (preview.has_error()) {
                return nullptr; // not expressible: the engine runs it over the subtree's own nodes
            }
            std::vector<types::complex_logical_type> columns(node->output_types().begin(), node->output_types().end());
            auto discovered = node->type() == logical_plan::node_type::aggregate_t
                                  ? plain_columns(*node, nullptr, columns.size())
                                  : plain_columns_t{};
            if (node->type() == logical_plan::node_type::join_t) {
                plain_columns_t origins;
                column_origins(node, origins);
                if (origins.size() == columns.size()) {
                    discovered = std::move(origins);
                }
            }
            std::pmr::vector<types::complex_logical_type> declared(node->output_types().begin(),
                                                                   node->output_types().end(),
                                                                   resource);
            std::string name;
            std::vector<std::string> named;
            for (const auto& relation : relations) {
                if (std::find(named.begin(), named.end(), relation.target.name) != named.end()) {
                    continue;
                }
                named.push_back(relation.target.name);
                name += (name.empty() ? "" : ", ") + relation.target.name;
            }
            auto extension = logical_plan::node_ptr{new logical_plan::node_extension_t(
                resource,
                name,
                std::move(declared),
                &make_remote_query,
                logical_plan::extension_payload_ptr{new select_payload_t{std::move(target),
                                                                         std::move(relations),
                                                                         node,
                                                                         std::move(columns),
                                                                         discovered,
                                                                         std::move(preview.value())}})};
            extension->set_result_alias(node->result_alias());
            extension->set_output_types(std::pmr::vector<types::complex_logical_type>(node->output_types().begin(),
                                                                                      node->output_types().end(),
                                                                                      resource));
            return extension;
        }

        logical_plan::node_ptr push_modify(std::pmr::memory_resource* resource, const logical_plan::node_ptr& node) {
            auto* storage = own_storage(*node);
            if (storage == nullptr) {
                return nullptr;
            }
            const bool is_update = node->type() == logical_plan::node_type::update_t;
            const auto& returning = is_update ? static_cast<const logical_plan::node_update_t&>(*node).returning()
                                              : static_cast<const logical_plan::node_delete_t&>(*node).returning();
            if (!returning.empty()) {
                return nullptr;
            }
            std::optional<remote_target_t> source;
            for (const auto& child : node->children()) {
                if (!child) {
                    return nullptr;
                }
                if (child->type() == logical_plan::node_type::match_t ||
                    child->type() == logical_plan::node_type::limit_t) {
                    continue;
                }
                // The FROM / USING source: one plain table of the same server.
                auto* source_storage = own_storage(*child);
                if (source || child->type() != logical_plan::node_type::aggregate_t || source_storage == nullptr ||
                    !child->children().empty() || !same_server(*storage, *source_storage)) {
                    return nullptr;
                }
                source = target_of(*source_storage);
            }
            auto target = target_of(*storage);
            auto preview = generate(node, target, source ? &*source : nullptr, {}, nullptr, true, resource);
            if (preview.has_error()) {
                return nullptr; // the engine's batch path: scan, then the storage's sink by row number
            }
            auto name = target.name;
            return logical_plan::node_ptr{new logical_plan::node_extension_t(
                resource,
                name,
                std::pmr::vector<types::complex_logical_type>{resource},
                &make_remote_statement,
                logical_plan::extension_payload_ptr{new statement_payload_t{is_update ? write_kind_t::update
                                                                                       : write_kind_t::delete_,
                                                                            std::move(target),
                                                                            std::move(source),
                                                                            node,
                                                                            std::move(preview.value())}})};
        }

        logical_plan::node_ptr push_insert(std::pmr::memory_resource* resource, const logical_plan::node_ptr& node) {
            auto* storage = own_storage(*node);
            if (storage == nullptr) {
                return nullptr;
            }
            const auto& insert = static_cast<const logical_plan::node_insert_t&>(*node);
            // The columns the statement wrote, for the sink if the statement stays with the engine.
            if (!insert.key_translation().empty()) {
                std::vector<std::size_t> positions;
                const auto& columns = storage->table().columns;
                for (const auto& key : insert.key_translation()) {
                    if (key.storage().size() != 1) {
                        positions.clear();
                        break;
                    }
                    const std::string name{key.storage().front().data(), key.storage().front().size()};
                    auto it = std::find_if(columns.begin(), columns.end(), [&name](const auto& column) {
                        return column.has_alias() && column.alias() == name;
                    });
                    if (it == columns.end()) {
                        positions.clear();
                        break;
                    }
                    positions.push_back(static_cast<std::size_t>(it - columns.begin()));
                }
                storage->set_insert_columns(std::move(positions));
            }
            if (!insert.returning().empty() || node->children().size() != 1 || !node->children().front()) {
                return nullptr;
            }
            const auto& child = node->children().front();
            std::optional<remote_target_t> source;
            if (child->type() == logical_plan::node_type::aggregate_t) {
                auto* source_storage = own_storage(*child);
                if (!is_query_block(*child) || source_storage == nullptr || !same_server(*storage, *source_storage)) {
                    return nullptr;
                }
                source = target_of(*source_storage);
            } else if (child->type() != logical_plan::node_type::data_t) {
                return nullptr;
            }
            auto target = target_of(*storage);
            auto preview = generate(node, target, source ? &*source : nullptr, {}, nullptr, true, resource);
            if (preview.has_error()) {
                return nullptr; // the engine reads the source and hands the rows to the insert sink
            }
            auto name = target.name;
            return logical_plan::node_ptr{new logical_plan::node_extension_t(
                resource,
                name,
                std::pmr::vector<types::complex_logical_type>{resource},
                &make_remote_statement,
                logical_plan::extension_payload_ptr{new statement_payload_t{write_kind_t::insert,
                                                                            std::move(target),
                                                                            std::move(source),
                                                                            node,
                                                                            std::move(preview.value())}})};
        }

        logical_plan::node_ptr rewrite(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
            if (!node) {
                return node;
            }
            switch (node->type()) {
                case logical_plan::node_type::insert_t:
                    if (auto pushed = push_insert(resource, node)) {
                        return pushed;
                    }
                    break;
                case logical_plan::node_type::update_t:
                case logical_plan::node_type::delete_t:
                    if (auto pushed = push_modify(resource, node)) {
                        return pushed;
                    }
                    break;
                default:
                    break;
            }
            // Top down: the largest subtree one server can run whole goes first; what it cannot say
            // falls apart into its children, each tried the same way.
            if (node->type() == logical_plan::node_type::aggregate_t) {
                if (auto pushed = push_select(resource, node)) {
                    return pushed;
                }
            }
            if (auto pushed = push_relations(resource, node)) {
                return pushed;
            }
            for (auto& child : node->children()) {
                child = rewrite(resource, child);
            }
            return node;
        }


    } // namespace

    logical_plan::node_ptr pushdown(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        OTX_ZONE_N("federation::rules::pushdown");
        return rewrite(resource, std::move(node));
    }

} // namespace otterstax::federation::rules
