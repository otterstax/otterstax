// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// storage_offer: what of a block otterbrix offers the backend can run as one statement of its
// dialect. Decided by generating the statement with every parameter written `?`
// (sql_gen::generation_options_t::placeholders); a take keeps the block and generates the statement
// again with the statement's values when it runs (storage_run_block / storage_run_write).
//
//  - an UPDATE / DELETE (always the block itself): one statement, or the offer is refused
//    (unimplemented_yet): OtterStax numbers no remote row, so otterbrix's own path (the backend's
//    scan, then storage_update / storage_delete_rows by row number) cannot run it. RETURNING is
//    PostgreSQL's alone;
//  - top down, a relation (a SELECT block, a JOIN) the dialect says whole: taken, its answer the
//    node's columns; else, for a SELECT block of one table, its WHERE: the conjuncts the dialect
//    says are sent with a read of the columns the block reads, the others stay otterbrix's
//    (residual); else its children are tried.

#include "backend_actor.hpp"

#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_update.hpp>

#include <algorithm>
#include <deque>

namespace otterstax::federation {

    namespace disk = services::disk;
    using namespace components::logical_plan;
    using components::types::complex_logical_type;

    namespace {

        void preorder(const node_t& node, std::vector<const node_t*>& order, std::vector<std::size_t>& sizes) {
            const auto at = order.size();
            order.push_back(&node);
            sizes.push_back(1);
            for (const auto& child : node.children()) {
                if (child) {
                    preorder(*child, order, sizes);
                }
            }
            sizes[at] = order.size() - at;
        }

        bool is_clause(node_type type) noexcept {
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

        // A SELECT block of one table: the aggregate naming the table, nothing under it but clauses.
        bool table_block(const node_t& node) {
            return node.type() == node_type::aggregate_t && node.table_oid() != components::catalog::INVALID_OID &&
                   std::all_of(node.children().begin(), node.children().end(), [](const node_ptr& child) {
                       return child && is_clause(child->type());
                   });
        }

        // The conjuncts of a WHERE, as otterbrix counts them for a residual: an AND's operands, or
        // the one condition.
        std::vector<components::expressions::expression_ptr>
        conjuncts_of(const components::expressions::expression_ptr& where) {
            using components::expressions::compare_expression_t;
            using components::expressions::compare_type;
            using components::expressions::expression_group;
            if (where && where->group() == expression_group::compare) {
                const auto& compare = static_cast<const compare_expression_t&>(*where);
                if (compare.type() == compare_type::union_and) {
                    return {compare.children().begin(), compare.children().end()};
                }
            }
            return {where};
        }

        // The tables of a statement as the generator names them, kept while it writes.
        struct spelled_t {
            std::deque<sql_gen::column_map_t> maps;
            std::deque<std::vector<std::string>> columns;
            std::vector<sql_gen::relation_t> relations;
        };

    } // namespace

    core::result_wrapper_t<std::string>
    backend_actor_t::take_sql(const take_t& take,
                              const components::vector::storage_parameters* parameters,
                              bool placeholders,
                              bool returning) const {
        const auto& block = *blocks_.at(take.block);
        const auto kind = connection_.kind;
        const sql_gen::generation_options_t options{.placeholders = placeholders, .validated = true, .returning = returning};
        spelled_t spelled;
        // The detached table `oid` in the backend's spelling: its name, its columns in the block's
        // order (a column the actor has no spelling of keeps otterbrix's name).
        auto spell = [&](oid_t oid) -> std::optional<qualified_name_t> {
            const auto* detached = block.table(oid);
            auto registered = table_locked(oid);
            if (detached == nullptr || !registered) {
                return std::nullopt;
            }
            auto& map = spelled.maps.emplace_back();
            auto& columns = spelled.columns.emplace_back();
            for (const auto& column : detached->columns) {
                std::string remote{column.name};
                for (const auto& known : registered->columns) {
                    if (known.attoid == column.attoid) {
                        remote = known.remote;
                    }
                }
                map.emplace(std::string{column.name}, sql_gen::remote_column_t{remote, column.type});
                columns.push_back(std::move(remote));
            }
            return generator_target(*registered);
        };
        auto missing = [&](oid_t oid) {
            return core::error_t{core::error_code_t::undefined_object,
                                 std::pmr::string{("the backend of OtterStax has no table of oid " + std::to_string(oid)).c_str(),
                                                  resource_}};
        };

        if (take.table_read) {
            auto name = spell(take.table);
            if (!name) {
                return missing(take.table);
            }
            const sql_gen::statement_target_t target{*name, {}, &spelled.maps.back(), nullptr, spelled.columns.back(), {}};
            return sql_gen::generate_table_read(kind, target, take.columns, take.where, parameters, -1, resource_, options);
        }
        const auto& node = *take.node;
        if (node.type() == node_type::update_t || node.type() == node_type::delete_t) {
            auto name = spell(node.table_oid());
            if (!name) {
                return missing(node.table_oid());
            }
            const auto* map = &spelled.maps.back();
            const std::span<const std::string> all{spelled.columns.back()};
            qualified_name_t from;
            const sql_gen::column_map_t* from_map = nullptr;
            for (const auto& child : node.children()) {
                if (child && child->type() == node_type::aggregate_t &&
                    child->table_oid() != components::catalog::INVALID_OID) {
                    auto source = spell(child->table_oid());
                    if (!source) {
                        return missing(child->table_oid());
                    }
                    from = *source;
                    from_map = &spelled.maps.back();
                }
            }
            const sql_gen::statement_target_t target{*name, from, map, from_map, all, {}};
            return sql_gen::generate_query(take.node, parameters, kind, target, resource_, options);
        }
        if (table_block(node)) {
            auto name = spell(node.table_oid());
            if (!name) {
                return missing(node.table_oid());
            }
            const sql_gen::statement_target_t target{*name, {}, &spelled.maps.back(), nullptr, spelled.columns.back(), {}};
            return sql_gen::generate_query(take.node, parameters, kind, target, resource_, options);
        }
        // A JOIN tree or a derived table: every table read under the node, found by its node.
        std::vector<const node_t*> order;
        std::vector<std::size_t> sizes;
        preorder(node, order, sizes);
        for (const auto* leaf : order) {
            if (leaf->type() != node_type::aggregate_t || leaf->table_oid() == components::catalog::INVALID_OID) {
                continue;
            }
            auto name = spell(leaf->table_oid());
            if (!name) {
                return missing(leaf->table_oid());
            }
            spelled.relations.push_back(sql_gen::relation_t{leaf, *name, &spelled.maps.back(), spelled.columns.back()});
        }
        const sql_gen::statement_target_t target{{}, {}, nullptr, nullptr, {}, spelled.relations};
        return sql_gen::generate_query(take.node, parameters, kind, target, resource_, options);
    }

    core::result_wrapper_t<std::pmr::vector<disk::block_take_t>>
    backend_actor_t::offer_block(const execution_context_t& ctx,
                                 std::unique_ptr<detached_block_t> block,
                                 const components::vector::storage_parameters& parameters) {
        OTX_ZONE_N("backend_actor::offer_block");
        std::pmr::vector<disk::block_take_t> takes{resource_};
        if (!block->root || block->tables.empty()) {
            return takes;
        }
        if (!owns(block->tables.front().name)) {
            return takes;
        }
        const auto& target = connection_;
        const auto block_id = next_block_id_++;
        blocks_.emplace(block_id, std::move(block));
        const auto& root = *blocks_.at(block_id)->root;

        auto add = [&](uint32_t preorder, take_t take, const std::string& preview, std::pmr::vector<uint32_t> residual) {
            const auto request = "take " + std::to_string(next_take_id_++);
            disk::block_take_t answer{preorder,
                                      std::pmr::string{request, resource_},
                                      std::pmr::vector<std::pmr::string>{resource_},
                                      std::move(residual)};
            answer.explain.emplace_back(("Remote SQL: " + preview).c_str());
            takes.push_back(std::move(answer));
            takes_.emplace(request, std::move(take));
        };
        auto refuse = [&](const std::string& why) {
            blocks_.erase(block_id);
            return core::error_t{core::error_code_t::unimplemented_yet, std::pmr::string{why.c_str(), resource_}};
        };

        if (root.type() == node_type::update_t || root.type() == node_type::delete_t) {
            const bool update = root.type() == node_type::update_t;
            const auto& returning = update ? static_cast<const node_update_t&>(root).returning()
                                           : static_cast<const node_delete_t&>(root).returning();
            const auto* table = blocks_.at(block_id)->table(root.table_oid());
            const std::string name = table ? table->name.to_string() : std::to_string(root.table_oid());
            const std::string verb = update ? "UPDATE" : "DELETE";
            if (target.kind == backend_type_t::ClickHouse && ctx.txn.command_id != 0) {
                return refuse(verb + " of \"" + name + "\": connection \"" + target.name +
                              "\" is ClickHouse, which has no transactions: a write inside a transaction block is "
                              "not supported");
            }
            if (!returning.empty() && target.kind != backend_type_t::PostgreSQL) {
                return refuse(verb + " ... RETURNING of \"" + name + "\" is not supported: only PostgreSQL answers "
                                     "the rows it wrote");
            }
            take_t take;
            take.block = block_id;
            take.node = blocks_.at(block_id)->root;
            take.write = true;
            auto preview = take_sql(take, &parameters, true, !returning.empty());
            if (preview.has_error()) {
                // OtterStax numbers no remote row: otterbrix's own UPDATE / DELETE cannot run here.
                return refuse(verb + " of \"" + name + "\" cannot run as one statement of connection \"" +
                              target.name + "\": " + std::string{preview.error().what});
            }
            add(0, std::move(take), preview.value(), std::pmr::vector<uint32_t>{resource_});
            return takes;
        }

        std::vector<const node_t*> order;
        std::vector<std::size_t> sizes;
        preorder(root, order, sizes);
        for (std::size_t i = 0; i < order.size();) {
            const auto* node = order[i];
            const bool relation = node->type() == node_type::aggregate_t || node->type() == node_type::join_t;
            if (!relation || !node->has_output_types()) {
                ++i;
                continue;
            }
            take_t whole;
            whole.block = block_id;
            whole.node = node_ptr{const_cast<node_t*>(node)};
            whole.types.assign(node->output_types().begin(), node->output_types().end());
            if (auto preview = take_sql(whole, &parameters, true, false); !preview.has_error()) {
                add(static_cast<uint32_t>(i), std::move(whole), preview.value(), std::pmr::vector<uint32_t>{resource_});
                i += sizes[i];
                continue;
            }
            if (table_block(*node)) {
                // Its WHERE: what the dialect says goes with the read, the rest stays otterbrix's.
                const auto* table = blocks_.at(block_id)->table(node->table_oid());
                const auto& aggregate = static_cast<const node_aggregate_t&>(*node);
                const node_t* where = nullptr;
                for (const auto& child : node->children()) {
                    if (child->type() == node_type::match_t && !child->expressions().empty()) {
                        where = child.get();
                    }
                }
                if (where != nullptr && table != nullptr) {
                    take_t read;
                    read.block = block_id;
                    read.node = whole.node;
                    read.table_read = true;
                    read.table = node->table_oid();
                    read.columns.assign(aggregate.projected_cols().begin(), aggregate.projected_cols().end());
                    std::pmr::vector<uint32_t> residual{resource_};
                    const auto conjuncts = conjuncts_of(where->expressions().front());
                    for (std::size_t c = 0; c < conjuncts.size(); ++c) {
                        read.where = {conjuncts[c]};
                        if (take_sql(read, &parameters, true, false).has_error()) {
                            residual.push_back(static_cast<uint32_t>(c));
                        }
                    }
                    if (residual.size() < conjuncts.size()) {
                        read.where.clear();
                        for (std::size_t c = 0; c < conjuncts.size(); ++c) {
                            if (std::find(residual.begin(), residual.end(), c) == residual.end()) {
                                read.where.push_back(conjuncts[c]);
                            }
                        }
                        auto positions = read.columns;
                        if (positions.empty()) {
                            for (std::size_t c = 0; c < table->columns.size(); ++c) {
                                positions.push_back(c);
                            }
                        }
                        for (auto position : positions) {
                            if (position < table->columns.size()) {
                                auto type = table->columns[position].type;
                                type.set_alias(std::string{table->columns[position].name});
                                read.types.push_back(std::move(type));
                            }
                        }
                        auto preview = take_sql(read, &parameters, true, false);
                        if (!preview.has_error()) {
                            const auto at = std::find(order.begin() + static_cast<std::ptrdiff_t>(i), order.end(), where);
                            add(static_cast<uint32_t>(at - order.begin()), std::move(read), preview.value(), std::move(residual));
                            i += sizes[i];
                            continue;
                        }
                    }
                }
            }
            ++i;
        }
        if (takes.empty()) {
            blocks_.erase(block_id);
        }
        return takes;
    }

} // namespace otterstax::federation
