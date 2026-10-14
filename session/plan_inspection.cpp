// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "session/plan_inspection.hpp"

#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "otterbrix/parser/grammar_extention/external_node.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_transaction.hpp>

#include <algorithm>
#include <deque>
#include <string_view>
#include <utility>
#include <variant>

namespace otterstax::session::plan {

    using namespace components;

    namespace {

        core::error_t make_error(std::pmr::memory_resource* resource, core::error_code_t code, std::string_view what) {
            return core::error_t{code, std::pmr::string{what, resource}};
        }

        // An UPDATE / DELETE whose plan carries a source to join against; its
        // match and limit children are the statement's own, not a source.
        bool joins_a_source(const logical_plan::node_t& statement) {
            for (const auto& child : statement.children()) {
                const auto type = child->type();
                if (type != logical_plan::node_type::match_t && type != logical_plan::node_type::limit_t) {
                    return true;
                }
            }
            return false;
        }

        // Whether a RETURNING expression is a column of the answer at all — the
        // same set the planner projects (projects_column,
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
        // expression's key — the alias where it was written with one — and for
        // a bare column reference the column it reads.
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

        // The column a RETURNING expression READS, which is what types it;
        // empty for an expression that reads none (a call, a literal, a value
        // the binder still holds). An output alias renames the column but does
        // not retype it, so the name looked up here is the operand's.
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

        // Adds the aggregates under `root` to `dependencies`, breadth first.
        void collect_dependencies(dependencies_t& dependencies, const logical_plan::node_ptr& root) {
            if (!root) {
                return;
            }
            std::deque<logical_plan::node_ptr> nodes_traverse;
            nodes_traverse.emplace_back(root);
            while (!nodes_traverse.empty()) {
                auto& n = nodes_traverse.front();
                if (n->type() == logical_plan::node_type::aggregate_t) {
                    const size_t index = dependencies.size();
                    dependencies.emplace(n->target(), index);
                }
                for (auto& child : n->children()) {
                    nodes_traverse.emplace_back(child);
                }
                nodes_traverse.pop_front();
            }
        }

    } // namespace

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

    transaction_command_t transaction_command(const ParsedQueryData& data) {
        if (!data.otterbrix_params || !data.otterbrix_params->node ||
            data.otterbrix_params->node->type() != logical_plan::node_type::transaction_t) {
            return transaction_command_t::none;
        }
        switch (static_cast<const logical_plan::node_transaction_t&>(*data.otterbrix_params->node).op()) {
            case logical_plan::transaction_op::begin:
                return transaction_command_t::begin;
            case logical_plan::transaction_op::commit:
                return transaction_command_t::commit;
            case logical_plan::transaction_op::abort:
                return transaction_command_t::rollback;
        }
        return transaction_command_t::none;
    }

    // Both DDL statements keep the name on the DDL node itself — catalog
    // resolves live outside the plan tree. The sequence unwrap stays as the
    // defensive read of a wrapped root.
    core::result_wrapper_t<std::pmr::string> database_ddl_target(std::pmr::memory_resource* resource,
                                                                 const ParsedQueryData& data) {
        const logical_plan::node_t* root = plan_statement_node(data.otterbrix_params->node.get());
        if (root == nullptr) {
            return std::pmr::string{resource};
        }
        if (root->type() == logical_plan::node_type::create_database_t) {
            const auto& create = static_cast<const logical_plan::node_create_database_t&>(*root);
            return std::pmr::string{std::string_view{create.target().database.t}, resource};
        }
        if (root->type() != logical_plan::node_type::drop_t) {
            return std::pmr::string{resource};
        }
        const auto& drop = static_cast<const logical_plan::node_drop_t&>(*root);
        if (drop.kind() != logical_plan::drop_target_kind::database) {
            return std::pmr::string{resource};
        }
        if (drop.target().database.t.empty()) {
            return make_error(resource, core::error_code_t::invalid_parameter, "DROP DATABASE names no database");
        }
        return std::pmr::string{std::string_view{drop.target().database.t}, resource};
    }

    std::pmr::vector<qualified_name_t> statement_names(std::pmr::memory_resource* resource,
                                                       const OtterbrixStatement& statement) {
        OTX_ZONE_N("session::statement_names");
        std::pmr::vector<qualified_name_t> names(resource);
        auto collect = [&names](const logical_plan::node_ptr& root) {
            if (!root) {
                return;
            }
            std::deque<const logical_plan::node_t*> pending{root.get()};
            while (!pending.empty()) {
                const auto* node = pending.front();
                pending.pop_front();
                const auto& target = node->target();
                if (!target.collection.t.empty() && std::find(names.begin(), names.end(), target) == names.end()) {
                    names.push_back(target);
                }
                for (const auto& child : node->children()) {
                    if (child) {
                        pending.push_back(child.get());
                    }
                }
            }
        };
        collect(statement.node);
        for (const auto& sub_query : statement.sub_queries) {
            collect(sub_query);
        }
        return names;
    }

    table_ddl_t table_ddl(const ParsedQueryData& data) {
        const logical_plan::node_t* root = plan_statement_node(data.otterbrix_params->node.get());
        if (root == nullptr) {
            return {};
        }
        // The written name when the parser kept it (it holds the uid and schema
        // slots the node's target lost), the node's target otherwise.
        auto written = [&data](const qualified_name_t& node_target) {
            return data.ddl_target.collection.t.empty() ? node_target : data.ddl_target;
        };
        switch (root->type()) {
            case logical_plan::node_type::create_collection_t:
                return {root, written(root->target()), {}, table_ddl_t::kind_t::create_table};
            case logical_plan::node_type::create_index_t:
                return {root, written(root->target()), {}, table_ddl_t::kind_t::index};
            case logical_plan::node_type::drop_t: {
                const auto& drop = static_cast<const logical_plan::node_drop_t&>(*root);
                if (drop.kind() == logical_plan::drop_target_kind::collection) {
                    return {root, written(drop.target()), {}, table_ddl_t::kind_t::drop_table};
                }
                if (drop.kind() == logical_plan::drop_target_kind::index) {
                    return {root, written(drop.target()), drop.index_name().t, table_ddl_t::kind_t::index};
                }
                return {};
            }
            default:
                return {};
        }
    }

    dependencies_t plan_dependencies(std::pmr::memory_resource* resource, const OtterbrixStatement& statement) {
        OTX_ZONE_N("session::plan_dependencies");
        dependencies_t dependencies(resource);
        collect_dependencies(dependencies, statement.node);
        return dependencies;
    }

    // RETURNING makes an INSERT / UPDATE / DELETE answer ROWS, and which
    // relation resolves the names is the engine validator's rule
    // (resolve_returning_columns, services/dispatcher/validate_logical_plan.cpp):
    // an INSERT resolves its RETURNING against the target table ALONE, so
    // INSERT ... SELECT is described from the target just like INSERT ... VALUES.
    // An UPDATE ... FROM / DELETE ... USING puts the SOURCE's columns in scope as
    // well, and which side a bare name belongs to is not decidable from the plan
    // here, so that one shape is left undescribed rather than described wrongly.
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
                return {&insert.returning(), &insert.target(), true};
            }
            case logical_plan::node_type::update_t: {
                const auto& update = static_cast<const logical_plan::node_update_t&>(*statement);
                if (update.returning().empty()) {
                    return {};
                }
                if (joins_a_source(update)) {
                    return {nullptr, nullptr, true};
                }
                return {&update.returning(), &update.target(), true};
            }
            case logical_plan::node_type::delete_t: {
                const auto& remove = static_cast<const logical_plan::node_delete_t&>(*statement);
                if (remove.returning().empty()) {
                    return {};
                }
                if (joins_a_source(remove)) {
                    return {nullptr, nullptr, true};
                }
                return {&remove.returning(), &remove.target(), true};
            }
            default:
                return {};
        }
    }

    // `*` is the relation's own columns in its own order — what the engine
    // expands it to — and a named column carries that column's type. What this
    // cannot name it leaves NA, exactly as the SELECT projection does: the kernel
    // of a function call is not consulted here and a value the binder still holds
    // has no type before Bind; the frontend's check on the executed result is
    // what catches the difference. No statement that returns rows is ever
    // described as a row of zero columns.
    types::complex_logical_type returning_schema(std::pmr::memory_resource* resource,
                                                 const std::pmr::vector<expressions::expression_ptr>& returning,
                                                 const std::pmr::vector<types::complex_logical_type>& columns) {
        OTX_ZONE_N("session::returning_schema");
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

} // namespace otterstax::session::plan
