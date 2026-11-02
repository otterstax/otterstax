// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "session/plan_inspection.hpp"

#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"
#include "otterbrix/parser/grammar_extention/external_node.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_set_setting.hpp>
#include <components/logical_plan/node_transaction.hpp>

#include <algorithm>
#include <deque>
#include <string_view>

namespace otterstax::session::plan {

    using namespace components;

    namespace {

        core::error_t make_error(std::pmr::memory_resource* resource, core::error_code_t code, std::string_view what) {
            return core::error_t{code, std::pmr::string{what, resource}};
        }

    } // namespace

    extension_root_t extension_root(const ParsedQueryData& data) {
        extension_root_t root;
        auto* node = plan_root(*data.plan).get();
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
        if (!data.plan || !plan_root(*data.plan) ||
            plan_root(*data.plan)->type() != logical_plan::node_type::transaction_t) {
            return transaction_command_t::none;
        }
        switch (static_cast<const logical_plan::node_transaction_t&>(*plan_root(*data.plan)).op()) {
            case logical_plan::transaction_op::begin:
                return transaction_command_t::begin;
            case logical_plan::transaction_op::commit:
                return transaction_command_t::commit;
            case logical_plan::transaction_op::abort:
                return transaction_command_t::rollback;
            case logical_plan::transaction_op::savepoint:
                return transaction_command_t::savepoint;
            case logical_plan::transaction_op::release_savepoint:
                return transaction_command_t::release;
            case logical_plan::transaction_op::rollback_to_savepoint:
                return transaction_command_t::rollback_to;
        }
        return transaction_command_t::none;
    }

    // Both DDL statements keep the name on the DDL node itself — catalog
    // resolves live outside the plan tree. The sequence unwrap stays as the
    // defensive read of a wrapped root.
    core::result_wrapper_t<std::pmr::string> database_ddl_target(std::pmr::memory_resource* resource,
                                                                 const ParsedQueryData& data) {
        const logical_plan::node_t* root = plan_statement_node(plan_root(*data.plan).get());
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

    std::optional<write_target_t> write_target(const ParsedQueryData& data) {
        if (!data.plan || !plan_root(*data.plan)) {
            return std::nullopt;
        }
        const logical_plan::node_t* statement = plan_statement_node(plan_root(*data.plan).get());
        if (statement == nullptr) {
            return std::nullopt;
        }
        switch (statement->type()) {
            case logical_plan::node_type::drop_t: {
                const auto kind = static_cast<const logical_plan::node_drop_t&>(*statement).kind();
                if (kind == logical_plan::drop_target_kind::database) {
                    return std::nullopt;
                }
                return write_target_t{statement->target(), kind == logical_plan::drop_target_kind::schema};
            }
            case logical_plan::node_type::insert_t:
            case logical_plan::node_type::insert_on_conflict_t:
            case logical_plan::node_type::update_t:
            case logical_plan::node_type::delete_t:
            case logical_plan::node_type::create_collection_t:
            case logical_plan::node_type::create_schema_t:
            case logical_plan::node_type::create_index_t:
            case logical_plan::node_type::create_type_t:
            case logical_plan::node_type::create_sequence_t:
            case logical_plan::node_type::create_view_t:
            case logical_plan::node_type::create_matview_t:
            case logical_plan::node_type::refresh_matview_t:
            case logical_plan::node_type::create_macro_t:
            case logical_plan::node_type::alter_table_t:
            case logical_plan::node_type::create_constraint_t:
                return write_target_t{statement->target(), false};
            default:
                return std::nullopt;
        }
    }

    std::vector<session_payload::setting_change_t> setting_changes(std::pmr::memory_resource* resource,
                                                                   const ParsedQueryData& data) {
        OTX_ZONE_N("plan::setting_changes");
        std::vector<session_payload::setting_change_t> changes;
        if (!data.plan || !plan_root(*data.plan) ||
            plan_root(*data.plan)->type() != logical_plan::node_type::set_setting_t) {
            return changes;
        }
        const auto& set = static_cast<const logical_plan::node_set_setting_t&>(*plan_root(*data.plan));
        if (set.scope() == catalog::setting_scope_t::system) {
            return changes;
        }
        const bool local = set.scope() == catalog::setting_scope_t::local;
        for (const auto& assignment : set.assignments()) {
            if (assignment.is_default) {
                changes.push_back({assignment.id, std::nullopt, local});
                continue;
            }
            auto canonical = catalog::canonical_setting_value(assignment.id, assignment.value, resource);
            if (canonical.has_error()) {
                continue; // the engine refuses it: the statement fails
            }
            changes.push_back({assignment.id, std::string{canonical.value()}, local});
        }
        return changes;
    }

    std::optional<std::string> savepoint_name(const ParsedQueryData& data) {
        switch (transaction_command(data)) {
            case transaction_command_t::savepoint:
            case transaction_command_t::release:
            case transaction_command_t::rollback_to:
                return std::string{
                    static_cast<const logical_plan::node_transaction_t&>(*plan_root(*data.plan)).savepoint_name()};
            default:
                return std::nullopt;
        }
    }

    bool is_query(const ParsedQueryData& data) {
        if (!data.plan || data.plan->explain != logical_plan::explain_type::none) {
            return false;
        }
        const logical_plan::node_t* statement = plan_statement_node(plan_root(*data.plan).get());
        if (statement == nullptr) {
            return false;
        }
        switch (statement->type()) {
            case logical_plan::node_type::aggregate_t:
            case logical_plan::node_type::data_t: // a VALUES list
            case logical_plan::node_type::union_t:
            case logical_plan::node_type::intersect_t:
                return true;
            default:
                return false;
        }
    }

    std::pmr::vector<qualified_name_t> statement_names(std::pmr::memory_resource* resource,
                                                       const components::logical_plan::execution_plan_t& statement) {
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
        // The statement first, then the sub-queries that run ahead of it.
        collect(plan_root(statement));
        for (std::size_t i = 0; i + 1 < statement.sub_queries.size(); ++i) {
            collect(statement.sub_queries[i]);
        }
        return names;
    }

} // namespace otterstax::session::plan
