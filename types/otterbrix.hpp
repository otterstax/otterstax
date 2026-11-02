// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/vector/data_chunk.hpp>

#include <algorithm>
#include <memory>
#include <memory_resource>
#include <utility>
#include <vector>

// One parsed statement as the engine runs it: the engine's own
// execution_plan_t, as the transformer's finalize() answers it — the root (the
// last of `sub_queries`), the sub-queries the transformer flattened out of it
// ahead of the root, the mapping that binds each one's result into the
// parameter the root reads, the catalog lookups the transformer registered, and
// the parameter node. Every name in the plan is the name the client wrote.
using ExecutionPlanPtr = std::unique_ptr<components::logical_plan::execution_plan_t>;

// The statement of `plan`: the last of its sub-queries (the ones before it run
// first). nullptr for a plan without one.
inline const components::logical_plan::node_ptr& plan_root(const components::logical_plan::execution_plan_t& plan) {
    static const components::logical_plan::node_ptr none{};
    return plan.sub_queries.empty() ? none : plan.sub_queries.back();
}

// The statement node of the plan rooted at `root`: the root itself, or — for a
// sequence_t that wraps its consumer behind leading catalog_resolve children —
// the last non-resolve child. A sequence without a leading resolve child (a
// planner-style rewrite) and a sequence of resolves only are their own
// statement. nullptr only for a null root.
//
// The parser hands over a bare consumer: catalog lookups live outside the plan
// tree, in execution_plan_t::catalog_resolves, so nothing builds a resolve
// sequence any more. The sequence branch is the defensive read of a root that
// arrives wrapped all the same, and it is what the sequence cases of
// tests/system/test_session_pipeline.cpp pin.
inline components::logical_plan::node_t* plan_statement_node(components::logical_plan::node_t* root) {
    using components::logical_plan::node_type;
    if (!root || root->type() != node_type::sequence_t) {
        return root;
    }
    auto& children = root->children();
    if (children.empty() || !children.front() || children.front()->type() != node_type::catalog_resolve_t) {
        return root;
    }
    for (auto it = children.rbegin(); it != children.rend(); ++it) {
        if (*it && (*it)->type() != node_type::catalog_resolve_t) {
            return it->get();
        }
    }
    return root;
}

// True when the statement rooted at `root` is an INSERT/UPDATE/DELETE with no
// RETURNING clause, i.e. one that reports only "N rows affected".
//
// The DML node is the plan's statement node (plan_statement_node): either the
// root itself or the consumer child of a resolve-wrapping sequence. The
// predicate is returning(), stamped by the transformer at parse time;
// has_output_types() cannot serve here because the validator never stamps DML
// nodes, with or without RETURNING.
inline bool dml_without_returning(const components::logical_plan::node_ptr& root) {
    using namespace components::logical_plan;
    const node_t* statement = plan_statement_node(root.get());
    if (!statement) {
        return false;
    }
    switch (statement->type()) {
        case node_type::delete_t:
            return static_cast<const node_delete_t*>(statement)->returning().empty();
        case node_type::update_t:
            return static_cast<const node_update_t*>(statement)->returning().empty();
        case node_type::insert_t:
            return static_cast<const node_insert_t*>(statement)->returning().empty();
        default:
            return false;
    }
}

// The kind of a statement that answers ROWS — "SELECT" for a query (an
// aggregate or set-operation root), the DML spelling for an INSERT/UPDATE/DELETE
// with a RETURNING clause — or nullptr for one that answers no rows: DDL, a
// no-RETURNING DML (its count travels on the cursor). The effective root is
// located as in dml_without_returning.
//
// Every frontend tells a resultset from an affected count by the payload's
// column count alone, so a row-producing statement whose result has no columns
// would be reported to the client as a DML statement; the name is what the
// refusal of such a result carries.
inline const char* row_producing_statement(const components::logical_plan::node_ptr& root) {
    using namespace components::logical_plan;
    const node_t* statement = plan_statement_node(root.get());
    if (!statement) {
        return nullptr;
    }
    switch (statement->type()) {
        case node_type::aggregate_t:
        case node_type::union_t:
        case node_type::intersect_t:
            return "SELECT";
        case node_type::delete_t:
            return static_cast<const node_delete_t*>(statement)->returning().empty() ? nullptr
                                                                                       : "DELETE ... RETURNING";
        case node_type::update_t:
            return static_cast<const node_update_t*>(statement)->returning().empty() ? nullptr
                                                                                       : "UPDATE ... RETURNING";
        case node_type::insert_t:
            return static_cast<const node_insert_t*>(statement)->returning().empty() ? nullptr
                                                                                       : "INSERT ... RETURNING";
        default:
            return nullptr;
    }
}

