// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/qualified_name.hpp"

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

// One parsed statement as the engine runs it: the plan root, its parameter
// node, and what travels beside the root in the engine's execution_plan_t.
// Every name in the plan is the name the client wrote; a remote table's name
// (`<alias>.<db>.<table>`, `<alias>.<db>.<schema>.<table>`) is resolved by the
// engine through the federation's name-resolution hooks
// (otterbrix/federation/name_resolution.hpp), never by OtterStax ahead of it.
struct OtterbrixStatement {
    components::logical_plan::parameter_node_ptr params_node;
    components::logical_plan::node_ptr node;
    const size_t parameters_count;

    // The catalog lookups this statement depends on, as the parser's transformer
    // registered them.
    //
    // They do not live in the plan tree: the transformer collects them into
    // `execution_plan_t::catalog_resolves`, a SIBLING of the root node, so a plan
    // rebuilt from (node, params) — which is what every execution site here does
    // — arrives carrying none. The engine puts back only what it can read off
    // the node names (namespace / table / type targets, enrich_logical_plan.cpp's
    // register_plan_targets). The CONSTRAINT entries name no node and are gone
    // with the rest: the CHECK expressions and the parent tables an
    // INSERT/UPDATE/DELETE is validated against, leaving every constraint
    // silently unenforced and an FK's parent reading as "referenced relation does
    // not exist". Hence they ride the statement instead.
    //
    // Carried IN only. The slots are shared with the plan handed to the engine,
    // which stamps the resolved metadata into them while it runs, so nothing here
    // is ever read back — what a statement needs to know comes out of the reply.
    //
    // Sharing them is safe only because a prepared entry is single-use: the
    // query session erases it on execute, so no second run ever sees slots
    // another run stamped. A statement run a second time (the rediscovery retry)
    // is parsed again.
    components::logical_plan::catalog_resolves_t catalog_resolves;

    // The sub-queries the transformer flattened out of the statement — an
    // `IN (SELECT ...)`, `EXISTS (SELECT ...)` or scalar `(SELECT ...)` —
    // in the order the engine runs them, all AHEAD of `node`, together with the
    // mapping that binds each one's compacted result into the parameter `node`
    // reads it from (`sub_query_results[i]` belongs to `sub_queries[i]`).
    //
    // Like `catalog_resolves` they are siblings of the root in the engine's
    // `execution_plan_t`, so a plan rebuilt from (node, params) carries none:
    // the engine would run `node` against a result parameter nobody filled and
    // answer wrong rows without an error. Every execution site therefore puts
    // them back (otterbrix/operators/engine_requests.cpp, statement_plan). A
    // sub-query may read a remote table like any other: the engine resolves it.
    //
    // Describing a parameterized statement walks `node` only
    // (session::plan::plan_dependencies); a scalar sub-query in its SELECT list
    // is described as the engine's describe answers it.
    std::pmr::vector<components::logical_plan::node_ptr> sub_queries;
    std::pmr::vector<components::logical_plan::id_result_mapping> sub_query_results;
};

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

using OtterbrixStatementPtr = std::unique_ptr<OtterbrixStatement>;
