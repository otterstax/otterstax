// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/name_resolution.hpp"

#include <otterbrix/otterbrix.hpp>

#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/vector/data_chunk.hpp>

#include <algorithm>
#include <memory_resource>
#include <vector>

// One external slot: pointer to the plan-node reference the backend manager
// swaps for fetched data, together with the parser-resolved full table name
// (oid stamped later by CatalogManager).
struct external_entry_t {
    components::logical_plan::node_ptr* node;
    otterstax::names::resolved_target_t target;
};

struct OtterbrixStatement {
    // External slots (outer = batch index, inner = slot within the batch).
    // Populated by the parser; construction sites must pass a
    // resource-constructed vector explicitly.
    std::pmr::vector<std::pmr::vector<external_entry_t>> external_nodes;
    components::logical_plan::parameter_node_ptr params_node;
    components::logical_plan::node_ptr node;
    size_t external_nodes_count;
    const size_t parameters_count;

    // Affected-row count of a DML that ran ENTIRELY on a backend, or
    // `no_remote_dml`.
    //
    // It cannot travel the normal way. The backend reports the count out of
    // band (boost.mysql's OK packet, libpq's command tag), and the input
    // translators encode it the way the engine encodes a local DML result — a
    // column-less chunk whose cardinality IS the count. But the engine's
    // pipeline pump treats a zero-COLUMN batch as its drain sentinel
    // (services/collection/executor.cpp: `if (batch.data.empty()) break;`), so
    // that carrier is read as "source exhausted" and the count is dropped.
    //
    // It also cannot be recovered later: the backend manager replaces the DML
    // node with a `data_t`, after which a fully-remote DML and a fully-remote
    // SELECT have byte-identical plans. So the count is captured at the moment
    // of substitution, while the node type still says INSERT/UPDATE/DELETE, and
    // routed around the engine by OtterbrixManager::execute.
    static constexpr size_t no_remote_dml = static_cast<size_t>(-1);
    size_t remote_affected_rows{no_remote_dml};

    // The catalog lookups this statement depends on, as the parser's transformer
    // registered them.
    //
    // They do not live in the plan tree: the transformer collects them into
    // `execution_plan_t::catalog_resolves`, a SIBLING of the root node, so a plan
    // rebuilt from (node, params) — which is what every execution site here does,
    // because a backend manager may have replaced the root — arrives carrying
    // none. The engine puts back only what it can read off the node names
    // (namespace / table / type targets, enrich_logical_plan.cpp's
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
    // Sharing them is safe only because a prepared entry is single-use: Worker
    // erases it on execute, so no second run ever sees slots another run stamped.
    // A multi-use prepared entry would have to clone these per execution.
    components::logical_plan::catalog_resolves_t catalog_resolves;
};

// Record the affected-row count of a fully-remote DML at the one moment it is
// still knowable: `node` is the plan slot a backend manager is about to replace
// with `fetched`. All three backend managers call this at their substitution
// site, so the rule lives in one place.
//
// Only a statement-level INSERT/UPDATE/DELETE qualifies. A remote SELECT
// feeding a local DML also passes through here, but with an aggregate node —
// its row count is data, not an affected-row count, and must not be captured.
inline void capture_remote_dml_count(OtterbrixStatement& statement,
                                     const components::logical_plan::node_ptr& node,
                                     const components::vector::data_chunk_t& fetched) {
    using components::logical_plan::node_type;
    if (!node || fetched.column_count() != 0) {
        return;
    }
    switch (node->type()) {
        case node_type::insert_t:
        case node_type::update_t:
        case node_type::delete_t:
            statement.remote_affected_rows = fetched.size();
            break;
        default:
            break;
    }
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
// tests/system/test_scheduler.cpp pin.
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
// no-RETURNING DML (its count travels column-less), a backend slice inlined as
// raw data. The effective root is located as in dml_without_returning.
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

// The affected-row carrier of a no-RETURNING DML in the shape every frontend
// reads it: column-less chunks whose cardinalities sum to `affected`, each at
// most DEFAULT_VECTOR_CAPACITY rows — a data_chunk_t never exceeds that capacity.
// An affected count of zero is one empty chunk, so front() stays defined.
inline std::pmr::vector<components::vector::data_chunk_t>
make_affected_count_carrier(std::pmr::memory_resource* resource, size_t affected) {
    using components::vector::data_chunk_t;
    using components::vector::DEFAULT_VECTOR_CAPACITY;
    std::pmr::vector<data_chunk_t> carrier(resource);
    carrier.reserve(std::max<size_t>(1, (affected + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY));
    const std::pmr::vector<components::types::complex_logical_type> no_columns(resource);
    size_t remaining = affected;
    do {
        const size_t window = std::min<size_t>(DEFAULT_VECTOR_CAPACITY, remaining);
        data_chunk_t chunk(resource, no_columns, window);
        chunk.set_cardinality(window);
        carrier.emplace_back(std::move(chunk));
        remaining -= window;
    } while (remaining > 0);
    return carrier;
}

using OtterbrixSchemaParams = std::pmr::vector<std::pair<database_name_t, collection_name_t>>;

using OtterbrixStatementPtr = std::unique_ptr<OtterbrixStatement>;
