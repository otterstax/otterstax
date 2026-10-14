// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/parser.hpp"
#include "session/transaction_state.hpp"
#include "types/otterbrix.hpp"

#include <components/base/collection_full_name.hpp>

#include <components/expressions/expression.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <map>
#include <memory_resource>
#include <string>

namespace otterstax::external {
    class external_node_t;
}
namespace otterstax::kafka {
    class kafka_node_t;
}

// What a query session reads off a parsed statement before it routes or
// describes it. Pure functions over the plan: no actor, no I/O.
namespace otterstax::session::plan {

    // Root carriers produced by the grammar extensions. Both are
    // node_type::unused (the engine never executes them), so the plan root's
    // type cannot tell them apart; the parser records the claiming extension on
    // ParsedQueryData::extension_kind and that is the only discriminator the
    // split needs.
    struct extension_root_t {
        otterstax::external::external_node_t* external{nullptr};
        otterstax::kafka::kafka_node_t* kafka{nullptr};
    };

    extension_root_t extension_root(const ParsedQueryData& data);

    // The transaction statement (BEGIN / COMMIT / ROLLBACK) the plan root is;
    // `none` for every other statement.
    transaction_command_t transaction_command(const ParsedQueryData& data);

    // The database a CREATE DATABASE / DROP DATABASE statement names; empty
    // for every other statement. A DROP DATABASE root without one names nothing
    // and is refused (invalid_parameter) rather than passed on.
    core::result_wrapper_t<std::pmr::string> database_ddl_target(std::pmr::memory_resource* resource,
                                                                 const ParsedQueryData& data);

    using dependencies_t = std::pmr::map<qualified_name_t, size_t>;

    // Every table name the statement writes — its root and its sub-queries,
    // in plan order, each once, every slot as written (a remote table keeps its
    // uid and schema): what a rediscovery looks among for a remote table.
    std::pmr::vector<qualified_name_t> statement_names(std::pmr::memory_resource* resource,
                                                       const OtterbrixStatement& statement);

    // A table-level DDL statement — CREATE TABLE, CREATE INDEX, DROP TABLE,
    // DROP INDEX — and the name it targets as written (ParsedQueryData::ddl_target,
    // every slot, when the parser kept it; the node's target otherwise); empty
    // `node` for every other statement (CREATE / DROP DATABASE are
    // database_ddl_target's).
    struct table_ddl_t {
        const components::logical_plan::node_t* node{nullptr};
        qualified_name_t target;
        // DROP INDEX: the index; empty otherwise.
        std::string index;
        // CREATE TABLE / DROP TABLE change which tables exist; an index
        // statement does not.
        enum class kind_t
        {
            create_table,
            drop_table,
            index,
        } kind{kind_t::index};
    };
    table_ddl_t table_ddl(const ParsedQueryData& data);

    // The relations a statement reads, as the dependency map the federated
    // schema computation resolves its aggregates against: one entry per
    // DISTINCT aggregate key, numbered in insertion order, because the engine
    // answers a vector of that many probes and compute_otterbrix_schema indexes
    // into it by these numbers. Two aggregates over one relation share a key
    // and so share a slot — the number is the map's own size, never a node
    // counter, which for a self-join would run past the vector the map sizes.
    //
    // The statement's ROOT only: the describe of a statement probes the
    // relations its result is computed from. Its sub-queries are siblings of the
    // root, and a relation only a sub-query reads costs a probe the statement's
    // own columns do not need — and a probe that fails would fail the prepare.
    dependencies_t plan_dependencies(std::pmr::memory_resource* resource, const OtterbrixStatement& statement);

    // The RETURNING list of a DML statement and the relation whose columns
    // resolve it, or an empty answer for every other statement (and for an
    // UPDATE ... FROM / DELETE ... USING, whose RETURNING may name the source).
    struct returning_clause_t {
        const std::pmr::vector<components::expressions::expression_ptr>* list{nullptr};
        // The target relation as the statement wrote it (every slot: a remote
        // target keeps its uid and schema).
        const qualified_name_t* target{nullptr};
        // Whether the statement carries a RETURNING list at all (an UPDATE ...
        // FROM / DELETE ... USING included, which has no `list` here).
        bool present{false};
    };

    returning_clause_t dml_returning(const components::logical_plan::node_ptr& root);

    // The columns a RETURNING list projects, typed from the columns the target
    // relation's probe answered; NA for a list that projects no column.
    components::types::complex_logical_type
    returning_schema(std::pmr::memory_resource* resource,
                     const std::pmr::vector<components::expressions::expression_ptr>& returning,
                     const std::pmr::vector<components::types::complex_logical_type>& columns);

} // namespace otterstax::session::plan
