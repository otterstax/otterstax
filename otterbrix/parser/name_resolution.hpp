// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace otterstax::names {

    // Registry mapping the (dbname, relname) pair carried on transformed
    // logical-plan nodes back to the original fully-qualified table name
    // (uid.db.schema.rel) collected from the raw AST pre-pass, where
    // RangeVar still carries uid/catalogname/schemaname/relname.
    //
    // The key rule MUST agree with the dbname the transformer stamps on nodes
    // (rangevar_to_qualified_name: the RangeVar's catalogname, not folded):
    //   key = name.database + '\0' + name.collection
    // The grammar puts the database of every qualified name into catalogname
    // and fills schemaname only together with it, so whatever (dbname, relname)
    // the transformer stamped on a node looks up the full name registered from
    // the raw AST.
    class name_registry_t {
    public:
        explicit name_registry_t(std::pmr::memory_resource* resource);

        // Registers a full name under its (database, collection) key. If two
        // DIFFERENT full names fall onto the same key, the FIRST entry is kept
        // and the key is recorded as collided; find() on a collided key returns
        // nullptr.
        void add(qualified_name_t name);

        // Returns nullptr on a miss OR when the key is ambiguous (collided).
        // The caller turns nullptr into a resolution error; there are NO
        // fallback lookups. Use collided() to distinguish ambiguity from a
        // plain miss when wording the error.
        const qualified_name_t* find(std::string_view dbname, std::string_view relname) const;

        // True when two different full names folded onto this key.
        bool collided(std::string_view dbname, std::string_view relname) const;

    private:
        std::pmr::string make_key_(std::string_view dbname, std::string_view relname) const;

        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<std::pmr::string, qualified_name_t> entries_;
        std::pmr::unordered_set<std::pmr::string> collisions_;
    };

    // One resolved table target of an external node. `from_name` is the second
    // name a statement targets: the index of a DROP INDEX, or — as the
    // generator reads it — the secondary table of UPDATE ... FROM /
    // DELETE ... USING. Empty otherwise.
    struct resolved_target_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        qualified_name_t name;
        qualified_name_t from_name;
    };

    // Resolves a bare (dbname, relname) pair through `reg`, producing exactly
    // the same errors node_names() does (ambiguous_name on a key collision,
    // table_not_exists on a miss). Used by the parser for the index of a
    // DROP INDEX, which the node names apart from its table.
    core::result_wrapper_t<qualified_name_t> resolve_table_name(std::pmr::memory_resource* resource,
                                                                const name_registry_t& reg,
                                                                std::string_view dbname,
                                                                std::string_view relname);

    // Extracts the (dbname, relname) pair a logical-plan node names (per-type
    // switch mirroring the engine's enrich pass), then resolves it through
    // `reg` to the original full qualified name (preserving uid + schema).
    //
    // Every node names its own target: DML and table-level DDL nodes their
    // target table (a DROP INDEX its indexed table), create_collection_t its
    // database as written (empty for an unqualified `CREATE TABLE t` — then
    // the local entry). Errors: database-level DDL (always local, must not be
    // resolved) and node_type::unused (otterstax schema_node_t — resolved by
    // the caller).
    core::result_wrapper_t<qualified_name_t> node_names(const components::logical_plan::node_t& node,
                                                        const name_registry_t& reg);

} // namespace otterstax::names
