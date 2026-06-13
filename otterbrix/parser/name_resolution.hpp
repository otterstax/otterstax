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
    // The key rule MUST mirror the transformer's rangevar_to_qualified_name
    // folding:
    //   key_db = name.database non-empty ? name.database : name.schema
    //   key    = key_db + '\0' + name.collection
    // so whatever (dbname, relname) the transformer stamped on a node looks
    // up the full name registered from the raw AST.
    class name_registry_t {
    public:
        explicit name_registry_t(std::pmr::memory_resource* resource);

        // Registers a full name under its folded key. If two DIFFERENT full
        // names fold onto the same key, the FIRST entry is kept and the key
        // is recorded as collided; find() on a collided key returns nullptr.
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

    // One resolved table target of an external node. `from_name` stays empty
    // unless the statement references a secondary table
    // (UPDATE ... FROM / DELETE ... USING).
    struct resolved_target_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        qualified_name_t name;
        qualified_name_t from_name;
    };

    // Resolves a bare (dbname, relname) pair through `reg`, producing exactly
    // the same errors node_names() does (ambiguous_name on a key collision,
    // table_not_exists on a miss). Used by the parser for the secondary table
    // of UPDATE ... FROM / DELETE ... USING, where the caller reads the second
    // node_catalog_resolve_table_t sibling itself.
    core::result_wrapper_t<qualified_name_t> resolve_table_name(std::pmr::memory_resource* resource,
                                                                const name_registry_t& reg,
                                                                std::string_view dbname,
                                                                std::string_view relname);

    // Extracts the (dbname, relname) pair from a logical-plan node (per-type
    // switch mirroring the engine's enrich pass), then resolves it through
    // `reg` to the original full qualified name (preserving uid + schema).
    //
    // DML and table-level DDL nodes carry no names; their target table lives
    // on the FIRST node_catalog_resolve_table_t sibling inside the wrapping
    // node_sequence_t — pass that sequence as `seq_ctx`. create_collection_t
    // carries relname itself and its database name on a
    // node_catalog_resolve_namespace_t sibling (absent for unqualified
    // `CREATE TABLE t` — then db is empty / local). Errors: seq_ctx == nullptr
    // where a resolve sibling is required, database-level DDL (always local,
    // must not be resolved), and node_type::unused (otterstax schema_node_t —
    // resolved by the caller).
    core::result_wrapper_t<qualified_name_t> node_names(const components::logical_plan::node_t& node,
                                                        const name_registry_t& reg,
                                                        const components::logical_plan::node_t* seq_ctx);

} // namespace otterstax::names
