// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

// The engine's qualified_name_t lost its defaulted ordering; OtterStax keys
// std::map's by it (the schema dependency maps), so the ordering it had —
// lexicographic over (unique_identifier, database, schema, collection) — is
// restored here. Declared in the global namespace, qualified_name_t's own, so
// std::less finds it by ADL.
inline bool operator<(const qualified_name_t& lhs, const qualified_name_t& rhs) {
    return std::tie(lhs.unique_identifier.t, lhs.database.t, lhs.schema.t, lhs.collection.t) <
           std::tie(rhs.unique_identifier.t, rhs.database.t, rhs.schema.t, rhs.collection.t);
}

namespace otterstax::names {

    // A table as OtterStax messages name it: `<database>.<collection>`, or
    // `NonCollectionData` for an empty name — the spelling of the engine's
    // qualified_name_t::to_string() before it began to spell every slot
    // (uid.db.schema.rel). The messages keep the spelling they had.
    inline std::string display_name(const qualified_name_t& name) {
        if (name.empty()) {
            return "NonCollectionData";
        }
        return name.database.t + "." + name.collection.t;
    }

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

        // The target of a CREATE TABLE exactly as the raw AST wrote it. The
        // transformer stamps `public` on the create node of an unqualified
        // `CREATE TABLE t` (its default_public namespace policy), so the node
        // alone cannot tell `t` from `public.t`; node_names() resolves the
        // create node by the database recorded here instead. Set once per
        // statement by collect_qualified_names; nullptr when none was recorded.
        void set_create_target(qualified_name_t written);
        const qualified_name_t* create_target() const;

    private:
        std::pmr::string make_key_(std::string_view dbname, std::string_view relname) const;

        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<std::pmr::string, qualified_name_t> entries_;
        std::pmr::unordered_set<std::pmr::string> collisions_;
        std::optional<qualified_name_t> create_target_;
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
    // database as written — read off name_registry_t::create_target(), never
    // off the `public` the transformer stamps on an unqualified name — so an
    // unqualified `CREATE TABLE t` resolves to the local `("", t)` entry even
    // when a backend database named `public` registered `("public", t)`.
    // Errors: database-level DDL (always local, must not be resolved) and
    // node_type::unused (otterstax schema_node_t — resolved by the caller).
    core::result_wrapper_t<qualified_name_t> node_names(const components::logical_plan::node_t& node,
                                                        const name_registry_t& reg);

} // namespace otterstax::names
