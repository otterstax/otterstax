// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace otterstax::catalog {

    // qualified_name_t only defaults ==/<=> — the engine provides no hash for
    // it, so supply one locally.
    struct collection_name_hash {
        inline std::size_t operator()(const qualified_name_t& key) const {
            return std::hash<std::string>()(key.unique_identifier) ^ std::hash<std::string>()(key.database) ^
                   std::hash<std::string>()(key.schema) ^ std::hash<std::string>()(key.collection);
        }
    };

    // Actor-confined registry of external table schemas, keyed by the engine
    // pg_class OID assigned at registration time. Single writer (CatalogManager
    // actor); intentionally has NO internal locking.
    //
    // The store is the OtterStax-side mirror of what was registered in the
    // Otterbrix engine catalog: full qualified name (uid.db.schema.rel) plus
    // the discovered STRUCT column schema.
    class schema_store_t {
    public:
        explicit schema_store_t(std::pmr::memory_resource* resource);

        // Registers a (oid, name, schema) triple. A duplicate oid OR a
        // duplicate full qualified name is an error — never overwrites.
        core::error_t put(components::catalog::oid_t oid,
                          qualified_name_t name,
                          components::types::complex_logical_type struct_schema);

        // Exact full-qualified-name lookup. Returns INVALID_OID on a miss.
        components::catalog::oid_t find(const qualified_name_t& name) const;

        // nullptr on miss.
        const components::types::complex_logical_type* schema_by_oid(components::catalog::oid_t oid) const;

        // All oids whose name.unique_identifier equals `uid`.
        std::pmr::vector<components::catalog::oid_t> oids_by_uid(std::string_view uid) const;

        void erase(components::catalog::oid_t oid);

        // fn(const qualified_name_t&, components::catalog::oid_t,
        //    const components::types::complex_logical_type&)
        template<typename Fn>
        void for_each(Fn&& fn) const {
            for (const auto& [oid, entry] : by_oid_) {
                fn(entry.name, oid, entry.schema);
            }
        }

    private:
        struct entry_t {
            qualified_name_t name;
            components::types::complex_logical_type schema;
        };

        std::pmr::unordered_map<components::catalog::oid_t, entry_t> by_oid_;
        std::pmr::unordered_map<qualified_name_t, components::catalog::oid_t, collection_name_hash> by_name_;
        std::pmr::memory_resource* resource_;
    };

} // namespace otterstax::catalog
