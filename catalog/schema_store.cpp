// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "schema_store.hpp"

#include <cassert>

namespace otterstax::catalog {

    schema_store_t::schema_store_t(std::pmr::memory_resource* resource)
        : by_oid_(resource)
        , by_name_(resource)
        , resource_(resource) {
        assert(resource != nullptr);
    }

    core::error_t schema_store_t::put(components::catalog::oid_t oid,
                                      qualified_name_t name,
                                      components::types::complex_logical_type struct_schema) {
        if (oid == components::catalog::INVALID_OID) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{"schema_store: cannot register INVALID_OID", resource_});
        }
        if (by_oid_.contains(oid)) {
            return core::error_t(
                core::error_code_t::already_exists,
                std::pmr::string{("schema_store: oid already registered: " + std::to_string(oid)).c_str(), resource_});
        }
        if (by_name_.contains(name)) {
            return core::error_t(
                core::error_code_t::already_exists,
                std::pmr::string{("schema_store: name already registered: " + name.to_string()).c_str(), resource_});
        }
        by_name_.emplace(name, oid);
        by_oid_.emplace(oid, entry_t{std::move(name), std::move(struct_schema)});
        return core::error_t::no_error();
    }

    components::catalog::oid_t schema_store_t::find(const qualified_name_t& name) const {
        auto it = by_name_.find(name);
        return it == by_name_.end() ? components::catalog::INVALID_OID : it->second;
    }

    const components::types::complex_logical_type* schema_store_t::schema_by_oid(components::catalog::oid_t oid) const {
        auto it = by_oid_.find(oid);
        return it == by_oid_.end() ? nullptr : &it->second.schema;
    }

    std::pmr::vector<components::catalog::oid_t> schema_store_t::oids_by_uid(std::string_view uid) const {
        std::pmr::vector<components::catalog::oid_t> oids(resource_);
        for (const auto& [oid, entry] : by_oid_) {
            if (entry.name.unique_identifier == uid) {
                oids.push_back(oid);
            }
        }
        return oids;
    }

    void schema_store_t::erase(components::catalog::oid_t oid) {
        auto it = by_oid_.find(oid);
        if (it == by_oid_.end()) {
            return;
        }
        by_name_.erase(it->second.name);
        by_oid_.erase(it);
    }

} // namespace otterstax::catalog
