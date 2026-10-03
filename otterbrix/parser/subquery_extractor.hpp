// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "alias_registry.hpp"
#include "name_resolution.hpp"

#include <components/base/collection_full_name.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

struct Node;

namespace otterstax::parser {
    struct qualifier_rewrite_t {
        int start;
        int length;
        qualified_name_t name;
    };

    // Every string and container is allocated from the resource prepare_sql
    // was given. A copy must name its resource explicitly: a pmr container
    // copy-constructed without one takes the process default resource.
    struct subquery_stub_t {
        std::pmr::string stub_id;
        std::pmr::string source_uid;
        std::pmr::string raw_sql;

        std::pmr::vector<qualifier_rewrite_t> qualifiers;
    };

    struct extraction_result_t {
        std::pmr::string modified_sql;
        std::pmr::vector<subquery_stub_t> stubs;
    };

    // `arena` receives the raw parse tree (*out_root_if_unmodified points into
    // it); `resource` owns the returned extraction, which never points into
    // the arena. The tree is canonicalized (canonicalize_names) before the
    // sub-queries are located, so a sub-query's backend and its rewritten
    // qualifiers come from the canonical names; a name the aliases refuse is
    // that refusal.
    [[nodiscard]] core::result_wrapper_t<extraction_result_t>
    prepare_sql(std::string_view sql,
                const otterstax::names::alias_registry_t& aliases,
                std::pmr::memory_resource* arena,
                std::pmr::memory_resource* resource,
                ::Node** out_root_if_unmodified = nullptr);

    // Reads every table reference of the raw tree against the connection aliases and writes a federated reading back
    [[nodiscard]] core::error_t canonicalize_names(std::pmr::memory_resource* arena,
                                                   std::pmr::memory_resource* resource,
                                                   const otterstax::names::alias_registry_t& aliases,
                                                   ::Node* root);

    // Walks the (already-canonicalized) raw AST rooted at `root` and registers
    // the fully-qualified name (uid.catalogname.schemaname.relname) of EVERY
    // RangeVar — uid-qualified AND local — plus DROP TABLE / DROP INDEX name
    // lists, so the transformed logical plan's (dbname, relname) pairs can be
    // resolved back to full names. A DROP name is not a RangeVar and is read
    // against the aliases here; its refusal is the error returned. `resource`
    // backs the walk's scratch storage and the refusal.
    [[nodiscard]] core::error_t collect_qualified_names(std::pmr::memory_resource* resource,
                                                        const otterstax::names::alias_registry_t& aliases,
                                                        ::Node* root,
                                                        otterstax::names::name_registry_t& out);

    constexpr std::string_view k_stub_prefix = "__otterstax_subq_";

} // namespace otterstax::parser
