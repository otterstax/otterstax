// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "name_resolution.hpp"

#include <components/base/collection_full_name.hpp>

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

    struct subquery_stub_t {
        std::string stub_id;
        std::string source_uid;
        std::string raw_sql;

        std::vector<qualifier_rewrite_t> qualifiers;
    };

    struct extraction_result_t {
        std::string modified_sql;
        std::vector<subquery_stub_t> stubs;
    };

    extraction_result_t
    prepare_sql(std::string_view sql, std::pmr::memory_resource* arena, ::Node** out_root_if_unmodified = nullptr);

    void promote_three_part_qualifiers(::Node* root);

    // Walks the (already-promoted) raw AST rooted at `root` and registers the
    // fully-qualified name (uid.catalogname.schemaname.relname) of EVERY
    // RangeVar — uid-qualified AND local — plus DROP TABLE / DROP INDEX name
    // lists, so the transformed logical plan's (dbname, relname) pairs can be
    // resolved back to full names.
    void collect_qualified_names(::Node* root, otterstax::names::name_registry_t& out);

    constexpr std::string_view k_stub_prefix = "__otterstax_subq_";

} // namespace otterstax::parser
