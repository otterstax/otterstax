// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_types.hpp>

#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

struct Node;

namespace otterstax::parser {
    struct qualifier_rewrite_t {
        int start;
        int length;
        collection_full_name_t name;
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

    constexpr std::string_view k_stub_prefix = "__otterstax_subq_";

} // namespace otterstax::parser
