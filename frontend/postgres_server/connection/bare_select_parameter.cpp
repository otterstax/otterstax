// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "bare_select_parameter.hpp"

#include "deallocate_statement.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/sql/parser/parser.h>

namespace frontend::postgres {
    std::optional<int> bare_select_parameter(std::pmr::memory_resource* resource, std::string_view sql) {
        OTX_ZONE_N("pg::bare_select_parameter");
        if (!keyword_is(first_keyword(sql), "select")) {
            return std::nullopt;
        }
        std::pmr::monotonic_buffer_resource arena(resource);
        const auto* select = reinterpret_cast<const ::SelectStmt*>(only_statement(arena, sql, T_SelectStmt));
        if (select == nullptr || select->targetList == nullptr) {
            return std::nullopt;
        }
        // The first bare parameter in the raw tree's select list: a list of
        // ResTarget, the analyzed TargetEntry comes later — the scan order is
        // the select-list order PostgreSQL's analysis names.
        for (const auto& cell : select->targetList->lst) {
            const auto* target = static_cast<const ::ResTarget*>(cell.data);
            if (target->val == nullptr) {
                continue;
            }
            const auto* expr = reinterpret_cast<const ::Node*>(target->val);
            if (nodeTag(expr) == T_ParamRef) {
                return reinterpret_cast<const ::ParamRef*>(expr)->number;
            }
        }
        return std::nullopt;
    }
} // namespace frontend::postgres
