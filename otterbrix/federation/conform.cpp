// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "conform.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/execution_context/graph_execution_context.hpp>

#include <algorithm>
#include <string>

namespace otterstax::federation {

    core::error_t conform_to_result(std::pmr::memory_resource* resource,
                                    const components::casts::cast_registry_t& casts,
                                    std::string_view statement,
                                    std::span<const components::types::complex_logical_type> declared,
                                    core::date::timezone_offset_t timezone,
                                    components::vector::data_chunk_t& chunk) {
        OTX_ZONE_N("federation::conform_to_result");
        if (chunk.column_count() != declared.size()) {
            return core::error_t(core::error_code_t::datatype_mismatch,
                                 std::pmr::string{("the remote statement " + std::string{statement} + " answered " +
                                                   std::to_string(chunk.column_count()) + " columns where " +
                                                   std::to_string(declared.size()) +
                                                   " are expected: the remote table changed since it was registered")
                                                      .c_str(),
                                                  resource});
        }
        const auto count = chunk.size();
        const components::graph_execution_context context{timezone};
        for (std::size_t i = 0; i < declared.size(); ++i) {
            auto& column = chunk.data[i];
            const auto alias = declared[i].has_alias() ? declared[i].alias() : std::string{};
            if (column.type() == declared[i]) {
                if (!alias.empty()) {
                    column.set_type_alias(alias);
                }
                continue;
            }
            auto name_of = [&] {
                return "column " + std::to_string(i + 1) + " (" + alias + ") of the remote statement " +
                       std::string{statement};
            };
            auto cast = casts.resolve(column.type(), declared[i], components::casts::cast_type::explicit_only);
            if (!cast) {
                return core::error_t(
                    core::error_code_t::datatype_mismatch,
                    std::pmr::string{(name_of() + ": the backend's type has no cast to the declared one").c_str(),
                                     resource});
            }
            components::vector::vector_t converted(chunk.resource(), declared[i], std::max<std::uint64_t>(count, 1));
            if (auto error = (*cast)(components::casts::cast_kind::cast, column, &converted, context, count);
                error.contains_error()) {
                return core::error_t(error.type,
                                     std::pmr::string{(name_of() + ": " + std::string{error.what}).c_str(), resource});
            }
            if (!alias.empty()) {
                converted.set_type_alias(alias);
            }
            column = std::move(converted);
        }
        return core::error_t::no_error();
    }

} // namespace otterstax::federation
