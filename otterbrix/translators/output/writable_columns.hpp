// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/translators/error.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory_resource>
#include <string>
#include <string_view>

namespace tsl {

    // The file writers (csv / parquet / ndjson) serialise flat rows: every column must be a scalar
    // the target format can represent and must carry a name, because the name is the CSV header,
    // the parquet field and the JSON key. Nested columns (STRUCT / LIST / ARRAY) and unnamed
    // columns are rejected up front; a nested column written as NULLs or a column written under an
    // empty name would silently corrupt the export.
    //
    // The gate reads the LOGICAL type, not the physical one alone, because INT128 is shared by
    // three unrelated meanings: HUGEINT (a 128-bit integer), a DECIMAL whose width needs 128-bit
    // storage, and UUID. The first two are carried — each by its own reading, HUGEINT as an
    // integer and a DECIMAL as decimal128(width, scale) — and UUID, which a writer handed under
    // HUGEINT's reading would emit as a wrong number, is refused at that width.
    //
    // A DECIMAL of a narrower width passes through its physical type below (INT16 / INT32 /
    // INT64), as any admitted scalar does; it is the WRITERS that must key on the logical type,
    // since the physical one says nothing about the scale.
    inline bool is_writable_scalar(const components::types::complex_logical_type& type) {
        using components::types::logical_type;
        using components::types::physical_type;
        switch (type.to_physical_type()) {
            case physical_type::BOOL:
            case physical_type::INT8:
            case physical_type::INT16:
            case physical_type::INT32:
            case physical_type::INT64:
            case physical_type::UINT8:
            case physical_type::UINT16:
            case physical_type::UINT32:
            case physical_type::UINT64:
            case physical_type::FLOAT:
            case physical_type::DOUBLE:
            case physical_type::STRING:
            case physical_type::NA:
                return true;
            case physical_type::INT128:
                return type.type() == logical_type::HUGEINT || type.type() == logical_type::DECIMAL;
            default:
                return false;
        }
    }

    inline core::result_wrapper_t<bool> validate_writable_columns(std::pmr::memory_resource* res,
                                                                  const components::vector::data_chunk_t& chunk,
                                                                  std::string_view scope) {
        const auto types = chunk.types();
        for (size_t c = 0; c < types.size(); ++c) {
            if (!types[c].has_alias()) {
                std::pmr::string message{res};
                message.append(scope.data(), scope.size());
                message.append(": column ");
                message.append(std::to_string(c).c_str());
                message.append(" has no name");
                return core::error_t(core::error_code_t::schema_error, std::move(message));
            }
            if (!is_writable_scalar(types[c])) {
                std::pmr::string message{res};
                message.append(scope.data(), scope.size());
                message.append(": unsupported column type for '");
                message.append(types[c].alias().c_str());
                message.append("' (physical type ");
                message.append(std::to_string(static_cast<unsigned>(types[c].to_physical_type())).c_str());
                message.append(")");
                return core::error_t(core::error_code_t::conversion_failure, std::move(message));
            }
        }
        return true;
    }

} // namespace tsl
