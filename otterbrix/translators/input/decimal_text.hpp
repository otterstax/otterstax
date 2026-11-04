// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>

// The exact numeric a backend declares — MySQL / MariaDB DECIMAL(p, s), PostgreSQL NUMERIC(p, s),
// ClickHouse Decimal(P, S) — as the engine's DECIMAL(p, s). The decimal text those backends print
// for a value of it is read with the engine's own parse
// (components::casts::kernels::detail::parse_decimal, <components/casts/kernels/string_cast.hpp>).
//
// The engine's DECIMAL holds at most DECIMAL_MAX_WIDTH (38) digits, scale 0 <= s <= p (an int128
// at most: components/types/types.hpp, is_valid_decimal_spec). A declaration outside that window —
// MySQL DECIMAL(65, 30), PostgreSQL NUMERIC(50, 2), NUMERIC(5, -2), NUMERIC(2, 5) (PostgreSQL 15
// takes a scale outside 0..p), an unconstrained PostgreSQL NUMERIC, ClickHouse Decimal256 — has no
// DECIMAL that holds every value of it, and the translators read such a column as the exact text
// the backend prints (STRING), never as a DECIMAL that would round or overflow, nor as DOUBLE.
namespace tsl {

    // DECIMAL(width, scale) named `alias` when the engine's DECIMAL can declare it; nullopt otherwise.
    inline std::optional<components::types::complex_logical_type>
    decimal_type(long long width, long long scale, const std::string& alias) {
        using components::types::DECIMAL_MAX_WIDTH;
        if (width < 1 || width > DECIMAL_MAX_WIDTH || scale < 0 || scale > width) {
            return std::nullopt;
        }
        auto type = components::types::complex_logical_type::create_decimal(std::pmr::get_default_resource(),
                                                                            static_cast<uint8_t>(width),
                                                                            static_cast<uint8_t>(scale),
                                                                            alias);
        if (type.has_error()) {
            return std::nullopt;
        }
        return std::move(type.value());
    }

    // Stores the unscaled integer `raw` of a DECIMAL value in row `row` of the DECIMAL `column`, in the
    // integer width the column's type is stored as (INT16 / INT32 / INT64 / INT128).
    inline void set_decimal_raw(components::vector::vector_t& column, uint64_t row, components::types::int128_t raw) {
        using components::types::physical_type;
        switch (column.type().to_physical_type()) {
            case physical_type::INT16:
                column.set_value(row, static_cast<int16_t>(raw));
                break;
            case physical_type::INT32:
                column.set_value(row, static_cast<int32_t>(raw));
                break;
            case physical_type::INT64:
                column.set_value(row, static_cast<int64_t>(raw));
                break;
            default:
                column.set_value(row, raw);
                break;
        }
    }

} // namespace tsl
