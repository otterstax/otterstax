// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// The DECIMAL/HUGEINT carrying rules shared by every consumer that turns an
// engine payload into a wide-type carrier (arrow decimal128 for the file
// writers, the custom Flight SQL IPC for its batches): which logical types
// travel as a fixed-point unscaled integer, the
// (precision, scale) they declare, and how the stored integer is read back at
// the width its precision needs.

#include <components/types/types.hpp>

#include <cstdint>
#include <optional>
#include <string>

namespace tsl {

    // alias() asserts on a column that carries no alias (an unnamed
    // expression), so the presence check comes first; every wire names such a
    // field with the empty string.
    inline std::string field_name(const components::types::complex_logical_type& t) {
        return t.has_alias() ? t.alias() : std::string{};
    }

    // Arrow has no 128-bit integer type. decimal128 is its only 128-bit
    // integral carrier, and 38 is the widest precision it declares; scale 0
    // keeps the value an integer rather than a fixed-point number. The
    // engine's precision window (1 … 38, DECIMAL_MAX_WIDTH) is exactly the one
    // decimal128 declares, so every DECIMAL the engine can build has a carrier
    // and none of them needs decimal256.
    inline constexpr std::int32_t hugeint_precision = 38;

    // The (precision, scale) the column's values are declared under. A DECIMAL
    // carries its own; a HUGEINT has none of its own and rides the widest
    // precision decimal128 declares, at scale 0, which leaves the value an
    // integer.
    struct decimal_spec {
        std::int32_t precision;
        std::int32_t scale;
    };

    inline decimal_spec spec_of(const components::types::complex_logical_type& t) {
        using components::types::logical_type;
        if (t.type() == logical_type::DECIMAL) {
            const auto* extension = t.extension_as<components::types::decimal_logical_type_extension>();
            return {static_cast<std::int32_t>(extension->width()),
                    static_cast<std::int32_t>(extension->scale())};
        }
        return {hugeint_precision, 0};
    }

    // The two logical types whose values travel as a decimal128: the engine's
    // fixed-point DECIMAL, and HUGEINT for want of a 128-bit integer type on
    // the Arrow side.
    inline bool travels_as_decimal(const components::types::complex_logical_type& t) {
        using components::types::logical_type;
        return t.type() == logical_type::DECIMAL || t.type() == logical_type::HUGEINT;
    }

    // The unscaled integer a decimal128 slot carries: the engine keeps a
    // DECIMAL's unscaled integer at the width its precision needs, so it is
    // read back at that same width and sign-extended; a HUGEINT is already
    // 128 bits wide. A storage width no carrier reads answers nullopt — the
    // caller reports it in its own error shape.
    inline std::optional<components::types::int128_t>
    read_unscaled_decimal(const components::types::complex_logical_type& type,
                          const components::types::logical_value_t& value) {
        using namespace components::types;
        switch (type.to_physical_type()) {
            case physical_type::INT16:
                return static_cast<int128_t>(value.value<std::int16_t>());
            case physical_type::INT32:
                return static_cast<int128_t>(value.value<std::int32_t>());
            case physical_type::INT64:
                return static_cast<int128_t>(value.value<std::int64_t>());
            case physical_type::INT128:
                return value.value<int128_t>();
            default:
                return std::nullopt;
        }
    }

    // int128 reaches ±1.7e38 while decimal128(38, 0) only reaches ±(10^38 - 1):
    // the top of the engine's HUGEINT range has no precision to be declared
    // under. A DECIMAL is inside its own window by construction; this catches
    // one that is not. Keeping the value under the schema it declares keeps
    // every file and every stream readable.
    inline bool fits_decimal128_precision(components::types::int128_t raw, std::int32_t precision) {
        components::types::int128_t limit = 1;
        for (std::int32_t i = 0; i < precision; ++i) {
            limit *= 10;
        }
        return raw < limit && raw >= -limit;
    }

} // namespace tsl
