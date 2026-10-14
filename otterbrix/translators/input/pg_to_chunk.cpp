// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "pg_to_chunk.hpp"
#include "decimal_text.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/casts/kernels/string_cast.hpp>

#include <algorithm>
#include <charconv>
#include <cstring>
#include <optional>
#include <string>
#include <utility>
#include <string_view>

// PostgreSQL OID constants (from pg_type.h)
namespace pg_oid {
    constexpr Oid BOOLOID = 16;
    constexpr Oid BYTEAOID = 17;
    constexpr Oid CHAROID = 18;
    constexpr Oid INT8OID = 20;
    constexpr Oid INT2OID = 21;
    constexpr Oid INT4OID = 23;
    constexpr Oid TEXTOID = 25;
    constexpr Oid OIDOID = 26;
    constexpr Oid FLOAT4OID = 700;
    constexpr Oid FLOAT8OID = 701;
    constexpr Oid VARCHAROID = 1043;
    constexpr Oid DATEOID = 1082;
    constexpr Oid TIMEOID = 1083;
    constexpr Oid TIMESTAMPOID = 1114;
    constexpr Oid TIMESTAMPTZOID = 1184;
    constexpr Oid NUMERICOID = 1700;
    constexpr Oid UUIDOID = 2950;
    constexpr Oid JSONOID = 114;
    constexpr Oid JSONBOID = 3802;
} // namespace pg_oid

namespace tsl {

    // Internal linkage: the input translators reuse helper names (value_translator_t,
    // set_string, to_local_translator, ...); with external linkage the linker keeps one
    // definition of each for every translator, whatever its layout.
    namespace {
        // The column's own type travels with every call so an ENUM cell can be built
        // without a per-column closure.
        // False: the value has no reading as its column's type.
        using rows_to_otterbrix = bool (*)(data_chunk_t&, PGresult*, int, int, const types::complex_logical_type&);

        bool set_bool(data_chunk_t& chunk,
                      PGresult* result,
                      int row_index,
                      int column_index,
                      const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            bool bval = (val[0] == 't' || val[0] == 'T' || val[0] == '1');
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), bval});
            return true;
        }

        bool set_int16(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            int16_t value = 0;
            auto [ptr, ec] = std::from_chars(val, val + std::strlen(val), value);
            if (ec != std::errc{}) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
            return true;
        }

        bool set_int32(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            int32_t value = 0;
            auto [ptr, ec] = std::from_chars(val, val + std::strlen(val), value);
            if (ec != std::errc{}) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
            return true;
        }

        bool set_int64(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            int64_t value = 0;
            auto [ptr, ec] = std::from_chars(val, val + std::strlen(val), value);
            if (ec != std::errc{}) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
            return true;
        }

        bool set_float(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            char* end = nullptr;
            float value = std::strtof(val, &end);
            if (end == val) {
                // Conversion failed
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
            return true;
        }

        bool set_double(data_chunk_t& chunk,
                        PGresult* result,
                        int row_index,
                        int column_index,
                        const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            char* end = nullptr;
            double value = std::strtod(val, &end);
            if (end == val) {
                // Conversion failed
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
            return true;
        }

        bool set_string(data_chunk_t& chunk,
                        PGresult* result,
                        int row_index,
                        int column_index,
                        const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), std::string(val)});
            return true;
        }

        bool set_bytea(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            size_t len = 0;
            unsigned char* unescaped =
                PQunescapeBytea(reinterpret_cast<const unsigned char*>(PQgetvalue(result, row_index, column_index)),
                                &len);
            if (unescaped) {
                std::string blob(reinterpret_cast<char*>(unescaped), len);
                PQfreemem(unescaped);
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), std::move(blob)});
            } else {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
            }
            return true;
        }

        // A NUMERIC(p, s) value: the text PostgreSQL prints for it (exactly s fraction digits) read as
        // the unscaled integer of the column's DECIMAL, every digit kept. A constrained NUMERIC takes
        // NaN (never an infinity), which is the engine's DECIMAL NaN.
        bool set_decimal(data_chunk_t& chunk,
                         PGresult* result,
                         int row_index,
                         int column_index,
                         const types::complex_logical_type& type) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            const std::string_view text{PQgetvalue(result, row_index, column_index)};
            const auto* spec = type.extension_as<types::decimal_logical_type_extension>();
            if (text == "NaN") {
                chunk.set_value(
                    column_index,
                    row_index,
                    types::logical_value_t::create_decimal(chunk.resource(),
                                                           type,
                                                           types::decimal_special::not_a_number(spec->stored_as())));
                return true;
            }
            types::int128_t unscaled = 0;
            if (!components::casts::kernels::detail::parse_decimal(text, spec->width(), spec->scale(), &unscaled)) {
                return false;
            }
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t::create_decimal(chunk.resource(), type, unscaled));
            return true;
        }

        // The enum value whose entry is named `key`, or NA for a key the type
        // does not list — what the engine's create_enum(key) answered before it
        // was dropped (the entries carry the ordinal and the name as alias).
        types::logical_value_t
        enum_value_of(std::pmr::memory_resource* resource, const types::complex_logical_type& type, std::string_view key) {
            const auto* extension = static_cast<const types::enum_logical_type_extension*>(type.extension());
            const auto& entries = extension->entries();
            auto it = std::find_if(entries.begin(), entries.end(), [key](const types::logical_value_t& entry) {
                return entry.type().alias() == key;
            });
            if (it == entries.end()) {
                return types::logical_value_t{resource, types::complex_logical_type{types::logical_type::NA}};
            }
            return types::logical_value_t::create_enum(resource, type, it->value<int32_t>());
        }

        bool set_enum(data_chunk_t& chunk,
                      PGresult* result,
                      int row_index,
                      int column_index,
                      const types::complex_logical_type& type) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return true;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            chunk.set_value(column_index,
                            row_index,
                            enum_value_of(chunk.resource(), type, std::string_view(val)));
            return true;
        }

        struct value_translator_t {
            rows_to_otterbrix conversion_func;
            types::complex_logical_type type;
        };

        // The precision and scale a NUMERIC column's type modifier declares (PQfmod): ((p << 16) | s)
        // + VARHDRSZ, the scale an 11-bit signed field since PostgreSQL 15 (NUMERIC(5, -2) and
        // NUMERIC(2, 5) are legal there); nullopt for an unconstrained NUMERIC (-1).
        std::optional<std::pair<long long, long long>> numeric_spec(int typmod) {
            constexpr int varhdrsz = 4;
            if (typmod < varhdrsz) {
                return std::nullopt;
            }
            const int modifier = typmod - varhdrsz;
            const long long precision = (modifier >> 16) & 0xffff;
            const long long scale = ((modifier & 0x7ff) ^ 1024) - 1024;
            return std::make_pair(precision, scale);
        }

        value_translator_t to_local_translator(Oid pg_type, int typmod, const char* column_name) {
            switch (pg_type) {
                case pg_oid::BOOLOID:
                    return {set_bool, {types::logical_type::BOOLEAN, column_name}};

                case pg_oid::INT2OID:
                    return {set_int16, {types::logical_type::SMALLINT, column_name}};

                case pg_oid::INT4OID:
                case pg_oid::OIDOID:
                    return {set_int32, {types::logical_type::INTEGER, column_name}};

                case pg_oid::INT8OID:
                    return {set_int64, {types::logical_type::BIGINT, column_name}};

                case pg_oid::FLOAT4OID:
                    return {set_float, {types::logical_type::FLOAT, column_name}};

                case pg_oid::FLOAT8OID:
                    return {set_double, {types::logical_type::DOUBLE, column_name}};

                // NUMERIC(p, s) is the engine's DECIMAL(p, s) when it can declare one (p <= 38,
                // 0 <= s <= p). Any other NUMERIC is carried as the exact text PostgreSQL prints:
                // a wider one, a scale outside 0..p, and the unconstrained NUMERIC, whose values each
                // keep their own scale (up to 16383 fraction digits) and up to 131072 integer
                // digits, so no DECIMAL(38, s) holds them without rounding or overflowing and
                // DOUBLE would round every one past 15 digits. It is also what PostgreSQL answers
                // for SUM(bigint) / SUM(numeric) / AVG(...) — conform_to_result reads that text
                // into the engine's result type with every digit it has.
                case pg_oid::NUMERICOID: {
                    if (const auto spec = numeric_spec(typmod)) {
                        if (auto decimal = decimal_type(spec->first, spec->second, column_name)) {
                            return {set_decimal, std::move(*decimal)};
                        }
                    }
                    return {set_string, {types::logical_type::STRING_LITERAL, column_name}};
                }

                case pg_oid::CHAROID:
                case pg_oid::TEXTOID:
                case pg_oid::VARCHAROID:
                case pg_oid::UUIDOID:
                case pg_oid::JSONOID:
                case pg_oid::JSONBOID:
                case pg_oid::DATEOID:
                case pg_oid::TIMEOID:
                case pg_oid::TIMESTAMPOID:
                case pg_oid::TIMESTAMPTZOID:
                    return {set_string, {types::logical_type::STRING_LITERAL, column_name}};

                case pg_oid::BYTEAOID:
                    return {set_bytea, {types::logical_type::STRING_LITERAL, column_name}};

                default: {
                    // Default to string for unknown types
                    return {set_string, {types::logical_type::STRING_LITERAL, column_name}};
                }
            }
        }

    } // namespace

    core::result_wrapper_t<data_chunk_t> pg_to_chunk(std::pmr::memory_resource* resource, PGresult* result) {
        return pg_to_chunk(resource, result, pg_enum_oid_map{});
    }

    types::complex_logical_type pg_to_struct(std::pmr::memory_resource* resource, PGresult* result) {
        return pg_to_struct(resource, result, pg_enum_oid_map{});
    }

    namespace {
        std::optional<types::complex_logical_type> try_make_enum_type(std::pmr::memory_resource* res,
                                                                      Oid pg_type,
                                                                      const char* column_name,
                                                                      const pg_enum_oid_map& enum_oids) {
            auto it = enum_oids.find(static_cast<unsigned int>(pg_type));
            if (it == enum_oids.end()) {
                return std::nullopt;
            }
            const auto& desc = it->second;
            std::vector<types::logical_value_t> entries;
            entries.reserve(desc.values.size());
            int32_t counter = 0;
            for (const auto& v : desc.values) {
                types::logical_value_t entry{res, counter++};
                entry.set_alias(v);
                entries.emplace_back(std::move(entry));
            }
            return types::complex_logical_type::create_enum(desc.typname, std::move(entries), column_name);
        }
    } // namespace

    core::result_wrapper_t<data_chunk_t>
    pg_to_chunk(std::pmr::memory_resource* resource, PGresult* result, const pg_enum_oid_map& enum_oids) {
        OTX_ZONE_N("tsl::pg_to_chunk");
        const int ncolumns = PQnfields(result);
        const size_t nrows = static_cast<size_t>(PQntuples(result));

        std::pmr::vector<value_translator_t> translators(resource);
        std::pmr::vector<types::complex_logical_type> types(resource);
        translators.reserve(ncolumns);
        types.reserve(ncolumns);

        for (int col = 0; col < ncolumns; ++col) {
            const char* column_name = PQfname(result, col);
            Oid column_type = PQftype(result, col);

            if (auto enum_type = try_make_enum_type(resource, column_type, column_name, enum_oids)) {
                translators.emplace_back(value_translator_t{set_enum, std::move(*enum_type)});
            } else {
                translators.emplace_back(to_local_translator(column_type, PQfmod(result, col), column_name));
            }
            types.emplace_back(translators.back().type);
        }

        data_chunk_t chunk(resource, types, nrows);
        chunk.set_cardinality(nrows);

        for (size_t i = 0; i < nrows; i++) {
            for (int j = 0; j < ncolumns; j++) {
                const auto& translator = translators.at(static_cast<size_t>(j));
                if (!translator.conversion_func(chunk, result, static_cast<int>(i), j, translator.type)) {
                    std::pmr::string what{"pg_to_chunk: the value '", resource};
                    what.append(PQgetvalue(result, static_cast<int>(i), j));
                    what.append("' in row ");
                    what.append(std::to_string(i));
                    what.append(" of column '");
                    what.append(PQfname(result, j));
                    what.append("' has no reading as the column's type");
                    return core::error_t(core::error_code_t::conversion_failure, std::move(what));
                }
            }
        }
        return chunk;
    }

    types::complex_logical_type
    pg_to_struct(std::pmr::memory_resource* resource, PGresult* result, const pg_enum_oid_map& enum_oids) {
        OTX_ZONE_N("tsl::pg_to_struct");
        const int ncolumns = PQnfields(result);

        std::pmr::vector<types::complex_logical_type> fields(resource);
        fields.reserve(ncolumns);

        for (int col = 0; col < ncolumns; ++col) {
            const char* column_name = PQfname(result, col);
            Oid column_type = PQftype(result, col);

            if (auto enum_type = try_make_enum_type(resource, column_type, column_name, enum_oids)) {
                fields.emplace_back(std::move(*enum_type));
            } else {
                auto translator = to_local_translator(column_type, PQfmod(result, col), column_name);
                fields.emplace_back(std::move(translator.type));
            }
        }

        return types::complex_logical_type::create_struct("", std::move(fields));
    }

} // namespace tsl
