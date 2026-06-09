// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "pg_to_chunk.hpp"

#include "utility/tracy_profiler.hpp"

#include <charconv>
#include <cstdlib>
#include <cstring>
#include <sstream>

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

    namespace impl {
        using rows_to_otterbrix = std::function<void(data_chunk_t&, PGresult*, int, int)>;

        void set_bool(data_chunk_t& chunk, PGresult* result, int row_index, int column_index) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            bool bval = (val[0] == 't' || val[0] == 'T' || val[0] == '1');
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), bval});
        }

        void set_int16(data_chunk_t& chunk, PGresult* result, int row_index, int column_index) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            int16_t value = 0;
            auto [ptr, ec] = std::from_chars(val, val + std::strlen(val), value);
            if (ec != std::errc{}) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
        }

        void set_int32(data_chunk_t& chunk, PGresult* result, int row_index, int column_index) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            int32_t value = 0;
            auto [ptr, ec] = std::from_chars(val, val + std::strlen(val), value);
            if (ec != std::errc{}) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
        }

        void set_int64(data_chunk_t& chunk, PGresult* result, int row_index, int column_index) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            int64_t value = 0;
            auto [ptr, ec] = std::from_chars(val, val + std::strlen(val), value);
            if (ec != std::errc{}) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
        }

        void set_float(data_chunk_t& chunk, PGresult* result, int row_index, int column_index) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            char* end = nullptr;
            float value = std::strtof(val, &end);
            if (end == val) {
                // Conversion failed
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
        }

        void set_double(data_chunk_t& chunk, PGresult* result, int row_index, int column_index) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            char* end = nullptr;
            double value = std::strtod(val, &end);
            if (end == val) {
                // Conversion failed
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), value});
        }

        void set_string(data_chunk_t& chunk, PGresult* result, int row_index, int column_index) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), std::string(val)});
        }

        void set_bytea(data_chunk_t& chunk, PGresult* result, int row_index, int column_index) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
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
        }

        struct value_translator_t {
            rows_to_otterbrix conversion_func;
            types::complex_logical_type type;
        };

        value_translator_t to_local_translator(Oid pg_type, const char* column_name) {
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
                case pg_oid::NUMERICOID:
                    return {set_double, {types::logical_type::DOUBLE, column_name}};

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

    } // namespace impl

    data_chunk_t pg_to_chunk(std::pmr::memory_resource* resource, PGresult* result) {
        OTX_ZONE_N("tsl::pg_to_chunk");
        const int ncolumns = PQnfields(result);
        const int nrows = PQntuples(result);

        std::pmr::vector<impl::value_translator_t> translators(resource);
        std::pmr::vector<types::complex_logical_type> types(resource);
        translators.reserve(ncolumns);
        types.reserve(ncolumns);

        // Collect schema information
        for (int col = 0; col < ncolumns; ++col) {
            const char* column_name = PQfname(result, col);
            Oid column_type = PQftype(result, col);

            translators.emplace_back(impl::to_local_translator(column_type, column_name));
            types.emplace_back(translators.back().type);
        }

        data_chunk_t chunk(resource, types, nrows);
        chunk.set_cardinality(nrows);

        // Convert PostgreSQL rows to otterbrix data chunk
        for (int i = 0; i < nrows; i++) {
            for (int j = 0; j < ncolumns; j++) {
                translators.at(j).conversion_func(chunk, result, i, j);
            }
        }

        return chunk;
    }

    types::complex_logical_type pg_to_struct(PGresult* result) {
        const int ncolumns = PQnfields(result);

        std::vector<types::complex_logical_type> fields;
        fields.reserve(ncolumns);

        for (int col = 0; col < ncolumns; ++col) {
            const char* column_name = PQfname(result, col);
            Oid column_type = PQftype(result, col);

            auto translator = impl::to_local_translator(column_type, column_name);
            fields.emplace_back(translator.type);
        }

        return types::complex_logical_type::create_struct("", std::move(fields));
    }

    namespace impl {
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
    } // namespace impl

    data_chunk_t pg_to_chunk(std::pmr::memory_resource* resource, PGresult* result, const pg_enum_oid_map& enum_oids) {
        OTX_ZONE_N("tsl::pg_to_chunk(enum)");
        const int ncolumns = PQnfields(result);
        const int nrows = PQntuples(result);

        std::pmr::vector<impl::value_translator_t> translators(resource);
        std::pmr::vector<types::complex_logical_type> types(resource);
        translators.reserve(ncolumns);
        types.reserve(ncolumns);

        for (int col = 0; col < ncolumns; ++col) {
            const char* column_name = PQfname(result, col);
            Oid column_type = PQftype(result, col);

            if (auto enum_type = impl::try_make_enum_type(resource, column_type, column_name, enum_oids)) {
                auto et = *enum_type; // by-value capture, ENUM type travels with the lambda
                impl::rows_to_otterbrix enum_set = [et](data_chunk_t& chunk,
                                                        PGresult* res,
                                                        int row_index,
                                                        int column_index) {
                    if (PQgetisnull(res, row_index, column_index)) {
                        chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                        return;
                    }
                    const char* val = PQgetvalue(res, row_index, column_index);
                    chunk.set_value(column_index,
                                    row_index,
                                    types::logical_value_t::create_enum(chunk.resource(), et, std::string_view(val)));
                };
                translators.emplace_back(impl::value_translator_t{std::move(enum_set), et});
            } else {
                translators.emplace_back(impl::to_local_translator(column_type, column_name));
            }
            types.emplace_back(translators.back().type);
        }

        data_chunk_t chunk(resource, types, nrows);
        chunk.set_cardinality(nrows);

        for (int i = 0; i < nrows; i++) {
            for (int j = 0; j < ncolumns; j++) {
                translators.at(j).conversion_func(chunk, result, i, j);
            }
        }
        return chunk;
    }

    types::complex_logical_type pg_to_struct(PGresult* result, const pg_enum_oid_map& enum_oids) {
        const int ncolumns = PQnfields(result);

        std::vector<types::complex_logical_type> fields;
        fields.reserve(ncolumns);

        std::pmr::memory_resource* enum_arena = std::pmr::get_default_resource();
        for (int col = 0; col < ncolumns; ++col) {
            const char* column_name = PQfname(result, col);
            Oid column_type = PQftype(result, col);

            if (auto enum_type = impl::try_make_enum_type(enum_arena, column_type, column_name, enum_oids)) {
                fields.emplace_back(*enum_type);
            } else {
                auto translator = impl::to_local_translator(column_type, column_name);
                fields.emplace_back(translator.type);
            }
        }

        return types::complex_logical_type::create_struct("", std::move(fields));
    }

} // namespace tsl
