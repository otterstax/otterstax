// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "pg_to_chunk.hpp"
#include "affected_rows_carrier.hpp"
#include "otterbrix/translators/error.hpp"

#include "utility/tracy_profiler.hpp"

#include <charconv>
#include <cstring>
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
        using rows_to_otterbrix = void (*)(data_chunk_t&, PGresult*, int, int, const types::complex_logical_type&);

        void set_bool(data_chunk_t& chunk,
                      PGresult* result,
                      int row_index,
                      int column_index,
                      const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            bool bval = (val[0] == 't' || val[0] == 'T' || val[0] == '1');
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), bval});
        }

        void set_int16(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
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

        void set_int32(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
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

        void set_int64(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
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

        void set_float(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
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

        void set_double(data_chunk_t& chunk,
                        PGresult* result,
                        int row_index,
                        int column_index,
                        const types::complex_logical_type&) {
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

        void set_string(data_chunk_t& chunk,
                        PGresult* result,
                        int row_index,
                        int column_index,
                        const types::complex_logical_type&) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), std::string(val)});
        }

        void set_bytea(data_chunk_t& chunk,
                       PGresult* result,
                       int row_index,
                       int column_index,
                       const types::complex_logical_type&) {
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

        void set_enum(data_chunk_t& chunk,
                      PGresult* result,
                      int row_index,
                      int column_index,
                      const types::complex_logical_type& type) {
            if (PQgetisnull(result, row_index, column_index)) {
                chunk.set_value(column_index, row_index, types::logical_value_t{chunk.resource(), nullptr});
                return;
            }
            const char* val = PQgetvalue(result, row_index, column_index);
            chunk.set_value(column_index,
                            row_index,
                            types::logical_value_t::create_enum(chunk.resource(), type, std::string_view(val)));
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

        // libpq reports a DML row count out of band: PQntuples() is 0 and the count is the
        // last word of the command tag ("UPDATE 5000", "INSERT 0 5000", "MERGE 3"). Tags of
        // statements that count nothing ("CREATE TABLE", "BEGIN") mean zero affected rows. A
        // counting tag whose count is missing, non-numeric or out of range is a protocol
        // violation and is reported, never read as 0.
        core::result_wrapper_t<uint64_t> command_tag_affected_rows(std::pmr::memory_resource* resource,
                                                                   PGresult* result) {
            const char* status = PQcmdStatus(result);
            if (status == nullptr) {
                return uint64_t{0};
            }
            const std::string_view tag{status};
            constexpr std::string_view counting_verbs[] =
                {"INSERT ", "UPDATE ", "DELETE ", "MERGE ", "SELECT ", "MOVE ", "FETCH ", "COPY "};
            bool counting = false;
            for (const auto verb : counting_verbs) {
                if (tag.starts_with(verb)) {
                    counting = true;
                    break;
                }
            }
            if (!counting) {
                return uint64_t{0};
            }
            const auto digits = tag.substr(tag.rfind(' ') + 1);
            uint64_t value = 0;
            const auto [end, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), value);
            if (digits.empty() || ec != std::errc{} || end != digits.data() + digits.size()) {
                std::pmr::string what{"pg_to_chunk: malformed command tag '", resource};
                what.append(tag.data(), tag.size());
                what.append("'");
                return core::error_t(core::error_code_t::conversion_failure, std::move(what));
            }
            return value;
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
        // A DML result has no columns and no tuples; its count comes from the command
        // tag and travels in the same column-less carrier the engine uses for a local
        // DML result.
        if (ncolumns == 0) {
            auto affected = command_tag_affected_rows(resource, result);
            if (affected.has_error()) {
                return affected.convert_error<data_chunk_t>();
            }
            return make_affected_rows_carrier(resource, affected.value());
        }
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
                translators.emplace_back(to_local_translator(column_type, column_name));
            }
            types.emplace_back(translators.back().type);
        }

        data_chunk_t chunk(resource, types, nrows);
        chunk.set_cardinality(nrows);

        for (size_t i = 0; i < nrows; i++) {
            for (int j = 0; j < ncolumns; j++) {
                const auto& translator = translators.at(static_cast<size_t>(j));
                translator.conversion_func(chunk, result, static_cast<int>(i), j, translator.type);
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
                auto translator = to_local_translator(column_type, column_name);
                fields.emplace_back(std::move(translator.type));
            }
        }

        return types::complex_logical_type::create_struct("", std::move(fields));
    }

} // namespace tsl
