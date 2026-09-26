// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "arrow_to_chunk.hpp"

#include "otterbrix/translators/error.hpp"
#include "utility/tracy_profiler.hpp"

#include <arrow/array.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
// arrow/array.h only forward-declares Decimal128; the precision check needs the definition.
#include <arrow/util/decimal.h>

#include <bit>
#include <cstring>
#include <string>

using namespace components::vector;
using namespace components::types;

namespace tsl {

namespace impl {

    // decimal128's storage is a two's-complement 128-bit integer in the platform's own
    // byte order; the halves are read back by their offsets, which only holds on a
    // little-endian target.
    static_assert(std::endian::native == std::endian::little,
                  "arrow_to_chunk reads decimal128 storage as low-word-first");

    // The widest precision a decimal128 can declare, and the width chunk_to_arrow gives a
    // HUGEINT. Scale 0 at any precision up to this is an integer, and every such value
    // fits int128 with room to spare (10^38 - 1 < 2^127 - 1), so HUGEINT never clips one —
    // not even a value a rogue file wrote past its own declared precision.
    constexpr int32_t max_decimal_precision = 38;

    static core::result_wrapper_t<complex_logical_type> arrow_field_to_type(std::pmr::memory_resource* res,
                                                                            const arrow::Field& field) {
        const auto& type = *field.type();
        switch (type.id()) {
            case arrow::Type::NA:           return complex_logical_type{logical_type::NA,             field.name().c_str()};
            case arrow::Type::BOOL:         return complex_logical_type{logical_type::BOOLEAN,        field.name().c_str()};
            case arrow::Type::INT8:         return complex_logical_type{logical_type::TINYINT,        field.name().c_str()};
            case arrow::Type::INT16:        return complex_logical_type{logical_type::SMALLINT,       field.name().c_str()};
            case arrow::Type::INT32:        return complex_logical_type{logical_type::INTEGER,        field.name().c_str()};
            case arrow::Type::INT64:        return complex_logical_type{logical_type::BIGINT,         field.name().c_str()};
            case arrow::Type::UINT8:        return complex_logical_type{logical_type::UTINYINT,       field.name().c_str()};
            case arrow::Type::UINT16:       return complex_logical_type{logical_type::USMALLINT,      field.name().c_str()};
            case arrow::Type::UINT32:       return complex_logical_type{logical_type::UINTEGER,       field.name().c_str()};
            case arrow::Type::UINT64:       return complex_logical_type{logical_type::UBIGINT,        field.name().c_str()};
            case arrow::Type::FLOAT:        return complex_logical_type{logical_type::FLOAT,          field.name().c_str()};
            case arrow::Type::DOUBLE:       return complex_logical_type{logical_type::DOUBLE,         field.name().c_str()};
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING: return complex_logical_type{logical_type::STRING_LITERAL, field.name().c_str()};
            case arrow::Type::DECIMAL128: {
                const auto& decimal = static_cast<const arrow::Decimal128Type&>(type);
                // Scale 0 means the stored integer IS the value, so it is an integer column
                // written under Arrow's only 128-bit carrier — HUGEINT reads it exactly, and
                // spans the whole int128 range, so not even a value a foreign writer put past
                // its own declared precision is clipped. This is the reading chunk_to_arrow's
                // HUGEINT carrier round-trips through.
                if (decimal.scale() == 0 && decimal.precision() <= max_decimal_precision) {
                    return complex_logical_type{logical_type::HUGEINT, field.name().c_str()};
                }
                // A scale makes it a fixed-point number, which is exactly what the engine's
                // DECIMAL is. chunk_to_arrow writes one as decimal128(width, scale), so reading
                // it back under the same pair is the symmetric half — the refusal that stood
                // here belonged to a writer that could only emit the unscaled integer as a bare
                // int64. The engine's window is the narrower one on scale (positive, and at
                // most the precision, since `scale <= width` is what it will construct), so a
                // decimal outside it keeps its refusal instead of being read under a spec the
                // engine would reinterpret; create_decimal answers the same verdict.
                if (decimal.scale() > 0 && decimal.scale() <= decimal.precision() &&
                    decimal.precision() <= max_decimal_precision) {
                    return complex_logical_type::create_decimal(res,
                                                                static_cast<uint8_t>(decimal.precision()),
                                                                static_cast<uint8_t>(decimal.scale()),
                                                                field.name());
                }
                break;
            }
            default:
                break;
        }
        return make_error(res,
                          core::error_code_t::conversion_failure,
                          "arrow_to_chunk: column '" + field.name() + "' has Arrow type " + type.ToString() +
                              ", which has no reading in the engine");
    }

    // Writes one cell, answering false for an Arrow type it has no reading for (the caller turns
    // that into the refusal naming the column and its Arrow type). The types arrow_field_to_type
    // admits and the cases below are the same set; the answer keeps a type added to one and not
    // the other from being read under a wrong layout. An error, as opposed to false, is a value
    // this column's own type cannot hold — the type has a reading, this value has none.
    static core::result_wrapper_t<bool>
    set_column_value(data_chunk_t& chunk, size_t col, size_t row, const arrow::Array& arr) {
        auto* res = chunk.resource();
        const auto r = static_cast<int64_t>(row);
        if (arr.IsNull(r)) {
            chunk.set_value(col, row, logical_value_t{res, nullptr});
            return true;
        }
        switch (arr.type_id()) {
            case arrow::Type::NA:
                // Every slot of a null-typed column is null and was handled above.
                return true;
            case arrow::Type::BOOL:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<bool>(static_cast<const arrow::BooleanArray&>(arr).Value(r))});
                return true;
            case arrow::Type::INT8:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<int8_t>(static_cast<const arrow::Int8Array&>(arr).Value(r))});
                return true;
            case arrow::Type::INT16:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<int16_t>(static_cast<const arrow::Int16Array&>(arr).Value(r))});
                return true;
            case arrow::Type::INT32:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<int32_t>(static_cast<const arrow::Int32Array&>(arr).Value(r))});
                return true;
            case arrow::Type::INT64:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<int64_t>(static_cast<const arrow::Int64Array&>(arr).Value(r))});
                return true;
            case arrow::Type::UINT8:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<uint8_t>(static_cast<const arrow::UInt8Array&>(arr).Value(r))});
                return true;
            case arrow::Type::UINT16:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<uint16_t>(static_cast<const arrow::UInt16Array&>(arr).Value(r))});
                return true;
            case arrow::Type::UINT32:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<uint32_t>(static_cast<const arrow::UInt32Array&>(arr).Value(r))});
                return true;
            case arrow::Type::UINT64:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<uint64_t>(static_cast<const arrow::UInt64Array&>(arr).Value(r))});
                return true;
            case arrow::Type::FLOAT:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<float>(static_cast<const arrow::FloatArray&>(arr).Value(r))});
                return true;
            case arrow::Type::DOUBLE:
                chunk.set_value(col, row,
                    logical_value_t{res, static_cast<double>(static_cast<const arrow::DoubleArray&>(arr).Value(r))});
                return true;
            case arrow::Type::STRING:
                chunk.set_value(col, row,
                    logical_value_t{res, std::string(static_cast<const arrow::StringArray&>(arr).GetString(r))});
                return true;
            case arrow::Type::LARGE_STRING:
                // 64-bit offsets: the same text under a different layout, and reading it
                // through StringArray would walk 32-bit offsets over 64-bit ones.
                chunk.set_value(col, row,
                    logical_value_t{res, std::string(static_cast<const arrow::LargeStringArray&>(arr).GetString(r))});
                return true;
            case arrow::Type::DECIMAL128: {
                // The stored 16-byte two's-complement integer, low word first on the
                // little-endian target asserted above. It is the unscaled integer under either
                // reading, so nothing is rescaled here.
                const auto* bytes = static_cast<const arrow::Decimal128Array&>(arr).GetValue(r);
                uint64_t low = 0;
                int64_t high = 0;
                std::memcpy(&low, bytes, sizeof(low));
                std::memcpy(&high, bytes + sizeof(low), sizeof(high));
                const auto raw = absl::MakeInt128(high, low);
                const auto& column_type = chunk.data[col].type();
                if (column_type.type() != logical_type::DECIMAL) {
                    // Admitted at scale 0, where the stored integer IS the value and HUGEINT
                    // holds every one a decimal128 can carry.
                    chunk.set_value(col, row, logical_value_t{res, raw});
                    return true;
                }
                // A DECIMAL keeps that integer at the width its precision needs, which below
                // precision 19 is narrower than 128 bits: a payload the declared precision
                // cannot hold would be cut down to a different number there, or land on one of
                // the engine's non-finite DECIMAL sentinels and read back as Infinity. Refused
                // rather than read as a value the file does not hold.
                const auto* spec = column_type.extension_as<decimal_logical_type_extension>();
                const arrow::Decimal128 carried{high, low};
                if (!carried.FitsInPrecision(static_cast<int32_t>(spec->width()))) {
                    return make_error(res,
                                      core::error_code_t::conversion_failure,
                                      "arrow_to_chunk: column '" + column_type.alias() + "' holds " +
                                          carried.ToString(0) + ", which its own decimal128(" +
                                          std::to_string(static_cast<unsigned>(spec->width())) + ", " +
                                          std::to_string(static_cast<unsigned>(spec->scale())) +
                                          ") cannot declare");
                }
                chunk.set_value(col, row, logical_value_t::create_decimal(res, column_type, raw));
                return true;
            }
            default:
                return false;
        }
    }

} // namespace impl

core::result_wrapper_t<data_chunk_t> arrow_to_chunk(std::pmr::memory_resource* res,
                                                     const std::shared_ptr<arrow::RecordBatch>& batch) {
    OTX_ZONE_N("tsl::arrow_to_chunk");
    if (!batch || batch->num_rows() == 0) {
        return data_chunk_t(res, {}, 0);
    }
    const int ncols = batch->num_columns();
    const int64_t nrows = batch->num_rows();

    std::pmr::vector<complex_logical_type> types(res);
    types.reserve(ncols);
    for (int c = 0; c < ncols; c++) {
        auto type = impl::arrow_field_to_type(res, *batch->schema()->field(c));
        if (type.has_error()) {
            return type.convert_error<data_chunk_t>();
        }
        types.emplace_back(std::move(type.value()));
    }

    data_chunk_t chunk(res, types, static_cast<size_t>(nrows));
    chunk.set_cardinality(static_cast<size_t>(nrows));

    for (int c = 0; c < ncols; c++) {
        const auto& arr = *batch->column(c);
        for (int64_t r = 0; r < nrows; r++) {
            auto written = impl::set_column_value(chunk, static_cast<size_t>(c), static_cast<size_t>(r), arr);
            if (written.has_error()) {
                return written.convert_error<data_chunk_t>();
            }
            if (!written.value()) {
                return make_error(res,
                                  core::error_code_t::conversion_failure,
                                  "arrow_to_chunk: column '" + batch->schema()->field(c)->name() +
                                      "' has Arrow type " + arr.type()->ToString() + ", which has no reading");
            }
        }
    }
    return chunk;
}

core::result_wrapper_t<complex_logical_type> arrow_schema_to_struct(std::pmr::memory_resource* res,
                                                                     const std::shared_ptr<arrow::Schema>& schema) {
    OTX_ZONE_N("tsl::arrow_schema_to_struct");
    std::pmr::vector<complex_logical_type> fields(res);
    fields.reserve(schema->num_fields());
    for (int i = 0; i < schema->num_fields(); i++) {
        auto type = impl::arrow_field_to_type(res, *schema->field(i));
        if (type.has_error()) {
            // Same result type, so convert_error (which requires a different one) does not
            // apply: the error travels on as it is.
            return type.error();
        }
        fields.emplace_back(std::move(type.value()));
    }
    return complex_logical_type::create_struct("", std::move(fields));
}

} // namespace tsl
