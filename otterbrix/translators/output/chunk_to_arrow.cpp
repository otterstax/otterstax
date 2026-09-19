// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_arrow.hpp"
#include "decimal_carrier.hpp"
#include "otterbrix/translators/error.hpp"
#include "writable_columns.hpp"

#include "utility/tracy_profiler.hpp"
#include <arrow/builder.h>

#include <string>

using namespace components::vector;
using namespace components::types;
using namespace components;

namespace {
    using arrow_type_result = core::result_wrapper_t<std::shared_ptr<arrow::DataType>>;
    using arrow_schema_result = core::result_wrapper_t<std::shared_ptr<arrow::Schema>>;

    // The DECIMAL/HUGEINT carrying rules live in decimal_carrier.hpp, shared
    // with the custom Flight SQL IPC converter (chunk_to_ipc).
    using tsl::decimal_spec;
    using tsl::field_name;
    using tsl::hugeint_precision;
    using tsl::spec_of;
    using tsl::travels_as_decimal;

    // Recursive over nested types; the zone sits on the two public entry points.
    arrow_type_result arrow_type_from_logical(std::pmr::memory_resource* res, const types::complex_logical_type& t) {
        // A DECIMAL's physical type is the width its precision needs (INT16 … INT128), shared
        // with the plain integers of that width — so dispatching on it writes the stored
        // unscaled integer as a bare int and leaves the scale nowhere: DECIMAL(18, 4) 1.2345
        // reaches the client as 12345, a wrong number under a schema that cannot say so. The
        // logical type decides it instead, and the scale rides in the Arrow type. The engine's
        // precision window (1 … 38, DECIMAL_MAX_WIDTH) is exactly the one decimal128 declares
        // (Decimal128Type::kMaxPrecision), so every DECIMAL the engine can build has a carrier
        // and none of them needs decimal256.
        if (t.type() == types::logical_type::DECIMAL) {
            const auto spec = spec_of(t);
            return std::shared_ptr<arrow::DataType>{arrow::decimal128(spec.precision, spec.scale)};
        }
        switch (t.to_physical_type()) {
            case types::physical_type::BOOL:
                return std::shared_ptr<arrow::DataType>{arrow::boolean()};
            case types::physical_type::UINT8:
                return std::shared_ptr<arrow::DataType>{arrow::uint8()};
            case types::physical_type::UINT16:
                return std::shared_ptr<arrow::DataType>{arrow::uint16()};
            case types::physical_type::UINT32:
                return std::shared_ptr<arrow::DataType>{arrow::uint32()};
            case types::physical_type::UINT64:
                return std::shared_ptr<arrow::DataType>{arrow::uint64()};
            case types::physical_type::INT8:
                return std::shared_ptr<arrow::DataType>{arrow::int8()};
            case types::physical_type::INT16:
                return std::shared_ptr<arrow::DataType>{arrow::int16()};
            case types::physical_type::INT32:
                return std::shared_ptr<arrow::DataType>{arrow::int32()};
            case types::physical_type::INT64:
                return std::shared_ptr<arrow::DataType>{arrow::int64()};
            case types::physical_type::FLOAT:
                return std::shared_ptr<arrow::DataType>{arrow::float32()};
            case types::physical_type::DOUBLE:
                return std::shared_ptr<arrow::DataType>{arrow::float64()};
            case types::physical_type::STRING:
                return std::shared_ptr<arrow::DataType>{arrow::utf8()};
            case types::physical_type::NA:
                return std::shared_ptr<arrow::DataType>{arrow::null()};
            case types::physical_type::STRUCT: {
                arrow::FieldVector fields;
                fields.reserve(t.child_types().size());
                for (const auto& child : t.child_types()) {
                    auto child_type = arrow_type_from_logical(res, child);
                    if (child_type.has_error()) {
                        return child_type;
                    }
                    fields.push_back(arrow::field(field_name(child), std::move(child_type.value())));
                }
                return std::shared_ptr<arrow::DataType>{arrow::struct_(std::move(fields))};
            }
            case types::physical_type::LIST:
            case types::physical_type::ARRAY: {
                // ARRAY emits a variable-length arrow::list as well: the wire format is identical
                auto element = arrow_type_from_logical(res, t.child_type());
                if (element.has_error()) {
                    return element;
                }
                return std::shared_ptr<arrow::DataType>{arrow::list(std::move(element.value()))};
            }
            case types::physical_type::INT128:
                // Only HUGEINT means "128-bit integer" at this width. UUID shares the physical
                // type with a different meaning, so it falls through to the error below rather
                // than being reinterpreted. (A DECIMAL of this width was decided above, by its
                // logical type, and never reaches here.)
                if (t.type() == types::logical_type::HUGEINT) {
                    return std::shared_ptr<arrow::DataType>{arrow::decimal128(hugeint_precision, 0)};
                }
                break;
            default:
                break;
        }
        return tsl::make_error(res,
                               core::error_code_t::conversion_failure,
                               "to_arrow_schema: column '" + field_name(t) + "' has logical type " +
                                   std::to_string(static_cast<int>(t.type())) + " (physical type " +
                                   std::to_string(static_cast<int>(t.to_physical_type())) +
                                   "), which has no Arrow mapping");
    }

    arrow_schema_result fields_to_schema(std::pmr::memory_resource* res,
                                         const std::pmr::vector<components::types::complex_logical_type>& types) {
        arrow::FieldVector field_vector;
        field_vector.reserve(types.size());
        for (const auto& type : types) {
            auto arrow_type = arrow_type_from_logical(res, type);
            if (arrow_type.has_error()) {
                return arrow_type.convert_error<std::shared_ptr<arrow::Schema>>();
            }
            field_vector.push_back(arrow::field(field_name(type), std::move(arrow_type.value())));
        }
        return std::shared_ptr<arrow::Schema>{arrow::schema(std::move(field_vector))};
    }
} // namespace

core::result_wrapper_t<std::shared_ptr<arrow::Schema>>
to_arrow_schema(std::pmr::memory_resource* res, const std::pmr::vector<components::types::complex_logical_type>& types) {
    OTX_ZONE_N("tsl::to_arrow_schema(vec)");
    return fields_to_schema(res, types);
}

core::result_wrapper_t<std::shared_ptr<arrow::Schema>>
to_arrow_schema(std::pmr::memory_resource* res, const components::types::complex_logical_type& struct_t) {
    OTX_ZONE_N("tsl::to_arrow_schema(struct)");
    if (struct_t.type() != types::logical_type::STRUCT) {
        // logical_type::NA case - empty schema
        return std::shared_ptr<arrow::Schema>{arrow::schema({})};
    }
    return fields_to_schema(res, struct_t.child_types());
}

// Arrow's builders report failure through a returned Status; every one is checked so
// a failed Append never drops a row and a failed Finish never hands a null array to
// RecordBatch::Make.
core::result_wrapper_t<std::shared_ptr<arrow::RecordBatch>>
chunk_to_record_batch(std::pmr::memory_resource* res, const data_chunk_t& chunk) {
    OTX_ZONE_N("tsl::chunk_to_record_batch");
    auto writable = tsl::validate_writable_columns(res, chunk, "chunk_to_record_batch");
    if (writable.has_error()) {
        return writable.convert_error<std::shared_ptr<arrow::RecordBatch>>();
    }

    const auto types = chunk.types();
    auto schema = to_arrow_schema(res, types);
    if (schema.has_error()) {
        return schema.convert_error<std::shared_ptr<arrow::RecordBatch>>();
    }
    const auto nrows  = static_cast<int64_t>(chunk.size());
    const size_t ncols = static_cast<size_t>(chunk.column_count());

    arrow::ArrayVector arrays;
    arrays.reserve(ncols);

#define BUILD_COL(Builder, cpp_type)                                                                     \
    {                                                                                                     \
        arrow::Builder b;                                                                                 \
        for (int64_t r = 0; r < nrows; r++) {                                                            \
            auto v = chunk.value(static_cast<uint64_t>(c), static_cast<uint64_t>(r));                    \
            auto st = v.is_null() ? b.AppendNull() : b.Append(v.value<cpp_type>());                       \
            if (!st.ok()) {                                                                               \
                return tsl::arrow_error(res, core::error_code_t::conversion_failure,                     \
                                        "chunk_to_record_batch: append", st);                            \
            }                                                                                             \
        }                                                                                                 \
        std::shared_ptr<arrow::Array> arr;                                                                \
        if (auto st = b.Finish(&arr); !st.ok()) {                                                         \
            return tsl::arrow_error(res, core::error_code_t::conversion_failure,                         \
                                    "chunk_to_record_batch: finish", st);                                \
        }                                                                                                 \
        arrays.push_back(std::move(arr));                                                                 \
        break;                                                                                            \
    }

    for (size_t c = 0; c < ncols; c++) {
        if (travels_as_decimal(types[c])) {
            // The schema field for this column is decimal128(precision, scale) and a builder
            // carries its own type, so both must name it. What is appended is the stored
            // unscaled integer: the scale lives in the type and is never applied to the number,
            // which is exactly what keeps the pair exact in both directions.
            const auto spec = spec_of(types[c]);
            const std::string name = field_name(types[c]);
            arrow::Decimal128Builder b{arrow::decimal128(spec.precision, spec.scale)};
            for (int64_t r = 0; r < nrows; r++) {
                auto v = chunk.value(static_cast<uint64_t>(c), static_cast<uint64_t>(r));
                arrow::Status st;
                if (v.is_null()) {
                    st = b.AppendNull();
                } else {
                    auto carried = to_arrow_decimal(res, v, "chunk_to_record_batch", name);
                    if (carried.has_error()) {
                        return carried.convert_error<std::shared_ptr<arrow::RecordBatch>>();
                    }
                    st = b.Append(carried.value());
                }
                if (!st.ok()) {
                    return tsl::arrow_error(res, core::error_code_t::conversion_failure,
                                            "chunk_to_record_batch: append decimal", st);
                }
            }
            std::shared_ptr<arrow::Array> arr;
            if (auto st = b.Finish(&arr); !st.ok()) {
                return tsl::arrow_error(res, core::error_code_t::conversion_failure,
                                        "chunk_to_record_batch: finish", st);
            }
            arrays.push_back(std::move(arr));
            continue;
        }
        switch (types[c].to_physical_type()) {
            case physical_type::BOOL:   BUILD_COL(BooleanBuilder, bool)
            case physical_type::INT8:   BUILD_COL(Int8Builder,    int8_t)
            case physical_type::INT16:  BUILD_COL(Int16Builder,   int16_t)
            case physical_type::INT32:  BUILD_COL(Int32Builder,   int32_t)
            case physical_type::INT64:  BUILD_COL(Int64Builder,   int64_t)
            case physical_type::UINT8:  BUILD_COL(UInt8Builder,   uint8_t)
            case physical_type::UINT16: BUILD_COL(UInt16Builder,  uint16_t)
            case physical_type::UINT32: BUILD_COL(UInt32Builder,  uint32_t)
            case physical_type::UINT64: BUILD_COL(UInt64Builder,  uint64_t)
            case physical_type::FLOAT:  BUILD_COL(FloatBuilder,   float)
            case physical_type::DOUBLE: BUILD_COL(DoubleBuilder,  double)
            case physical_type::STRING: {
                arrow::StringBuilder b;
                for (int64_t r = 0; r < nrows; r++) {
                    auto v = chunk.value(static_cast<uint64_t>(c),
                                        static_cast<uint64_t>(r));
                    auto st = v.is_null() ? b.AppendNull() : b.Append(v.value<const std::string&>());
                    if (!st.ok()) {
                        return tsl::arrow_error(res, core::error_code_t::conversion_failure,
                                                "chunk_to_record_batch: append string", st);
                    }
                }
                std::shared_ptr<arrow::Array> arr;
                if (auto st = b.Finish(&arr); !st.ok()) {
                    return tsl::arrow_error(res, core::error_code_t::conversion_failure,
                                            "chunk_to_record_batch: finish", st);
                }
                arrays.push_back(std::move(arr));
                break;
            }
            case physical_type::NA: {
                // The column's type IS null (schema field arrow::null()), so a NullArray is
                // the faithful representation, not a stand-in.
                arrow::NullBuilder b;
                if (auto st = b.AppendNulls(nrows); !st.ok()) {
                    return tsl::arrow_error(res, core::error_code_t::conversion_failure,
                                            "chunk_to_record_batch: append nulls", st);
                }
                std::shared_ptr<arrow::Array> arr;
                if (auto st = b.Finish(&arr); !st.ok()) {
                    return tsl::arrow_error(res, core::error_code_t::conversion_failure,
                                            "chunk_to_record_batch: finish", st);
                }
                arrays.push_back(std::move(arr));
                break;
            }
            default:
                // Unreachable after validate_writable_columns; kept as an error so a new
                // physical type can never fall through to a wrong array.
                return tsl::make_error(res, core::error_code_t::conversion_failure,
                                       "chunk_to_record_batch: unsupported column type");
        }
    }

#undef BUILD_COL

    return arrow::RecordBatch::Make(std::move(schema.value()), nrows, std::move(arrays));
}

core::result_wrapper_t<arrow::Decimal128> to_arrow_decimal(std::pmr::memory_resource* res,
                                                           const components::types::logical_value_t& value,
                                                           std::string_view scope,
                                                           std::string_view column_name) {
    const auto& type = value.type();
    const auto spec = spec_of(type);
    const auto stored_as = type.to_physical_type();

    // The engine keeps a DECIMAL's unscaled integer at the width its precision needs, so it is
    // read back at that same width and sign-extended; reading a narrower one as int128 would
    // take the neighbouring bytes with it. A HUGEINT is already 128 bits wide.
    const auto raw_storage = tsl::read_unscaled_decimal(type, value);
    if (!raw_storage.has_value()) {
        return tsl::make_error(res,
                               core::error_code_t::conversion_failure,
                               std::string{scope} + ": column '" + std::string{column_name} +
                                   "' is stored at a width no decimal carrier reads");
    }
    const types::int128_t raw = *raw_storage;

    // ±Infinity and NaN are ordinary payloads of the storage integer — the extremes of its
    // range — and decimal128 has no representation for any of the three, so carrying one would
    // put a perfectly finite number where the engine holds none.
    if (type.type() == types::logical_type::DECIMAL && types::decimal_special::is_special(stored_as, raw)) {
        return tsl::make_error(res,
                               core::error_code_t::conversion_failure,
                               std::string{scope} + ": column '" + std::string{column_name} +
                                   "' holds a non-finite DECIMAL (the engine's Infinity / NaN sentinel), which "
                                   "decimal128 has no representation for");
    }

    const arrow::Decimal128 carried{absl::Int128High64(raw), absl::Int128Low64(raw)};
    // int128 reaches ±1.7e38 while decimal128(38, 0) only reaches ±(10^38 - 1), so the top of
    // the engine's HUGEINT range has no precision to be declared under; a DECIMAL is inside its
    // own window by construction, and this catches one that is not. Arrow appends such a value
    // without complaint and hands it back unchanged, but its own ValidateFull calls the array
    // Invalid, the parquet spec forbids writing a value larger than the annotation allows, and
    // a reader that checks is free to refuse it or read it as NULL. Refusing keeps every file
    // and every stream readable under the schema it declares.
    if (!carried.FitsInPrecision(spec.precision)) {
        return tsl::make_error(res,
                               core::error_code_t::conversion_failure,
                               std::string{scope} + ": column '" + std::string{column_name} + "' holds " +
                                   carried.ToString(0) + ", which decimal128(" + std::to_string(spec.precision) +
                                   ", " + std::to_string(spec.scale) +
                                   ") cannot declare: the unscaled integer must be below 10^" +
                                   std::to_string(spec.precision));
    }
    return carried;
}
