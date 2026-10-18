// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_arrow.hpp"
#include "decimal_carrier.hpp"
#include "otterbrix/translators/arrow_memory_pool.hpp"
#include "otterbrix/translators/error.hpp"
#include "writable_columns.hpp"

#include "utility/tracy_profiler.hpp"

// Clash between otterbrix parser macros and Arrow headers
#undef DAY
#undef SECOND

// otterbrix's Arrow C data interface exporter. Its C structs and Arrow's own
// (arrow/c/abi.h, through bridge.h) share the ARROW_C_DATA_INTERFACE guard and
// layout, so whichever header comes first defines them for both.
#include <components/vector/arrow/arrow_converter.hpp>

#include <arrow/builder.h>
#include <arrow/c/bridge.h>

#include <exception>
#include <string>
#include <vector>

// Not `using namespace components::vector`: it holds a namespace `arrow` (the
// exporter's), which would make every `arrow::` below ambiguous.
using components::vector::data_chunk_t;
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
        // Temporal types by logical type too, as otterbrix's exporter writes them (tdD / ttu /
        // tsu: / tsu:UTC). Their physical INT32 / INT64 count from the engine's 2000-01-01 epoch, so
        // only the exporter writes their values (exported_natively), moving them to Arrow's 1970.
        switch (t.type()) {
            case types::logical_type::DATE:
                return std::shared_ptr<arrow::DataType>{arrow::date32()};
            case types::logical_type::TIME:
                return std::shared_ptr<arrow::DataType>{arrow::time64(arrow::TimeUnit::MICRO)};
            case types::logical_type::TIMESTAMP:
                return std::shared_ptr<arrow::DataType>{arrow::timestamp(arrow::TimeUnit::MICRO)};
            case types::logical_type::TIMESTAMP_TZ:
                return std::shared_ptr<arrow::DataType>{arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")};
            default:
                break;
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

    // Columns otterbrix's exporter writes (one typed pass, its own malloc'd buffers): BOOLEAN, the
    // integers, FLOAT, DOUBLE, temporal types, and STRING as large_utf8 when `large_strings`.
    // The builders keep utf8 STRING (FlightSQL advertises utf8), DECIMAL (sentinel / precision
    // refusals), HUGEINT and UUID (the exporter throws on them) and NA (no appender).
    bool exported_natively(const types::complex_logical_type& t, bool large_strings) {
        switch (t.type()) {
            case types::logical_type::BOOLEAN:
            case types::logical_type::TINYINT:
            case types::logical_type::SMALLINT:
            case types::logical_type::INTEGER:
            case types::logical_type::BIGINT:
            case types::logical_type::UTINYINT:
            case types::logical_type::USMALLINT:
            case types::logical_type::UINTEGER:
            case types::logical_type::UBIGINT:
            case types::logical_type::FLOAT:
            case types::logical_type::DOUBLE:
            case types::logical_type::DATE:
            case types::logical_type::TIME:
            case types::logical_type::TIMESTAMP:
            case types::logical_type::TIMESTAMP_TZ:
                return true;
            case types::logical_type::STRING_LITERAL:
                return large_strings;
            default:
                return false;
        }
    }

    // The `columns` of `chunk` (all exported_natively) as Arrow arrays, in that order, through
    // otterbrix's exporter: to_arrow_array appends each column in one typed pass (constant and
    // dictionary vectors included, through their unified format), and Arrow imports the result
    // without copying it.
    core::result_wrapper_t<arrow::ArrayVector> export_natively(std::pmr::memory_resource* res,
                                                               const data_chunk_t& chunk,
                                                               const std::vector<uint64_t>& columns) {
        namespace ca = components::vector::arrow;
        std::pmr::vector<types::complex_logical_type> view_types(res);
        view_types.reserve(columns.size());
        for (const auto c : columns) {
            view_types.push_back(chunk.data[c].type());
        }
        // A chunk of just these columns, sharing their buffers.
        data_chunk_t view(res, view_types, 0);
        for (size_t i = 0; i < columns.size(); ++i) {
            view.data[i].reference(chunk.data[columns[i]]);
        }
        view.set_capacity(chunk.size());
        view.set_cardinality(chunk.size());

        ca::arrow_schema_wrapper_t schema;
        ca::arrow_array_wrapper_t array;
        // The exporter reports a type it has no mapping for by throwing. exported_natively admits
        // none such, so a throw here is a broken invariant (or an allocation failure) — reported
        // as a value at this boundary, never let out of the translator.
        try {
            ca::to_arrow_schema(&schema.arrow_schema, view_types);
            ca::to_arrow_array(view, &array.arrow_array);
        } catch (const std::exception& e) {
            return tsl::make_error(res,
                                   core::error_code_t::conversion_failure,
                                   std::string{"chunk_to_record_batch: native export failed: "} + e.what());
        }
        // Takes both structs over (the wrappers release whatever an import leaves behind).
        auto batch = arrow::ImportRecordBatch(&array.arrow_array, &schema.arrow_schema);
        if (!batch.ok()) {
            return tsl::arrow_error(res, core::error_code_t::conversion_failure,
                                    "chunk_to_record_batch: import batch", batch.status());
        }
        return (*batch)->columns();
    }

    // `large_strings`: a STRING field is large_utf8, the layout otterbrix's exporter writes
    // (chunk_to_file_batch); otherwise utf8.
    arrow_schema_result fields_to_schema(std::pmr::memory_resource* res,
                                         const std::pmr::vector<components::types::complex_logical_type>& types,
                                         bool large_strings = false) {
        arrow::FieldVector field_vector;
        field_vector.reserve(types.size());
        for (const auto& type : types) {
            if (large_strings && type.type() == logical_type::STRING_LITERAL) {
                field_vector.push_back(arrow::field(field_name(type), arrow::large_utf8()));
                continue;
            }
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

namespace {

// Both batch builders: every exported_natively column through otterbrix's exporter, the rest
// through Arrow's builders on the caller's resource. Arrow's builders report failure through a
// returned Status; every one is checked so a failed Append never drops a row and a failed Finish
// never hands a null array to RecordBatch::Make.
core::result_wrapper_t<std::shared_ptr<arrow::RecordBatch>>
make_batch(std::pmr::memory_resource* res, const data_chunk_t& chunk, bool large_strings) {
    auto writable = tsl::validate_writable_columns(res, chunk, "chunk_to_record_batch");
    if (writable.has_error()) {
        return writable.convert_error<std::shared_ptr<arrow::RecordBatch>>();
    }

    const auto types = chunk.types();
    auto built = fields_to_schema(res, types, large_strings);
    if (built.has_error()) {
        return built.convert_error<std::shared_ptr<arrow::RecordBatch>>();
    }
    const std::shared_ptr<arrow::Schema> schema = std::move(built.value());
    const auto nrows  = static_cast<int64_t>(chunk.size());
    const size_t ncols = static_cast<size_t>(chunk.column_count());

    // Nothing to export: every column empty, under its type.
    if (nrows == 0) {
        auto empty = arrow::RecordBatch::MakeEmpty(schema, tsl::arrow_pool(res));
        if (!empty.ok()) {
            return tsl::arrow_error(res, core::error_code_t::conversion_failure, "chunk_to_record_batch: empty batch",
                                    empty.status());
        }
        return std::move(*empty);
    }

    arrow::ArrayVector arrays;
    arrays.reserve(ncols);

    // The natively exported columns in one pass; the loop below takes them in column order.
    std::vector<uint64_t> native;
    for (size_t c = 0; c < ncols; c++) {
        if (exported_natively(types[c], large_strings)) {
            native.push_back(c);
        }
    }
    arrow::ArrayVector native_arrays;
    if (!native.empty()) {
        auto exported = export_natively(res, chunk, native);
        if (exported.has_error()) {
            return exported.convert_error<std::shared_ptr<arrow::RecordBatch>>();
        }
        native_arrays = std::move(exported.value());
        // The exporter's Arrow types are the schema's by construction; a mismatch (an otterbrix
        // whose mapping moved) is refused rather than streamed under the wrong field type.
        for (size_t i = 0; i < native.size(); ++i) {
            const auto& expected = schema->field(static_cast<int>(native[i]))->type();
            if (!native_arrays[i]->type()->Equals(*expected)) {
                return tsl::make_error(res,
                                       core::error_code_t::conversion_failure,
                                       "chunk_to_record_batch: column '" + field_name(types[native[i]]) +
                                           "' was exported as " + native_arrays[i]->type()->ToString() + ", not " +
                                           expected->ToString());
            }
        }
    }
    size_t next_native = 0;

    // Every builder allocates on the caller's resource (arrow_memory_pool.hpp).
    arrow::MemoryPool* pool = tsl::arrow_pool(res);

#define BUILD_COL(Builder, cpp_type)                                                                     \
    {                                                                                                     \
        arrow::Builder b{pool};                                                                           \
        if (auto st = b.Reserve(nrows); !st.ok()) {                                                       \
            return tsl::arrow_error(res, core::error_code_t::conversion_failure,                         \
                                    "chunk_to_record_batch: reserve", st);                               \
        }                                                                                                 \
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
        if (next_native < native.size() && native[next_native] == c) {
            arrays.push_back(native_arrays[next_native++]);
            continue;
        }
        if (travels_as_decimal(types[c])) {
            // The schema field for this column is decimal128(precision, scale) and a builder
            // carries its own type, so both must name it. What is appended is the stored
            // unscaled integer: the scale lives in the type and is never applied to the number,
            // which is exactly what keeps the pair exact in both directions.
            const auto spec = spec_of(types[c]);
            const std::string name = field_name(types[c]);
            arrow::Decimal128Builder b{arrow::decimal128(spec.precision, spec.scale), pool};
            if (auto st = b.Reserve(nrows); !st.ok()) {
                return tsl::arrow_error(res, core::error_code_t::conversion_failure,
                                        "chunk_to_record_batch: reserve", st);
            }
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
                arrow::StringBuilder b{pool};
                if (auto st = b.Reserve(nrows); !st.ok()) {
                    return tsl::arrow_error(res, core::error_code_t::conversion_failure,
                                            "chunk_to_record_batch: reserve", st);
                }
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
                arrow::NullBuilder b{pool};
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

    return arrow::RecordBatch::Make(std::move(schema), nrows, std::move(arrays));
}

} // namespace

core::result_wrapper_t<std::shared_ptr<arrow::RecordBatch>>
chunk_to_record_batch(std::pmr::memory_resource* res, const data_chunk_t& chunk) {
    OTX_ZONE_N("tsl::chunk_to_record_batch");
    return make_batch(res, chunk, /*large_strings=*/false);
}

core::result_wrapper_t<std::shared_ptr<arrow::RecordBatch>>
chunk_to_file_batch(std::pmr::memory_resource* res, const data_chunk_t& chunk) {
    OTX_ZONE_N("tsl::chunk_to_file_batch");
    return make_batch(res, chunk, /*large_strings=*/true);
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
