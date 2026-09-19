// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_ipc.hpp"

#include "otterbrix/translators/output/decimal_carrier.hpp"
#include "otterbrix/translators/output/writable_columns.hpp"
#include "utility/tracy_profiler.hpp"

#include <absl/numeric/int128.h>

#include <optional>
#include <string>
#include <vector>

using namespace components;
using namespace components::types;
using namespace components::vector;

namespace flight::conv {

    namespace {

        // alias() asserts on a column that carries no alias (an unnamed
        // expression), so the presence check comes first; Arrow itself names
        // such a field with the empty string.
        std::string field_name(const complex_logical_type& t) {
            return t.has_alias() ? t.alias() : std::string{};
        }

        // Arrow has no 128-bit integer type. decimal128 is its only 128-bit
        // integral carrier, and 38 is the widest precision it declares; scale 0
        // keeps the value an integer rather than a fixed-point number.
        constexpr int32_t hugeint_precision = 38;

        // The (precision, scale) the column's values are declared under. A
        // DECIMAL carries its own; a HUGEINT has none of its own and rides the
        // widest precision decimal128 declares, at scale 0.
        struct decimal_spec {
            std::int32_t precision;
            std::int32_t scale;
        };

        decimal_spec spec_of(const complex_logical_type& t) {
            if (t.type() == logical_type::DECIMAL) {
                const auto* extension = t.extension_as<decimal_logical_type_extension>();
                return {static_cast<std::int32_t>(extension->width()),
                        static_cast<std::int32_t>(extension->scale())};
            }
            return {hugeint_precision, 0};
        }

        // The two logical types whose values travel as a decimal128: the
        // engine's fixed-point DECIMAL, and HUGEINT for want of a 128-bit
        // integer type on the Arrow side.
        bool travels_as_decimal(const complex_logical_type& t) {
            return t.type() == logical_type::DECIMAL || t.type() == logical_type::HUGEINT;
        }

        // The ipc type of one column; throws core::EngineError on a logical
        // type with no mapping. Nested types (STRUCT / LIST / ARRAY) are
        // refused here — the record batch stream encodes scalars only, the
        // same contract the old Arrow-based frontend enforced.
        ipc::TypePtr ipc_type_of(const complex_logical_type& t) {
            if (travels_as_decimal(t)) {
                const auto spec = spec_of(t);
                return ipc::decimal128_type(spec.precision, spec.scale);
            }
            switch (t.to_physical_type()) {
                case physical_type::BOOL: return ipc::bool_type();
                case physical_type::UINT8: return ipc::uint8_type();
                case physical_type::UINT16: return ipc::uint16_type();
                case physical_type::UINT32: return ipc::uint32_type();
                case physical_type::UINT64: return ipc::uint64_type();
                case physical_type::INT8: return ipc::int8_type();
                case physical_type::INT16: return ipc::int16_type();
                case physical_type::INT32: return ipc::int32_type();
                case physical_type::INT64: return ipc::int64_type();
                case physical_type::FLOAT: return ipc::float32_type();
                case physical_type::DOUBLE: return ipc::float64_type();
                case physical_type::STRING: return ipc::utf8_type();
                case physical_type::NA: return ipc::null_type();
                case physical_type::INT128:
                    // Only HUGEINT means "128-bit integer" at this width; UUID
                    // shares the physical type with a different meaning and is
                    // refused rather than reinterpreted. (A DECIMAL of this
                    // width was decided above, by its logical type.)
                    if (t.type() == logical_type::HUGEINT) {
                        return ipc::decimal128_type(tsl::hugeint_precision, 0);
                    }
                    break;
                default:
                    break;
            }
            throw core::EngineError("chunk_to_ipc: column '" + field_name(t) + "' has logical type " +
                                    std::to_string(static_cast<int>(t.type())) + " (physical type " +
                                    std::to_string(static_cast<int>(t.to_physical_type())) +
                                    "), which has no Arrow mapping");
        }

        // 10^p for p in 0..38 — the decimal128 precision window, as int128.
        types::int128_t pow10(std::int32_t p) {
            types::int128_t v = 1;
            for (std::int32_t i = 0; i < p; ++i) {
                v *= 10;
            }
            return v;
        }

        // The engine keeps a DECIMAL's unscaled integer at the width its
        // precision needs, so it is read back at that same width and
        // sign-extended; a HUGEINT is already 128 bits wide.
        types::int128_t decimal_raw(const complex_logical_type& type, const logical_value_t& value,
                                    const std::string& column_name) {
            const auto stored_as = type.to_physical_type();
            switch (stored_as) {
                case physical_type::INT16:
                    return static_cast<types::int128_t>(value.value<std::int16_t>());
                case physical_type::INT32:
                    return static_cast<types::int128_t>(value.value<std::int32_t>());
                case physical_type::INT64:
                    return static_cast<types::int128_t>(value.value<std::int64_t>());
                case physical_type::INT128:
                    return value.value<types::int128_t>();
                default:
                    throw core::EngineError("chunk_to_ipc: column '" + column_name +
                                            "' is stored at a width no decimal carrier reads");
            }
        }

        // A decimal128 column: validity bitmap + 16-byte little-endian slots of
        // the unscaled integer (the scale rides in the field type and is never
        // applied to the number, which is exactly what keeps the pair exact).
        ipc::ArrayData decimal_column(const ipc::TypePtr& field_type, const complex_logical_type& col_type,
                                      const data_chunk_t& chunk, std::size_t col) {
            const auto spec = spec_of(col_type);
            const std::string name = field_name(col_type);
            const auto rows = chunk.size();

            ipc::detail::BitmapBuilder validity;
            std::vector<std::uint8_t> data;
            std::int64_t nulls = 0;
            for (std::size_t r = 0; r < rows; ++r) {
                const auto v = chunk.value(col, r);
                if (v.is_null()) {
                    validity.append(false);
                    data.resize(data.size() + 16, 0); // slot placeholder
                    ++nulls;
                    continue;
                }
                const auto raw_storage = tsl::read_unscaled_decimal(col_type, v);
                if (!raw_storage.has_value()) {
                    throw core::EngineError("chunk_to_ipc: column '" + name +
                                            "' is stored at a width no decimal carrier reads");
                }
                const auto raw = *raw_storage;
                // ±Infinity and NaN are ordinary payloads of the storage
                // integer — the extremes of its range — and decimal128 has no
                // representation for any of the three.
                if (col_type.type() == logical_type::DECIMAL &&
                    decimal_special::is_special(col_type.to_physical_type(), raw)) {
                    throw core::EngineError("chunk_to_ipc: column '" + name +
                                            "' holds a non-finite DECIMAL (the engine's Infinity / NaN "
                                            "sentinel), which decimal128 has no representation for");
                }
                // int128 reaches ±1.7e38 while decimal128(38, 0) only reaches
                // ±(10^38 - 1): the top of the engine's HUGEINT range has no
                // precision to be declared under. Refusing keeps every stream
                // readable under the schema it declares.
                if (!tsl::fits_decimal128_precision(raw, spec.precision)) {
                    throw core::EngineError("chunk_to_ipc: column '" + name + "' holds a value decimal128(" +
                                            std::to_string(spec.precision) + ", " + std::to_string(spec.scale) +
                                            ") cannot declare: the unscaled integer must be within ±10^" +
                                            std::to_string(spec.precision));
                }
                validity.append(true);
                ipc::detail::put_le(data, absl::Int128Low64(raw));
                ipc::detail::put_le(data, absl::Int128High64(raw));
            }

            ipc::ArrayData out;
            out.type = field_type;
            out.length = static_cast<std::int64_t>(rows);
            out.null_count = nulls;
            if (nulls > 0) {
                out.buffers.push_back(std::move(validity.bytes));
            } else {
                out.buffers.emplace_back();
            }
            out.buffers.push_back(std::move(data));
            return out;
        }

        template <typename T>
        ipc::ArrayData primitive_column(const ipc::TypePtr& field_type, const data_chunk_t& chunk,
                                        std::size_t col) {
            std::vector<std::optional<T>> values;
            values.reserve(chunk.size());
            for (std::size_t r = 0; r < chunk.size(); ++r) {
                const auto v = chunk.value(col, r);
                values.push_back(v.is_null() ? std::optional<T>{} : std::optional<T>{v.value<T>()});
            }
            return ipc::make_primitive_column(field_type, values);
        }

        ipc::ArrayData utf8_column(const ipc::TypePtr& field_type, const data_chunk_t& chunk,
                                   std::size_t col) {
            std::vector<std::optional<std::string>> values;
            values.reserve(chunk.size());
            for (std::size_t r = 0; r < chunk.size(); ++r) {
                const auto v = chunk.value(col, r);
                if (v.is_null()) {
                    values.emplace_back();
                } else {
                    values.emplace_back(v.value<const std::string&>());
                }
            }
            return ipc::make_utf8_column(field_type, values);
        }

        // The column's type IS null, so a null array is the faithful
        // representation, not a stand-in: no buffers at all, every value null.
        ipc::ArrayData null_column(std::size_t rows) {
            ipc::ArrayData out;
            out.type = ipc::null_type();
            out.length = static_cast<std::int64_t>(rows);
            out.null_count = static_cast<std::int64_t>(rows);
            return out;
        }

        // One column of a chunk, dispatched on the column's own logical type
        // (the physical one says nothing about the scale of a DECIMAL).
        ipc::ArrayData build_column(const ipc::TypePtr& field_type, const complex_logical_type& col_type,
                                    const data_chunk_t& chunk, std::size_t col) {
            if (travels_as_decimal(col_type)) {
                return decimal_column(field_type, col_type, chunk, col);
            }
            switch (col_type.to_physical_type()) {
                case physical_type::BOOL: return primitive_column<bool>(field_type, chunk, col);
                case physical_type::INT8: return primitive_column<std::int8_t>(field_type, chunk, col);
                case physical_type::INT16: return primitive_column<std::int16_t>(field_type, chunk, col);
                case physical_type::INT32: return primitive_column<std::int32_t>(field_type, chunk, col);
                case physical_type::INT64: return primitive_column<std::int64_t>(field_type, chunk, col);
                case physical_type::UINT8: return primitive_column<std::uint8_t>(field_type, chunk, col);
                case physical_type::UINT16: return primitive_column<std::uint16_t>(field_type, chunk, col);
                case physical_type::UINT32: return primitive_column<std::uint32_t>(field_type, chunk, col);
                case physical_type::UINT64: return primitive_column<std::uint64_t>(field_type, chunk, col);
                case physical_type::FLOAT: return primitive_column<float>(field_type, chunk, col);
                case physical_type::DOUBLE: return primitive_column<double>(field_type, chunk, col);
                case physical_type::STRING: return utf8_column(field_type, chunk, col);
                case physical_type::NA: return null_column(chunk.size());
                default:
                    // Unreachable after validate_writable_columns; kept as an
                    // error so a new type can never fall through to a wrong
                    // buffer layout.
                    throw core::EngineError("chunk_to_ipc: column '" + field_name(col_type) +
                                            "' has a type the record batch stream cannot encode");
            }
        }

    } // namespace

    ipc::SchemaPtr schema_to_ipc(const complex_logical_type& schema) {
        OTX_ZONE_N("flight::schema_to_ipc");
        std::vector<ipc::FieldPtr> fields;
        if (schema.type() == logical_type::STRUCT) {
            for (const auto& child : schema.child_types()) {
                fields.push_back(std::make_shared<ipc::Field>(field_name(child), true, ipc_type_of(child)));
            }
        }
        // A schema that is not a STRUCT carries no result set: an empty schema.
        return ipc::make_schema(std::move(fields));
    }

    std::vector<ipc::RecordBatch> chunks_to_ipc(const session_payload& payload, const ipc::SchemaPtr& schema) {
        OTX_ZONE_N("flight::chunks_to_ipc");
        std::pmr::memory_resource* resource = payload.chunks.get_allocator().resource();
        std::vector<ipc::RecordBatch> batches;

        for (const auto& chunk : payload.chunks) {
            if (chunk.empty()) {
                // skip empty chunks but keep scanning for trailing data
                continue;
            }
            if (auto writable = tsl::validate_writable_columns(resource, chunk, "chunk_to_ipc");
                writable.has_error()) {
                throw core::EngineError(writable.error().what.c_str());
            }

            const auto num_fields = schema->fields.size();
            // The schema handed to the client is the contract: every schema
            // field must be fed from exactly one chunk column, matched by name
            // because the chunk's column order may differ. Names are not
            // unique — an engine JOIN result keeps both key columns — so the
            // n-th chunk column named X feeds the n-th schema field named X.
            std::vector<std::uint8_t> field_filled(num_fields, 0);
            std::vector<ipc::ArrayData> columns(num_fields);
            for (std::size_t i = 0; i < chunk.column_count(); ++i) {
                const auto& col_type = chunk.data[i].type();
                if (!col_type.has_alias()) {
                    throw core::EngineError("chunk_to_ipc: result column " + std::to_string(i) +
                                            " has no name and cannot be matched to the schema");
                }
                const std::string& name = col_type.alias();
                std::size_t index = num_fields;
                for (std::size_t j = 0; j < num_fields; ++j) {
                    if (field_filled[j] == 0 && schema->fields[j]->name == name) {
                        index = j;
                        break;
                    }
                }
                if (index == num_fields) {
                    // the chunk carries more than the schema promised; the
                    // extra column is not deliverable through this stream
                    continue;
                }
                field_filled[index] = 1;
                columns[index] = build_column(schema->fields[index]->type, col_type, chunk, i);
            }

            for (std::size_t j = 0; j < num_fields; ++j) {
                if (field_filled[j] == 0) {
                    // An unfed field would finish as an empty array next to
                    // arrays of chunk.size() rows: an invalid batch, not a
                    // batch with NULLs.
                    throw core::EngineError("chunk_to_ipc: schema field '" + schema->fields[j]->name +
                                            "' is missing from the result chunk");
                }
            }

            batches.push_back(ipc::RecordBatch{schema, std::move(columns),
                                               static_cast<std::int64_t>(chunk.size())});
        }
        return batches;
    }

    ipc::SchemaPtr parameter_ipc_schema(std::size_t parameter_count) {
        OTX_ZONE_N("flight::parameter_ipc_schema");
        std::vector<ipc::FieldPtr> fields;
        fields.reserve(parameter_count);
        for (std::size_t i = 0; i < parameter_count; ++i) {
            // int64 "$N": the model the reference drivers bind against
            // (arrow-go refuses to coerce a typed value into a utf8 field);
            // the adapter re-types whatever arrives before the engine binds.
            fields.push_back(std::make_shared<ipc::Field>("$" + std::to_string(i + 1), true,
                                                          ipc::int64_type()));
        }
        return ipc::make_schema(std::move(fields));
    }

} // namespace flight::conv
