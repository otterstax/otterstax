// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "arrow_to_chunk.hpp"

#include "otterbrix/translators/error.hpp"
#include "utility/tracy_profiler.hpp"

// otterbrix's Arrow C data interface importer. Its C structs and Arrow's own
// (arrow/c/abi.h, through bridge.h) share the ARROW_C_DATA_INTERFACE guard and
// layout, so whichever header comes first defines them for both.
#include <components/vector/arrow/arrow_converter.hpp>
#include <components/vector/arrow/scaner/arrow_type.hpp>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/type.h>
// arrow/array.h only forward-declares Decimal128; the precision check needs the definition.
#include <arrow/util/decimal.h>

#include <bit>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

// Not `using namespace components::vector`: it holds a namespace `arrow` (the
// importer's), which would make every `arrow::` below ambiguous.
using components::vector::data_chunk_t;
using namespace components::types;

namespace tsl {

namespace impl {

    // Internal linkage: other translators reuse names in tsl, and an external definition would be
    // merged with theirs at link time.
    namespace {

    namespace ca = components::vector::arrow;

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

    // The two Arrow types read here; otterbrix's importer reads (or refuses) every other one.
    //  - decimal128: the importer narrows values with an unchecked cast and reads (p, 0) as
    //    DECIMAL where our writers' HUGEINT carrier needs HUGEINT.
    //  - null: the importer drops the column's Arrow owner — for column 0, every column's.
    bool read_by_shim(arrow::Type::type id) { return id == arrow::Type::DECIMAL128 || id == arrow::Type::NA; }

    // The engine type of a read_by_shim column.
    core::result_wrapper_t<complex_logical_type> shim_type(std::pmr::memory_resource* res, const arrow::Field& field) {
        const auto& type = *field.type();
        switch (type.id()) {
            case arrow::Type::NA:
                return complex_logical_type{logical_type::NA, field.name().c_str()};
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

    // One cell of a read_by_shim column. An error is a value this column's own type cannot hold —
    // the type has a reading, this value has none.
    core::error_t read_cell(data_chunk_t& chunk, size_t col, size_t row, const arrow::Array& arr) {
        auto* res = chunk.resource();
        const auto r = static_cast<int64_t>(row);
        // Every slot of a null-typed column is null.
        if (arr.IsNull(r) || arr.type_id() == arrow::Type::NA) {
            chunk.set_value(col, row, logical_value_t{res, nullptr});
            return core::error_t::no_error();
        }
        switch (arr.type_id()) {
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
                    return core::error_t::no_error();
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
                return core::error_t::no_error();
            }
            default:
                return make_error(res,
                                  core::error_code_t::conversion_failure,
                                  "arrow_to_chunk: column '" + chunk.data[col].type().alias() + "' has Arrow type " +
                                      arr.type()->ToString() + ", which has no reading");
        }
    }

    // The engine type otterbrix's importer reads each field of `schema` as, named after the field
    // (the importer keeps names apart from its types, and renames an empty or a repeated one; the
    // file's own name is kept here). A field it has no reading for is refused naming the column.
    core::result_wrapper_t<std::pmr::vector<complex_logical_type>> imported_types(std::pmr::memory_resource* res,
                                                                                  const arrow::Schema& schema) {
        ca::arrow_schema_wrapper_t whole;
        if (auto exported = arrow::ExportSchema(schema, &whole.arrow_schema); !exported.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure, "arrow_to_chunk: export schema", exported);
        }
        auto table = ca::schema_from_arrow(res, &whole.arrow_schema);
        if (table.has_error()) {
            // The importer's verdict does not say which column; asking it one field at a time does.
            for (int i = 0; i < schema.num_fields(); ++i) {
                ca::arrow_schema_wrapper_t one;
                if (!arrow::ExportSchema(arrow::Schema({schema.field(i)}), &one.arrow_schema).ok()) {
                    continue;
                }
                auto single = ca::schema_from_arrow(res, &one.arrow_schema);
                if (single.has_error()) {
                    return make_error(res,
                                      core::error_code_t::conversion_failure,
                                      "arrow_to_chunk: column '" + schema.field(i)->name() + "' has Arrow type " +
                                          schema.field(i)->type()->ToString() + ", which has no reading in the engine: " +
                                          single.error().what.c_str());
                }
            }
            return make_error(res,
                              core::error_code_t::conversion_failure,
                              std::string{"arrow_to_chunk: "} + table.error().what.c_str());
        }
        std::pmr::vector<complex_logical_type> types(res);
        types.reserve(static_cast<size_t>(schema.num_fields()));
        for (int i = 0; i < schema.num_fields(); ++i) {
            types.push_back(table.value().get_types()[static_cast<size_t>(i)]);
            types.back().set_alias(schema.field(i)->name());
        }
        return types;
    }

    // data_chunk_from_arrow leaves the buffers' owner with column 0 only (and a dictionary column
    // loses even that), so every column gets a second, zero-copy export of the batch as its owner:
    // the buffers then live as long as ANY column does.
    core::error_t hold_batch_in_every_column(std::pmr::memory_resource* res,
                                             const arrow::RecordBatch& batch,
                                             data_chunk_t& chunk) {
        ca::arrow_array_wrapper_ptr owner{new ca::arrow_array_wrapper_t()};
        if (auto exported = arrow::ExportRecordBatch(batch, &owner->arrow_array); !exported.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure, "arrow_to_chunk: export owner", exported);
        }
        for (uint64_t c = 0; c < chunk.column_count(); ++c) {
            if (auto buffer = chunk.data[c].get_buffer()) {
                buffer->set_auxiliary(std::make_unique<ca::arrow_auxiliary_data_t>(owner));
            }
        }
        return core::error_t::no_error();
    }

    // Every column of `batch` through the Arrow C data interface: Arrow exports its buffers
    // without copying them, and otterbrix's importer reads them into a chunk that keeps them alive,
    // each column named after its field.
    core::result_wrapper_t<data_chunk_t> import_columns(std::pmr::memory_resource* res,
                                                        const arrow::RecordBatch& batch) {
        ca::arrow_schema_wrapper_t schema;
        ca::arrow_array_wrapper_t array;
        if (auto exported = arrow::ExportRecordBatch(batch, &array.arrow_array, &schema.arrow_schema);
            !exported.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure, "arrow_to_chunk: export", exported);
        }
        auto table = ca::schema_from_arrow(res, &schema.arrow_schema);
        if (table.has_error()) {
            // The refusal naming the column (imported_types asks one field at a time).
            auto named = imported_types(res, *batch.schema());
            return named.has_error() ? named.error() : table.error();
        }
        auto imported = ca::data_chunk_from_arrow(res, &array.arrow_array, std::move(table.value()));
        if (imported.has_error()) {
            return imported;
        }
        if (auto held = hold_batch_in_every_column(res, batch, imported.value()); held.contains_error()) {
            return held;
        }
        for (int c = 0; c < batch.num_columns(); ++c) {
            imported.value().data[static_cast<size_t>(c)].set_type_alias(batch.schema()->field(c)->name());
        }
        return imported;
    }

    } // namespace

} // namespace impl

core::result_wrapper_t<data_chunk_t> arrow_to_chunk(std::pmr::memory_resource* res,
                                                     const std::shared_ptr<arrow::RecordBatch>& batch) {
    OTX_ZONE_N("tsl::arrow_to_chunk");
    if (!batch || batch->num_rows() == 0) {
        return data_chunk_t(res, {}, 0);
    }
    const int ncols = batch->num_columns();
    const int64_t nrows = batch->num_rows();

    std::vector<int> native;
    std::vector<int> own;
    for (int c = 0; c < ncols; c++) {
        (impl::read_by_shim(batch->column(c)->type_id()) ? own : native).push_back(c);
    }

    // Every column read by the importer: the imported chunk is the answer.
    if (own.empty()) {
        return impl::import_columns(res, *batch);
    }

    // Otherwise the importer reads its columns apart, and the chunk takes their buffers (and, for a
    // nested column, their children) next to the columns this shim reads cell by cell.
    data_chunk_t imported(res, {}, 0);
    if (!native.empty()) {
        auto view = batch->SelectColumns(native); // shares the columns, copies nothing
        if (!view.ok()) {
            return arrow_error(res, core::error_code_t::conversion_failure, "arrow_to_chunk: select columns",
                               view.status());
        }
        auto read = impl::import_columns(res, **view);
        if (read.has_error()) {
            return read;
        }
        imported = std::move(read.value());
    }

    std::pmr::vector<complex_logical_type> types(res);
    types.reserve(static_cast<size_t>(ncols));
    for (int c = 0, next_native = 0; c < ncols; c++) {
        if (!impl::read_by_shim(batch->column(c)->type_id())) {
            types.push_back(imported.data[static_cast<size_t>(next_native++)].type());
            continue;
        }
        auto type = impl::shim_type(res, *batch->schema()->field(c));
        if (type.has_error()) {
            return type.convert_error<data_chunk_t>();
        }
        types.push_back(std::move(type.value()));
    }

    data_chunk_t chunk(res, types, static_cast<size_t>(nrows));
    chunk.set_cardinality(static_cast<size_t>(nrows));
    for (size_t i = 0; i < native.size(); ++i) {
        chunk.data[static_cast<size_t>(native[i])].reinterpret(imported.data[i]);
    }
    for (int c : own) {
        const auto& arr = *batch->column(c);
        for (int64_t r = 0; r < nrows; r++) {
            if (auto read = impl::read_cell(chunk, static_cast<size_t>(c), static_cast<size_t>(r), arr);
                read.contains_error()) {
                return read;
            }
        }
    }
    return chunk;
}

core::result_wrapper_t<complex_logical_type> arrow_schema_to_struct(std::pmr::memory_resource* res,
                                                                     const std::shared_ptr<arrow::Schema>& schema) {
    OTX_ZONE_N("tsl::arrow_schema_to_struct");
    // The same answer arrow_to_chunk's load gives, so a table is never created with a column type
    // the load would then refuse or read differently: the importer's types for its columns, this
    // shim's for decimal128 and null.
    std::vector<std::shared_ptr<arrow::Field>> native_fields;
    for (const auto& field : schema->fields()) {
        if (!impl::read_by_shim(field->type()->id())) {
            native_fields.push_back(field);
        }
    }
    std::pmr::vector<complex_logical_type> native(res);
    if (!native_fields.empty()) {
        auto imported = impl::imported_types(res, arrow::Schema(native_fields));
        if (imported.has_error()) {
            return imported.error();
        }
        native = std::move(imported.value());
    }
    std::pmr::vector<complex_logical_type> fields(res);
    fields.reserve(static_cast<size_t>(schema->num_fields()));
    size_t next_native = 0;
    for (const auto& field : schema->fields()) {
        if (!impl::read_by_shim(field->type()->id())) {
            fields.push_back(std::move(native[next_native++]));
            continue;
        }
        auto own = impl::shim_type(res, *field);
        if (own.has_error()) {
            return own.error();
        }
        fields.push_back(std::move(own.value()));
    }
    return complex_logical_type::create_struct("", fields);
}

} // namespace tsl
