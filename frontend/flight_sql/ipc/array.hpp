// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Arrow data columns: buffers + children (list/struct/union/map) and builders.

#include "types.hpp"

#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace flight::ipc {

struct ArrayData {
    TypePtr type;
    std::int64_t length = 0;
    std::int64_t null_count = 0;

    // Flat column buffers in Arrow IPC order:
    //  primitives:   [validity?, data]
    //  utf8/binary:  [validity?, offsets(int32), data]
    //  list/map:     [validity?, offsets(int32)] + children[0]
    //  struct:       [validity?] + children
    //  dense_union:  [type_ids(int8), offsets(int32)] + children
    std::vector<std::vector<std::uint8_t>> buffers;
    std::vector<ArrayData> children;
};

struct RecordBatch {
    SchemaPtr schema;
    std::vector<ArrayData> columns;
    std::int64_t num_rows = 0;
};

namespace detail {

inline void put_u8(std::vector<std::uint8_t>& out, std::uint8_t v) { out.push_back(v); }

template <typename T>
void put_le(std::vector<std::uint8_t>& out, T v) {
    static_assert(std::is_trivially_copyable_v<T>);
    const auto* p = reinterpret_cast<const std::uint8_t*>(&v);
    out.insert(out.end(), p, p + sizeof(T));
}

// Bitmap: bit i (LSB first) = 1 -> the value is valid
struct BitmapBuilder {
    std::vector<std::uint8_t> bytes;
    std::int64_t bits = 0;
    std::int64_t set_bits = 0;

    void append(bool v) {
        const std::size_t byte = static_cast<std::size_t>(bits / 8);
        if (byte >= bytes.size()) bytes.push_back(0);
        if (v) {
            bytes[byte] |= static_cast<std::uint8_t>(1u << (bits % 8));
            ++set_bits;
        }
        ++bits;
    }
};

template <typename T>
struct PrimitiveBuilder {
    std::vector<std::uint8_t> validity;
    std::vector<std::uint8_t> data;
    BitmapBuilder bits;
    std::int64_t length = 0;
    std::int64_t nulls = 0;

    void append(const std::optional<T>& v) {
        if (!v.has_value()) {
            bits.append(false);
            put_le<T>(data, T{}); // slot placeholder
            ++nulls;
        } else {
            bits.append(true);
            put_le<T>(data, *v);
        }
        ++length;
    }

    ArrayData finish(TypePtr type) const {
        ArrayData out;
        out.type = std::move(type);
        out.length = length;
        out.null_count = nulls;
        if (nulls > 0) {
            out.buffers.push_back(bits.bytes);
        } else {
            out.buffers.emplace_back(); // no validity (len 0)
        }
        out.buffers.push_back(data);
        return out;
    }
};

struct VarBinaryBuilder {
    BitmapBuilder bits;
    std::vector<std::uint8_t> offsets; // int32: length+1 entries
    std::vector<std::uint8_t> data;
    std::int64_t length = 0;
    std::int64_t nulls = 0;
    std::int32_t last_offset = 0;

    VarBinaryBuilder() { put_le<std::int32_t>(offsets, 0); }

    void append(const std::optional<std::string>& v) {
        if (!v.has_value()) {
            bits.append(false);
            put_le<std::int32_t>(offsets, last_offset);
            ++nulls;
        } else {
            bits.append(true);
            data.insert(data.end(), v->begin(), v->end());
            last_offset = static_cast<std::int32_t>(data.size());
            put_le<std::int32_t>(offsets, last_offset);
        }
        ++length;
    }

    ArrayData finish(TypePtr type) const {
        ArrayData out;
        out.type = std::move(type);
        out.length = length;
        out.null_count = nulls;
        if (nulls > 0) {
            out.buffers.push_back(bits.bytes);
        } else {
            out.buffers.emplace_back();
        }
        out.buffers.push_back(offsets);
        out.buffers.push_back(data);
        return out;
    }
};

} // namespace detail

using detail::put_le;
using detail::put_u8;

// Convenience column factories from std::vector<std::optional<T>>.
template <typename T>
ArrayData make_primitive_column(TypePtr type, const std::vector<std::optional<T>>& values) {
    detail::PrimitiveBuilder<T> b;
    for (const auto& v : values) b.append(v);
    return b.finish(std::move(type));
}

// Bool is special: Arrow stores the values themselves as a bitmap (1 bit per
// value), not one byte per slot — a byte-per-bool data buffer would be read by
// every Arrow client as a bitmap and every value after the first would land in
// the wrong row.
inline ArrayData make_primitive_column(TypePtr type, const std::vector<std::optional<bool>>& values) {
    ArrayData out;
    out.type = std::move(type);
    out.length = static_cast<std::int64_t>(values.size());
    detail::BitmapBuilder validity;
    detail::BitmapBuilder data;
    for (const auto& v : values) {
        validity.append(v.has_value());
        data.append(v.value_or(false));
        if (!v.has_value()) ++out.null_count;
    }
    if (out.null_count > 0) {
        out.buffers.push_back(std::move(validity.bytes));
    } else {
        out.buffers.emplace_back();
    }
    out.buffers.push_back(std::move(data.bytes));
    return out;
}

inline ArrayData make_utf8_column(TypePtr type, const std::vector<std::optional<std::string>>& values) {
    detail::VarBinaryBuilder b;
    for (const auto& v : values) b.append(v);
    return b.finish(std::move(type));
}

// Fixed-width raw column: validity bitmap (empty when no nulls) + data slots
// copied verbatim. Slot width is implied by the type (e.g. 16 bytes for
// Decimal128); data.size() must be length * slot_width.
inline ArrayData make_fixed_width_column(TypePtr type, std::int64_t length, std::int64_t nulls,
                                         std::vector<std::uint8_t> validity,
                                         std::vector<std::uint8_t> data) {
    ArrayData out;
    out.type = std::move(type);
    out.length = length;
    out.null_count = nulls;
    if (nulls > 0) {
        out.buffers.push_back(std::move(validity));
    } else {
        out.buffers.emplace_back();
    }
    out.buffers.push_back(std::move(data));
    return out;
}

// list<child>: offsets are assembled from the child slice lengths.
inline ArrayData make_list_column(TypePtr type, const std::vector<std::optional<std::pair<std::int32_t, std::int32_t>>>& ranges,
                                  ArrayData child) {
    // ranges[i] = {offset, length} inside the child array
    ArrayData out;
    out.type = std::move(type);
    out.length = static_cast<std::int64_t>(ranges.size());
    detail::BitmapBuilder bits;
    std::vector<std::uint8_t> offsets;
    put_le<std::int32_t>(offsets, 0);
    std::int32_t cur = 0;
    std::int64_t nulls = 0;
    for (const auto& r : ranges) {
        if (!r.has_value()) {
            bits.append(false);
            put_le<std::int32_t>(offsets, cur);
            ++nulls;
        } else {
            bits.append(true);
            cur += r->second;
            put_le<std::int32_t>(offsets, cur);
        }
    }
    out.null_count = nulls;
    if (nulls > 0) {
        out.buffers.push_back(bits.bytes);
    } else {
        out.buffers.emplace_back();
    }
    out.buffers.push_back(offsets);
    out.children.push_back(std::move(child));
    return out;
}

// dense_union: type_ids + offsets over the children (children are full-sized to the max).
inline ArrayData make_dense_union_column(TypePtr type, const std::vector<std::int8_t>& type_ids,
                                         const std::vector<std::int32_t>& value_offsets,
                                         std::vector<ArrayData> children) {
    ArrayData out;
    out.type = std::move(type);
    out.length = static_cast<std::int64_t>(type_ids.size());
    out.null_count = 0; // null in a union = a separate Null-type variant
    std::vector<std::uint8_t> ids;
    for (auto id : type_ids) put_u8(ids, static_cast<std::uint8_t>(id));
    std::vector<std::uint8_t> offs;
    for (auto o : value_offsets) put_le<std::int32_t>(offs, o);
    out.buffers.push_back(std::move(ids));
    out.buffers.push_back(std::move(offs));
    out.children = std::move(children);
    return out;
}

// map: like list over struct{key,value}, no top-level nulls.
inline ArrayData make_map_column(TypePtr type, const std::vector<std::int32_t>& lengths,
                                 ArrayData entries_struct) {
    ArrayData out;
    out.type = std::move(type);
    out.length = static_cast<std::int64_t>(lengths.size());
    out.null_count = 0;
    out.buffers.emplace_back(); // no validity
    std::vector<std::uint8_t> offsets;
    put_le<std::int32_t>(offsets, 0);
    std::int32_t cur = 0;
    for (auto len : lengths) {
        cur += len;
        put_le<std::int32_t>(offsets, cur);
    }
    out.buffers.push_back(std::move(offsets));
    out.children.push_back(std::move(entries_struct));
    return out;
}

// struct column: validity + children only.
inline ArrayData make_struct_column(TypePtr type, std::int64_t length, std::int64_t nulls,
                                    std::vector<ArrayData> children) {
    ArrayData out;
    out.type = std::move(type);
    out.length = length;
    out.null_count = nulls;
    if (nulls > 0) {
        throw std::runtime_error("flight::ipc: null struct fields not supported in builder");
    }
    out.buffers.emplace_back();
    out.children = std::move(children);
    return out;
}

// Slice of a column [offset, offset+length) — for LIMIT-style truncation.
// Buffers/offsets are rebuilt correctly; list/map/union children are left
// as is (their offsets keep pointing inside the child).
ArrayData slice_array(const ArrayData& arr, std::int64_t offset, std::int64_t length);

inline RecordBatch slice_batch(const RecordBatch& batch, std::int64_t offset, std::int64_t length) {
    RecordBatch out;
    out.schema = batch.schema;
    out.num_rows = length;
    out.columns.reserve(batch.columns.size());
    for (const auto& col : batch.columns) {
        out.columns.push_back(slice_array(col, offset, length));
    }
    return out;
}

} // namespace flight::ipc
