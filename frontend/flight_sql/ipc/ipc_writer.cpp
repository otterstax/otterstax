// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "ipc_writer.hpp"

#include "Message_generated.h"
#include "Schema_generated.h"

#include <flatbuffers/flatbuffers.h>

#include <stdexcept>

namespace flight::ipc {

namespace fb = org::apache::arrow::flatbuf;

namespace {

constexpr std::size_t kAlignment = 8;

std::size_t align8(std::size_t v) { return (v + kAlignment - 1) / kAlignment * kAlignment; }

// --- serializing a column type into the flatbuffers union ------------------

struct TypeSerialization {
    fb::Type type_enum;
    flatbuffers::Offset<void> offset;
};

TypeSerialization serialize_type(const Type& t, flatbuffers::FlatBufferBuilder& fbb) {
    switch (t.id) {
        case TypeId::Null:
            return {fb::Type::Null, fb::CreateNull(fbb).Union()};
        case TypeId::Bool:
            return {fb::Type::Bool, fb::CreateBool(fbb).Union()};
        case TypeId::Int8:
            return {fb::Type::Int, fb::CreateInt(fbb, 8, true).Union()};
        case TypeId::Int16:
            return {fb::Type::Int, fb::CreateInt(fbb, 16, true).Union()};
        case TypeId::Int32:
            return {fb::Type::Int, fb::CreateInt(fbb, 32, true).Union()};
        case TypeId::Int64:
            return {fb::Type::Int, fb::CreateInt(fbb, 64, true).Union()};
        case TypeId::UInt8:
            return {fb::Type::Int, fb::CreateInt(fbb, 8, false).Union()};
        case TypeId::UInt16:
            return {fb::Type::Int, fb::CreateInt(fbb, 16, false).Union()};
        case TypeId::UInt32:
            return {fb::Type::Int, fb::CreateInt(fbb, 32, false).Union()};
        case TypeId::UInt64:
            return {fb::Type::Int, fb::CreateInt(fbb, 64, false).Union()};
        case TypeId::Float32:
            return {fb::Type::FloatingPoint, fb::CreateFloatingPoint(fbb, fb::Precision::SINGLE).Union()};
        case TypeId::Float64:
            return {fb::Type::FloatingPoint, fb::CreateFloatingPoint(fbb, fb::Precision::DOUBLE).Union()};
        case TypeId::Utf8:
            return {fb::Type::Utf8, fb::CreateUtf8(fbb).Union()};
        case TypeId::Binary:
            return {fb::Type::Binary, fb::CreateBinary(fbb).Union()};
        case TypeId::Decimal128:
            return {fb::Type::Decimal, fb::CreateDecimal(fbb, t.precision, t.scale, 128).Union()};
        case TypeId::Date32:
            return {fb::Type::Date, fb::CreateDate(fbb, fb::DateUnit::DAY).Union()};
        case TypeId::Date64:
            return {fb::Type::Date, fb::CreateDate(fbb, fb::DateUnit::MILLISECOND).Union()};
        case TypeId::Timestamp: {
            fb::TimeUnit unit;
            switch (t.unit) {
                case Type::TimeUnit::Second: unit = fb::TimeUnit::SECOND; break;
                case Type::TimeUnit::Millisecond: unit = fb::TimeUnit::MILLISECOND; break;
                case Type::TimeUnit::Microsecond: unit = fb::TimeUnit::MICROSECOND; break;
                case Type::TimeUnit::Nanosecond: unit = fb::TimeUnit::NANOSECOND; break;
            }
            flatbuffers::Offset<flatbuffers::String> tz;
            if (!t.timezone.empty()) {
                tz = fbb.CreateString(t.timezone);
            }
            return {fb::Type::Timestamp, fb::CreateTimestamp(fbb, unit, tz).Union()};
        }
        default:
            throw std::runtime_error("flight::ipc: nested types need Field serialization");
    }
}

flatbuffers::Offset<fb::Field> serialize_field(const Field& field, flatbuffers::FlatBufferBuilder& fbb);

std::vector<flatbuffers::Offset<fb::Field>> serialize_children(const Type& t,
                                                               flatbuffers::FlatBufferBuilder& fbb) {
    std::vector<flatbuffers::Offset<fb::Field>> out;
    for (const auto& child : t.children) {
        out.push_back(serialize_field(*child, fbb));
    }
    return out;
}

// Type serialization including nested ones (list/struct/union/map):
// children go on the Field, the type tables themselves are empty.
TypeSerialization serialize_type_full(const Type& t, flatbuffers::FlatBufferBuilder& fbb) {
    switch (t.id) {
        case TypeId::List:
            return {fb::Type::List, fb::CreateList(fbb).Union()};
        case TypeId::Struct:
            return {fb::Type::Struct_, fb::CreateStruct_(fbb).Union()};
        case TypeId::DenseUnion:
            return {fb::Type::Union,
                    fb::CreateUnion(fbb, fb::UnionMode::Dense, fbb.CreateVector(t.union_type_ids))
                        .Union()};
        case TypeId::Map:
            return {fb::Type::Map, fb::CreateMap(fbb, false).Union()};
        default:
            return serialize_type(t, fbb);
    }
}

flatbuffers::Offset<fb::Field> serialize_field(const Field& field, flatbuffers::FlatBufferBuilder& fbb) {
    auto name = fbb.CreateString(field.name);
    auto type = serialize_type_full(*field.type, fbb);
    auto children = serialize_children(*field.type, fbb);
    return fb::CreateField(fbb, name, field.nullable, type.type_enum, type.offset, 0,
                           children.empty() ? 0 : fbb.CreateVector(children));
}

// --- walking the columns: FieldNode + Buffer in IPC order ------------------

struct BodyLayout {
    std::vector<fb::FieldNode> nodes;
    std::vector<fb::Buffer> buffer_meta;
    std::vector<std::uint8_t> body;

    void add_buffer(const std::vector<std::uint8_t>& bytes) {
        const std::size_t offset = body.size();
        body.insert(body.end(), bytes.begin(), bytes.end());
        body.resize(align8(body.size()));
        buffer_meta.emplace_back(offset, bytes.size());
    }
};

void walk_array(const ArrayData& arr, BodyLayout& out) {
    out.nodes.emplace_back(arr.length, arr.null_count);
    switch (arr.type->id) {
        case TypeId::List:
        case TypeId::Map:
            for (std::size_t i = 0; i < 2 && i < arr.buffers.size(); ++i) {
                out.add_buffer(arr.buffers[i]);
            }
            for (const auto& child : arr.children) walk_array(child, out);
            break;
        case TypeId::Struct:
            if (!arr.buffers.empty()) out.add_buffer(arr.buffers[0]);
            for (const auto& child : arr.children) walk_array(child, out);
            break;
        case TypeId::DenseUnion:
            for (const auto& b : arr.buffers) out.add_buffer(b);
            for (const auto& child : arr.children) walk_array(child, out);
            break;
        default:
            for (const auto& b : arr.buffers) out.add_buffer(b);
            break;
    }
}

} // namespace

std::vector<std::uint8_t> serialize_schema_message(const Schema& schema) {
    flatbuffers::FlatBufferBuilder fbb;
    std::vector<flatbuffers::Offset<fb::Field>> fields;
    for (const auto& field : schema.fields) {
        fields.push_back(serialize_field(*field, fbb));
    }
    auto schema_offset = fb::CreateSchema(fbb, fb::Endianness::Little, fbb.CreateVector(fields), 0, 0);
    auto message = fb::CreateMessage(fbb, fb::MetadataVersion::V5, fb::MessageHeader::Schema,
                                     schema_offset.Union(), 0);
    fbb.Finish(message);
    const std::uint8_t* begin = fbb.GetBufferPointer();
    std::vector<std::uint8_t> out{begin, begin + fbb.GetSize()};
    // Clients (arrow-go) reconstruct the IPC stream from FlightData and expect
    // the metadata to be a multiple of 8 — pad with zeros (flatbuffers allows it).
    out.resize(align8(out.size()));
    return out;
}

std::vector<std::uint8_t> encapsulate(const std::vector<std::uint8_t>& bare_message,
                                      const std::vector<std::uint8_t>& body) {
    // The metadata is zero-padded TO a multiple of 8 inside the declared
    // length: readers (pyarrow/arrow) do not skip outer padding after metadata.
    std::vector<std::uint8_t> meta = bare_message;
    meta.resize(align8(meta.size()));

    std::vector<std::uint8_t> out;
    out.reserve(meta.size() + body.size() + 16);
    // continuation + little-endian u32 length
    out.push_back(0xFF); out.push_back(0xFF); out.push_back(0xFF); out.push_back(0xFF);
    const std::uint32_t len = static_cast<std::uint32_t>(meta.size());
    out.push_back(static_cast<std::uint8_t>(len));
    out.push_back(static_cast<std::uint8_t>(len >> 8));
    out.push_back(static_cast<std::uint8_t>(len >> 16));
    out.push_back(static_cast<std::uint8_t>(len >> 24));
    out.insert(out.end(), meta.begin(), meta.end());
    out.insert(out.end(), body.begin(), body.end());
    out.resize(align8(out.size()));
    return out;
}

std::vector<std::uint8_t> schema_ipc_bytes(const Schema& schema) {
    return encapsulate(serialize_schema_message(schema), {});
}

RecordBatchMessage serialize_record_batch(const RecordBatch& batch) {
    BodyLayout layout;
    for (const auto& column : batch.columns) {
        walk_array(column, layout);
    }

    flatbuffers::FlatBufferBuilder fbb;
    auto nodes = fbb.CreateVectorOfStructs(layout.nodes);
    auto buffers = fbb.CreateVectorOfStructs(layout.buffer_meta);
    auto rb = fb::CreateRecordBatch(fbb, batch.num_rows, nodes, buffers, 0);
    auto message = fb::CreateMessage(fbb, fb::MetadataVersion::V5, fb::MessageHeader::RecordBatch,
                                     rb.Union(), static_cast<std::int64_t>(layout.body.size()));
    fbb.Finish(message);
    const std::uint8_t* begin = fbb.GetBufferPointer();
    RecordBatchMessage out;
    out.bare_message = {begin, begin + fbb.GetSize()};
    out.bare_message.resize(align8(out.bare_message.size())); // see serialize_schema_message
    out.body = std::move(layout.body);
    return out;
}


} // namespace flight::ipc
