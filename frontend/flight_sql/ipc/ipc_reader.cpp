// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "ipc_reader.hpp"

#include "Message_generated.h"
#include "Schema_generated.h"

#include <cstring>
#include <stdexcept>

namespace flight::ipc {

namespace fb = org::apache::arrow::flatbuf;

namespace {

TypePtr parse_type(const fb::Field* field);

SchemaPtr schema_from_fb(const fb::Schema* s) {
    std::vector<FieldPtr> fields;
    if (s->fields()) {
        for (const auto* f : *s->fields()) {
            TypePtr t = parse_type(f);
            std::string name = f->name() ? f->name()->str() : "";
            fields.push_back(std::make_shared<Field>(std::move(name), f->nullable(), std::move(t)));
        }
    }
    return make_schema(std::move(fields));
}

std::vector<FieldPtr> children_of(const fb::Field* field) {
    std::vector<FieldPtr> out;
    if (field->children()) {
        for (const auto* c : *field->children()) {
            out.push_back(std::make_shared<Field>(c->name() ? c->name()->str() : "",
                                                  c->nullable(), parse_type(c)));
        }
    }
    return out;
}

TypePtr parse_type(const fb::Field* field) {
    switch (field->type_type()) {
        case fb::Type::Null: return null_type();
        case fb::Type::Bool: return bool_type();
        case fb::Type::Int: {
            const auto* t = field->type_as_Int();
            const int bits = t->bitWidth();
            if (t->is_signed()) {
                switch (bits) {
                    case 8: return int8_type();
                    case 16: return int16_type();
                    case 32: return int32_type();
                    case 64: return int64_type();
                }
            } else {
                switch (bits) {
                    case 8: return uint8_type();
                    case 16: return uint16_type();
                    case 32: return uint32_type();
                    case 64: return uint64_type();
                }
            }
            break;
        }
        case fb::Type::FloatingPoint: {
            const auto* t = field->type_as_FloatingPoint();
            if (t->precision() == fb::Precision::SINGLE) return float32_type();
            if (t->precision() == fb::Precision::DOUBLE) return float64_type();
            break;
        }
        case fb::Type::Utf8: return utf8_type();
        case fb::Type::Binary: return binary_type();
        case fb::Type::Decimal: {
            const auto* t = field->type_as_Decimal();
            if (t->bitWidth() == 128) {
                return decimal128_type(t->precision(), t->scale());
            }
            break;
        }
        case fb::Type::Date: {
            const auto* t = field->type_as_Date();
            return t->unit() == fb::DateUnit::DAY ? date32_type() : date64_type();
        }
        case fb::Type::Timestamp: {
            const auto* t = field->type_as_Timestamp();
            Type::TimeUnit unit = Type::TimeUnit::Microsecond;
            switch (t->unit()) {
                case fb::TimeUnit::SECOND: unit = Type::TimeUnit::Second; break;
                case fb::TimeUnit::MILLISECOND: unit = Type::TimeUnit::Millisecond; break;
                case fb::TimeUnit::NANOSECOND: unit = Type::TimeUnit::Nanosecond; break;
                default: break;
            }
            std::string tz = t->timezone() ? t->timezone()->str() : "";
            return timestamp_type(unit, std::move(tz));
        }
        case fb::Type::List: {
            auto children = children_of(field);
            if (children.size() == 1) {
                TypePtr t = std::make_shared<Type>(Type{TypeId::List});
                const_cast<Type*>(t.get())->children.push_back(children[0]);
                return t;
            }
            break;
        }
        case fb::Type::Map: {
            auto children = children_of(field);
            if (children.size() == 1) {
                TypePtr t = std::make_shared<Type>(Type{TypeId::Map});
                const_cast<Type*>(t.get())->children.push_back(children[0]);
                return t;
            }
            break;
        }
        case fb::Type::Struct_: {
            TypePtr t = std::make_shared<Type>(Type{TypeId::Struct});
            const_cast<Type*>(t.get())->children = children_of(field);
            return t;
        }
        default: break;
    }
    // unknown/unsupported type — a null stub (the reader will not decode it)
    return null_type();
}

template <typename T>
T read_le(const std::uint8_t* p) {
    T v;
    std::memcpy(&v, p, sizeof(T));
    return v;
}

bool bit_at(const std::uint8_t* bitmap, std::size_t bytes_len, std::int64_t i) {
    if (bitmap == nullptr || bytes_len == 0) return true; // no validity = all valid
    const std::size_t byte = static_cast<std::size_t>(i / 8);
    if (byte >= bytes_len) return true;
    return (bitmap[byte] >> (i % 8)) & 1;
}

// Decoder of a scalar column: consumes one FieldNode + its buffers from the
// header sequence (the depth-first walk mirrors the writer's walk_array).
std::vector<Value> decode_column(const Type& type, const std::uint8_t* body,
                                 std::size_t body_size,
                                 const flatbuffers::Vector<const fb::Buffer*>& buffers,
                                 std::size_t& cur_buffer,
                                 const flatbuffers::Vector<const fb::FieldNode*>& nodes,
                                 std::size_t& cur_node, std::int64_t rows) {
    const fb::FieldNode* node = nodes.Get(cur_node++);
    std::vector<Value> out(static_cast<std::size_t>(rows));

    const bool has_validity = buffers.Get(cur_buffer)->length() > 0;
    const std::uint8_t* validity = body + buffers.Get(cur_buffer)->offset();
    const std::size_t validity_len = buffers.Get(cur_buffer)->length();
    ++cur_buffer;

    auto is_valid = [&](std::int64_t r) {
        if (!has_validity) return node->null_count() == 0;
        if (r >= node->length()) return false;
        return bit_at(validity, validity_len, r);
    };

    switch (type.id) {
        case TypeId::Bool: {
            const auto* b = buffers.Get(cur_buffer++);
            for (std::int64_t r = 0; r < rows; ++r) {
                // the bitmap bit is an int; cast it or the Value variant takes
                // it for the int64 alternative instead of bool
                out[r] = is_valid(r)
                             ? Value{static_cast<bool>((body[b->offset() + r / 8] >> (r % 8)) & 1)}
                             : Value{};
            }
            break;
        }
// Reads a fixed-width slot at its OWN storage width (STORAGE) and widens it
// to the variant alternative (CXX): reading the wide type directly would
// consume the neighbouring slot's bytes on every narrow column (an int32
// column read as int64 glued two ids into one value).
#define FLIGHT_PRIMITIVE_CASE(ID, STORAGE, CXX)                                                       \
    case TypeId::ID: {                                                                                \
        const auto* b = buffers.Get(cur_buffer++);                                                    \
        for (std::int64_t r = 0; r < rows; ++r) {                                                     \
            out[r] = is_valid(r)                                                                      \
                         ? Value{static_cast<CXX>(read_le<STORAGE>(body + b->offset() + r * sizeof(STORAGE)))} \
                         : Value{};                                                                   \
        }                                                                                             \
        break;                                                                                        \
    }
        FLIGHT_PRIMITIVE_CASE(Int8, std::int8_t, std::int64_t)
        FLIGHT_PRIMITIVE_CASE(Int16, std::int16_t, std::int64_t)
        FLIGHT_PRIMITIVE_CASE(Int32, std::int32_t, std::int64_t)
        FLIGHT_PRIMITIVE_CASE(Int64, std::int64_t, std::int64_t)
        FLIGHT_PRIMITIVE_CASE(UInt8, std::uint8_t, std::uint64_t)
        FLIGHT_PRIMITIVE_CASE(UInt16, std::uint16_t, std::uint64_t)
        FLIGHT_PRIMITIVE_CASE(UInt32, std::uint32_t, std::uint64_t)
        FLIGHT_PRIMITIVE_CASE(UInt64, std::uint64_t, std::uint64_t)
        FLIGHT_PRIMITIVE_CASE(Float32, float, double)
        FLIGHT_PRIMITIVE_CASE(Float64, double, double)
#undef FLIGHT_PRIMITIVE_CASE
        case TypeId::Date32: {
            const auto* b = buffers.Get(cur_buffer++);
            for (std::int64_t r = 0; r < rows; ++r) {
                out[r] = is_valid(r) ? Value{static_cast<std::int64_t>(
                                           read_le<std::int32_t>(body + b->offset() + r * 4))}
                                     : Value{};
            }
            break;
        }
        case TypeId::Date64:
        case TypeId::Timestamp: {
            const auto* b = buffers.Get(cur_buffer++);
            for (std::int64_t r = 0; r < rows; ++r) {
                out[r] = is_valid(r)
                             ? Value{read_le<std::int64_t>(body + b->offset() + r * 8)}
                             : Value{};
            }
            break;
        }
        case TypeId::Utf8:
        case TypeId::Binary: {
            const auto* offsets = buffers.Get(cur_buffer++);
            const auto* data = buffers.Get(cur_buffer++);
            for (std::int64_t r = 0; r < rows; ++r) {
                if (!is_valid(r)) {
                    out[r] = Value{};
                    continue;
                }
                const std::int32_t begin =
                    read_le<std::int32_t>(body + offsets->offset() + r * 4);
                const std::int32_t end =
                    read_le<std::int32_t>(body + offsets->offset() + (r + 1) * 4);
                out[r] = Value{std::string{
                    reinterpret_cast<const char*>(body + data->offset() + begin),
                    static_cast<std::size_t>(end - begin)}};
            }
            break;
        }
        case TypeId::Decimal128:
            throw std::runtime_error("flight::ipc: decimal parameters are not supported");
        default:
            throw std::runtime_error("flight::ipc: only scalar parameter columns supported");
    }
    return out;
}

} // namespace

SchemaPtr parse_schema_message(const std::uint8_t* data, std::size_t size) {
    const auto* message = fb::GetMessage(data);
    if (message->header_type() != fb::MessageHeader::Schema) {
        throw std::runtime_error("flight::ipc: expected schema message");
    }
    return schema_from_fb(message->header_as_Schema());
}

std::vector<std::vector<Value>> decode_record_batch(const Schema& schema,
                                                    const std::uint8_t* message,
                                                    std::size_t message_size,
                                                    const std::uint8_t* body,
                                                    std::size_t body_size) {
    (void)message_size;
    (void)body_size;
    const auto* msg = fb::GetMessage(message);
    if (msg->header_type() != fb::MessageHeader::RecordBatch) {
        throw std::runtime_error("flight::ipc: expected record batch message");
    }
    const auto* rb = msg->header_as_RecordBatch();
    const auto* nodes = rb->nodes();
    const auto* buffers = rb->buffers();
    if (!nodes || !buffers || nodes->size() < schema.fields.size()) {
        return {};
    }
    const std::int64_t rows = rb->length();
    std::vector<std::vector<Value>> out(static_cast<std::size_t>(rows),
                                        std::vector<Value>(schema.fields.size()));
    std::size_t cur_node = 0, cur_buffer = 0;
    for (std::size_t c = 0; c < schema.fields.size(); ++c) {
        std::vector<Value> column = decode_column(*schema.fields[c]->type, body, body_size,
                                                  *buffers, cur_buffer, *nodes, cur_node, rows);
        for (std::size_t r = 0; r < out.size(); ++r) {
            out[r][c] = std::move(column[r]);
        }
    }
    return out;
}

bool FlightDataSink::feed(const std::uint8_t* header, std::size_t header_size,
                          const std::uint8_t* body_data, std::size_t body_size,
                          std::vector<std::vector<Value>>& rows) {
    if (header == nullptr || header_size == 0) return false;
    const auto* msg = fb::GetMessage(header);
    if (msg->header_type() == fb::MessageHeader::Schema) {
        schema_ = schema_from_fb(msg->header_as_Schema());
        return false;
    }
    if (msg->header_type() == fb::MessageHeader::RecordBatch) {
        if (!schema_) {
            throw std::runtime_error("flight::ipc: record batch before schema");
        }
        rows = decode_record_batch(*schema_, header, header_size, body_data, body_size);
        return !rows.empty();
    }
    return false; // dictionaries and the rest — ignored
}

} // namespace flight::ipc
