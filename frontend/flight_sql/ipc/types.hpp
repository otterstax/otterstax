// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Arrow type model sufficient for Flight SQL: primitives, utf8/binary,
// decimal128, date/timestamp, list, dense_union, map (for SqlInfoResult/Tables),
// struct.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace flight::ipc {

enum class TypeId : std::uint8_t {
    Null,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Decimal128,
    Date32,
    Date64,
    Timestamp,
    List,
    Struct,
    DenseUnion,
    Map,
};

struct Field;
using FieldPtr = std::shared_ptr<const Field>;
struct Type;
using TypePtr = std::shared_ptr<const Type>;

struct Field {
    std::string name;
    bool nullable;
    TypePtr type;

    Field(std::string name_, bool nullable_, TypePtr type_)
        : name(std::move(name_)), nullable(nullable_), type(std::move(type_)) {}
};

struct Type {
    TypeId id;

    // Timestamp
    enum class TimeUnit : std::uint8_t { Second, Millisecond, Microsecond, Nanosecond };
    TimeUnit unit = TimeUnit::Microsecond;
    std::string timezone; // empty = no timezone

    // Decimal128: total digits and digits after the decimal point. The value
    // is the unscaled 128-bit integer, as in Arrow.
    std::int32_t precision = 38;
    std::int32_t scale = 0;

    // List / Struct / DenseUnion / Map: child fields
    //  List       -> 1 child (the element)
    //  Struct     -> the struct's fields
    //  DenseUnion -> one child per variant
    //  Map        -> 1 child Struct{key, value} (Arrow convention)
    std::vector<FieldPtr> children;

    // DenseUnion: variant codes (column type ids), usually 0..n-1
    std::vector<std::int32_t> union_type_ids;
};

// Factories for the simple types
inline TypePtr null_type() { return std::make_shared<Type>(Type{TypeId::Null}); }
inline TypePtr bool_type() { return std::make_shared<Type>(Type{TypeId::Bool}); }
inline TypePtr int8_type() { return std::make_shared<Type>(Type{TypeId::Int8}); }
inline TypePtr int16_type() { return std::make_shared<Type>(Type{TypeId::Int16}); }
inline TypePtr int32_type() { return std::make_shared<Type>(Type{TypeId::Int32}); }
inline TypePtr int64_type() { return std::make_shared<Type>(Type{TypeId::Int64}); }
inline TypePtr uint8_type() { return std::make_shared<Type>(Type{TypeId::UInt8}); }
inline TypePtr uint16_type() { return std::make_shared<Type>(Type{TypeId::UInt16}); }
inline TypePtr uint32_type() { return std::make_shared<Type>(Type{TypeId::UInt32}); }
inline TypePtr uint64_type() { return std::make_shared<Type>(Type{TypeId::UInt64}); }
inline TypePtr float32_type() { return std::make_shared<Type>(Type{TypeId::Float32}); }
inline TypePtr float64_type() { return std::make_shared<Type>(Type{TypeId::Float64}); }
inline TypePtr utf8_type() { return std::make_shared<Type>(Type{TypeId::Utf8}); }
inline TypePtr binary_type() { return std::make_shared<Type>(Type{TypeId::Binary}); }
inline TypePtr date32_type() { return std::make_shared<Type>(Type{TypeId::Date32}); }
inline TypePtr date64_type() { return std::make_shared<Type>(Type{TypeId::Date64}); }

inline TypePtr decimal128_type(std::int32_t precision, std::int32_t scale) {
    Type t;
    t.id = TypeId::Decimal128;
    t.precision = precision;
    t.scale = scale;
    return std::make_shared<Type>(std::move(t));
}

inline TypePtr timestamp_type(Type::TimeUnit u, std::string tz = "") {
    Type t;
    t.id = TypeId::Timestamp;
    t.unit = u;
    t.timezone = std::move(tz);
    return std::make_shared<Type>(std::move(t));
}

inline TypePtr list_type(FieldPtr child) {
    TypePtr t = std::make_shared<Type>(Type{TypeId::List});
    const_cast<Type*>(t.get())->children.push_back(std::move(child));
    return t;
}

inline TypePtr struct_type(std::vector<FieldPtr> fields) {
    TypePtr t = std::make_shared<Type>(Type{TypeId::Struct});
    const_cast<Type*>(t.get())->children = std::move(fields);
    return t;
}

inline TypePtr dense_union_type(std::vector<FieldPtr> fields) {
    TypePtr t = std::make_shared<Type>(Type{TypeId::DenseUnion});
    auto* raw = const_cast<Type*>(t.get());
    raw->children = std::move(fields);
    for (std::size_t i = 0; i < raw->children.size(); ++i) {
        raw->union_type_ids.push_back(static_cast<std::int32_t>(i));
    }
    return t;
}

// map<key_type, item_type>: the child is an "entries" field of Struct{key, value}
inline TypePtr map_type(TypePtr key, TypePtr item) {
    auto key_field = std::make_shared<Field>("key", false, std::move(key));
    auto value_field = std::make_shared<Field>("value", true, std::move(item));
    TypePtr entries_type = struct_type({key_field, value_field});
    auto entries_field = std::make_shared<Field>("entries", false, entries_type);
    TypePtr t = std::make_shared<Type>(Type{TypeId::Map});
    const_cast<Type*>(t.get())->children.push_back(std::move(entries_field));
    return t;
}

struct Schema {
    std::vector<FieldPtr> fields;
};

using SchemaPtr = std::shared_ptr<const Schema>;

inline SchemaPtr make_schema(std::vector<FieldPtr> fields) {
    return std::make_shared<Schema>(Schema{std::move(fields)});
}

} // namespace flight::ipc
