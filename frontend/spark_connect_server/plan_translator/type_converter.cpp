// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "type_converter.hpp"

#include <string>
#include <utility>

namespace frontend::spark {

namespace {

using namespace components::types;

::spark::connect::DataType make_unparsed(std::string data_type_string) {
    ::spark::connect::DataType result;
    result.mutable_unparsed()->set_data_type_string(std::move(data_type_string));
    return result;
}

}  // namespace

::spark::connect::DataType to_spark_data_type(const complex_logical_type& type) {
    ::spark::connect::DataType result;
    switch (type.type()) {
        case logical_type::BOOLEAN:
            result.mutable_boolean();
            break;
        // Spark has no unsigned integer types; map each to the same-size signed
        // Spark type (matches the result encoder's schema re-tag).
        case logical_type::UTINYINT:
        case logical_type::TINYINT:
            result.mutable_byte();
            break;
        case logical_type::USMALLINT:
        case logical_type::SMALLINT:
            result.mutable_short_();
            break;
        case logical_type::UINTEGER:
        case logical_type::INTEGER:
            result.mutable_integer();
            break;
        case logical_type::UBIGINT:
        case logical_type::BIGINT:
            result.mutable_long_();
            break;
        case logical_type::FLOAT:
            result.mutable_float_();
            break;
        case logical_type::DOUBLE:
            result.mutable_double_();
            break;
        case logical_type::DECIMAL: {
            auto* ext = type.extension();
            auto* decimal = result.mutable_decimal();
            if (ext != nullptr) {
                const auto* dec = static_cast<const decimal_logical_type_extension*>(ext);
                decimal->set_precision(dec->width());
                decimal->set_scale(dec->scale());
            }
            break;
        }
        case logical_type::STRING_LITERAL:
        // Spark Connect has no TIME type; fall back to STRING.
        case logical_type::TIME:
            result.mutable_string();
            break;
        case logical_type::BLOB:
            result.mutable_binary();
            break;
        case logical_type::DATE:
            result.mutable_date();
            break;
        case logical_type::TIMESTAMP:
        case logical_type::TIMESTAMP_TZ:
            // Spark `timestamp` is UTC-valued; TIMESTAMP_TZ maps here too.
            result.mutable_timestamp();
            break;
        case logical_type::STRUCT: {
            auto* structure = result.mutable_struct_();
            const auto& children = type.child_types();
            for (const auto& child : children) {
                auto* field = structure->add_fields();
                field->set_name(child.alias());
                *field->mutable_data_type() = to_spark_data_type(child);
                field->set_nullable(true);
            }
            break;
        }
        case logical_type::LIST:
        case logical_type::ARRAY: {
            auto* array = result.mutable_array();
            *array->mutable_element_type() = to_spark_data_type(type.child_type());
            array->set_contains_null(true);
            break;
        }
        case logical_type::MAP: {
            auto* ext = type.extension();
            if (ext != nullptr) {
                const auto* map_ext = static_cast<const map_logical_type_extension*>(ext);
                auto* map = result.mutable_map();
                *map->mutable_key_type() = to_spark_data_type(map_ext->key());
                *map->mutable_value_type() = to_spark_data_type(map_ext->value());
                map->set_value_contains_null(true);
            } else {
                result = make_unparsed("map");
            }
            break;
        }
        case logical_type::NA:
            result.mutable_null();
            break;
        default:
            result = make_unparsed("unknown");
            break;
    }
    return result;
}

::spark::connect::DataType to_spark_schema(const complex_logical_type& schema) {
    if (schema.type() == logical_type::STRUCT) {
        return to_spark_data_type(schema);
    }
    // Non-struct (e.g. NA for an empty result set) -> empty struct schema.
    ::spark::connect::DataType result;
    result.mutable_struct_();
    return result;
}

::spark::connect::DataType to_spark_schema(const std::pmr::vector<complex_logical_type>& columns) {
    ::spark::connect::DataType result;
    auto* structure = result.mutable_struct_();
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& column = columns[i];
        auto* field = structure->add_fields();
        // complex_logical_type::alias() dereferences extension_ with no null guard
        // and crashes on an unaliased (leaf) type, so fall back to a stable "colN"
        // name — mirroring the Arrow encoder (result_encoder.cpp).
        if (column.has_alias()) {
            field->set_name(column.alias());
        } else {
            field->set_name("col" + std::to_string(i));
        }
        *field->mutable_data_type() = to_spark_data_type(column);
        field->set_nullable(true);
    }
    return result;
}

}  // namespace frontend::spark
