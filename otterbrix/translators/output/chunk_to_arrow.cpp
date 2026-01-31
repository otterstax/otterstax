// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "chunk_to_arrow.hpp"

using namespace components::vector;
using namespace components::types;
using namespace components;

void append_physical_to_arrow_fields(arrow::FieldVector& field_vector, std::string key, types::physical_type type) {
    switch (type) {
        case types::physical_type::BOOL: {
            field_vector.push_back(arrow::field(std::move(key), arrow::boolean()));
            break;
        }
        case types::physical_type::UINT8: {
            field_vector.push_back(arrow::field(std::move(key), arrow::uint8()));
            break;
        }
        case types::physical_type::UINT16: {
            field_vector.push_back(arrow::field(std::move(key), arrow::uint16()));
            break;
        }
        case types::physical_type::UINT32: {
            field_vector.push_back(arrow::field(std::move(key), arrow::uint32()));
            break;
        }
        case types::physical_type::UINT64: {
            field_vector.push_back(arrow::field(std::move(key), arrow::uint64()));
            break;
        }
        case types::physical_type::INT8: {
            field_vector.push_back(arrow::field(std::move(key), arrow::int8()));
            break;
        }
        case types::physical_type::INT16: {
            field_vector.push_back(arrow::field(std::move(key), arrow::int16()));
            break;
        }
        case types::physical_type::INT32: {
            field_vector.push_back(arrow::field(std::move(key), arrow::int32()));
            break;
        }
        case types::physical_type::INT64: {
            field_vector.push_back(arrow::field(std::move(key), arrow::int64()));
            break;
        }
        case types::physical_type::FLOAT: {
            field_vector.push_back(arrow::field(std::move(key), arrow::float32()));
            break;
        }
        case types::physical_type::DOUBLE: {
            field_vector.push_back(arrow::field(std::move(key), arrow::float64()));
            break;
        }
        case types::physical_type::STRING: {
            field_vector.push_back(arrow::field(std::move(key), arrow::utf8()));
            break;
        }
        case types::physical_type::NA: {
            field_vector.push_back(arrow::field(std::move(key), arrow::null()));
            break;
        }
        default: {
            throw std::runtime_error("Chunk to arrow: Unknown type: " + std::to_string(static_cast<uint8_t>(type)));
        }
    }
}

// data_chunk_t uses std::pmr::vector of types
// we put schema intp struct type, which holds std::vector
// 2 functions just for convenience
std::shared_ptr<arrow::Schema> to_arrow_schema(const std::pmr::vector<components::types::complex_logical_type>& types) {
    arrow::FieldVector field_vector;
    field_vector.reserve(types.size());
    for (const auto& type : types) {
        append_physical_to_arrow_fields(field_vector, type.alias(), type.to_physical_type());
    }
    return arrow::schema(std::move(field_vector));
}

std::shared_ptr<arrow::Schema> to_arrow_schema(const components::types::complex_logical_type& struct_t) {
    if (struct_t.type() != types::logical_type::STRUCT) {
        // logical_type::NA case - empty schema
        return arrow::schema({});
    }

    arrow::FieldVector field_vector;
    for (const auto& type : struct_t.child_types()) {
        append_physical_to_arrow_fields(field_vector, type.alias(), type.to_physical_type());
    }

    return arrow::schema(std::move(field_vector));
}