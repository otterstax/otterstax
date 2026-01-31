// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "field_description.hpp"

namespace frontend::postgres {
    field_description::field_description(std::string col_name, frontend::postgres::field_type type)
        : name(std::move(col_name)) {
        init_type(type);
    }

    void field_description::init_type(field_type type) {
        type_oid = static_cast<oid_t>(type);
        switch (type) {
            case field_type::BIT:
            case field_type::BOOL:
                type_size = 1;
                break;
            case field_type::INT2:
                type_size = 2;
                break;
            case field_type::FLOAT4:
            case field_type::INT4:
                type_size = 4;
                break;
            case field_type::FLOAT8:
            case field_type::INT8:
            case field_type::TIMESTAMP:
            case field_type::TIMESTAMPTZ:
                type_size = 8;
                break;
            case field_type::UUID:
                type_size = 16;
                break;
            case field_type::NUMERIC:
            case field_type::BYTEA:
            case field_type::TEXT:
            default:
                type_size = -1;
                break;
        }
    }

    int32_t field_description::field_size() const { return name.size() + 4 + 2 + 4 + 2 + 4 + 2; }

    void field_description::write_field(field_description&& desc, packet_writer& writer, result_encoding encoding) {
        writer.write_string_null(std::move(desc.name));
        writer.write_int32(desc.table_oid);
        writer.write_int16(desc.column_attr_number);
        writer.write_int32(desc.type_oid);
        writer.write_int16(desc.type_size);
        writer.write_int32(desc.type_modifier);
        writer.write_int16(encoding == result_encoding::BINARY);
    }

} // namespace frontend::postgres
