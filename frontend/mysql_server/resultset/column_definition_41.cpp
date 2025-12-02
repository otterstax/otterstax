// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "column_definition_41.hpp"

namespace mysql_front {
    constexpr uint8_t COLUMN_DEF_FIXED_FIELDS_SIZE = 0x0C;

    column_definition_41::column_definition_41(std::string col_name, field_type type)
        : name(col_name)
        , org_name(std::move(col_name))
        , column_type(type) {
        init_type(type);
        packet_size_ = get_length_encoded_string_size(catalog.size()) + get_length_encoded_string_size(schema.size()) +
                       get_length_encoded_string_size(table.size()) + get_length_encoded_string_size(org_table.size()) +
                       get_length_encoded_string_size(name.size()) + get_length_encoded_string_size(org_name.size());
        packet_size_ += COLUMN_DEF_FIXED_FIELDS_SIZE + 1;
    }

    size_t column_definition_41::packet_size() const { return packet_size_; }

    std::vector<uint8_t> column_definition_41::write_packet(mysql_front::column_definition_41&& column_def,
                                                            mysql_front::packet_writer& writer,
                                                            uint8_t sequence_id) {
        writer.reserve_payload(column_def.packet_size_);

        writer.write_length_encoded_string(std::move(column_def.catalog));
        writer.write_length_encoded_string(std::move(column_def.schema));
        writer.write_length_encoded_string(std::move(column_def.table));
        writer.write_length_encoded_string(std::move(column_def.org_table));
        writer.write_length_encoded_string(std::move(column_def.name));
        writer.write_length_encoded_string(std::move(column_def.org_name));

        writer.write_uint8(COLUMN_DEF_FIXED_FIELDS_SIZE); // fixed-length fields size [0x0C]
        writer.write_uint16_le(static_cast<uint16_t>(column_def.charset));
        writer.write_uint32_le(column_def.column_length);
        writer.write_uint8(static_cast<uint8_t>(column_def.column_type));
        writer.write_uint16_le(column_def.column_flags);
        writer.write_uint8(column_def.decimals);
        writer.write_uint16_le(0x0000); // reserved (string[2]) filler

        return writer.build_from_payload(sequence_id);
    }

    void column_definition_41::init_type(field_type type) {
        switch (type) {
            case field_type::MYSQL_TYPE_TINY:
                column_length = 4;
                charset = character_set::BINARY;
                break;
            case field_type::MYSQL_TYPE_SHORT:
                column_length = 6;
                charset = character_set::BINARY;
                break;
            case field_type::MYSQL_TYPE_LONG:
                column_length = 11;
                charset = character_set::BINARY;
                break;
            case field_type::MYSQL_TYPE_LONGLONG:
                column_length = 20;
                charset = character_set::BINARY;
                break;
            case field_type::MYSQL_TYPE_FLOAT:
                column_length = 12;
                charset = character_set::BINARY;
                decimals = 31; // MySQL default for float
                break;
            case field_type::MYSQL_TYPE_DOUBLE:
                column_length = 22;
                charset = character_set::BINARY;
                decimals = 31; // MySQL default for double
                break;
            case field_type::MYSQL_TYPE_VARCHAR:
            case field_type::MYSQL_TYPE_VAR_STRING:
            case field_type::MYSQL_TYPE_STRING:
                column_length = 0xFFFF; // 65535
                break;
            case field_type::MYSQL_TYPE_DATETIME:
                column_length = 19;
                charset = character_set::BINARY;
                break;
            case field_type::MYSQL_TYPE_DATE:
                column_length = 10;
                charset = character_set::BINARY;
                break;
            case field_type::MYSQL_TYPE_TIME:
                column_length = 8;
                charset = character_set::BINARY;
                break;
            default:
                column_length = 255;
                charset = character_set::UTF8_GENERAL_CI;
                break;
        }
    }
} // namespace mysql_front