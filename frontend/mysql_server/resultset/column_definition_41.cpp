// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "column_definition_41.hpp"

#include "../mysql_defs/column_flags.hpp"

#include <cassert>

namespace frontend::mysql {
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

    std::vector<uint8_t>
    column_definition_41::write_packet(column_definition_41&& column_def, packet_writer& writer, uint8_t sequence_id) {
        writer.reserve_payload(column_def.packet_size_);

        writer.write_length_encoded_string(std::move(column_def.catalog));
        writer.write_length_encoded_string(std::move(column_def.schema));
        writer.write_length_encoded_string(std::move(column_def.table));
        writer.write_length_encoded_string(std::move(column_def.org_table));
        writer.write_length_encoded_string(std::move(column_def.name));
        writer.write_length_encoded_string(std::move(column_def.org_name));

        writer.write_uint8(COLUMN_DEF_FIXED_FIELDS_SIZE); // fixed-length fields size [0x0C]
        writer.write_uint16(static_cast<uint16_t>(column_def.charset));
        writer.write_uint32(column_def.column_length);
        writer.write_uint8(static_cast<uint8_t>(column_def.column_type));
        writer.write_uint16(column_def.column_flags);
        writer.write_uint8(column_def.decimals);
        writer.write_uint16(0x0000); // reserved (string[2]) filler

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
            case field_type::MYSQL_TYPE_NEWDECIMAL:
                // A numeric column, so the binary charset as every other number
                // above. The length stands for the widest DECIMAL the engine can
                // build until apply_type_metadata puts the column's own
                // (width, scale) here.
                column_length = components::types::DECIMAL_MAX_WIDTH + 2;
                charset = character_set::BINARY;
                break;
            default:
                column_length = 255;
                // The handshake's collation: utf-8 text compared byte by byte.
                charset = character_set::UTF8MB4_BIN;
                break;
        }
    }

    void apply_type_metadata(column_definition_41& column, const components::types::complex_logical_type& type) {
        using components::types::logical_type;
        // The flags MySQL's Item::init_make_field derives from the type: a
        // column that compares as binary (a number, a date: the binary
        // charset init_type gave it) is BINARY_FLAG, an unsigned integer
        // UNSIGNED_FLAG (what makes a client read 200 of a TINYINT as 200 and
        // not -56), an ENUM's label ENUM_FLAG. NOT_NULL_FLAG is never set: a
        // result column says nothing of its nullability. A text column has
        // none of them: MySQL would mark one under utf8mb4_bin BINARY too
        // (my_binary_compare), which some drivers read as bytes (older
        // mysql-connector-python answered such a column with a bytearray),
        // while the collation (SERVER_COLLATION_ID) already says how it
        // compares.
        column.column_flags = 0;
        if (column.charset == character_set::BINARY) {
            column.column_flags |= BINARY_FLAG;
        }
        switch (type.type()) {
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT:
                column.column_flags |= UNSIGNED_FLAG;
                break;
            case logical_type::ENUM:
                column.column_flags |= ENUM_FLAG;
                break;
            default:
                break;
        }
        if (type.type() == logical_type::BOOLEAN) {
            // TINYINT(1), what MySQL declares a BOOLEAN as (Connector/J reads
            // such a column as a boolean).
            column.column_length = 1;
            return;
        }
        if (type.type() != components::types::logical_type::DECIMAL) {
            return;
        }
        const auto* extension = type.extension_as<components::types::decimal_logical_type_extension>();
        // A DECIMAL always carries its (width, scale): create_decimal is the only
        // way to build one.
        assert(extension != nullptr && "apply_type_metadata: a DECIMAL column without its extension");
        column.decimals = extension->scale();
        // The digits, the sign, and the point when there is a fractional part —
        // the display length MySQL reports for DECIMAL(width, scale).
        column.column_length = extension->width() + 1u + (extension->scale() > 0 ? 1u : 0u);
    }
} // namespace frontend::mysql
