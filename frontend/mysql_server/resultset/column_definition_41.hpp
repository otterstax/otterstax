// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "../mysql_defs/character_set.hpp"
#include "../mysql_defs/field_type.hpp"
#include "../packet/length_encoded.hpp"
#include "../packet/packet_writer.hpp"
#include <string>

namespace mysql_front {
    // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_com_query_response_text_resultset_column_definition.html
    struct column_definition_41 {
        column_definition_41() = default;
        column_definition_41(std::string col_name, field_type type);

        std::string catalog = "def";
        std::string schema;    // database name
        std::string table;     // table name (or alias)
        std::string org_table; // original table name
        std::string name;      // column name (or alias)
        std::string org_name;  // original column name

        character_set charset = character_set::UTF8_GENERAL_CI;
        field_type column_type = field_type::MYSQL_TYPE_NULL;
        uint32_t column_length = 0;
        uint16_t column_flags = 0;
        uint8_t decimals = 0;

        [[nodiscard]] size_t packet_size() const;
        [[nodiscard]] static std::vector<uint8_t>
        write_packet(column_definition_41&& column_def, packet_writer& writer, uint8_t sequence_id);

    private:
        size_t packet_size_ = 0;

        void init_type(field_type type);
    };

} // namespace mysql_front