// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "packet_utils.hpp"

namespace mysql_front {
    constexpr size_t OK_PAYLOAD_SIZE = 7;
    constexpr uint8_t OK_PACKET_HEADER = 0x00;

    constexpr size_t ERR_PAYLOAD_FIXED_SIZE = 9;
    constexpr uint8_t ERR_PACKET_HEADER = 0xFF;

    constexpr size_t EOF_PAYLOAD_SIZE = 5;
    constexpr uint8_t EOF_PACKET_HEADER = 0xFE;

    constexpr size_t HANDSHAKE_PAYLOAD_SIZE = 77;
    constexpr size_t HANDSHAKE_FILLER_SIZE = 10;

    constexpr size_t STMT_PREPARE_OK_SIZE = 12;

    std::vector<uint8_t>
    build_ok(packet_writer& writer, uint8_t sequence_id, uint64_t affected_rows, server_status_flags_t server_flags) {
        writer.reserve_payload(OK_PAYLOAD_SIZE);
        writer.write_uint8(OK_PACKET_HEADER); // OK header
        writer.write_uint8(static_cast<uint8_t>(affected_rows));
        writer.write_uint8(0x00);             // last insert-id
        writer.write_uint16_le(server_flags); // status flags (autocommit)
        writer.write_uint16_le(0x0000);       // warnings
        return writer.build_from_payload(sequence_id);
    }

    std::vector<uint8_t>
    build_error(packet_writer& writer, uint8_t sequence_id, mysql_error error_code, std::string message) {
        const char* sql_state; // 5 chars

        switch (error_code) {
            case mysql_error::ER_ACCESS_DENIED_ERROR:
            case mysql_error::ER_DBACCESS_DENIED_ERROR:
            case mysql_error::ER_TABLEACCESS_DENIED_ERROR:
                sql_state = sql_state::ACCESS_DENIED;
                break;
            case mysql_error::ER_PACKET_TOO_LARGE:
            case mysql_error::ER_MALFORMED_PACKET:
            case mysql_error::ER_SEQUENCE_ERROR:
            case mysql_error::ER_UNKNOWN_ERROR:
                sql_state = sql_state::PACKET_ERROR;
                break;
            case mysql_error::ER_OUT_OF_RESOURCES:
                sql_state = sql_state::RESOURCE_ERROR;
                break;
            case mysql_error::ER_UNKNOWN_COM_ERROR:
            case mysql_error::ER_PARSE_ERROR:
            case mysql_error::ER_SYNTAX_ERROR:
            case mysql_error::ER_WRONG_VALUE_COUNT_ON_ROW:
            case mysql_error::ER_EMPTY_QUERY:
                sql_state = sql_state::COMMAND_ERROR;
                break;
            case mysql_error::ER_NET_READ_ERROR:
                sql_state = sql_state::CONNECTION_ERROR;
                break;
            case mysql_error::ER_BAD_DB_ERROR:
            case mysql_error::ER_NO_SUCH_TABLE:
            case mysql_error::ER_UNKNOWN_TABLE:
            case mysql_error::ER_DB_CREATE_EXISTS:
            case mysql_error::ER_DB_DROP_EXISTS:
            case mysql_error::ER_TABLE_EXISTS_ERROR:
            case mysql_error::ER_UNKNOWN_STMT_HANDLER:
                sql_state = sql_state::COMMAND_ERROR;
                break;
            case mysql_error::ER_CON_COUNT_ERROR:
            case mysql_error::ER_NOT_SUPPORTED_AUTH_MODE:
                sql_state = sql_state::NOT_SUPPORTED_AUTH_ERROR;
                break;
            default:
                sql_state = sql_state::PROTOCOL_ERROR;
                break;
        }

        writer.reserve_payload(ERR_PAYLOAD_FIXED_SIZE + message.size());
        writer.write_uint8(ERR_PACKET_HEADER); // ERR header
        writer.write_uint16_le(static_cast<uint16_t>(error_code));
        writer.write_uint8('#');                       // SQL state marker
        writer.write_string_fixed(sql_state);          // SQL state (len = 5)
        writer.write_string_fixed(std::move(message)); // EOF string
        return writer.build_from_payload(sequence_id);
    }

    std::vector<uint8_t>
    build_eof(packet_writer& writer, uint8_t sequence_id, uint16_t warnings, server_status_flags_t flags) {
        writer.reserve_payload(EOF_PAYLOAD_SIZE);
        writer.write_uint8(EOF_PACKET_HEADER);
        writer.write_uint16_le(warnings);
        writer.write_uint16_le(flags);
        return writer.build_from_payload(sequence_id);
    }

    std::vector<uint8_t> build_handshake_10(packet_writer& writer,
                                            uint32_t connection_id,
                                            std::string auth_data,
                                            server_status_flags_t flags) {
        writer.reserve_payload(HANDSHAKE_PAYLOAD_SIZE);
        writer.write_uint8(PROTOCOL_VERSION);
        writer.write_string_null(SERVER_VERSION.data());
        writer.write_uint32_le(connection_id);

        // Auth plugin data part 1
        for (int i = 0; i < AUTH_DATA_PART1_LENGTH; ++i) {
            writer.write_uint8(auth_data[i]);
        }
        writer.write_uint8(0); // filler

        // CLIENT_CONNECT_WITH_DB for HandshakeResponse41
        uint32_t capabilities =
            CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_WITH_DB;
        writer.write_uint16_le(capabilities & 0xFFFF);                            // lower 2 bytes
        writer.write_uint8(static_cast<uint8_t>(character_set::UTF8_GENERAL_CI)); // character set
        writer.write_uint16_le(flags);
        writer.write_uint16_le((capabilities >> 16) & 0xFFFF); // upper 2 bytes
        writer.write_uint8(AUTH_DATA_FULL_LENGTH + 1);         // + null terminator
        writer.write_zeros(HANDSHAKE_FILLER_SIZE);             // reserved (must be 0x00)

        // Auth plugin data part 2 (remaining + null terminator)
        for (int i = 8; i < AUTH_DATA_FULL_LENGTH; ++i) {
            writer.write_uint8(auth_data[i]);
        }
        writer.write_uint8(0); // null terminator for auth plugin data
        writer.write_string_null(AUTH_PLUGIN_NAME.data());

        return writer.build_from_payload(0); // handshake - seq_id always 0
    }

    std::vector<uint8_t> build_stmt_prepare_ok(packet_writer& writer,
                                               uint8_t sequence_id,
                                               uint32_t statement_id,
                                               uint16_t num_columns,
                                               uint16_t num_params,
                                               uint16_t warning_count) {
        writer.reserve_payload(STMT_PREPARE_OK_SIZE);
        writer.write_uint8(OK_PACKET_HEADER); // always 0x00
        writer.write_uint32_le(statement_id);
        writer.write_uint16_le(num_columns);
        writer.write_uint16_le(num_params);
        writer.write_uint8(0x00); // filler
        writer.write_uint16_le(warning_count);

        return writer.build_from_payload(sequence_id);
    }

} // namespace mysql_front