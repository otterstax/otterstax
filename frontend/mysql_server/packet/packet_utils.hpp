// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include "../mysql_defs/capabilities.hpp"
#include "../mysql_defs/character_set.hpp"
#include "../mysql_defs/error.hpp"
#include "../mysql_defs/server_status.hpp"
#include "../protocol_const.hpp"
#include "length_encoded.hpp"
#include "packet_writer.hpp"

namespace mysql_front {
    constexpr uint8_t AUTH_DATA_PART1_LENGTH = 8;
    constexpr uint8_t AUTH_DATA_FULL_LENGTH = 20;

    // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_basic_ok_packet.html
    std::vector<uint8_t>
    build_ok(packet_writer& writer,
             uint8_t sequence_id,
             uint64_t affected_rows = 0,
             server_status_flags_t flags = static_cast<uint16_t>(server_status::SERVER_STATUS_AUTOCOMMIT));

    // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_basic_err_packet.html
    std::vector<uint8_t>
    build_error(packet_writer& writer, uint8_t sequence_id, mysql_error error_code, std::string message);

    // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_basic_eof_packet.html
    std::vector<uint8_t>
    build_eof(packet_writer& writer,
              uint8_t sequence_id,
              uint16_t warnings = 0,
              server_status_flags_t flags = static_cast<uint16_t>(server_status::SERVER_STATUS_AUTOCOMMIT));

    // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_connection_phase_packets_protocol_handshake_v10.html
    std::vector<uint8_t>
    build_handshake_10(packet_writer& writer,
                       uint32_t connection_id,
                       std::string auth_data,
                       server_status_flags_t flags = static_cast<uint16_t>(server_status::SERVER_STATUS_AUTOCOMMIT));

    // https://dev.mysql.com/doc/dev/mysql-server/9.5.0/page_protocol_command_phase_ps.html
    std::vector<uint8_t> build_stmt_prepare_ok(packet_writer& writer,
                                               uint8_t sequence_id,
                                               uint32_t statement_id,
                                               uint16_t num_columns,
                                               uint16_t num_params,
                                               uint16_t warning_count = 0);
} // namespace mysql_front