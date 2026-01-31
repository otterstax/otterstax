// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../postgres_defs/error.hpp"
#include "../postgres_defs/field_type.hpp"
#include "../postgres_defs/message_type.hpp"
#include "../resultset/field_description.hpp"
#include "packet_writer.hpp"
#include <components/sql/parser/nodes/nodes.h>

namespace frontend::postgres {
    enum class transaction_status : char
    {
        IDLE = 'I',
        IN_TRANSACTION = 'T',
        TRANSACTION_ERROR = 'E',
    };

    struct error_severity {
        std::string tag;

        static error_severity fatal();
        static error_severity error();
        static error_severity warning();
        static error_severity info();

    private:
        error_severity(std::string tag);
    };

    struct command_complete_tag {
        static command_complete_tag simple_command(NodeTag node);
        static command_complete_tag select(int32_t rows = 0);
        static command_complete_tag insert(int32_t rows = 0);
        static command_complete_tag update(int32_t rows = 0);
        static command_complete_tag delete_rows(int32_t rows = 0);
        static command_complete_tag begin();
        static command_complete_tag commit();
        static command_complete_tag rollback();
        static command_complete_tag savepoint();
        static command_complete_tag release();

        std::string tag;

    private:
        command_complete_tag(std::string tag);
    };

    std::optional<frontend::result_encoding> get_format_code(const std::vector<frontend::result_encoding>& format,
                                                             size_t i);

    std::vector<uint8_t> build_auth_ok(packet_writer& writer);

    std::vector<uint8_t> build_error_response(packet_writer& writer,
                                              const char* sqlstate,
                                              std::string message,
                                              error_severity severity = error_severity::fatal());

    std::vector<uint8_t> build_parameter_status(packet_writer& writer, std::string key, std::string value);

    std::vector<uint8_t> build_backend_key_data(packet_writer& writer, int32_t pid, std::vector<uint8_t> key);

    std::vector<uint8_t> build_row_description(packet_writer& writer,
                                               std::vector<field_description>&& fields,
                                               std::vector<result_encoding> encoding);

    std::vector<uint8_t> build_ready_for_query(packet_writer& writer, transaction_status status);

    std::vector<uint8_t> build_empty_query_response(packet_writer& writer);

    std::vector<uint8_t> build_command_complete(packet_writer& writer, command_complete_tag tag);

    std::vector<uint8_t> build_parse_complete(packet_writer& writer);

    std::vector<uint8_t> build_bind_complete(packet_writer& writer);

    std::vector<uint8_t> build_close_complete(packet_writer& writer);

    std::vector<uint8_t> build_no_data(packet_writer& writer);
} // namespace frontend::postgres
