// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>
#include <unordered_map>

namespace frontend::postgres {
    enum class describe_close_arg : char
    {
        STATEMENT = 'S',
        PORTAL = 'P',
    };

    namespace message_code {
        inline constexpr int32_t PROTOCOL_VERSION_3_0 = 0x00030000; // version 3.0, startup message
        inline constexpr int32_t SSL_REQUEST_CODE = 80877103;
    } // namespace message_code

    namespace message_type {
        namespace backend {
            inline constexpr char AUTHENTICATION = 'R';
            inline constexpr char BACKEND_KEY_DATA = 'K';
            inline constexpr char PARAMETER_STATUS = 'S';
            inline constexpr char READY_FOR_QUERY = 'Z';
            inline constexpr char ROW_DESCRIPTION = 'T';
            inline constexpr char DATA_ROW = 'D';
            inline constexpr char COMMAND_COMPLETE = 'C';
            inline constexpr char EMPTY_QUERY_RESPONSE = 'I';
            inline constexpr char ERROR_RESPONSE = 'E';
            inline constexpr char NOTICE_RESPONSE = 'N';
            inline constexpr char PARSE_COMPLETE = '1';
            inline constexpr char BIND_COMPLETE = '2';
            inline constexpr char CLOSE_COMPLETE = '3';
            inline constexpr char NO_DATA_MSG = 'n';
            inline constexpr char PORTAL_SUSPENDED = 's';
            inline constexpr char COPY_IN_RESPONSE = 'G';
            inline constexpr char COPY_OUT_RESPONSE = 'H';
            inline constexpr char COPY_DATA = 'd';
            inline constexpr char COPY_DONE = 'c';
            inline constexpr char NOTIFICATION_RESPONSE = 'A';
        } // namespace backend

        namespace frontend {
            inline constexpr char QUERY = 'Q';
            inline constexpr char PARSE = 'P';
            inline constexpr char BIND = 'B';
            inline constexpr char EXECUTE = 'E';
            inline constexpr char DESCRIBE = 'D';
            inline constexpr char CLOSE = 'C';
            inline constexpr char SYNC = 'S';
            inline constexpr char FLUSH = 'H';
            inline constexpr char TERMINATE = 'X';
            inline constexpr char COPY_DATA = 'd';
            inline constexpr char COPY_DONE = 'c';
            inline constexpr char COPY_FAIL = 'f';
            inline constexpr char PASSWORD_MESSAGE = 'p';
        } // namespace frontend
    } // namespace message_type
} // namespace frontend::postgres
