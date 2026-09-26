// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

namespace ch {

    // Maps a ClickHouse server error code (clickhouse::ServerException::GetCode,
    // the numbers of ErrorCodes.cpp) onto the engine's error space so the frontend
    // reports the backend's verdict. A server code without an engine counterpart
    // stays other_error; transport-level codes are io_error.
    inline core::error_code_t classify_server_code(int code) noexcept {
        switch (code) {
            case 62: // SYNTAX_ERROR
                return core::error_code_t::sql_parse_error;
            case 60: // UNKNOWN_TABLE
                return core::error_code_t::table_not_exists;
            case 81: // UNKNOWN_DATABASE
                return core::error_code_t::database_not_exists;
            case 10: // NOT_FOUND_COLUMN_IN_BLOCK
            case 16: // NO_SUCH_COLUMN_IN_TABLE
            case 47: // UNKNOWN_IDENTIFIER
                return core::error_code_t::field_not_exists;
            case 57: // TABLE_ALREADY_EXISTS
                return core::error_code_t::table_already_exists;
            case 82: // DATABASE_ALREADY_EXISTS
                return core::error_code_t::database_already_exists;
            case 207: // AMBIGUOUS_IDENTIFIER
            case 352: // AMBIGUOUS_COLUMN_NAME
                return core::error_code_t::ambiguous_name;
            case 15: // DUPLICATE_COLUMN
                return core::error_code_t::duplicate_field;
            case 46: // UNKNOWN_FUNCTION
                return core::error_code_t::unrecognized_function;
            case 42: // NUMBER_OF_ARGUMENTS_DOESNT_MATCH
            case 43: // ILLEGAL_TYPE_OF_ARGUMENT
                return core::error_code_t::incorrect_function_argument;
            case 6:  // CANNOT_PARSE_TEXT
            case 27: // CANNOT_PARSE_INPUT_ASSERTION_FAILED
            case 53: // TYPE_MISMATCH
            case 70: // CANNOT_CONVERT_TYPE
            case 72: // CANNOT_PARSE_NUMBER
                return core::error_code_t::conversion_failure;
            case 48: // NOT_IMPLEMENTED
                return core::error_code_t::unimplemented_yet;
            case 241: // MEMORY_LIMIT_EXCEEDED
                return core::error_code_t::out_of_memory;
            case 32:  // ATTEMPT_TO_READ_AFTER_EOF
            case 209: // SOCKET_TIMEOUT
            case 210: // NETWORK_ERROR
                return core::error_code_t::io_error;
            default:
                return core::error_code_t::other_error;
        }
    }

} // namespace ch
