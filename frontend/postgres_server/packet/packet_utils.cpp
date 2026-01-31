// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_utils.hpp"

namespace frontend::postgres {
    constexpr size_t AUTH_OK_SIZE = 4;
    constexpr size_t ERROR_FIXED_SIZE = 3 + 5; // S, C, M + sqlstate
    constexpr size_t BACKEND_KEY_SIZE = 8;
    constexpr size_t READY_FOR_QUERY_SIZE = 1;

    error_severity::error_severity(std::string tag)
        : tag(std::move(tag)) {}

    error_severity error_severity::fatal() { return {"FATAL"}; }
    error_severity error_severity::error() { return {"ERROR"}; }
    error_severity error_severity::warning() { return {"WARNING"}; }
    error_severity error_severity::info() { return {"INFO"}; }

    command_complete_tag::command_complete_tag(std::string tag)
        : tag(std::move(tag)) {}

    command_complete_tag command_complete_tag::simple_command(NodeTag node) {
        switch (node) {
            case T_CreateStmt:
                return {"CREATE TABLE"};
            case T_CreateTableAsStmt:
                return {"CREATE TABLE AS"};
            case T_CreateSchemaStmt:
                return {"CREATE SCHEMA"};
            case T_CreatedbStmt:
                return {"CREATE DATABASE"};
            case T_IndexStmt:
                return {"CREATE INDEX"};
            case T_SelectStmt:
                return select();
            case T_UpdateStmt:
                return update();
            case T_InsertStmt:
                return insert();
            case T_DeleteStmt:
                return delete_rows();
            case T_DropStmt:
                return {"DROP"};
            default:
                return {"COMMAND"};
        }
    }
    command_complete_tag command_complete_tag::select(int32_t rows) { return {"SELECT " + std::to_string(rows)}; }
    command_complete_tag command_complete_tag::insert(int32_t rows) { return {"INSERT 0 " + std::to_string(rows)}; }
    command_complete_tag command_complete_tag::update(int32_t rows) { return {"UPDATE " + std::to_string(rows)}; }
    command_complete_tag command_complete_tag::delete_rows(int32_t rows) { return {"DELETE " + std::to_string(rows)}; }
    command_complete_tag command_complete_tag::begin() { return {"BEGIN"}; }
    command_complete_tag command_complete_tag::commit() { return {"COMMIT"}; }
    command_complete_tag command_complete_tag::rollback() { return {"ROLLBACK"}; }
    command_complete_tag command_complete_tag::savepoint() { return {"SAVEPOINT"}; }
    command_complete_tag command_complete_tag::release() { return {"RELEASE"}; }

    std::optional<frontend::result_encoding> get_format_code(const std::vector<frontend::result_encoding>& format,
                                                             size_t i) {
        if (format.empty()) {
            // text default
            return frontend::result_encoding::TEXT;
        } else if (format.size() == 1) {
            // same code for all parameters
            return format.front();
        } else {
            // code for each parameter
            if (i >= format.size()) {
                return std::nullopt;
            }
            return format[i];
        }
    }

    std::vector<uint8_t> build_auth_ok(packet_writer& writer) {
        writer.reserve_payload(AUTH_OK_SIZE);
        writer.write_int32(0); // Auth type 0 = OK
        return writer.build_from_payload(message_type::backend::AUTHENTICATION);
    }

    std::vector<uint8_t>
    build_error_response(packet_writer& writer, const char* sqlstate, std::string message, error_severity severity) {
        writer.reserve_payload(ERROR_FIXED_SIZE + message.size() + severity.tag.size());
        writer.write_uint8(static_cast<uint8_t>('S'));
        writer.write_string_null(std::move(severity.tag));
        writer.write_uint8(static_cast<uint8_t>('C'));
        writer.write_string_null(sqlstate);
        writer.write_uint8(static_cast<uint8_t>('M'));
        writer.write_string_null(std::move(message));
        writer.write_uint8(0x00); // terminator
        return writer.build_from_payload(message_type::backend::ERROR_RESPONSE);
    }

    std::vector<uint8_t> build_parameter_status(packet_writer& writer, std::string key, std::string value) {
        writer.reserve_payload(key.size() + value.size());
        writer.write_string_null(key);
        writer.write_string_null(value);
        return writer.build_from_payload(message_type::backend::PARAMETER_STATUS);
    }

    std::vector<uint8_t> build_backend_key_data(packet_writer& writer, int32_t pid, std::vector<uint8_t> key) {
        writer.reserve_payload(4 + key.size());
        writer.write_int32(pid);
        for (auto byte : key) {
            writer.write_uint8(byte);
        }
        return writer.build_from_payload(message_type::backend::BACKEND_KEY_DATA);
    }

    std::vector<uint8_t> build_row_description(packet_writer& writer,
                                               std::vector<field_description>&& fields,
                                               std::vector<result_encoding> encoding) {
        int32_t sz = 2;
        for (const auto& desc : fields) {
            sz += desc.field_size();
        }

        writer.reserve_payload(sz);
        writer.write_int16(fields.size());

        size_t i = 0;
        for (auto&& desc : fields) {
            field_description::write_field(std::move(desc),
                                           writer,
                                           get_format_code(encoding, i++).value_or(result_encoding::TEXT));
        }

        return writer.build_from_payload(message_type::backend::ROW_DESCRIPTION);
    }

    std::vector<uint8_t> build_ready_for_query(packet_writer& writer, transaction_status status) {
        writer.reserve_payload(READY_FOR_QUERY_SIZE);
        writer.write_uint8(static_cast<uint8_t>(status));
        return writer.build_from_payload(message_type::backend::READY_FOR_QUERY);
    }

    std::vector<uint8_t> build_empty_query_response(packet_writer& writer) {
        return writer.build_from_payload(message_type::backend::EMPTY_QUERY_RESPONSE);
    }

    std::vector<uint8_t> build_command_complete(packet_writer& writer, command_complete_tag tag) {
        writer.reserve_payload(tag.tag.size());
        writer.write_string_null(std::move(tag.tag));
        return writer.build_from_payload(message_type::backend::COMMAND_COMPLETE);
    }

    std::vector<uint8_t> build_parse_complete(packet_writer& writer) {
        return writer.build_from_payload(message_type::backend::PARSE_COMPLETE);
    }

    std::vector<uint8_t> build_bind_complete(packet_writer& writer) {
        return writer.build_from_payload(message_type::backend::BIND_COMPLETE);
    }

    std::vector<uint8_t> build_close_complete(packet_writer& writer) {
        return writer.build_from_payload(message_type::backend::CLOSE_COMPLETE);
    }

    std::vector<uint8_t> build_no_data(packet_writer& writer) {
        return writer.build_from_payload(message_type::backend::NO_DATA_MSG);
    }
} // namespace frontend::postgres
