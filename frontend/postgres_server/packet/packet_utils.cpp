// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_utils.hpp"

#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <system_error>
#include <type_traits>

namespace {

    using components::types::logical_value_t;

    core::error_t bad_literal(std::pmr::memory_resource* resource, const std::string& text) {
        std::pmr::string what{"Invalid text literal for the declared parameter type: ", resource};
        what += text.c_str();
        return core::error_t{core::error_code_t::conversion_failure, std::move(what)};
    }

    // The whole text must be one number of the target type: no sign-less
    // truncation, no trailing characters, no out-of-range wrap-around.
    template<typename Int>
    core::result_wrapper_t<logical_value_t>
    parse_integer(std::pmr::memory_resource* resource, const std::string& text) {
        Int value{};
        const char* const end = text.data() + text.size();
        const auto [parsed_to, ec] = std::from_chars(text.data(), end, value);
        if (text.empty() || ec != std::errc{} || parsed_to != end) {
            return bad_literal(resource, text);
        }
        return logical_value_t{resource, value};
    }

    template<typename Float>
    core::result_wrapper_t<logical_value_t>
    parse_floating(std::pmr::memory_resource* resource, const std::string& text) {
        // strtod/strtof report errors through errno and the end pointer: a
        // non-throwing parse with the range check the wire format requires.
        char* parsed_to = nullptr;
        errno = 0;
        Float value{};
        if constexpr (std::is_same_v<Float, float>) {
            value = std::strtof(text.c_str(), &parsed_to);
        } else {
            value = std::strtod(text.c_str(), &parsed_to);
        }
        if (text.empty() || errno == ERANGE || parsed_to != text.c_str() + text.size()) {
            return bad_literal(resource, text);
        }
        return logical_value_t{resource, value};
    }

} // namespace

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

    command_complete_tag command_complete_tag::simple_command(NodeTag node, int64_t rows) {
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
                return select(rows);
            case T_UpdateStmt:
                return update(rows);
            case T_InsertStmt:
                return insert(rows);
            case T_DeleteStmt:
                return delete_rows(rows);
            case T_DropStmt:
                return {"DROP"};
            default:
                return {"COMMAND"};
        }
    }
    command_complete_tag command_complete_tag::select(int64_t rows) { return {"SELECT " + std::to_string(rows)}; }
    command_complete_tag command_complete_tag::insert(int64_t rows) { return {"INSERT 0 " + std::to_string(rows)}; }
    command_complete_tag command_complete_tag::update(int64_t rows) { return {"UPDATE " + std::to_string(rows)}; }
    command_complete_tag command_complete_tag::delete_rows(int64_t rows) { return {"DELETE " + std::to_string(rows)}; }
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

    core::result_wrapper_t<logical_value_t>
    parse_text_parameter(std::pmr::memory_resource* resource, field_type type, std::string text) {
        switch (type) {
            case field_type::BOOL:
                if (text != "t" && text != "f") {
                    return bad_literal(resource, text);
                }
                return logical_value_t{resource, text == "t"};
            case field_type::INT2:
                return parse_integer<int16_t>(resource, text);
            case field_type::INT4:
                return parse_integer<int32_t>(resource, text);
            case field_type::INT8:
                return parse_integer<int64_t>(resource, text);
            case field_type::FLOAT4:
                return parse_floating<float>(resource, text);
            case field_type::FLOAT8:
                return parse_floating<double>(resource, text);
            case field_type::TEXT:
                return logical_value_t{resource, std::move(text)};
            default:
                return core::error_t{core::error_code_t::unimplemented_yet,
                                     std::pmr::string{"Unsupported parameter type", resource}};
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

    std::vector<uint8_t> build_parameter_description(packet_writer& writer,
                                                     const std::pmr::vector<field_type>& parameter_types) {
        writer.reserve_payload(2 + 4 * parameter_types.size());
        writer.write_int16(static_cast<int16_t>(parameter_types.size()));
        for (auto type : parameter_types) {
            writer.write_int32(static_cast<int32_t>(static_cast<oid_t>(type)));
        }
        return writer.build_from_payload(message_type::backend::PARAMETER_DESCRIPTION);
    }

    std::vector<uint8_t> build_portal_suspended(packet_writer& writer) {
        return writer.build_from_payload(message_type::backend::PORTAL_SUSPENDED);
    }
} // namespace frontend::postgres
