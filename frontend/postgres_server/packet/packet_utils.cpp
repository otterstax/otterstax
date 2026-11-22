// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "packet_utils.hpp"

#include "../../common/utils.hpp"
#include "otterbrix/translators/input/decimal_text.hpp"

#include <components/casts/kernels/string_cast.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/date/date_parse.hpp>

#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <optional>
#include <string_view>
#include <system_error>
#include <type_traits>

namespace {

    using components::vector::vector_t;

    core::error_t bad_literal(std::pmr::memory_resource* resource, const std::string& text) {
        std::pmr::string what{"Invalid text literal for the declared parameter type: ", resource};
        what += text.c_str();
        return core::error_t{core::error_code_t::invalid_text_representation, std::move(what)};
    }

    // PostgreSQL 18's boolin (parse_bool_with_len): blanks around the value, any case, a prefix of true / yes /
    // false / no, on / of / off (two letters at least), 1 / 0.
    std::optional<bool> parse_pg_bool(std::string_view text) {
        const auto blank = [](char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'; };
        while (!text.empty() && blank(text.front())) {
            text.remove_prefix(1);
        }
        while (!text.empty() && blank(text.back())) {
            text.remove_suffix(1);
        }
        if (text.empty()) {
            return std::nullopt;
        }
        const auto prefix_of = [&](std::string_view word) {
            if (text.size() > word.size()) {
                return false;
            }
            for (std::size_t i = 0; i < text.size(); ++i) {
                if (std::tolower(static_cast<unsigned char>(text[i])) != word[i]) {
                    return false;
                }
            }
            return true;
        };
        switch (std::tolower(static_cast<unsigned char>(text.front()))) {
            case 't':
                return prefix_of("true") ? std::optional<bool>{true} : std::nullopt;
            case 'y':
                return prefix_of("yes") ? std::optional<bool>{true} : std::nullopt;
            case 'f':
                return prefix_of("false") ? std::optional<bool>{false} : std::nullopt;
            case 'n':
                return prefix_of("no") ? std::optional<bool>{false} : std::nullopt;
            case 'o':
                if (text.size() >= 2 && prefix_of("on")) {
                    return true;
                }
                if (text.size() >= 2 && prefix_of("off")) {
                    return false;
                }
                return std::nullopt;
            case '1':
                return text.size() == 1 ? std::optional<bool>{true} : std::nullopt;
            case '0':
                return text.size() == 1 ? std::optional<bool>{false} : std::nullopt;
            default:
                return std::nullopt;
        }
    }

    core::error_t literal_past_precision(std::pmr::memory_resource* resource, const std::string& text) {
        std::pmr::string what{"The value is past the declared parameter type's precision: ", resource};
        what += text.c_str();
        return core::error_t{core::error_code_t::numeric_value_out_of_range, std::move(what)};
    }

    // The whole text must be one number of the target type: no sign-less
    // truncation, no trailing characters, no out-of-range wrap-around.
    template<typename Int>
    core::result_wrapper_t<vector_t>
    parse_integer(std::pmr::memory_resource* resource, const std::string& text) {
        Int value{};
        const char* const end = text.data() + text.size();
        const auto [parsed_to, ec] = std::from_chars(text.data(), end, value);
        if (text.empty() || ec != std::errc{} || parsed_to != end) {
            return bad_literal(resource, text);
        }
        return components::vector::make_scalar(resource, value);
    }

    template<typename Float>
    core::result_wrapper_t<vector_t>
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
        return components::vector::make_scalar(resource, value);
    }

    // The NUMERIC literal as the engine's DECIMAL: read with the engine's own
    // parse into the described (p, s) — a longer fraction rounds half away
    // from zero, a value past the precision is refused — and stored at the
    // width the type is held in.
    core::result_wrapper_t<vector_t>
    parse_decimal_parameter(std::pmr::memory_resource* resource,
                            const components::types::complex_logical_type& type,
                            const std::string& text) {
        const auto* spec = type.extension_as<components::types::decimal_logical_type_extension>();
        components::types::int128_t unscaled = 0;
        using components::casts::kernels::detail::parse_decimal;
        using components::casts::kernels::detail::parse_outcome;
        switch (parse_decimal(text, spec->width(), spec->scale(), &unscaled)) {
            case parse_outcome::parsed:
                break;
            case parse_outcome::out_of_range:
                return literal_past_precision(resource, text);
            default:
                return bad_literal(resource, text);
        }
        auto bound = type;
        bound.set_alias(std::string{});
        return components::vector::make_decimal(resource, bound, unscaled);
    }

    // What the wire's NUMERIC literal binds as: the engine's type of the
    // parameter decides. A parameter the engine typed as neither DECIMAL nor a
    // string takes no NUMERIC literal.
    core::result_wrapper_t<vector_t>
    parse_numeric_parameter(std::pmr::memory_resource* resource,
                            const components::types::complex_logical_type* engine_type,
                            std::string text) {
        if (engine_type == nullptr || engine_type->type() == components::types::logical_type::NA ||
            engine_type->type() == components::types::logical_type::TEXT ||
            engine_type->type() == components::types::logical_type::STRING_LITERAL) {
            return components::vector::make_scalar(resource, std::string_view{text});
        }
        if (engine_type->type() != components::types::logical_type::DECIMAL) {
            std::pmr::string what{"The parameter is typed ", resource};
            what += frontend::logical_type_name(engine_type->type());
            what += ", which does not take a NUMERIC literal: ";
            what += text.c_str();
            return core::error_t{core::error_code_t::datatype_mismatch, std::move(what)};
        }
        return parse_decimal_parameter(resource, *engine_type, text);
    }

} // namespace

namespace frontend::postgres {
    core::result_wrapper_t<components::vector::vector_t>
    parse_binary_numeric_parameter(std::pmr::memory_resource* resource,
                                   const components::types::complex_logical_type* engine_type,
                                   std::string_view value) {
        constexpr uint16_t NUMERIC_NEG = 0x4000;
        constexpr uint16_t NUMERIC_NAN = 0xC000;
        constexpr uint16_t NUMERIC_PINF = 0xD000;
        constexpr uint16_t NUMERIC_NINF = 0xF000;
        // A malformed value is a literal the format cannot hold (22P02 on the
        // wire, the Bind's default refusal), not a type conflict.
        const auto bad = [&](const char* what) {
            return core::error_t{core::error_code_t::invalid_text_representation,
                                 std::pmr::string{what, resource}};
        };
        if (value.size() < 8) {
            return bad("Truncated binary NUMERIC parameter");
        }
        const auto field = [&value](size_t at) {
            return static_cast<uint16_t>(static_cast<unsigned char>(value[at]) << 8 |
                                         static_cast<unsigned char>(value[at + 1]));
        };
        const auto ndigits = static_cast<int>(static_cast<int16_t>(field(0)));
        const auto weight = static_cast<int>(static_cast<int16_t>(field(2)));
        const auto sign = field(4);
        // field(6) is the display scale: the digits carry the value, the
        // engine's described DECIMAL carries the bound.
        if (ndigits < 0 || value.size() != 8u + 2u * static_cast<unsigned>(ndigits)) {
            return bad("Malformed binary NUMERIC parameter");
        }
        if (sign == NUMERIC_NAN || sign == NUMERIC_PINF || sign == NUMERIC_NINF) {
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"NaN and Infinity NUMERIC parameters are not supported", resource}};
        }
        if (sign != 0 && sign != NUMERIC_NEG) {
            return bad("Invalid binary NUMERIC sign");
        }
        // The digit groups of Σ d[i]·10^(4·(weight−i)): the first flat, every
        // later one exactly four digits; the point sits 4·(ndigits−1−weight)
        // digits from the right, which a zero count and any negative weight
        // fold into one fractional tail.
        if (ndigits == 0) {
            return parse_numeric_parameter(resource, engine_type, sign == NUMERIC_NEG ? "-0" : "0");
        }
        std::string digits;
        digits.reserve(static_cast<size_t>(ndigits) * 4u);
        for (int i = 0; i < ndigits; ++i) {
            const auto group = field(8 + 2u * static_cast<size_t>(i));
            if (group > 9999) {
                return bad("Invalid binary NUMERIC digit group");
            }
            const auto written = std::to_string(group);
            if (i > 0) {
                digits.append(4u - written.size(), '0');
            }
            digits += written;
        }
        std::string text;
        if (sign == NUMERIC_NEG) {
            text.push_back('-');
        }
        const auto point_from_right = 4 * (ndigits - 1 - weight);
        if (point_from_right <= 0) {
            text += digits;
            text.append(static_cast<size_t>(-point_from_right), '0');
        } else if (static_cast<int>(digits.size()) > point_from_right) {
            text.append(digits, 0, digits.size() - static_cast<size_t>(point_from_right));
            text.push_back('.');
            text.append(digits, digits.size() - static_cast<size_t>(point_from_right),
                        static_cast<size_t>(point_from_right));
        } else {
            text += "0.";
            text.append(static_cast<size_t>(point_from_right - static_cast<int>(digits.size())), '0');
            text += digits;
        }
        return parse_numeric_parameter(resource, engine_type, std::move(text));
    }

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
            case T_VariableSetStmt:
                return {"SET"};
            case T_VariableShowStmt:
                return {"SHOW"};
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
    command_complete_tag command_complete_tag::reset() { return {"RESET"}; }

    command_complete_tag command_complete_tag::release() { return {"RELEASE"}; }
    command_complete_tag command_complete_tag::deallocate(bool all) {
        return {all ? "DEALLOCATE ALL" : "DEALLOCATE"};
    }

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

    core::result_wrapper_t<vector_t>
    parse_text_parameter(std::pmr::memory_resource* resource,
                         field_type type,
                         const components::types::complex_logical_type* engine_type,
                         std::string text,
                         const core::date::time_zone_t* time_zone) {
        const core::date::time_zone_t utc;
        const auto& zone = time_zone != nullptr ? *time_zone : utc;
        const auto temporal = [&](auto parsed) -> core::result_wrapper_t<vector_t> {
            if (!parsed) {
                return bad_literal(resource, text);
            }
            return components::vector::make_scalar(resource, *parsed);
        };
        switch (type) {
            case field_type::DATE:
                return temporal(core::date::parse_date(text));
            case field_type::TIME:
                return temporal(core::date::parse_time(text));
            case field_type::TIMETZ:
                return temporal(core::date::parse_timetz(text, zone));
            case field_type::TIMESTAMP:
                return temporal(core::date::parse_timestamp(text));
            case field_type::TIMESTAMPTZ:
                return temporal(core::date::parse_timestamptz(text, zone));
            case field_type::INTERVAL:
                return temporal(core::date::parse_interval(text));
            case field_type::BOOL: {
                const auto value = parse_pg_bool(text);
                if (!value) {
                    return bad_literal(resource, text);
                }
                return components::vector::make_scalar(resource, *value);
            }
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
            case field_type::NUMERIC:
                return parse_numeric_parameter(resource, engine_type, text);
            case field_type::TEXT:
                return components::vector::make_scalar(resource, std::string_view{text});
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

    std::vector<uint8_t> build_error_response(packet_writer& writer,
                                              const char* sqlstate,
                                              std::string message,
                                              error_severity severity,
                                              std::string hint) {
        writer.reserve_payload(ERROR_FIXED_SIZE + message.size() + severity.tag.size() +
                               (hint.empty() ? 0 : hint.size() + 2));
        writer.write_uint8(static_cast<uint8_t>('S'));
        writer.write_string_null(std::move(severity.tag));
        writer.write_uint8(static_cast<uint8_t>('C'));
        writer.write_string_null(sqlstate);
        writer.write_uint8(static_cast<uint8_t>('M'));
        writer.write_string_null(std::move(message));
        if (!hint.empty()) {
            writer.write_uint8(static_cast<uint8_t>('H'));
            writer.write_string_null(std::move(hint));
        }
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
                                                     const std::pmr::vector<field_type>& parameter_oids) {
        writer.reserve_payload(2 + 4 * parameter_oids.size());
        writer.write_int16(static_cast<int16_t>(parameter_oids.size()));
        for (auto type : parameter_oids) {
            writer.write_int32(static_cast<int32_t>(static_cast<oid_t>(type)));
        }
        return writer.build_from_payload(message_type::backend::PARAMETER_DESCRIPTION);
    }

    std::vector<uint8_t> build_portal_suspended(packet_writer& writer) {
        return writer.build_from_payload(message_type::backend::PORTAL_SUSPENDED);
    }
} // namespace frontend::postgres
