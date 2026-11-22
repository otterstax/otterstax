// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../postgres_defs/error.hpp"
#include "../postgres_defs/field_type.hpp"
#include "../postgres_defs/message_type.hpp"
#include "../resultset/field_description.hpp"
#include "packet_writer.hpp"

#include <core/date/time_zone.hpp>
#include <components/sql/parser/nodes/nodes.h>
#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <vector>

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
        static command_complete_tag simple_command(NodeTag node, int64_t rows = 0);
        static command_complete_tag select(int64_t rows = 0);
        static command_complete_tag insert(int64_t rows = 0);
        static command_complete_tag update(int64_t rows = 0);
        static command_complete_tag delete_rows(int64_t rows = 0);
        static command_complete_tag begin();
        static command_complete_tag commit();
        static command_complete_tag rollback();
        static command_complete_tag savepoint();
        static command_complete_tag release();
        // RESET [name | ALL] (utility.c: VAR_RESET, VAR_RESET_ALL).
        static command_complete_tag reset();
        // DEALLOCATE <name> answers "DEALLOCATE", DEALLOCATE ALL "DEALLOCATE ALL".
        static command_complete_tag deallocate(bool all);

        std::string tag;

    private:
        command_complete_tag(std::string tag);
    };

    std::optional<frontend::result_encoding> get_format_code(const std::vector<frontend::result_encoding>& format,
                                                             size_t i);

    // A text Bind parameter of `type`. A date / time / timetz / timestamp / timestamptz / interval is read as
    // PostgreSQL reads it (core::date's ISO parse), a timestamptz or timetz written without an offset in `time_zone`
    // (the session's TimeZone; null UTC).
    core::result_wrapper_t<components::vector::vector_t>
    parse_text_parameter(std::pmr::memory_resource* resource,
                         field_type type,
                         const components::types::complex_logical_type* engine_type,
                         std::string text,
                         const core::date::time_zone_t* time_zone = nullptr);

    inline core::result_wrapper_t<components::vector::vector_t>
    parse_text_parameter(std::pmr::memory_resource* resource, field_type type, std::string text) {
        return parse_text_parameter(resource, type, nullptr, std::move(text));
    }

    // A binary Bind parameter of the declared NUMERIC type: PostgreSQL's
    // numeric_send layout (sign, weight, base-10000 digit groups) — backend
    // internal per https://www.postgresql.org/docs/current/protocol.html, read
    // as numeric_recv in src/backend/utils/adt/numeric.c. The number is rebuilt
    // as decimal text and bound like a text NUMERIC literal, refusals included;
    // a malformed body `datatype_mismatch`, NaN/Infinity `unimplemented_yet`.
    core::result_wrapper_t<components::vector::vector_t>
    parse_binary_numeric_parameter(std::pmr::memory_resource* resource,
                                   const components::types::complex_logical_type* engine_type,
                                   std::string_view value);

    std::vector<uint8_t> build_auth_ok(packet_writer& writer);

    // Fields S, C, M, and H (PostgreSQL's errhint) only when `hint` is given.
    std::vector<uint8_t> build_error_response(packet_writer& writer,
                                              const char* sqlstate,
                                              std::string message,
                                              error_severity severity = error_severity::fatal(),
                                              std::string hint = {});

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

    // ParameterDescription ('t'): the OID of every parameter of a prepared statement.
    std::vector<uint8_t> build_parameter_description(packet_writer& writer,
                                                     const std::pmr::vector<field_type>& parameter_oids);

    // PortalSuspended ('s'): Execute stopped at its row limit; the portal keeps the rest.
    std::vector<uint8_t> build_portal_suspended(packet_writer& writer);
} // namespace frontend::postgres
