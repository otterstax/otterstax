// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <string_view>

namespace pg {

    // Maps the SQLSTATE of a failed statement (PG_DIAG_SQLSTATE, five characters)
    // onto the engine's error space so the frontend reports the backend's verdict.
    // The engine's codes are PostgreSQL's (core/result_wrapper.hpp names the
    // SQLSTATE of each), so a SQLSTATE the engine has a code for keeps it exactly;
    // the rest go by their class. An empty SQLSTATE means libpq produced the
    // failure itself — the connection was lost or the client ran out of memory —
    // which is an io_error. A class without an engine counterpart is
    // internal_error.
    inline core::error_code_t classify_sqlstate(std::string_view sqlstate) noexcept {
        using code = core::error_code_t;
        if (sqlstate.empty()) {
            return code::io_error;
        }
        struct exact_t {
            std::string_view sqlstate;
            code type;
        };
        static constexpr exact_t exact[] = {
            {"0A000", code::unimplemented_yet},
            {"08P01", code::protocol_violation},
            {"21000", code::cardinality_violation},
            {"22002", code::null_value_no_indicator_parameter},
            {"22003", code::numeric_value_out_of_range},
            {"22004", code::null_value_not_allowed},
            {"22007", code::invalid_datetime_format},
            {"22012", code::division_by_zero},
            {"22023", code::invalid_parameter},
            {"2201B", code::invalid_regular_expression},
            {"2201W", code::invalid_row_count_in_limit_clause},
            {"2201X", code::invalid_row_count_in_result_offset_clause},
            {"22P02", code::invalid_text_representation},
            {"23502", code::not_null_violation},
            {"23503", code::foreign_key_violation},
            {"23505", code::unique_violation},
            {"23514", code::check_violation},
            {"25001", code::active_sql_transaction},
            {"25P01", code::no_active_sql_transaction},
            {"26000", code::undefined_prepared_statement},
            {"2BP01", code::dependent_objects_still_exist},
            {"34000", code::undefined_cursor},
            {"3B001", code::missing_savepoint},
            {"3D000", code::database_not_exists},
            {"3F000", code::schema_not_exists},
            {"42601", code::sql_parse_error},
            {"42701", code::duplicate_column},
            {"42702", code::ambiguous_column},
            {"42703", code::undefined_column},
            {"42704", code::undefined_object},
            {"42710", code::duplicate_object},
            {"42712", code::duplicate_alias},
            {"42723", code::duplicate_function},
            {"42725", code::ambiguous_function},
            {"42803", code::grouping_error},
            {"42804", code::datatype_mismatch},
            {"42809", code::wrong_object_type},
            {"42830", code::invalid_foreign_key},
            {"42846", code::cannot_coerce},
            {"42883", code::unrecognized_function},
            {"42P01", code::table_not_exists},
            {"42P02", code::undefined_parameter},
            {"42P03", code::duplicate_cursor},
            {"42P04", code::database_already_exists},
            {"42P05", code::duplicate_prepared_statement},
            {"42P06", code::schema_already_exists},
            {"42P07", code::table_already_exists},
            {"42P08", code::ambiguous_parameter},
            {"42P09", code::ambiguous_alias},
            {"42P10", code::invalid_column_reference},
            {"42P13", code::invalid_function_definition},
            {"42P16", code::invalid_table_definition},
            {"42P17", code::invalid_object_definition},
            {"42P18", code::indeterminate_datatype},
            {"42P20", code::windowing_error},
            {"53000", code::insufficient_resources},
            {"53200", code::out_of_memory},
            {"54000", code::program_limit_exceeded},
            {"54001", code::statement_too_complex},
            {"55000", code::object_not_in_prerequisite_state},
            {"55006", code::object_in_use},
            {"F0001", code::lock_file_exists},
            {"XX000", code::internal_error},
            {"XX001", code::data_corruption},
            {"XX002", code::index_corrupted},
        };
        for (const auto& entry : exact) {
            if (entry.sqlstate == sqlstate) {
                return entry.type;
            }
        }
        const std::string_view sql_class = sqlstate.substr(0, 2);
        if (sql_class == "08") {
            return code::connection_closed;
        }
        if (sql_class == "57" || sql_class == "58") {
            return code::io_error;
        }
        if (sql_class == "22") {
            return code::invalid_text_representation;
        }
        if (sql_class == "23") {
            return code::check_violation;
        }
        if (sql_class == "40") {
            return code::write_conflict;
        }
        return code::internal_error;
    }

} // namespace pg
