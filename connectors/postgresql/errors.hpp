// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <string_view>

namespace pg {

    // Maps the SQLSTATE of a failed statement (PG_DIAG_SQLSTATE, five characters)
    // onto the engine's error space so the frontend reports the backend's verdict.
    // An empty SQLSTATE means libpq produced the failure itself — the connection
    // was lost or the client ran out of memory — which is an io_error. A server
    // class without an engine counterpart stays other_error.
    inline core::error_code_t classify_sqlstate(std::string_view sqlstate) noexcept {
        if (sqlstate.empty()) {
            return core::error_code_t::io_error;
        }
        if (sqlstate == "42601") {
            return core::error_code_t::sql_parse_error;
        }
        if (sqlstate == "42P01") {
            return core::error_code_t::table_not_exists;
        }
        if (sqlstate == "3D000") {
            return core::error_code_t::database_not_exists;
        }
        if (sqlstate == "42703") {
            return core::error_code_t::field_not_exists;
        }
        if (sqlstate == "42P07") {
            return core::error_code_t::table_already_exists;
        }
        if (sqlstate == "42P04") {
            return core::error_code_t::database_already_exists;
        }
        if (sqlstate == "42702") {
            return core::error_code_t::ambiguous_name;
        }
        if (sqlstate == "42701") {
            return core::error_code_t::duplicate_field;
        }
        if (sqlstate == "42883") {
            return core::error_code_t::unrecognized_function;
        }
        if (sqlstate == "53200") {
            return core::error_code_t::out_of_memory;
        }
        const std::string_view sql_class = sqlstate.substr(0, 2);
        if (sql_class == "08" || sql_class == "57" || sql_class == "58") {
            return core::error_code_t::io_error;
        }
        if (sql_class == "22") {
            return core::error_code_t::conversion_failure;
        }
        if (sql_class == "23") {
            return core::error_code_t::invalid_constraint;
        }
        if (sql_class == "40") {
            return core::error_code_t::write_conflict;
        }
        return core::error_code_t::other_error;
    }

} // namespace pg
