// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <boost/mysql/common_server_errc.hpp>
#include <boost/mysql/error_categories.hpp>
#include <boost/system/error_code.hpp>

namespace mysql {

    // Maps a boost.mysql failure onto the engine's error space so the frontend
    // reports the backend's verdict, not a generic transport failure. A code the
    // server raised keeps its meaning (syntax, missing table/column/database,
    // duplicates, ambiguity, constraint) as the PostgreSQL code of the same verdict;
    // a server code without an engine counterpart is internal_error; anything the
    // client or the transport produced is an io_error.
    inline core::error_code_t classify_error(const boost::system::error_code& ec) noexcept {
        if (ec.category() == boost::mysql::get_common_server_category()) {
            using errc = boost::mysql::common_server_errc;
            switch (static_cast<errc>(ec.value())) {
                case errc::er_parse_error:
                case errc::er_syntax_error:
                    return core::error_code_t::sql_parse_error;
                case errc::er_no_such_table:
                case errc::er_bad_table_error:
                case errc::er_unknown_table:
                    return core::error_code_t::table_not_exists;
                case errc::er_bad_db_error:
                case errc::er_no_db_error:
                    return core::error_code_t::database_not_exists;
                case errc::er_bad_field_error:
                    return core::error_code_t::undefined_column;
                case errc::er_table_exists_error:
                    return core::error_code_t::table_already_exists;
                case errc::er_db_create_exists:
                    return core::error_code_t::database_already_exists;
                case errc::er_non_uniq_error:
                    return core::error_code_t::ambiguous_column;
                case errc::er_dup_fieldname:
                case errc::er_field_specified_twice:
                    return core::error_code_t::duplicate_column;
                case errc::er_dup_entry:
                case errc::er_dup_unique:
                    return core::error_code_t::unique_violation;
                case errc::er_bad_null_error:
                    return core::error_code_t::not_null_violation;
                case errc::er_no_referenced_row:
                case errc::er_no_referenced_row_2:
                case errc::er_row_is_referenced:
                case errc::er_row_is_referenced_2:
                    return core::error_code_t::foreign_key_violation;
                case errc::er_warn_data_out_of_range:
                    return core::error_code_t::numeric_value_out_of_range;
                case errc::er_truncated_wrong_value:
                case errc::er_truncated_wrong_value_for_field:
                    return core::error_code_t::invalid_text_representation;
                case errc::er_division_by_zero:
                    return core::error_code_t::division_by_zero;
                case errc::er_wrong_value_count:
                case errc::er_wrong_value_count_on_row:
                    return core::error_code_t::invalid_parameter;
                case errc::er_lock_wait_timeout:
                case errc::er_lock_deadlock:
                    return core::error_code_t::write_conflict;
                default:
                    return core::error_code_t::internal_error;
            }
        }
        if (ec.category() == boost::mysql::get_mysql_server_category() ||
            ec.category() == boost::mysql::get_mariadb_server_category()) {
            return core::error_code_t::internal_error;
        }
        return core::error_code_t::io_error;
    }

} // namespace mysql
