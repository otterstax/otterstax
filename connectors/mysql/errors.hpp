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
    // duplicates, ambiguity, constraint); a server code without an engine
    // counterpart stays other_error; anything the client or the transport produced
    // is an io_error.
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
                    return core::error_code_t::field_not_exists;
                case errc::er_table_exists_error:
                    return core::error_code_t::table_already_exists;
                case errc::er_db_create_exists:
                    return core::error_code_t::database_already_exists;
                case errc::er_non_uniq_error:
                    return core::error_code_t::ambiguous_name;
                case errc::er_dup_fieldname:
                case errc::er_field_specified_twice:
                    return core::error_code_t::duplicate_field;
                case errc::er_dup_entry:
                case errc::er_dup_unique:
                case errc::er_bad_null_error:
                    return core::error_code_t::invalid_constraint;
                case errc::er_wrong_value_count:
                case errc::er_wrong_value_count_on_row:
                    return core::error_code_t::invalid_parameter;
                case errc::er_lock_wait_timeout:
                    return core::error_code_t::write_conflict;
                default:
                    return core::error_code_t::other_error;
            }
        }
        if (ec.category() == boost::mysql::get_mysql_server_category() ||
            ec.category() == boost::mysql::get_mariadb_server_category()) {
            return core::error_code_t::other_error;
        }
        return core::error_code_t::io_error;
    }

} // namespace mysql
