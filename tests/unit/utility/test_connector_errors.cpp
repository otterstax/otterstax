// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Error classification of the three SQL connectors: a statement the backend
// rejects reaches the caller with the backend's verdict (syntax, missing
// table/column/database, duplicates, constraint ...) as the PostgreSQL code of
// the same verdict, not as a generic io_error or internal_error. Transport
// failures, and only those, are io_error.

#include "connectors/clickhouse/errors.hpp"
#include "connectors/mysql/errors.hpp"
#include "connectors/postgresql/errors.hpp"

#include <catch2/catch_all.hpp>

#include <boost/asio/error.hpp>
#include <boost/mysql/client_errc.hpp>
#include <boost/mysql/common_server_errc.hpp>
#include <boost/mysql/error_categories.hpp>
#include <boost/system/error_code.hpp>

TEST_CASE("mysql::classify_error: a server verdict keeps its meaning") {
    using boost::mysql::common_server_errc;
    using boost::mysql::make_error_code;

    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_parse_error)) ==
            core::error_code_t::sql_parse_error);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_syntax_error)) ==
            core::error_code_t::sql_parse_error);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_no_such_table)) ==
            core::error_code_t::table_not_exists);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_bad_db_error)) ==
            core::error_code_t::database_not_exists);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_bad_field_error)) ==
            core::error_code_t::undefined_column);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_table_exists_error)) ==
            core::error_code_t::table_already_exists);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_db_create_exists)) ==
            core::error_code_t::database_already_exists);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_non_uniq_error)) ==
            core::error_code_t::ambiguous_column);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_dup_fieldname)) ==
            core::error_code_t::duplicate_column);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_dup_entry)) ==
            core::error_code_t::unique_violation);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_bad_null_error)) ==
            core::error_code_t::not_null_violation);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_no_referenced_row_2)) ==
            core::error_code_t::foreign_key_violation);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_division_by_zero)) ==
            core::error_code_t::division_by_zero);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_wrong_value_count)) ==
            core::error_code_t::invalid_parameter);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_lock_wait_timeout)) ==
            core::error_code_t::write_conflict);
}

TEST_CASE("mysql::classify_error: a server code without an engine counterpart is internal_error, not io_error") {
    using boost::mysql::common_server_errc;
    REQUIRE(mysql::classify_error(boost::mysql::make_error_code(common_server_errc::er_access_denied_error)) ==
            core::error_code_t::internal_error);
    // Vendor-specific server categories carry codes this mapping does not name.
    REQUIRE(mysql::classify_error(boost::system::error_code(3024, boost::mysql::get_mysql_server_category())) ==
            core::error_code_t::internal_error);
    REQUIRE(mysql::classify_error(boost::system::error_code(1927, boost::mysql::get_mariadb_server_category())) ==
            core::error_code_t::internal_error);
}

TEST_CASE("mysql::classify_error: client and transport failures are io_error") {
    REQUIRE(mysql::classify_error(boost::mysql::make_error_code(boost::mysql::client_errc::incomplete_message)) ==
            core::error_code_t::io_error);
    REQUIRE(mysql::classify_error(boost::asio::error::make_error_code(boost::asio::error::connection_reset)) ==
            core::error_code_t::io_error);
    REQUIRE(mysql::classify_error(boost::asio::error::make_error_code(boost::asio::error::operation_aborted)) ==
            core::error_code_t::io_error);
}

TEST_CASE("pg::classify_sqlstate: a server verdict keeps its meaning") {
    REQUIRE(pg::classify_sqlstate("42601") == core::error_code_t::sql_parse_error);
    REQUIRE(pg::classify_sqlstate("42P01") == core::error_code_t::table_not_exists);
    REQUIRE(pg::classify_sqlstate("3D000") == core::error_code_t::database_not_exists);
    REQUIRE(pg::classify_sqlstate("42703") == core::error_code_t::undefined_column);
    REQUIRE(pg::classify_sqlstate("42P07") == core::error_code_t::table_already_exists);
    REQUIRE(pg::classify_sqlstate("42P04") == core::error_code_t::database_already_exists);
    REQUIRE(pg::classify_sqlstate("42702") == core::error_code_t::ambiguous_column);
    REQUIRE(pg::classify_sqlstate("42701") == core::error_code_t::duplicate_column);
    REQUIRE(pg::classify_sqlstate("42883") == core::error_code_t::unrecognized_function);
    REQUIRE(pg::classify_sqlstate("22P02") == core::error_code_t::invalid_text_representation);
    REQUIRE(pg::classify_sqlstate("22003") == core::error_code_t::numeric_value_out_of_range);
    REQUIRE(pg::classify_sqlstate("22001") == core::error_code_t::invalid_text_representation); // class 22
    REQUIRE(pg::classify_sqlstate("23505") == core::error_code_t::unique_violation);
    REQUIRE(pg::classify_sqlstate("23502") == core::error_code_t::not_null_violation);
    REQUIRE(pg::classify_sqlstate("23P01") == core::error_code_t::check_violation); // class 23
    REQUIRE(pg::classify_sqlstate("3F000") == core::error_code_t::schema_not_exists);
    REQUIRE(pg::classify_sqlstate("42704") == core::error_code_t::undefined_object);
    REQUIRE(pg::classify_sqlstate("42804") == core::error_code_t::datatype_mismatch);
    REQUIRE(pg::classify_sqlstate("25P01") == core::error_code_t::no_active_sql_transaction);
    REQUIRE(pg::classify_sqlstate("3B001") == core::error_code_t::missing_savepoint);
    REQUIRE(pg::classify_sqlstate("0A000") == core::error_code_t::unimplemented_yet);
    REQUIRE(pg::classify_sqlstate("40P01") == core::error_code_t::write_conflict);
    REQUIRE(pg::classify_sqlstate("53200") == core::error_code_t::out_of_memory);
}

TEST_CASE("pg::classify_sqlstate: connection loss is io_error or connection_closed, unknown classes are internal_error") {
    // libpq reports its own failures (lost connection, out of memory) without a
    // SQLSTATE at all.
    REQUIRE(pg::classify_sqlstate("") == core::error_code_t::io_error);
    REQUIRE(pg::classify_sqlstate("08006") == core::error_code_t::connection_closed);
    REQUIRE(pg::classify_sqlstate("57P01") == core::error_code_t::io_error);
    REQUIRE(pg::classify_sqlstate("58030") == core::error_code_t::io_error);
    REQUIRE(pg::classify_sqlstate("42501") == core::error_code_t::internal_error);
}

TEST_CASE("ch::classify_server_code: a server verdict keeps its meaning") {
    REQUIRE(ch::classify_server_code(62) == core::error_code_t::sql_parse_error);
    REQUIRE(ch::classify_server_code(60) == core::error_code_t::table_not_exists);
    REQUIRE(ch::classify_server_code(81) == core::error_code_t::database_not_exists);
    REQUIRE(ch::classify_server_code(47) == core::error_code_t::undefined_column);
    REQUIRE(ch::classify_server_code(57) == core::error_code_t::table_already_exists);
    REQUIRE(ch::classify_server_code(82) == core::error_code_t::database_already_exists);
    REQUIRE(ch::classify_server_code(207) == core::error_code_t::ambiguous_column);
    REQUIRE(ch::classify_server_code(15) == core::error_code_t::duplicate_column);
    REQUIRE(ch::classify_server_code(46) == core::error_code_t::unrecognized_function);
    REQUIRE(ch::classify_server_code(42) == core::error_code_t::unrecognized_function);
    REQUIRE(ch::classify_server_code(43) == core::error_code_t::datatype_mismatch);
    REQUIRE(ch::classify_server_code(53) == core::error_code_t::datatype_mismatch);
    REQUIRE(ch::classify_server_code(70) == core::error_code_t::cannot_coerce);
    REQUIRE(ch::classify_server_code(6) == core::error_code_t::invalid_text_representation);
    REQUIRE(ch::classify_server_code(48) == core::error_code_t::unimplemented_yet);
    REQUIRE(ch::classify_server_code(241) == core::error_code_t::out_of_memory);
}

TEST_CASE("ch::classify_server_code: transport codes are io_error, unknown codes are internal_error") {
    REQUIRE(ch::classify_server_code(210) == core::error_code_t::io_error);
    REQUIRE(ch::classify_server_code(209) == core::error_code_t::io_error);
    REQUIRE(ch::classify_server_code(32) == core::error_code_t::io_error);
    REQUIRE(ch::classify_server_code(497) == core::error_code_t::internal_error);
    REQUIRE(ch::classify_server_code(0) == core::error_code_t::internal_error);
}
