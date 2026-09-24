// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Error classification of the three SQL connectors: a statement the backend
// rejects reaches the caller with the backend's verdict (syntax, missing
// table/column/database, duplicates, constraint ...), not as a generic io_error
// or other_error. Transport failures, and only those, are io_error.

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
            core::error_code_t::field_not_exists);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_table_exists_error)) ==
            core::error_code_t::table_already_exists);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_db_create_exists)) ==
            core::error_code_t::database_already_exists);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_non_uniq_error)) ==
            core::error_code_t::ambiguous_name);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_dup_fieldname)) ==
            core::error_code_t::duplicate_field);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_dup_entry)) ==
            core::error_code_t::invalid_constraint);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_wrong_value_count)) ==
            core::error_code_t::invalid_parameter);
    REQUIRE(mysql::classify_error(make_error_code(common_server_errc::er_lock_wait_timeout)) ==
            core::error_code_t::write_conflict);
}

TEST_CASE("mysql::classify_error: a server code without an engine counterpart is other_error, not io_error") {
    using boost::mysql::common_server_errc;
    REQUIRE(mysql::classify_error(boost::mysql::make_error_code(common_server_errc::er_access_denied_error)) ==
            core::error_code_t::other_error);
    // Vendor-specific server categories carry codes this mapping does not name.
    REQUIRE(mysql::classify_error(boost::system::error_code(3024, boost::mysql::get_mysql_server_category())) ==
            core::error_code_t::other_error);
    REQUIRE(mysql::classify_error(boost::system::error_code(1927, boost::mysql::get_mariadb_server_category())) ==
            core::error_code_t::other_error);
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
    REQUIRE(pg::classify_sqlstate("42703") == core::error_code_t::field_not_exists);
    REQUIRE(pg::classify_sqlstate("42P07") == core::error_code_t::table_already_exists);
    REQUIRE(pg::classify_sqlstate("42P04") == core::error_code_t::database_already_exists);
    REQUIRE(pg::classify_sqlstate("42702") == core::error_code_t::ambiguous_name);
    REQUIRE(pg::classify_sqlstate("42701") == core::error_code_t::duplicate_field);
    REQUIRE(pg::classify_sqlstate("42883") == core::error_code_t::unrecognized_function);
    REQUIRE(pg::classify_sqlstate("22P02") == core::error_code_t::conversion_failure);
    REQUIRE(pg::classify_sqlstate("23505") == core::error_code_t::invalid_constraint);
    REQUIRE(pg::classify_sqlstate("40P01") == core::error_code_t::write_conflict);
    REQUIRE(pg::classify_sqlstate("53200") == core::error_code_t::out_of_memory);
}

TEST_CASE("pg::classify_sqlstate: connection loss is io_error, unknown classes are other_error") {
    // libpq reports its own failures (lost connection, out of memory) without a
    // SQLSTATE at all.
    REQUIRE(pg::classify_sqlstate("") == core::error_code_t::io_error);
    REQUIRE(pg::classify_sqlstate("08006") == core::error_code_t::io_error);
    REQUIRE(pg::classify_sqlstate("57P01") == core::error_code_t::io_error);
    REQUIRE(pg::classify_sqlstate("58030") == core::error_code_t::io_error);
    REQUIRE(pg::classify_sqlstate("42501") == core::error_code_t::other_error);
    REQUIRE(pg::classify_sqlstate("0A000") == core::error_code_t::other_error);
}

TEST_CASE("ch::classify_server_code: a server verdict keeps its meaning") {
    REQUIRE(ch::classify_server_code(62) == core::error_code_t::sql_parse_error);
    REQUIRE(ch::classify_server_code(60) == core::error_code_t::table_not_exists);
    REQUIRE(ch::classify_server_code(81) == core::error_code_t::database_not_exists);
    REQUIRE(ch::classify_server_code(47) == core::error_code_t::field_not_exists);
    REQUIRE(ch::classify_server_code(57) == core::error_code_t::table_already_exists);
    REQUIRE(ch::classify_server_code(82) == core::error_code_t::database_already_exists);
    REQUIRE(ch::classify_server_code(207) == core::error_code_t::ambiguous_name);
    REQUIRE(ch::classify_server_code(15) == core::error_code_t::duplicate_field);
    REQUIRE(ch::classify_server_code(46) == core::error_code_t::unrecognized_function);
    REQUIRE(ch::classify_server_code(42) == core::error_code_t::incorrect_function_argument);
    REQUIRE(ch::classify_server_code(53) == core::error_code_t::conversion_failure);
    REQUIRE(ch::classify_server_code(48) == core::error_code_t::unimplemented_yet);
    REQUIRE(ch::classify_server_code(241) == core::error_code_t::out_of_memory);
}

TEST_CASE("ch::classify_server_code: transport codes are io_error, unknown codes are other_error") {
    REQUIRE(ch::classify_server_code(210) == core::error_code_t::io_error);
    REQUIRE(ch::classify_server_code(209) == core::error_code_t::io_error);
    REQUIRE(ch::classify_server_code(32) == core::error_code_t::io_error);
    REQUIRE(ch::classify_server_code(497) == core::error_code_t::other_error);
    REQUIRE(ch::classify_server_code(0) == core::error_code_t::other_error);
}
