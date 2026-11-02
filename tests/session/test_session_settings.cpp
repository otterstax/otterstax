// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// A client changes its own session's settings only. ALTER SYSTEM, ALTER
// DATABASE ... SET and ALTER ROLE ... SET reach no engine through any SQL
// route a frontend takes (a direct execute: PG simple, spark.sql, Flight by
// text; a prepare: PG extended, Flight prepared): the parser refuses them as
// unimplemented_yet "server-wide settings are not changed by a client", which
// fails an open block as any refusal does. A SET of the session runs in the
// engine and reports what PostgreSQL reports.

#include "../system/session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <string>

using otterstax::session::query_session_t;
using otterstax::test::prepare_session_sql;
using otterstax::test::run_session_or_fail;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack_owner;

namespace {
    using status_t = query_session_t::transaction_status_t;
    constexpr const char* kServerWide = "server-wide settings are not changed by a client";
} // namespace

TEST_CASE("session settings: server-wide settings are refused on every SQL route") {
    session_stack_owner owner("/tmp/test_session_settings_server_wide");
    auto s = owner.stack();
    auto& session = owner.default_session();
    session_hash_t id = 21000;
    for (const char* sql : {"ALTER SYSTEM SET search_path = 'x.public'",
                            "ALTER SYSTEM RESET timezone",
                            "ALTER SYSTEM RESET ALL",
                            "ALTER DATABASE main SET timezone = 'UTC'",
                            "ALTER ROLE r SET timezone = 'UTC'"}) {
        INFO(sql);
        for (bool prepare : {false, true}) {
            auto refused = prepare ? prepare_session_sql(s, id++, sql) : run_session_sql_payload(s, id++, sql);
            REQUIRE(refused.has_error());
            CHECK(refused.error().type == core::error_code_t::unimplemented_yet);
            CHECK(std::string{refused.error().what.c_str()} == kServerWide);
            CHECK(session.transaction_status() == status_t::idle);
        }
    }
    run_session_or_fail(s, id++, "BEGIN;");
    REQUIRE(run_session_sql_payload(s, id++, "ALTER SYSTEM SET timezone = 'UTC'").has_error());
    CHECK(session.transaction_status() == status_t::failed);
    run_session_or_fail(s, id++, "ROLLBACK;");
}

// What a SET / RESET did to the engine session's settings, as the engine
// applies it: each assignment with its canonical value (nullopt for the
// default), LOCAL told apart, inside a block too (the frontend follows the
// block's end itself); a savepoint statement names its savepoint. A prepare
// changes nothing and names nothing.
TEST_CASE("session settings: an executed SET names what it assigned") {
    using components::catalog::setting_id;
    session_stack_owner owner("/tmp/test_session_settings_reported");
    auto s = owner.stack();
    session_hash_t id = 21100;
    auto set = run_session_sql_payload(s, id++, "SET search_path = 'main.public';");
    REQUIRE_FALSE(set.has_error());
    REQUIRE(set.value().setting_changes.size() == 1);
    CHECK(set.value().setting_changes[0].id == setting_id::search_path);
    CHECK(set.value().setting_changes[0].value == std::optional<std::string>{"main.public"});
    CHECK_FALSE(set.value().setting_changes[0].local);
    auto zone = run_session_sql_payload(s, id++, "SET TimeZone = 'Asia/Tokyo';");
    REQUIRE_FALSE(zone.has_error());
    REQUIRE(zone.value().setting_changes.size() == 1);
    CHECK(zone.value().setting_changes[0].value == std::optional<std::string>{"asia/tokyo"});
    auto reset = run_session_sql_payload(s, id++, "RESET timezone;");
    REQUIRE_FALSE(reset.has_error());
    REQUIRE(reset.value().setting_changes.size() == 1);
    CHECK(reset.value().setting_changes[0].id == setting_id::timezone);
    CHECK_FALSE(reset.value().setting_changes[0].value.has_value());
    auto all = run_session_sql_payload(s, id++, "RESET ALL;");
    REQUIRE_FALSE(all.has_error());
    CHECK(all.value().setting_changes.size() == components::catalog::all_settings().size());

    run_session_or_fail(s, id++, "BEGIN;");
    auto local = run_session_sql_payload(s, id++, "SET LOCAL timezone = 'UTC';");
    REQUIRE_FALSE(local.has_error());
    REQUIRE(local.value().setting_changes.size() == 1);
    CHECK(local.value().setting_changes[0].local);
    auto savepoint = run_session_sql_payload(s, id++, "SAVEPOINT sp;");
    REQUIRE_FALSE(savepoint.has_error());
    CHECK(savepoint.value().savepoint == std::optional<std::string>{"sp"});
    run_session_or_fail(s, id++, "ROLLBACK;");

    auto prepared = prepare_session_sql(s, id++, "SET timezone = 'UTC'");
    REQUIRE_FALSE(prepared.has_error());
    CHECK(prepared.value().setting_changes.empty());
}
