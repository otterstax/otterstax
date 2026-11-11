// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "federation_fixture.hpp"

#include <catch2/catch_all.hpp>

#include <string>
#include <vector>

namespace fedtest {
    namespace cat = otterstax::catalog;

    TEST_CASE("connection names: the registrar refuses an alias that names an existing local database") {
        // No connections at startup: the engine alone, so the local base is only the test's.
        fed_stack_t stack({});
        stack.open();
        stack.ok("CREATE DATABASE eg;");
        stack.ok("CREATE TABLE eg.public.t (id bigint);");
        REQUIRE(rows_of(stack.ok("SELECT count(*) FROM eg.public.t;")).size() == 1);

        // A connection aliased eg, its discovery answering nothing yet.
        std::vector<cat::connection_discovery_t> discoveries;
        discoveries.push_back(cat::connection_discovery_t{
            cat::configured_connection_t{"eg", backend_type_t::MySQL, "bill", "", {}, nullptr},
            true,
            fed::discovered_tables_t{stack.resource()}});
        cat::engine_calls_t calls{[&stack](const components::session::session_id_t& session, const std::string& sql) {
                                      return stack.dispatcher()->execute_sql(session, sql);
                                  },
                                  [&stack](const components::session::session_id_t& session,
                                           components::logical_plan::execution_plan_t plan) {
                                      return stack.dispatcher()->execute_plan(session, std::move(plan));
                                  }};
        auto registered = cat::register_connections(stack.resource(), calls, stack.registration_session(), discoveries);
        INFO("register_connections -> " << (registered.contains_error() ? std::string{registered.what} : "ok"));
        REQUIRE(registered.contains_error());
    }

    TEST_CASE("connection names: the registrar still takes an alias no local database names") {
        fed_stack_t stack({});
        stack.open();
        std::vector<cat::connection_discovery_t> discoveries;
        discoveries.push_back(cat::connection_discovery_t{
            cat::configured_connection_t{"shop", backend_type_t::PostgreSQL, "shop_db", "shop", {}, nullptr},
            true,
            fed::discovered_tables_t{stack.resource()}});
        cat::engine_calls_t calls{[&stack](const components::session::session_id_t& session, const std::string& sql) {
                                      return stack.dispatcher()->execute_sql(session, sql);
                                  },
                                  [&stack](const components::session::session_id_t& session,
                                           components::logical_plan::execution_plan_t plan) {
                                      return stack.dispatcher()->execute_plan(session, std::move(plan));
                                  }};
        auto registered = cat::register_connections(stack.resource(), calls, stack.registration_session(), discoveries);
        INFO("register_connections -> " << (registered.contains_error() ? std::string{registered.what} : "ok"));
        REQUIRE_FALSE(registered.contains_error());
    }

    TEST_CASE("connection names: a client's named database is its session's place") {
        const std::vector<cat::configured_connection_t> connections{
            {"pg", backend_type_t::PostgreSQL, "shop_db", "shop", {}, nullptr},
            {"pg_all", backend_type_t::PostgreSQL, "shop_db", "", {}, nullptr},
            {"my", backend_type_t::MySQL, "bill", "", {}, nullptr},
            {"ch", backend_type_t::ClickHouse, "ev", "", {}, nullptr}};
        const std::string refused = "!";
        struct case_t {
            std::string named;
            std::string place;
        };
        for (const auto& c : std::vector<case_t>{{"", ""},
                                                 {"pg", "pg.shop"},
                                                 {"PG", "pg.shop"},
                                                 {"pg_all", "pg_all.public"},
                                                 {"my", "my.bill"},
                                                 {"ch", "ch.ev"},
                                                 {"pg.sales", "pg.sales"},
                                                 {"my.ops", "my.ops"},
                                                 {"local", "local.public"},
                                                 {"LOCAL", "local.public"},
                                                 {"local.eg", "local.eg"},
                                                 {"shop", refused},
                                                 {"nope.x", refused},
                                                 {"main", refused}}) {
            INFO("named '" << c.named << "'");
            auto place = cat::client_place(std::pmr::new_delete_resource(), c.named, connections);
            if (c.place == refused) {
                REQUIRE(place.has_error());
                CHECK(place.error().type == core::error_code_t::database_not_exists);
                CHECK(std::string{place.error().what.c_str()} == "database \"" + c.named + "\" does not exist");
            } else {
                REQUIRE_FALSE(place.has_error());
                CHECK(place.value() == c.place);
            }
        }
    }
} // namespace fedtest
