// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// conform_to_result: a backend's answer held to the columns otterbrix planned.

#include "federation_fixture.hpp"

#include "otterbrix/federation/conform.hpp"

#include <components/casts/default_casts.hpp>

using namespace fedtest;

namespace {

    struct casts_t {
        std::pmr::synchronized_pool_resource resource;
        components::casts::cast_registry_t registry{&resource};
        casts_t() { components::casts::register_default_casts(registry); }
    };

} // namespace

TEST_CASE("conform: a column of another type is cast to the declared one and named") {
    casts_t casts;
    auto chunk = chunk_of(&casts.resource,
                     {column("x", logical_type::BIGINT), column("t", logical_type::STRING_LITERAL)},
                     {{std::int64_t{7}, std::string{"12"}}});
    const std::vector<complex_logical_type> declared{column("a", logical_type::BIGINT), column("b", logical_type::BIGINT)};
    auto refused = fed::conform_to_result(&casts.resource, casts.registry, "SELECT", declared, {}, chunk);
    INFO(std::string{refused.what});
    REQUIRE_FALSE(refused.contains_error());
    CHECK(chunk.data[1].type().type() == logical_type::BIGINT);
    CHECK(chunk.data[1].get_value<std::int64_t>(0) == 12);
    CHECK(chunk.data[0].type().alias() == "a");
}

TEST_CASE("conform: another column count is datatype_mismatch") {
    casts_t casts;
    auto chunk = chunk_of(&casts.resource, {column("x", logical_type::BIGINT)}, {{std::int64_t{1}}});
    const std::vector<complex_logical_type> declared{column("a", logical_type::BIGINT), column("b", logical_type::BIGINT)};
    auto refused = fed::conform_to_result(&casts.resource, casts.registry, "SELECT", declared, {}, chunk);
    CHECK(refused.type == core::error_code_t::datatype_mismatch);
}

TEST_CASE("conform: a text that is no number is the cast's refusal") {
    casts_t casts;
    auto chunk = chunk_of(&casts.resource, {column("x", logical_type::STRING_LITERAL)}, {{std::string{"abc"}}});
    const std::vector<complex_logical_type> declared{column("a", logical_type::BIGINT)};
    auto refused = fed::conform_to_result(&casts.resource, casts.registry, "SELECT", declared, {}, chunk);
    CHECK(refused.contains_error());
}

TEST_CASE("conform: max_rows_per_write per dialect") {
    CHECK(fed::max_rows_per_write(backend_type_t::PostgreSQL) == 1000);
    CHECK(fed::max_rows_per_write(backend_type_t::MySQL) == 1000);
    CHECK(fed::max_rows_per_write(backend_type_t::ClickHouse) == 10000);
}

TEST_CASE("the map open_engine takes: one address per connection, its own actor's") {
    fedtest::fed_stack_t stack({{"a", backend_type_t::PostgreSQL, "shop", "public"},
                                {"b", backend_type_t::MySQL, "bill", ""},
                                {"c", backend_type_t::ClickHouse, "ev", ""}});
    stack.open();
    std::vector<fedtest::fed::backend_actor_t*> actors;
    for (const char* name : {"a", "b", "c"}) {
        auto address = stack.actor(name).backend_address();
        CHECK(address.connection == name);
        CHECK(address.address == stack.actor(name).address());
        actors.push_back(&stack.actor(name));
    }
    CHECK(actors[0] != actors[1]);
    CHECK(actors[1] != actors[2]);
}
