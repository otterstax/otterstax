// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "security/access.hpp"

#include <boost/json.hpp>
#include <catch2/catch_all.hpp>

using namespace otterstax::security;

namespace {
    keycloak_config configuration() {
        keycloak_config config;
        config.issuer = "https://identity.example/realms/tenant";
        config.client_id = "tenant-database";
        return config;
    }

    boost::json::object valid_response() {
        return {{"active", true}, {"iss", "https://identity.example/realms/tenant"},
                {"aud", "tenant-database"}, {"sub", "subject-id"},
                {"token_type", "Bearer"}, {"exp", 2000}, {"nbf", 900}};
    }

    const auto now = std::chrono::system_clock::time_point{std::chrono::seconds{1000}};
}

TEST_CASE("Keycloak: active access tokens require exact issuer audience and validity") {
    auto response = valid_response();
    auto result = validate_introspection(boost::json::serialize(response), configuration(), now);
    REQUIRE(result.status == token_status::accepted);
    CHECK(result.subject == "subject-id");
    CHECK(result.expires_at == std::chrono::system_clock::time_point{std::chrono::seconds{2000}});
    response["aud"] = boost::json::array{"another-service", "tenant-database"};
    CHECK(validate_introspection(boost::json::serialize(response), configuration(), now).status == token_status::accepted);
    response.erase("nbf");
    CHECK(validate_introspection(boost::json::serialize(response), configuration(), now).status == token_status::accepted);
}

TEST_CASE("Keycloak: missing required claims and untrusted token types are refused") {
    const auto key = GENERATE("active", "iss", "aud", "sub", "token_type", "exp");
    auto response = valid_response();
    response.erase(key);
    CHECK(validate_introspection(boost::json::serialize(response), configuration(), now).status == token_status::rejected);
    response = valid_response();
    response[key] = nullptr;
    CHECK(validate_introspection(boost::json::serialize(response), configuration(), now).status == token_status::rejected);
    response[key] = false;
    CHECK(validate_introspection(boost::json::serialize(response), configuration(), now).status == token_status::rejected);
}

TEST_CASE("Keycloak: invalid claims cannot grant access") {
    const auto change = GENERATE(
        R"({"active":false})", R"({"active":"true"})",
        R"({"iss":"https://identity.example/realms/other"})",
        R"({"iss":"https://identity.example/realms/tenant/"})",
        R"({"aud":"other-tenant"})", R"({"aud":["other-tenant"]})",
        R"({"aud":[]})", R"({"aud":{}})", R"({"aud":["tenant-database",42]})", R"({"sub":""})",
        R"({"token_type":"Refresh"})", R"({"token_type":"Offline"})",
        R"({"exp":1000})", R"({"exp":999})", R"({"exp":-1})",
        R"({"exp":"2000"})", R"({"exp":2000.5})", R"({"exp":9223372036854775807})",
        R"({"nbf":1001})", R"({"nbf":null})", R"({"nbf":"900"})");
    auto response = valid_response();
    const auto parsed_change = boost::json::parse(change);
    for (const auto& item : parsed_change.as_object()) {
        response[item.key()] = item.value();
    }
    auto result = validate_introspection(boost::json::serialize(response), configuration(), now);
    CHECK(result.status == token_status::rejected);
    CHECK(result.subject.empty());
}

TEST_CASE("Keycloak: invalid and oversized provider responses are refused") {
    const auto body = GENERATE("", "{}", "null", "[]", "true", "{\"active\":true}", "invalid JSON");
    CHECK(validate_introspection(body, configuration(), now).status == token_status::rejected);
    auto config = configuration();
    config.max_response_bytes = 8;
    CHECK(validate_introspection(boost::json::serialize(valid_response()), config, now).status == token_status::rejected);
}

TEST_CASE("Keycloak: bearer syntax excludes whitespace separators and oversized input") {
    const auto invalid = GENERATE("", " ", "a b", "a\nb", "a\rb", "=a", "a=b", "a:b", "a&b", "a%b");
    CHECK_FALSE(valid_bearer_token(invalid, 16));
    CHECK_FALSE(valid_bearer_token(std::string_view{"abc\0def", 7}, 16));
    CHECK_FALSE(valid_bearer_token(std::string(17, 'a'), 16));
    CHECK(valid_bearer_token("abc.DEF_1-2", 16));
    CHECK(valid_bearer_token("abc+/~==", 16));
}
