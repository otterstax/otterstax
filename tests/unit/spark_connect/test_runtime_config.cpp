// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "frontend/spark_connect_server/runtime_config.hpp"
#include <catch2/catch_all.hpp>

namespace sc = ::spark::connect;
using frontend::spark::runtime_config;

TEST_CASE("Spark Config: mutations cannot report success without changing behavior", "[spark-config]") {
    sc::ConfigRequest request;
    sc::ConfigResponse response;
    request.set_session_id("session-a");
    auto* pair = request.mutable_operation()->mutable_set()->add_pairs();
    pair->set_key("spark.sql.session.timeZone");
    pair->set_value("Europe/Paris");
    CHECK(runtime_config(request, response).error_code() == grpc::StatusCode::UNIMPLEMENTED);
    CHECK(response.session_id() == "session-a");
    request.mutable_operation()->mutable_unset()->add_keys("spark.sql.session.timeZone");
    CHECK(runtime_config(request, response).error_code() == grpc::StatusCode::UNIMPLEMENTED);
    request.mutable_operation()->mutable_get()->add_keys("spark.sql.session.timeZone");
    REQUIRE(runtime_config(request, response).ok());
    REQUIRE(response.pairs_size() == 1);
    CHECK(response.pairs(0).value() == "UTC");
}

TEST_CASE("Spark Config: GetWithDefault prefers the actual value", "[spark-config]") {
    sc::ConfigRequest request;
    sc::ConfigResponse response;
    auto* pair = request.mutable_operation()->mutable_get_with_default()->add_pairs();
    pair->set_key("spark.sql.session.timeZone");
    pair->set_value("Europe/Paris");
    pair = request.mutable_operation()->mutable_get_with_default()->add_pairs();
    pair->set_key("unconfigured.key");
    pair->set_value("fallback");
    REQUIRE(runtime_config(request, response).ok());
    REQUIRE(response.pairs_size() == 2);
    CHECK(response.pairs(0).value() == "UTC");
    CHECK(response.pairs(1).value() == "fallback");
}

TEST_CASE("Spark Config: unknown option is absent, not an empty configured value", "[spark-config]") {
    sc::ConfigRequest request;
    sc::ConfigResponse response;
    request.mutable_operation()->mutable_get_option()->add_keys("unconfigured.key");
    REQUIRE(runtime_config(request, response).ok());
    REQUIRE(response.pairs_size() == 1);
    CHECK_FALSE(response.pairs(0).has_value());
    request.mutable_operation()->mutable_get()->add_keys("spark.sql.session.timeZone");
    request.mutable_operation()->mutable_get()->add_keys("unconfigured.key");
    CHECK(runtime_config(request, response).error_code() == grpc::StatusCode::NOT_FOUND);
    CHECK(response.pairs_size() == 0);
}

TEST_CASE("Spark Config: fixed and unknown settings are not modifiable", "[spark-config]") {
    sc::ConfigRequest request;
    sc::ConfigResponse response;
    request.mutable_operation()->mutable_is_modifiable()->add_keys("spark.sql.session.timeZone");
    request.mutable_operation()->mutable_is_modifiable()->add_keys("unconfigured.key");
    REQUIRE(runtime_config(request, response).ok());
    REQUIRE(response.pairs_size() == 2);
    CHECK(response.pairs(0).value() == "false");
    CHECK(response.pairs(1).value() == "false");
}

TEST_CASE("Spark Config: GetAll filters and removes the requested prefix", "[spark-config]") {
    sc::ConfigRequest request;
    sc::ConfigResponse response;
    request.mutable_operation()->mutable_get_all()->set_prefix("spark.sql.session.");
    REQUIRE(runtime_config(request, response).ok());
    REQUIRE(response.pairs_size() == 1);
    CHECK(response.pairs(0).key() == "timeZone");
    CHECK(response.pairs(0).value() == "UTC");
    request.mutable_operation()->mutable_get_all()->set_prefix("unconfigured.");
    REQUIRE(runtime_config(request, response).ok());
    CHECK(response.pairs_size() == 0);
}

TEST_CASE("Spark Config: missing operation is rejected", "[spark-config]") {
    sc::ConfigRequest request;
    sc::ConfigResponse response;
    CHECK(runtime_config(request, response).error_code() == grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_CASE("Spark Config: best-effort SET warns for every unapplied key without echoing values", "[spark-config]") {
    sc::ConfigRequest request;
    sc::ConfigResponse response;
    auto* operation = request.mutable_operation()->mutable_set();
    operation->set_silent(true);
    auto* pair = operation->add_pairs();
    pair->set_key("spark.sql.session.timeZone");
    pair->set_value("Europe/Paris");
    pair = operation->add_pairs();
    pair->set_key("unconfigured.key");
    pair->set_value("private-value");
    REQUIRE(runtime_config(request, response).ok());
    REQUIRE(response.warnings_size() == 2);
    CHECK(response.warnings(0).find("spark.sql.session.timeZone") != std::string::npos);
    CHECK(response.warnings(1).find("unconfigured.key") != std::string::npos);
    for (const auto& warning : response.warnings()) {
        CHECK(warning.find("Europe/Paris") == std::string::npos);
        CHECK(warning.find("private-value") == std::string::npos);
    }
    request.mutable_operation()->mutable_get()->add_keys("spark.sql.session.timeZone");
    REQUIRE(runtime_config(request, response).ok());
    REQUIRE(response.pairs_size() == 1);
    CHECK(response.pairs(0).value() == "UTC");
    CHECK(response.warnings_size() == 0);
}

TEST_CASE("Spark Config: empty mutation requests make no changes", "[spark-config]") {
    sc::ConfigRequest request;
    sc::ConfigResponse response;
    request.mutable_operation()->mutable_set();
    CHECK(runtime_config(request, response).ok());
    CHECK(response.warnings_size() == 0);
    request.mutable_operation()->mutable_unset();
    CHECK(runtime_config(request, response).ok());
    CHECK(response.warnings_size() == 0);
}
