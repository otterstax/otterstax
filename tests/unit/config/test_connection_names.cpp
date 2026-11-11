// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "config/config.hpp"
#include "config/connections/connection_config_reader.hpp"

#include <catch2/catch_all.hpp>
#include <yaml-cpp/yaml.h>

#include <memory_resource>
#include <string>
#include <string_view>

namespace {
    std::pmr::memory_resource* resource() { return std::pmr::new_delete_resource(); }

    YAML::Node connections_node(const std::string& yaml) { return YAML::Load(yaml)["connections"]; }

    core::error_t parse_error(const YAML::Node& node) {
        auto parsed = config::parse_connections(node, resource());
        REQUIRE(parsed.has_error());
        return parsed.error();
    }

    bool mentions(const core::error_t& error, std::string_view text) {
        return std::string_view{error.what.data(), error.what.size()}.find(text) != std::string_view::npos;
    }
} // namespace

TEST_CASE("parse_connections: the alias local is refused in every section and any case") {
    const char* yaml = R"(
connections:
  mysql:
    - { alias: local, host: h, username: u, database: d }
)";
    const auto err = parse_error(connections_node(yaml));
    CHECK(mentions(err, "reserved"));
    CHECK(mentions(err, "local"));
}

TEST_CASE("parse_connections: the alias local is refused in any case") {
    for (const char* alias : {"local", "Local", "LOCAL", "LoCaL"}) {
        INFO("alias " << alias);
        const std::string yaml = std::string{R"(
connections:
  postgresql:
    - { alias: )"} + alias + R"(, host: h, username: u, database: d, schema: s }
)";
        const auto err = parse_error(connections_node(yaml));
        CHECK(mentions(err, "reserved"));
    }
}

TEST_CASE("parse_connections: the alias local is refused in the s3 section too") {
    const char* yaml = R"(
connections:
  s3:
    - { alias: local, access_key: k, secret_key: s }
)";
    const auto err = parse_error(connections_node(yaml));
    CHECK(mentions(err, "reserved"));
}

TEST_CASE("parse_connections: a name containing local but not equal to it is an ordinary alias") {
    const char* yaml = R"(
connections:
  mysql:
    - { alias: localstore, host: h, username: u, database: d }
  s3:
    - { alias: my_local, access_key: k, secret_key: s }
)";
    auto parsed = config::parse_connections(connections_node(yaml), resource());
    REQUIRE_FALSE(parsed.has_error());
}
