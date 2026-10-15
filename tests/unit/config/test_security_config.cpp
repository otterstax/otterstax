// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "config/config.hpp"
#include "security/transport.hpp"
#include "tests/temporary_directory.hpp"

#include <catch2/catch_all.hpp>
#include <fstream>

TEST_CASE("Client security requires an explicit deployment mode") {
    using namespace otterstax::security;
    CHECK_THROWS_AS(make_server_access(access_config{}), std::invalid_argument);
    access_config config;
    config.mode = access_mode::trusted_private;
    auto access = make_server_access(config);
    CHECK_FALSE(access->requires_authentication());
    CHECK_FALSE(access->tls);
    CHECK(access->grpc_credentials);
    config.mode = access_mode::keycloak;
    CHECK_THROWS_AS(make_server_access(config), std::invalid_argument);
}

TEST_CASE("Client security configuration rejects malformed and incomplete policies") {
    const auto policy = GENERATE(
        "[]", "{}", "{mode: anonymous}", "{mode: [keycloak]}", "{mode: keycloak}",
        "{mode: trusted_private, certificate_file: cert.pem}",
        "{mode: trusted_private, private_key_file: key.pem}",
        "{mode: trusted_private, keycloak: []}",
        "{mode: trusted_private, keycloak: {max_in_flight: 0}}",
        "{mode: trusted_private, keycloak: {request_timeout_ms: -1}}",
        "{mode: trusted_private, keycloak: {max_token_bytes: nope}}");
    otterstax::test::temporary_directory directory("otterstax-security-config");
    auto path = directory.path / "config.yaml";
    { std::ofstream file(path); file << "service:\n  security: " << policy << '\n'; }
    config::ConfigReader reader(std::pmr::new_delete_resource());
    auto loaded = reader.load(path.string());
    REQUIRE(loaded.has_error());
    CHECK(loaded.error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("Client security configuration preserves explicit TLS and provider limits") {
    otterstax::test::temporary_directory directory("otterstax-security-config");
    auto path = directory.path / "config.yaml";
    {
        std::ofstream file(path);
        file << R"(service:
  security:
    mode: keycloak
    certificate_file: /tls/cert.pem
    private_key_file: /tls/key.pem
    keycloak:
      issuer: https://identity.example/realms/tenant
      client_id: database
      client_secret_env: TEST_IDENTITY_SECRET
      ca_file: /tls/ca.pem
      request_timeout_ms: 1500
      max_token_bytes: 8192
      max_response_bytes: 32768
      max_in_flight: 4
)";
    }
    config::ConfigReader reader(std::pmr::new_delete_resource());
    auto loaded = reader.load(path.string());
    REQUIRE_FALSE(loaded.has_error());
    const auto& security = loaded.value().security;
    CHECK(security.mode == otterstax::security::access_mode::keycloak);
    CHECK(security.certificate_file == "/tls/cert.pem");
    CHECK(security.private_key_file == "/tls/key.pem");
    CHECK(security.keycloak.client_secret_env == "TEST_IDENTITY_SECRET");
    CHECK(security.keycloak.request_timeout == std::chrono::milliseconds{1500});
    CHECK(security.keycloak.max_token_bytes == 8192);
    CHECK(security.keycloak.max_response_bytes == 32768);
    CHECK(security.keycloak.max_in_flight == 4);
}

TEST_CASE("Client file policy rejects malformed roots and limits") {
    const auto files = GENERATE("[]", "{read_roots: /tmp}", "{write_roots: [relative]}",
                                "{read_roots: [/tmp/../etc]}", "{write_roots: [[]]}",
                                "{max_file_bytes: 0}", "{max_file_bytes: -1}", "{max_file_bytes: many}");
    otterstax::test::temporary_directory directory("otterstax-file-config");
    const auto path = directory.path / "config.yaml";
    { std::ofstream file(path); file << "service:\n  security:\n    mode: trusted_private\n    files: " << files << '\n'; }
    config::ConfigReader reader(std::pmr::new_delete_resource());
    CHECK(reader.load(path.string()).has_error());
}

TEST_CASE("Client file policy preserves separate roots and bounded file size") {
    otterstax::test::temporary_directory directory("otterstax-file-config");
    const auto path = directory.path / "config.yaml";
    { std::ofstream file(path); file << R"(service:
  security:
    mode: trusted_private
    files:
      read_roots: [/imports, /shared/input]
      write_roots: [/exports]
      max_file_bytes: 4096
)"; }
    config::ConfigReader reader(std::pmr::new_delete_resource());
    auto loaded = reader.load(path.string());
    REQUIRE_FALSE(loaded.has_error());
    CHECK(loaded.value().security.files.read_roots == std::vector<std::string>{"/imports", "/shared/input"});
    CHECK(loaded.value().security.files.write_roots == std::vector<std::string>{"/exports"});
    CHECK(loaded.value().security.files.max_file_bytes == 4096);
}
