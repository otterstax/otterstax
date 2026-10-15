// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#include "config/connections/connection_config_reader.hpp"
#include "utility/s3_transport.hpp"
#include <catch2/catch_all.hpp>
#include <cstdlib>

TEST_CASE("S3 transport defaults to verified HTTPS and accepts explicit local HTTP") {
    config::S3CredentialDesc descriptor;
    CHECK(descriptor.scheme == "https");
    CHECK_FALSE(otterstax::validate_s3_transport("https", "storage.example:9443", "/tls/root.pem"));
    CHECK_FALSE(otterstax::validate_s3_transport("https", "[::1]:9443", ""));
    CHECK_FALSE(otterstax::validate_s3_transport("http", "localhost:9000", ""));
    CHECK_FALSE(otterstax::validate_s3_transport("https", "s3.us-east-1.amazonaws.com", ""));
    CHECK(otterstax::validate_s3_transport("https", "", ""));
    CHECK(otterstax::validate_s3_transport("ftp", "storage.example", ""));
    CHECK(otterstax::validate_s3_transport("http", "localhost:9000", "/tls/root.pem"));
    for (const auto* endpoint : {"http://storage.example", "user@storage.example", "host/path", "host?x=y", "host#x", "host\n",
                                  "127.1", "0x7f000001", "storage.example:0", "storage.example:65536", "[::1%lo]:443"}) {
        CHECK(otterstax::validate_s3_transport("https", endpoint, ""));
    }
    auto parsed = config::parse_connections(YAML::Load(R"(s3:
  - alias: storage
    access_key: test-only-key
    secret_key: test-only-secret
    endpoint: storage.example:9443
    ca_file: /tls/root.pem
)"), std::pmr::new_delete_resource());
    REQUIRE_FALSE(parsed.has_error());
    CHECK(parsed.value().s3[0].scheme == "https");
    CHECK(parsed.value().s3[0].ca_file == "/tls/root.pem");
}

TEST_CASE("S3 credentials require valid populated environment references without inline ambiguity") {
    const auto entry = GENERATE(
        "access_key_env: 9INVALID", "secret_key_env: ''", "session_token_env: []",
        "secret_key_env: OTTERSTAX_UNSET_S3_TEST_SECRET", "scheme: [https]", "scheme: ftp",
        "endpoint: http://storage.example", "scheme: http\n    ca_file: /tls/root.pem");
    auto node = YAML::Load(std::string{"s3:\n  - alias: storage\n    access_key: test-only-key\n    secret_key: test-only-secret\n    "} + entry);
    auto result = config::parse_connections(node, std::pmr::new_delete_resource());
    CHECK(result.has_error());
}
