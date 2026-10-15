// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#include "config/config.hpp"
#include "security/network_destinations.hpp"
#include "tests/temporary_directory.hpp"
#include <catch2/catch_all.hpp>
#include <fstream>

using otterstax::security::network_destinations;
using otterstax::security::canonical_destination;

TEST_CASE("Outbound destinations require exact canonical hosts and numeric ports") {
    network_destinations policy({"Kafka.EXAMPLE:09092", "127.0.0.1:9092", "[0:0:0:0:0:0:0:1]:9092"});
    CHECK(policy.allows("kafka.example:9092"));
    CHECK(policy.allows("[::1]:9092"));
    CHECK(policy.allows_list("127.0.0.1:9092,kafka.example:9092"));
    CHECK(canonical_destination("1broker:9092") == "1broker:9092");
    CHECK_FALSE(network_destinations{}.allows("127.0.0.1:9092"));
    for (const auto* endpoint : {"kafka.example:9093", "other.example:9092", "kafka.example.evil:9092",
                                 "kafka.example:9092,other.example:9092", "kafka.example:9092,", ""}) {
        CHECK_FALSE(policy.allows_list(endpoint));
    }
    for (const auto* endpoint : {"kafka.example", "kafka.example:", "kafka.example:0", "kafka.example:65536",
                                 "kafka.example:+9092", "kafka.example:9092/path", "kafka.example:9092?x",
                                 "user@kafka.example:9092", "plaintext://kafka.example:9092", "*.example:9092",
                                 "127.1:9092", "2130706433:9092", "0x7f000001:9092", "0177.0.0.1:9092",
                                 "[::1%lo]:9092", "::1:9092", "[127.0.0.1]:9092", "kafka.example.:9092",
                                 "kafka..example:9092", "-kafka.example:9092", "kafka_example:9092", "kafka.example:9092\n"}) {
        INFO(endpoint);
        CHECK_FALSE(canonical_destination(endpoint));
        CHECK_THROWS_AS(network_destinations({endpoint}), std::invalid_argument);
    }
    CHECK_FALSE(canonical_destination(std::string("kafka.example\0:9092", 19)));
}

TEST_CASE("Kafka administrator policy is parsed before the server starts") {
    otterstax::test::temporary_directory directory("otterstax-outbound-config");
    auto path = directory.path / "config.yaml";
    const auto entry = GENERATE("[]", "['Kafka.EXAMPLE:9092', '[::1]:9093']", "localhost:9092", "{}",
                                 "['*.example:9092']", "['localhost']", "['localhost:9092', 'bad:0']", "[[]]");
    { std::ofstream file(path); file << "service:\n  security:\n    mode: trusted_private\n    kafka_brokers: " << entry << '\n'; }
    config::ConfigReader reader(std::pmr::new_delete_resource());
    auto loaded = reader.load(path.string());
    const std::string_view text(entry);
    if (text == "[]") {
        REQUIRE_FALSE(loaded.has_error());
        CHECK(loaded.value().security.kafka_brokers.empty());
    } else if (text.starts_with("['Kafka.EXAMPLE")) {
        REQUIRE_FALSE(loaded.has_error());
        CHECK(loaded.value().security.kafka_brokers == std::vector<std::string>{"kafka.example:9092", "[::1]:9093"});
    } else {
        REQUIRE(loaded.has_error());
        CHECK(loaded.error().type == core::error_code_t::invalid_parameter);
    }
}
