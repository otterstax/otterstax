// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#include "integration/kafka/detail/kafka_consumer.hpp"
#include "integration/kafka/detail/kafka_reader.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/otterbrix/otterbrix_engine.hpp"
#include "otterbrix/config.hpp"
#include "otterbrix/parser/parser.hpp"
#include "tests/mock/aliases.hpp"
#include "tests/temporary_directory.hpp"
#include "drive_future.hpp"
#include <catch2/catch_all.hpp>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

namespace kd = otterstax::kafka::detail;
using otterstax::security::network_destinations;
using namespace std::chrono_literals;

TEST_CASE("Kafka denies unconfigured bootstrap destinations before creating clients", "[kafka-destinations]") {
    auto policy = std::make_shared<const network_destinations>(std::vector<std::string>{"allowed.example:9092"});
    for (const auto* destination : {"forbidden.example:9092", "allowed.example:9092,forbidden.example:9092",
                                    "allowed.example:9092/", "127.1:9092"}) {
        auto producer = kd::kafka_producer_t::create({destination, "topic", "", policy});
        REQUIRE(producer.has_error());
        auto consumer = kd::kafka_consumer_t::create({destination, "topic", "group", "earliest", {}, policy});
        REQUIRE(consumer.has_error());
    }
    CHECK(kd::kafka_producer_t::create({"allowed.example:9092", "topic", "", {}}).has_error());
}

TEST_CASE("Kafka applies destination policy to advertised brokers as well as bootstrap", "[kafka-destinations]") {
    char error[512]{};
    std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> handle(
        rd_kafka_new(RD_KAFKA_PRODUCER, rd_kafka_conf_new(), error, sizeof(error)), rd_kafka_destroy);
    REQUIRE(handle);
    std::unique_ptr<rd_kafka_mock_cluster_t, decltype(&rd_kafka_mock_cluster_destroy)> cluster(
        rd_kafka_mock_cluster_new(handle.get(), 2), rd_kafka_mock_cluster_destroy);
    REQUIRE(cluster);
    REQUIRE(rd_kafka_mock_topic_create(cluster.get(), "policy", 1, 1) == RD_KAFKA_RESP_ERR_NO_ERROR);
    REQUIRE(rd_kafka_mock_partition_set_leader(cluster.get(), "policy", 0, 2) == RD_KAFKA_RESP_ERR_NO_ERROR);
    const std::string endpoints(rd_kafka_mock_cluster_bootstraps(cluster.get()));
    const auto comma = endpoints.find(',');
    REQUIRE(comma != std::string::npos);
    const auto first = endpoints.substr(0, comma);
    const auto second = endpoints.substr(comma + 1);
    rd_kafka_mock_start_request_tracking(cluster.get());
    {
        auto policy = std::make_shared<const network_destinations>(std::vector<std::string>{first});
        auto created = kd::kafka_producer_t::create({first, "policy", "", policy});
        REQUIRE_FALSE(created.has_error());
        auto producer = std::move(created.value());
        REQUIRE_FALSE(producer.produce("must-not-reach-broker-two").contains_error());
        CHECK(producer.flush(1500ms).contains_error());
    }
    std::size_t count{};
    auto requests = rd_kafka_mock_get_requests(cluster.get(), &count);
    REQUIRE(count > 0); // Bootstrap broker was actually contacted.
    for (std::size_t i = 0; i < count; ++i) CHECK(rd_kafka_mock_request_id(requests[i]) == 1);
    rd_kafka_mock_request_destroy_array(requests, count);
    rd_kafka_mock_clear_requests(cluster.get());
    {
        auto policy = std::make_shared<const network_destinations>(std::vector<std::string>{first, second});
        auto created = kd::kafka_producer_t::create({first, "policy", "", policy});
        REQUIRE_FALSE(created.has_error());
        auto producer = std::move(created.value());
        REQUIRE_FALSE(producer.produce("allowed-on-broker-two").contains_error());
        REQUIRE_FALSE(producer.flush(10s).contains_error());
    }
    requests = rd_kafka_mock_get_requests(cluster.get(), &count);
    bool produced_on_second = false;
    for (std::size_t i = 0; i < count; ++i)
        produced_on_second |= rd_kafka_mock_request_id(requests[i]) == 2 && rd_kafka_mock_request_api_key(requests[i]) == 0;
    rd_kafka_mock_request_destroy_array(requests, count);
    CHECK(produced_on_second);
}

TEST_CASE("Kafka rejected DDL leaves no tables and recovery rechecks administrator policy", "[kafka-destinations]") {
    otterstax::test::temporary_directory directory("otterstax-kafka-policy");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto address = engine->engine_dispatcher_address();
    GreenplumParser parser(resource, no_aliases());
    auto run = [&](otterstax::kafka::KafkaManager& manager, const char* name) {
        auto parsed = parser.parse(std::string("CREATE SOURCE ") + name +
            " (id INT) WITH (KAFKA_TOPIC='policy', BOOTSTRAP_SERVERS='127.0.0.1:9092');");
        REQUIRE_FALSE(parsed.has_error());
        auto node = otterstax::kafka::kafka_node_ptr{
            static_cast<otterstax::kafka::kafka_node_t*>(parsed.value()->otterbrix_params->node.get())};
        auto [_, future] = actor_zeta::send(manager.address(), &otterstax::kafka::KafkaManager::execute,
                                            session_hash_t{1}, std::move(node));
        return drive_until_ready(std::move(future));
    };
    auto denied = actor_zeta::spawn<otterstax::kafka::KafkaManager>(resource, address);
    auto result = run(*denied, "blocked");
    REQUIRE(result);
    CHECK(result->is_error());
    auto table = drive_until_ready(kd::kafka_query(address, resource, "SELECT * FROM kafka.blocked;"));
    REQUIRE(table);
    CHECK(table->is_error());
    auto policy = std::make_shared<const network_destinations>(std::vector<std::string>{"127.0.0.1:9092"});
    auto creator = actor_zeta::spawn<otterstax::kafka::KafkaManager>(resource, address, false, policy);
    result = run(*creator, "persisted");
    REQUIRE(result);
    REQUIRE_FALSE(result->is_error());
    creator.reset();
    auto restarted = actor_zeta::spawn<otterstax::kafka::KafkaManager>(resource, address, true);
    auto recovered = restarted->recover();
    CHECK(recovered.contains_error());
    CHECK(std::string(recovered.what).find("not allowed") != std::string::npos);
}
