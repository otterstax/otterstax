// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include "integration/kafka/detail/kafka_producer.hpp"
#include <catch2/catch_all.hpp>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

namespace {
    namespace kd = otterstax::kafka::detail;
    using namespace std::chrono_literals;

    struct mock_cluster {
        std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> handle{nullptr, rd_kafka_destroy};
        std::unique_ptr<rd_kafka_mock_cluster_t, decltype(&rd_kafka_mock_cluster_destroy)>
            cluster{nullptr, rd_kafka_mock_cluster_destroy};

        mock_cluster() {
            char error[512]{};
            handle.reset(rd_kafka_new(RD_KAFKA_PRODUCER, rd_kafka_conf_new(), error, sizeof(error)));
            INFO(error);
            REQUIRE(handle);
            cluster.reset(rd_kafka_mock_cluster_new(handle.get(), 1));
            REQUIRE(cluster);
            REQUIRE(rd_kafka_mock_topic_create(cluster.get(), "delivery", 1, 1) == RD_KAFKA_RESP_ERR_NO_ERROR);
        }

        kd::kafka_producer_t producer(const std::string& transactional_id = {}) {
            auto result = kd::kafka_producer_t::create(
                {rd_kafka_mock_cluster_bootstraps(cluster.get()), "delivery", transactional_id,
                 std::make_shared<const otterstax::security::network_destinations>(
                     std::vector<std::string>{rd_kafka_mock_cluster_bootstraps(cluster.get())})});
            REQUIRE_FALSE(result.has_error());
            return std::move(result.value());
        }
    };
}

TEST_CASE("Kafka producer reports delivery rejection after its queue drains", "[kafka-delivery]") {
    mock_cluster mock;
    auto producer = mock.producer();
    REQUIRE_FALSE(producer.produce("accepted").contains_error());
    REQUIRE_FALSE(producer.flush(10s).contains_error());

    // Produce returns after queueing. The broker rejects the message later;
    // a drained queue alone is not evidence of successful delivery.
    rd_kafka_mock_push_request_errors(mock.cluster.get(), 0, 1, RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE);
    REQUIRE_FALSE(producer.produce("rejected").contains_error());
    CHECK(producer.flush(10s).contains_error());

    REQUIRE_FALSE(producer.produce("accepted-after-error").contains_error());
    CHECK_FALSE(producer.flush(10s).contains_error());
}

TEST_CASE("Kafka producer retains delivery tracking after move construction and assignment", "[kafka-delivery]") {
    mock_cluster mock;
    auto source = mock.producer();
    REQUIRE_FALSE(source.produce("accepted").contains_error());
    auto moved = std::move(source);
    REQUIRE_FALSE(moved.flush(10s).contains_error());
    auto assigned = mock.producer();
    REQUIRE_FALSE(assigned.produce("old-producer").contains_error());
    assigned = std::move(moved);
    rd_kafka_mock_push_request_errors(mock.cluster.get(), 0, 1, RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE);
    REQUIRE_FALSE(assigned.produce("rejected").contains_error());
    CHECK(assigned.flush(10s).contains_error());
}

TEST_CASE("Kafka producer delivery failure does not poison the next transaction after abort", "[kafka-delivery]") {
    mock_cluster mock;
    auto producer = mock.producer("delivery-abort");
    REQUIRE(producer.begin_transaction().ok());
    rd_kafka_mock_push_request_errors(mock.cluster.get(), 0, 1, RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE);
    REQUIRE_FALSE(producer.produce("rejected").contains_error());
    REQUIRE(producer.flush(10s).contains_error());
    REQUIRE(producer.abort_transaction(10s).ok());
    CHECK_FALSE(producer.flush(10s).contains_error());
    // librdkafka may need another abort to bump the idempotent producer epoch
    // after a broker rejection. Resume only the operation its result permits.
    bool committed = false;
    for (int attempt = 0; attempt < 3 && !committed; ++attempt) {
        REQUIRE(producer.begin_transaction().ok());
        REQUIRE_FALSE(producer.produce("committed").contains_error());
        auto result = producer.commit_transaction(10s);
        INFO(result.error.what);
        committed = result.ok();
        if (!committed) {
            REQUIRE_FALSE(result.fatal);
            REQUIRE(result.requires_abort);
            REQUIRE(producer.abort_transaction(10s).ok());
            CHECK_FALSE(producer.flush(10s).contains_error());
        }
    }
    REQUIRE(committed);
    CHECK_FALSE(producer.flush(10s).contains_error());
}
