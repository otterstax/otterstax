// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>
#include "security/network_destinations.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace RdKafka {
    class Producer;
    class TopicPartition;
    class ConsumerGroupMetadata;
} // namespace RdKafka

namespace otterstax::kafka::detail {
    class kafka_destination_guard;

    struct producer_config_t {
        std::string bootstrap_servers;
        std::string topic;
        std::string transactional_id; // non-empty -> exactly-once (transactional producer)
        std::shared_ptr<const security::network_destinations> destinations;
    };

    // Outcome of one transaction-lifecycle call. Carries librdkafka's classification
    // next to the error so a caller can follow the contract without inspecting the
    // message: retriable -> re-call the SAME op, requires_abort -> abort + reprocess
    // the batch, fatal -> the producer is dead and must be recreated
    struct kafka_txn_result {
        core::error_t error{core::error_t::no_error()};
        bool fatal{false};
        bool retriable{false};
        bool requires_abort{false};

        [[nodiscard]] bool ok() const noexcept { return !error.contains_error(); }
    };

    // RAII producer over a single topic. acks=all + enable.idempotence (no loss); a
    // transactional_id additionally enables the exactly-once transaction API. Built
    // via create(), which surfaces librdkafka setup failures as an error instead of
    // throwing from a constructor
    class kafka_producer_t {
    public:
        static core::result_wrapper_t<kafka_producer_t> create(producer_config_t config);

        ~kafka_producer_t();

        kafka_producer_t(kafka_producer_t&&) noexcept;
        kafka_producer_t& operator=(kafka_producer_t&&) noexcept;
        kafka_producer_t(const kafka_producer_t&) = delete;
        kafka_producer_t& operator=(const kafka_producer_t&) = delete;

        // Enqueue one message (copied into librdkafka's queue); flush() waits for the
        // broker ack. Both report a hard error so the caller can fail the INSERT
        // rather than silently drop records
        [[nodiscard]] core::error_t produce(const std::string& payload);
        [[nodiscard]] core::error_t flush(std::chrono::milliseconds timeout);

        [[nodiscard]] bool transactional() const noexcept { return transactional_; }

        // Exactly-once transaction lifecycle (valid only when transactional()).
        // Pattern: begin -> produce -> send_offsets_to_transaction -> commit
        [[nodiscard]] kafka_txn_result begin_transaction();
        [[nodiscard]] kafka_txn_result send_offsets_to_transaction(const std::vector<RdKafka::TopicPartition*>& offsets,
                                                                   const RdKafka::ConsumerGroupMetadata* group_metadata,
                                                                   std::chrono::milliseconds timeout);
        [[nodiscard]] kafka_txn_result commit_transaction(std::chrono::milliseconds timeout);
        [[nodiscard]] kafka_txn_result abort_transaction(std::chrono::milliseconds timeout);

    private:
        struct delivery_report_t;
        kafka_producer_t(std::unique_ptr<kafka_destination_guard> destinations,
                         std::unique_ptr<delivery_report_t> delivery_report,
                         std::unique_ptr<RdKafka::Producer> producer, std::string topic, bool transactional) noexcept;

        // librdkafka keeps a borrowed pointer to this callback. Its address is
        // stable across moves, and it outlives the producer during teardown.
        std::unique_ptr<kafka_destination_guard> destinations_;
        std::unique_ptr<delivery_report_t> delivery_report_;
        std::unique_ptr<RdKafka::Producer> producer_;
        std::string topic_;
        bool transactional_;
    };
} // namespace otterstax::kafka::detail
