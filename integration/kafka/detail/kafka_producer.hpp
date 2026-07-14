// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

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

    struct producer_config_t {
        std::string bootstrap_servers;
        std::string topic;
        std::string transactional_id; // non-empty -> exactly-once (transactional producer)
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
        // broker ack. Both throw on a hard error so the caller can fail the INSERT
        // rather than silently drop records
        void produce(const std::string& payload);
        void flush(std::chrono::milliseconds timeout);

        [[nodiscard]] bool transactional() const noexcept { return transactional_; }

        // Exactly-once transaction lifecycle (valid only when transactional()); each
        // throws on a fatal/abort-required error. Pattern: begin -> produce ->
        // send_offsets_to_transaction -> commit
        void begin_transaction();
        void send_offsets_to_transaction(const std::vector<RdKafka::TopicPartition*>& offsets,
                                         const RdKafka::ConsumerGroupMetadata* group_metadata,
                                         std::chrono::milliseconds timeout);
        void commit_transaction(std::chrono::milliseconds timeout);
        void abort_transaction(std::chrono::milliseconds timeout);

    private:
        kafka_producer_t(std::unique_ptr<RdKafka::Producer> producer, std::string topic, bool transactional) noexcept;

        std::unique_ptr<RdKafka::Producer> producer_;
        std::string topic_;
        bool transactional_;
    };
} // namespace otterstax::kafka::detail
