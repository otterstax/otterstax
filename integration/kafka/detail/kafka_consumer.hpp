// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "kafka_producer.hpp"

#include <core/result_wrapper.hpp>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace RdKafka {
    class KafkaConsumer;
} // namespace RdKafka

namespace otterstax::kafka::detail {
    struct offset_rebalance_cb;

    struct kafka_record_t {
        std::string payload; // message value (JSON for now)
        int32_t partition;
        int64_t offset;
    };

    struct consumer_config_t {
        std::string bootstrap_servers;
        std::string topic;
        std::string group_id;
        std::string offset_reset;
        // partition -> next offset to read on assign; empty map = broker-group resume
        std::map<int32_t, int64_t> committed_offsets;
    };

    // RAII consumer over a single topic. Built via create(), which surfaces
    // librdkafka setup failures as an error instead of throwing from a constructor
    class kafka_consumer_t {
    public:
        static core::result_wrapper_t<kafka_consumer_t> create(consumer_config_t config);

        ~kafka_consumer_t();

        kafka_consumer_t(kafka_consumer_t&&) noexcept;
        kafka_consumer_t& operator=(kafka_consumer_t&&) noexcept;
        kafka_consumer_t(const kafka_consumer_t&) = delete;
        kafka_consumer_t& operator=(const kafka_consumer_t&) = delete;

        // Poll up to max_records, returning early on timeout / end-of-partition. An
        // empty result just means nothing was available this tick
        std::vector<kafka_record_t> poll_batch(std::size_t max_records, std::chrono::milliseconds timeout);

        // Commit consumed offsets to the broker (group coordination)
        void commit();

        // Exactly-once: add this batch's positions (last offset + 1 per partition) to
        // `producer`'s open transaction, tagged with this consumer's group metadata, so
        // the produce and the source-offset advance commit atomically. All RdKafka
        // pointers are owned and freed here
        [[nodiscard]] kafka_txn_result send_offsets_to_transaction(kafka_producer_t& producer,
                                                                   const std::vector<kafka_record_t>& batch,
                                                                   std::chrono::milliseconds timeout);

        // Exactly-once abort recovery: rewind the in-memory fetch position to the start
        // of `batch` so it is re-delivered next poll (consume() already moved past it,
        // but an aborted txn never advanced the broker offset)
        [[nodiscard]] core::error_t seek_to_batch_start(const std::vector<kafka_record_t>& batch);

    private:
        kafka_consumer_t(std::unique_ptr<offset_rebalance_cb> rebalance_cb,
                         std::unique_ptr<RdKafka::KafkaConsumer> consumer,
                         std::string topic) noexcept;

        // rebalance_cb_ before consumer_: librdkafka holds the cb pointer for the
        // consumer's lifetime (incl. close()), so it must be destroyed after it
        std::unique_ptr<offset_rebalance_cb> rebalance_cb_;
        std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
        std::string topic_;
    };
} // namespace otterstax::kafka::detail
