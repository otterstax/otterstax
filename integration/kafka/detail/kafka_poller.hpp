// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "kafka_consumer.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"

#include <actor-zeta.hpp>
#include <components/vector/data_chunk.hpp>

#include <atomic>
#include <memory_resource>
#include <string>
#include <thread>
#include <vector>

namespace otterstax::kafka::detail {
    // owns a thread that polls the topic, not an actor
    class kafka_poller_t {
    public:
        kafka_poller_t(actor_zeta::address_t dispatcher_address,
                       std::pmr::memory_resource* resource,
                       std::string database,
                       std::string table,
                       std::vector<kafka_column_t> columns,
                       kafka_consumer_t consumer,
                       bool transactional) noexcept;
        ~kafka_poller_t();

        kafka_poller_t(const kafka_poller_t&) = delete;
        kafka_poller_t& operator=(const kafka_poller_t&) = delete;

    private:
        void run();
        void ingest_at_least_once(components::vector::data_chunk_t chunk);
        void ingest_transactional(const std::vector<kafka_record_t>& batch, components::vector::data_chunk_t chunk);

        actor_zeta::address_t dispatcher_address_;
        std::pmr::memory_resource* resource_;
        std::string database_;
        std::string table_;
        std::vector<kafka_column_t> columns_;
        bool transactional_;
        kafka_consumer_t consumer_;
        std::atomic<bool> stop_{false};
        std::thread thread_;
    };
} // namespace otterstax::kafka::detail
