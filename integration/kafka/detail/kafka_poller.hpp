// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "kafka_consumer.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"

#include <actor-zeta.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory_resource>
#include <string>
#include <thread>
#include <vector>

namespace otterstax::kafka::detail {
    // One at-least-once batch: INSERT `chunk` into database.table on a fresh
    // (autocommit) engine session, driven to completion on the calling thread.
    // Returns the engine's error so the caller can withhold the consumer commit
    // and rewind; an empty chunk (all messages rejected) is a success so the
    // caller still advances past unparseable messages
    core::error_t ingest_at_least_once(actor_zeta::address_t dispatcher_address,
                                       std::pmr::memory_resource* resource,
                                       const std::string& database,
                                       const std::string& table,
                                       components::vector::data_chunk_t chunk);

    // One exactly-once batch on a single engine session: BEGIN -> INSERT `chunk`
    // -> INSERT `next_offsets` into <table>__offsets -> COMMIT, so the rows and
    // the consumed positions land atomically (an empty chunk still advances the
    // offsets). On any failure the transaction is rolled back and the failing
    // step's error is returned; the caller rewinds the consumer to the batch start
    core::error_t ingest_transactional(actor_zeta::address_t dispatcher_address,
                                       std::pmr::memory_resource* resource,
                                       const std::string& database,
                                       const std::string& table,
                                       const std::map<int32_t, int64_t>& next_offsets,
                                       components::vector::data_chunk_t chunk);

    // Per-SOURCE ingestion worker: owns a thread that polls the topic, converts
    // each batch to a chunk and inserts it. Not an actor. A failed batch is never
    // committed: the consumer is rewound to the batch start and the batch retried
    // after a capped exponential pause
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
        // Park for the current retry pause (grows per consecutive failure), waking
        // early on stop_
        void pause_after_failure();

        actor_zeta::address_t dispatcher_address_;
        std::pmr::memory_resource* resource_;
        std::string database_;
        std::string table_;
        std::vector<kafka_column_t> columns_;
        bool transactional_;
        kafka_consumer_t consumer_;
        unsigned consecutive_failures_{0};
        std::atomic<bool> stop_{false};
        std::thread thread_;
    };
} // namespace otterstax::kafka::detail
