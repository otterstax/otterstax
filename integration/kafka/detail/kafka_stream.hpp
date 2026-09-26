// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "kafka_consumer.hpp"
#include "kafka_producer.hpp"
#include "otterbrix/parser/grammar_extension/kafka/kafka_node.hpp"

#include <actor-zeta.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <atomic>
#include <memory_resource>
#include <thread>
#include <vector>

namespace otterstax::kafka::detail {
    // The SELECT applied to each batch: source schema + rehomed operators + params,
    // and the stream's declared output columns every transformed batch must match
    // before it is produced (the engine never type-checks this path)
    struct stream_transform_t {
        std::vector<kafka_column_t> source_columns;
        std::vector<components::logical_plan::node_ptr> operators;
        components::logical_plan::parameter_node_ptr params;
        std::vector<kafka_column_t> output_columns;
    };

    // Continuous stateless STREAM worker: consume source topic -> apply the captured
    // SELECT per batch (aggregate(empty)+node_raw_data node-swap) -> produce to the
    // output topic. A transactional producer makes each batch exactly-once. A batch
    // that fails at any step (transform, schema guard, produce, transaction) is
    // never committed: the consumer is rewound to its start and it is retried
    // after a capped exponential pause
    class kafka_stream_t {
    public:
        kafka_stream_t(actor_zeta::address_t dispatcher_address,
                       std::pmr::memory_resource* resource,
                       stream_transform_t transform,
                       kafka_consumer_t consumer,
                       kafka_producer_t producer) noexcept;
        ~kafka_stream_t();

        kafka_stream_t(const kafka_stream_t&) = delete;
        kafka_stream_t& operator=(const kafka_stream_t&) = delete;

    private:
        void run();
        // Park for the current retry pause (grows per consecutive failure), waking
        // early on stop_
        void pause_after_failure();

        actor_zeta::address_t dispatcher_address_;
        std::pmr::memory_resource* resource_;
        std::vector<kafka_column_t> source_columns_;
        std::vector<components::logical_plan::node_ptr> operators_;
        components::logical_plan::parameter_node_ptr params_;
        std::vector<kafka_column_t> output_columns_;
        kafka_consumer_t consumer_;
        kafka_producer_t producer_;
        unsigned consecutive_failures_{0};
        std::atomic<bool> stop_{false};
        std::thread thread_;
    };
} // namespace otterstax::kafka::detail
