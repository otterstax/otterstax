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
    // The SELECT applied to each batch: source schema + rehomed operators + params
    struct stream_transform_t {
        std::vector<kafka_column_t> source_columns;
        std::vector<components::logical_plan::node_ptr> operators;
        components::logical_plan::parameter_node_ptr params;
    };

    // Continuous stateless STREAM worker: consume source topic -> apply the captured
    // SELECT per batch (aggregate(empty)+node_raw_data node-swap) -> produce to the
    // output topic. A transactional producer makes each batch exactly-once
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

        actor_zeta::address_t dispatcher_address_;
        std::pmr::memory_resource* resource_;
        std::vector<kafka_column_t> source_columns_;
        std::vector<components::logical_plan::node_ptr> operators_;
        components::logical_plan::parameter_node_ptr params_;
        kafka_consumer_t consumer_;
        kafka_producer_t producer_;
        std::atomic<bool> stop_{false};
        std::thread thread_;
    };
} // namespace otterstax::kafka::detail
