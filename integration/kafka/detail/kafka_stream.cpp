// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_stream.hpp"
#include "kafka_reader.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>

#include <chrono>
#include <utility>

namespace otterstax::kafka::detail {

    using namespace components;

    namespace {
        constexpr std::size_t MAX_BATCH = 500;
        constexpr std::chrono::milliseconds POLL_TIMEOUT{200};
        constexpr std::chrono::milliseconds FLUSH_TIMEOUT{10000};
        constexpr std::chrono::milliseconds TXN_TIMEOUT{30000};
        // Dedicated thread, nothing to do while the engine transforms the batch → park between is_ready checks
        constexpr std::chrono::microseconds ENGINE_POLL_STEP{100};
    } // namespace

    kafka_stream_t::kafka_stream_t(actor_zeta::address_t dispatcher_address,
                                   std::pmr::memory_resource* resource,
                                   stream_transform_t transform,
                                   kafka_consumer_t consumer,
                                   kafka_producer_t producer) noexcept
        : dispatcher_address_(std::move(dispatcher_address))
        , resource_(resource)
        , source_columns_(std::move(transform.source_columns))
        , operators_(std::move(transform.operators))
        , params_(std::move(transform.params))
        , consumer_(std::move(consumer))
        , producer_(std::move(producer)) {
        thread_ = std::thread([this] { run(); });
    }

    kafka_stream_t::~kafka_stream_t() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    void kafka_stream_t::run() {
        while (!stop_.load(std::memory_order_relaxed)) {
            auto records = consumer_.poll_batch(MAX_BATCH, POLL_TIMEOUT);
            if (records.empty()) {
                continue; // timeout / caught up — re-check stop_ and poll again
            }

            OTX_ZONE_N("kafka::stream_batch"); // one zone per processed batch (run() is an endless poll loop)
            std::vector<std::string> payloads;
            payloads.reserve(records.size());
            for (auto& record : records) {
                payloads.push_back(std::move(record.payload)); // partition/offset retained for the offset commit
            }

            // Apply the SELECT (read-only, outside any txn): node-swap
            // aggregate(empty)+[raw_data(batch), <operators>] -> output JSON rows
            std::vector<std::string> out_payloads;
            auto chunk = json_to_chunk(resource_, source_columns_, payloads);
            if (chunk.size() > 0) {
                auto agg = logical_plan::make_node_aggregate(resource_, {}, {});
                agg->append_child(logical_plan::make_node_raw_data(resource_, std::move(chunk)));
                for (const auto& op : operators_) {
                    agg->append_child(logical_plan::node_ptr(op)); // copy: keep operators_ intact
                }
                auto future = kafka_execute(dispatcher_address_,
                                            resource_,
                                            logical_plan::execution_plan_t{resource_, agg, params_});
                while (!future.is_ready()) {
                    std::this_thread::sleep_for(ENGINE_POLL_STEP); // dedicated thread: park, don't burn a core
                }
                auto cursor = std::move(future).take_ready();
                if (cursor && !cursor->is_error() && cursor->size() > 0) {
                    out_payloads = chunk_to_json(cursor->chunk_data());
                }
            }

            if (producer_.transactional()) {
                // Exactly-once: produce + advance the source offsets in ONE txn. On
                // failure, abort + rewind so the batch is reprocessed (a failing batch
                // retries indefinitely — never skip; a poison-pill DLQ is a follow-up)
                try {
                    producer_.begin_transaction();
                    for (const auto& payload : out_payloads) {
                        producer_.produce(payload);
                    }
                    consumer_.send_offsets_to_transaction(producer_, records, TXN_TIMEOUT);
                    producer_.commit_transaction(TXN_TIMEOUT);
                } catch (const std::exception&) {
                    try {
                        producer_.abort_transaction(TXN_TIMEOUT);
                        consumer_.seek_to_batch_start(records); // reprocess, don't skip
                    } catch (const std::exception&) {
                    }
                }
            } else {
                // At-least-once: produce (no-loss flush), then commit offsets
                try {
                    for (const auto& payload : out_payloads) {
                        producer_.produce(payload);
                    }
                    if (!out_payloads.empty()) {
                        producer_.flush(FLUSH_TIMEOUT);
                    }
                } catch (const std::exception&) {
                    // transient produce error; the source offset is still committed
                    // below, so a failed batch is not retried (at-least-once)
                }
                consumer_.commit();
            }
        }
    }

} // namespace otterstax::kafka::detail
