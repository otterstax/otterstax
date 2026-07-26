// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_stream.hpp"
#include "kafka_const.hpp"
#include "kafka_reader.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>

#include <chrono>
#include <utility>

namespace otterstax::kafka::detail {

    using namespace components;

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
                    out_payloads = chunk_to_json(cursor->chunks());
                }
            }

            auto log = get_logger(logger_tag::KAFKA_MANAGER);
            if (producer_.transactional()) {
                // Exactly-once: produce + advance the source offsets in ONE txn. On
                // failure, rewind so the batch is reprocessed (a failing batch retries
                // indefinitely — never skip; a poison-pill DLQ is a follow-up)
                auto log_txn = [&](const char* stage, const kafka_txn_result& status) {
                    log->error("kafka stream: {} failed for batch of {} (fatal={} retriable={} requires_abort={}): {}",
                               stage,
                               records.size(),
                               status.fatal,
                               status.retriable,
                               status.requires_abort,
                               status.error.what.c_str());
                };
                auto rewind = [&] {
                    if (auto error = consumer_.seek_to_batch_start(records); error.contains_error()) {
                        log->error("kafka stream: rewind to the batch start failed: {}", error.what.c_str());
                    }
                };
                auto abort_and_rewind = [&] {
                    if (auto status = producer_.abort_transaction(TXN_TIMEOUT); !status.ok()) {
                        log_txn("abort_transaction", status);
                    }
                    rewind();
                };

                if (auto status = producer_.begin_transaction(); !status.ok()) {
                    log_txn("begin_transaction", status);
                    rewind(); // nothing to abort — the transaction never opened
                    continue;
                }
                core::error_t produce_error = core::error_t::no_error();
                for (const auto& payload : out_payloads) {
                    produce_error = producer_.produce(payload);
                    if (produce_error.contains_error()) {
                        break;
                    }
                }
                if (produce_error.contains_error()) {
                    log->error("kafka stream: produce failed for batch of {}: {}",
                               records.size(),
                               produce_error.what.c_str());
                    abort_and_rewind();
                    continue;
                }
                if (auto status = consumer_.send_offsets_to_transaction(producer_, records, TXN_TIMEOUT);
                    !status.ok()) {
                    log_txn("send_offsets_to_transaction", status);
                    abort_and_rewind();
                    continue;
                }
                // A commit timeout is RETRIABLE — the transaction is still in flight, so
                // the contract is to call commit AGAIN (aborting here would wedge the
                // producer, which is the crash-recovery stall we hit). Only a
                // non-retriable failure falls through to abort + reprocess
                for (;;) {
                    auto status = producer_.commit_transaction(TXN_TIMEOUT);
                    if (status.ok()) {
                        break;
                    }
                    if (status.retriable && !stop_.load(std::memory_order_relaxed)) {
                        log_txn("commit_transaction (retriable, resuming)", status);
                        continue;
                    }
                    log_txn("commit_transaction", status);
                    abort_and_rewind();
                    break;
                }
            } else {
                // At-least-once: produce (no-loss flush), then commit offsets. A produce
                // error is surfaced but the source offset still advances below, so a
                // failed batch is not retried
                for (const auto& payload : out_payloads) {
                    if (auto error = producer_.produce(payload); error.contains_error()) {
                        log->error("kafka stream: produce failed: {}", error.what.c_str());
                        break;
                    }
                }
                if (!out_payloads.empty()) {
                    if (auto error = producer_.flush(FLUSH_TIMEOUT); error.contains_error()) {
                        log->error("kafka stream: flush failed: {}", error.what.c_str());
                    }
                }
                consumer_.commit();
            }
        }
    }

} // namespace otterstax::kafka::detail
