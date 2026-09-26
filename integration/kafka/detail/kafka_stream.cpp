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
#include <string>
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
        , output_columns_(std::move(transform.output_columns))
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
            auto log = get_logger(logger_tag::KAFKA_MANAGER);
            std::vector<std::string> payloads;
            payloads.reserve(records.size());
            for (auto& record : records) {
                payloads.push_back(std::move(record.payload)); // partition/offset retained for the offset commit
            }

            // A batch that fails before its offsets are committed is reprocessed:
            // rewind the consumer to its start (the next poll re-delivers it) and
            // back off. Nothing is skipped — a poison-pill DLQ is a follow-up
            auto rewind = [&] {
                if (auto error = consumer_.seek_to_batch_start(records); error.contains_error()) {
                    log->error("kafka stream: rewind to the batch start failed: {}", error.what.c_str());
                }
            };
            auto fail_batch = [&](const std::string& what) {
                ++consecutive_failures_;
                log->error("kafka stream: batch of {} failed: {}; rewinding, retry #{} in {} ms",
                           records.size(),
                           what,
                           consecutive_failures_,
                           batch_retry_backoff(consecutive_failures_).count());
                rewind();
                pause_after_failure();
            };

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
                if (!cursor) {
                    fail_batch("transform: no reply from the engine");
                    continue;
                }
                if (cursor->is_error()) {
                    const auto error = cursor->get_error();
                    fail_batch("transform (code " + std::to_string(static_cast<int32_t>(error.type)) +
                               "): " + error.what.c_str());
                    continue;
                }
                if (cursor->size() > 0) {
                    // The produced JSON is keyed by column alias: an output that does
                    // not round-trip into the stream's declared columns (an alias-less
                    // or mistyped column) would publish records no consumer of this
                    // stream can read
                    if (!chunk_matches_columns(resource_, cursor->chunks(), output_columns_)) {
                        fail_batch("transform output does not match the stream's declared columns "
                                   "(column alias or type)");
                        continue;
                    }
                    auto serialized = chunk_to_json(resource_, cursor->chunks());
                    if (serialized.has_error()) {
                        // The guard above round-trips these very rows, so a column the
                        // writer has no encoding for has already failed it. Carried as a
                        // value anyway: a record is never published with a field the
                        // writer could not encode
                        fail_batch(std::string{"transform output cannot be serialized: "} +
                                   serialized.error().what.c_str());
                        continue;
                    }
                    out_payloads = std::move(serialized.value());
                }
            }

            if (producer_.transactional()) {
                // Exactly-once: produce + advance the source offsets in ONE txn. On
                // failure, abort and reprocess the batch
                auto txn_what = [&](const char* stage, const kafka_txn_result& status) {
                    return std::string{stage} + " (fatal=" + (status.fatal ? "true" : "false") +
                           " retriable=" + (status.retriable ? "true" : "false") +
                           " requires_abort=" + (status.requires_abort ? "true" : "false") +
                           "): " + status.error.what.c_str();
                };
                auto abort_and_fail = [&](const std::string& what) {
                    if (auto status = producer_.abort_transaction(TXN_TIMEOUT); !status.ok()) {
                        log->error("kafka stream: {}", txn_what("abort_transaction", status));
                    }
                    fail_batch(what);
                };

                if (auto status = producer_.begin_transaction(); !status.ok()) {
                    fail_batch(txn_what("begin_transaction", status)); // nothing to abort — never opened
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
                    abort_and_fail(std::string{"produce: "} + produce_error.what.c_str());
                    continue;
                }
                if (auto status = consumer_.send_offsets_to_transaction(producer_, records, TXN_TIMEOUT);
                    !status.ok()) {
                    abort_and_fail(txn_what("send_offsets_to_transaction", status));
                    continue;
                }
                // A commit timeout is RETRIABLE — the transaction is still in flight, so
                // the contract is to call commit AGAIN (aborting here would wedge the
                // producer). Only a non-retriable failure falls through to abort +
                // reprocess
                bool committed = false;
                for (;;) {
                    auto status = producer_.commit_transaction(TXN_TIMEOUT);
                    if (status.ok()) {
                        committed = true;
                        break;
                    }
                    if (status.retriable && !stop_.load(std::memory_order_relaxed)) {
                        log->error("kafka stream: {}", txn_what("commit_transaction (retriable, resuming)", status));
                        continue;
                    }
                    abort_and_fail(txn_what("commit_transaction", status));
                    break;
                }
                if (committed) {
                    consecutive_failures_ = 0;
                }
            } else {
                // At-least-once: produce (no-loss flush), then commit the source
                // offsets. A produce or flush failure keeps the offsets where they are
                // so the batch is reprocessed rather than lost
                core::error_t error = core::error_t::no_error();
                for (const auto& payload : out_payloads) {
                    error = producer_.produce(payload);
                    if (error.contains_error()) {
                        break;
                    }
                }
                if (!error.contains_error() && !out_payloads.empty()) {
                    error = producer_.flush(FLUSH_TIMEOUT);
                }
                if (error.contains_error()) {
                    fail_batch(std::string{"produce: "} + error.what.c_str());
                    continue;
                }
                consumer_.commit();
                consecutive_failures_ = 0;
            }
        }
    }

    void kafka_stream_t::pause_after_failure() {
        const auto until = std::chrono::steady_clock::now() + batch_retry_backoff(consecutive_failures_);
        while (!stop_.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < until) {
            std::this_thread::sleep_for(BATCH_RETRY_SLEEP_STEP);
        }
    }

} // namespace otterstax::kafka::detail
