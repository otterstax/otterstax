// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_poller.hpp"
#include "kafka_const.hpp"
#include "kafka_reader.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/session/session.hpp>

#include <algorithm>
#include <chrono>
#include <map>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace otterstax::kafka::detail {
    namespace {
        // The caller has its own thread and therefore parks between is_ready checks
        components::cursor::cursor_t_ptr drive(actor_zeta::unique_future<components::cursor::cursor_t_ptr> future) {
            while (!future.is_ready()) {
                std::this_thread::sleep_for(ENGINE_POLL_STEP);
            }
            return std::move(future).take_ready();
        }

        // The engine's reply as an error_t: the engine's own code and text,
        // prefixed with the batch step that produced it
        core::error_t step_error(std::pmr::memory_resource* resource,
                                 const components::cursor::cursor_t_ptr& cursor,
                                 const char* step) {
            if (!cursor) {
                return core::error_t(core::error_code_t::other_error,
                                     std::pmr::string{std::string{step} + ": no reply from the engine", resource});
            }
            if (cursor->is_error()) {
                const auto error = cursor->get_error();
                return core::error_t(error.type,
                                     std::pmr::string{std::string{step} + ": " + error.what.c_str(), resource});
            }
            return core::error_t::no_error();
        }
    } // namespace

    core::error_t ingest_at_least_once(actor_zeta::address_t dispatcher_address,
                                       std::pmr::memory_resource* resource,
                                       const std::string& database,
                                       const std::string& table,
                                       components::vector::data_chunk_t chunk) {
        OTX_ZONE_N("kafka::poller::ingest_at_least_once");
        if (chunk.size() == 0) {
            return core::error_t::no_error();
        }
        return step_error(resource,
                          drive(kafka_insert(dispatcher_address, resource, database, table, std::move(chunk))),
                          "insert");
    }

    core::error_t ingest_transactional(actor_zeta::address_t dispatcher_address,
                                       std::pmr::memory_resource* resource,
                                       const std::string& database,
                                       const std::string& table,
                                       const std::map<int32_t, int64_t>& next_offsets,
                                       components::vector::data_chunk_t chunk) {
        OTX_ZONE_N("kafka::poller::ingest_transactional");
        const auto session = components::session::session_id_t::generate_uid();
        const std::string offsets_table = table + "__offsets";

        auto error = step_error(resource,
                                drive(kafka_query_session(dispatcher_address, resource, session, "BEGIN;")),
                                "BEGIN");
        const bool began = !error.contains_error();
        if (began && chunk.size() > 0) {
            error = step_error(
                resource,
                drive(kafka_insert_session(dispatcher_address, resource, session, database, table, std::move(chunk))),
                "insert");
        }
        if (!error.contains_error()) {
            error = step_error(resource,
                               drive(write_offsets_session(dispatcher_address,
                                                           resource,
                                                           session,
                                                           database,
                                                           offsets_table,
                                                           next_offsets)),
                               "offsets insert");
        }
        if (!error.contains_error()) {
            error = step_error(resource,
                               drive(kafka_query_session(dispatcher_address, resource, session, "COMMIT;")),
                               "COMMIT");
        }
        if (error.contains_error() && began) {
            // The failed step's error is what the caller acts on; a rollback failure
            // on top of it is only logged
            auto rollback = step_error(resource,
                                       drive(kafka_query_session(dispatcher_address, resource, session, "ROLLBACK;")),
                                       "ROLLBACK");
            if (rollback.contains_error()) {
                get_logger(logger_tag::KAFKA_MANAGER)
                    ->error("kafka poller: rollback for '{}.{}' failed (code {}): {}",
                            database,
                            table,
                            static_cast<int32_t>(rollback.type),
                            rollback.what.c_str());
            }
        }
        return error;
    }

    kafka_poller_t::kafka_poller_t(actor_zeta::address_t dispatcher_address,
                                   std::pmr::memory_resource* resource,
                                   std::string database,
                                   std::string table,
                                   std::vector<kafka_column_t> columns,
                                   kafka_consumer_t consumer,
                                   bool transactional) noexcept
        : dispatcher_address_(std::move(dispatcher_address))
        , resource_(resource)
        , database_(std::move(database))
        , table_(std::move(table))
        , columns_(std::move(columns))
        , transactional_(transactional)
        , consumer_(std::move(consumer)) {
        thread_ = std::thread([this] { run(); });
    }

    kafka_poller_t::~kafka_poller_t() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    void kafka_poller_t::run() {
        while (!stop_.load(std::memory_order_relaxed)) {
            auto records = consumer_.poll_batch(MAX_BATCH, POLL_TIMEOUT);
            if (records.empty()) {
                continue; // timeout / caught up — re-check stop_ and poll again
            }

            OTX_ZONE_N("kafka::poller::batch"); // one zone per processed batch (run() is an endless poll loop)
            std::vector<std::string> payloads;
            payloads.reserve(records.size());
            for (auto& record : records) {
                // Move just the payload: records keep partition/offset, needed for
                // the offsets upsert and for the rewind on failure
                payloads.push_back(std::move(record.payload));
            }

            auto chunk = json_to_chunk(resource_, columns_, payloads);
            core::error_t error = core::error_t::no_error();
            if (transactional_) {
                std::map<int32_t, int64_t> next_offsets;
                for (const auto& record : records) {
                    next_offsets[record.partition] = std::max(next_offsets[record.partition], record.offset + 1);
                }
                error = ingest_transactional(dispatcher_address_,
                                             resource_,
                                             database_,
                                             table_,
                                             next_offsets,
                                             std::move(chunk));
            } else {
                error = ingest_at_least_once(dispatcher_address_, resource_, database_, table_, std::move(chunk));
            }

            if (error.contains_error()) {
                // The batch is not lost: no consumer commit, rewind to its start so
                // the next poll re-delivers it, and back off before retrying
                ++consecutive_failures_;
                get_logger(logger_tag::KAFKA_MANAGER)
                    ->error("kafka poller: batch of {} record(s) for '{}.{}' failed (code {}): {}; rewinding, "
                            "retry #{} in {} ms",
                            records.size(),
                            database_,
                            table_,
                            static_cast<int32_t>(error.type),
                            error.what.c_str(),
                            consecutive_failures_,
                            batch_retry_backoff(consecutive_failures_).count());
                // A failed seek is non-fatal — restart's table-seek still resumes
                if (auto seek = consumer_.seek_to_batch_start(records); seek.contains_error()) {
                    get_logger(logger_tag::KAFKA_MANAGER)
                        ->error("kafka poller: rewind for '{}.{}' failed: {}", database_, table_, seek.what.c_str());
                }
                pause_after_failure();
                continue;
            }
            consecutive_failures_ = 0;
            if (!transactional_) {
                // Broker-group resume: commit the consumed positions (also for an
                // all-invalid batch, so the poller advances past unparseable messages).
                // The exactly-once path deliberately never commits — the offsets
                // table is the truth
                consumer_.commit();
            }
        }
    }

    void kafka_poller_t::pause_after_failure() {
        const auto until = std::chrono::steady_clock::now() + batch_retry_backoff(consecutive_failures_);
        while (!stop_.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < until) {
            std::this_thread::sleep_for(BATCH_RETRY_SLEEP_STEP);
        }
    }
} // namespace otterstax::kafka::detail
