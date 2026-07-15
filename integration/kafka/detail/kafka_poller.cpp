// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_poller.hpp"
#include "kafka_const.hpp"
#include "kafka_reader.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/session/session.hpp>

#include <algorithm>
#include <map>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace otterstax::kafka::detail {
    namespace {
        // The poller has its own thread and therefore parks between is_ready checks
        bool drive_ok(actor_zeta::unique_future<components::cursor::cursor_t_ptr> future) {
            while (!future.is_ready()) {
                std::this_thread::sleep_for(ENGINE_POLL_STEP);
            }
            auto cursor = std::move(future).take_ready();
            return cursor && !cursor->is_error();
        }
    } // namespace

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

            std::vector<std::string> payloads;
            payloads.reserve(records.size());
            for (auto& record : records) {
                // Move just the payload: records keep partition/offset, needed by the
                // transactional path for the offsets upsert and seek-on-abort
                payloads.push_back(std::move(record.payload));
            }

            auto chunk = json_to_chunk(resource_, columns_, payloads);
            if (transactional_) {
                ingest_transactional(records, std::move(chunk));
            } else {
                ingest_at_least_once(std::move(chunk));
            }
        }
    }

    void kafka_poller_t::ingest_at_least_once(components::vector::data_chunk_t chunk) {
        OTX_ZONE_N("kafka::poller::ingest_at_least_once");
        if (chunk.size() > 0) {
            std::ignore = drive_ok(kafka_insert(dispatcher_address_, resource_, database_, table_, std::move(chunk)));
            // errors are logged by a later sub-step
        }
        // Broker-group resume: commit the consumed positions (even for an
        // all-invalid batch, so the poller advances past unparseable messages)
        consumer_.commit();
    }

    void kafka_poller_t::ingest_transactional(const std::vector<kafka_record_t>& batch,
                                              components::vector::data_chunk_t chunk) {
        OTX_ZONE_N("kafka::poller::ingest_transactional");
        std::map<int32_t, int64_t> next_offsets;
        for (const auto& record : batch) {
            next_offsets[record.partition] = std::max(next_offsets[record.partition], record.offset + 1);
        }

        const auto session = components::session::session_id_t::generate_uid();
        const std::string offsets_table = table_ + "__offsets";

        // One session for BEGIN -> insert -> upsert(offsets) -> COMMIT (atomic). An
        // empty chunk still advances the offsets (skip the insert)
        bool ok = drive_ok(kafka_query_session(dispatcher_address_, resource_, session, "BEGIN;"));
        if (ok && chunk.size() > 0) {
            ok = drive_ok(
                kafka_insert_session(dispatcher_address_, resource_, session, database_, table_, std::move(chunk)));
        }
        if (ok) {
            ok = drive_ok(
                write_offsets_session(dispatcher_address_, resource_, session, database_, offsets_table, next_offsets));
        }
        if (ok) {
            ok = drive_ok(kafka_query_session(dispatcher_address_, resource_, session, "COMMIT;"));
        }

        if (!ok) {
            get_logger(logger_tag::KAFKA_MANAGER)
                ->error("kafka poller: transactional batch for '{}.{}' failed; rolling back + rewinding {} record(s)",
                        database_,
                        table_,
                        batch.size());
            std::ignore = drive_ok(kafka_query_session(dispatcher_address_, resource_, session, "ROLLBACK;"));
            // A failed seek is non-fatal — restart's table-seek still resumes
            if (auto error = consumer_.seek_to_batch_start(batch); error.contains_error()) {
                get_logger(logger_tag::KAFKA_MANAGER)
                    ->error("kafka poller: seek after rollback failed for '{}.{}': {}",
                            database_,
                            table_,
                            error.what.c_str());
            }
        }
        // EOS path: deliberately NO consumer_.commit() — the offsets table is truth
    }
} // namespace otterstax::kafka::detail
