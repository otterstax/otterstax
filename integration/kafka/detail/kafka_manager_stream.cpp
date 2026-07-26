// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "integration/kafka/kafka_manager.hpp"
#include "kafka_const.hpp"
#include "kafka_poller.hpp"
#include "kafka_producer.hpp"
#include "kafka_reader.hpp"
#include "kafka_stream.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/param_storage.hpp>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <memory_resource>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace otterstax::kafka {
    using namespace components;
    using namespace detail;

    namespace {
        // Positional type-compatibility for the INSERT-INTO-stream union guard
        bool columns_type_compatible(const std::vector<kafka_column_t>& a, const std::vector<kafka_column_t>& b) {
            if (a.size() != b.size()) {
                return false;
            }
            for (std::size_t i = 0; i < a.size(); ++i) {
                if (a[i].type.type() != b[i].type.type()) {
                    return false;
                }
            }
            return true;
        }
    } // namespace

    actor_zeta::unique_future<cursor::cursor_t_ptr>
    KafkaManager::produce(session_hash_t id, std::string relname, logical_plan::node_ptr source) {
        OTX_ZONE_N("KafkaManager::produce");
        try {
            log_->debug("produce id {}: object {}", id, relname);
            auto it = registry_.find(relname);
            if (it == registry_.end()) {
                co_return cursor::make_cursor(
                    resource_,
                    core::error_t(core::error_code_t::other_error,
                                  std::pmr::string{"kafka: INSERT into unknown object '" + relname + "'", resource_}));
            }
            const auto& options = it->second.options;
            const auto topic_it = options.find("kafka_topic");
            const auto bootstrap_it = options.find("bootstrap_servers");
            if (topic_it == options.end() || bootstrap_it == options.end()) {
                co_return cursor::make_cursor(
                    resource_,
                    core::error_t(
                        core::error_code_t::other_error,
                        std::pmr::string{"kafka: '" + relname + "' has no KAFKA_TOPIC/BOOTSTRAP_SERVERS", resource_}));
            }

            // Materialise the rows by executing the insert's source subplan — no
            // table write; the topic is the store and the poller re-ingests them
            auto cursor = co_await send_plan(
                logical_plan::execution_plan_t{resource_, source, logical_plan::make_parameter_node(resource_)});
            if (!cursor || cursor->is_error()) {
                const std::string what =
                    cursor ? std::string{cursor->get_error().what.c_str()} : std::string{"null cursor"};
                log_->error("kafka: produce source eval for '{}' failed: {}", relname, what);
                co_return cursor::make_cursor(
                    resource_,
                    core::error_t(core::error_code_t::other_error,
                                  std::pmr::string{"kafka: produce source eval failed: " + what, resource_}));
            }

            // This path runs only the source subplan (no backing-table write), so the
            // engine never type-checks the rows — reject a batch that wouldn't round-trip
            // into the declared schema rather than publish junk to the topic.
            // The result spans multiple <=1024-row chunks (never combined) —
            // validate and serialize ALL of them or rows are silently dropped.
            const auto& chunks = cursor->chunks();
            if (!chunk_matches_columns(resource_, chunks, it->second.columns)) {
                co_return cursor::make_cursor(
                    resource_,
                    core::error_t(core::error_code_t::other_error,
                                  std::pmr::string{"kafka: INSERT into '" + relname +
                                                       "' does not match its declared schema (column or type "
                                                       "mismatch); nothing produced",
                                                   resource_}));
            }

            const auto payloads = chunk_to_json(chunks);
            auto producer_r = ensure_producer(relname, bootstrap_it->second, topic_it->second);
            if (producer_r.has_error()) {
                co_return cursor::make_cursor(
                    resource_,
                    core::error_t(
                        core::error_code_t::other_error,
                        std::pmr::string{std::string{"kafka: "} + producer_r.error().what.c_str(), resource_}));
            }
            kafka_producer_t& producer = *producer_r.value();
            // Atomic batch: the whole VALUES set is one Kafka transaction. On any error,
            // abort (best-effort) and surface it — nothing partial is visible
            auto failed = [&](const char* what) {
                log_->error("kafka: produce to '{}' failed: {}", relname, what);
                return cursor::make_cursor(
                    resource_,
                    core::error_t(core::error_code_t::other_error,
                                  std::pmr::string{std::string{"kafka: produce failed: "} + what, resource_}));
            };
            auto abort = [&] {
                // abort itself failing is not actionable — the txn gets fenced/timed out
                if (auto status = producer.abort_transaction(TXN_TIMEOUT); !status.ok()) {
                    log_->error("kafka: abort after a failed produce to '{}': {}", relname, status.error.what.c_str());
                }
            };

            if (auto status = producer.begin_transaction(); !status.ok()) {
                co_return failed(status.error.what.c_str()); // never opened — nothing to abort
            }
            for (const auto& payload : payloads) {
                if (auto error = producer.produce(payload); error.contains_error()) {
                    abort();
                    co_return failed(error.what.c_str());
                }
            }
            if (auto status = producer.commit_transaction(TXN_TIMEOUT); !status.ok()) { // flushes + commits atomically
                abort();
                co_return failed(status.error.what.c_str());
            }
            log_->info("kafka: produced {} record(s) to '{}' (topic '{}')", payloads.size(), relname, topic_it->second);
            co_return cursor::make_cursor(resource_);
        } catch (const std::exception& e) {
            log_->error("KafkaManager::produce caught exception: {}", e.what());
            co_return cursor::make_cursor(
                resource_,
                core::error_t(core::error_code_t::other_error, std::pmr::string{e.what(), resource_}));
        }
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr>
    KafkaManager::add_stream_insert(session_hash_t id, std::string stream, std::string insert_sql) {
        OTX_ZONE_N("KafkaManager::add_stream_insert");
        try {
            log_->debug("add_stream_insert id {}: INSERT INTO stream '{}'", id, stream);

            // Target must be a registered STREAM — continuous fan-in is stream-only
            // (a SELECT into a SOURCE would be a silent one-shot snapshot)
            auto sink_it = registry_.find(stream);
            if (sink_it == registry_.end()) {
                co_return cursor::make_cursor(
                    resource_,
                    core::error_t(core::error_code_t::other_error,
                                  std::pmr::string{"kafka: INSERT INTO unknown object '" + stream + "'", resource_}));
            }
            if (sink_it->second.op != kafka_op::create_stream) {
                co_return cursor::make_cursor(
                    resource_,
                    core::error_t(core::error_code_t::invalid_parameter,
                                  std::pmr::string{"kafka: INSERT INTO ... SELECT target '" + stream +
                                                       "' is not a STREAM; continuous fan-in is only into a STREAM",
                                                   resource_}));
            }
            const auto& sink_opts = sink_it->second.options;
            const auto out_topic = sink_opts.find("kafka_topic");
            const auto out_bootstrap = sink_opts.find("bootstrap_servers");
            if (out_topic == sink_opts.end() || out_bootstrap == sink_opts.end()) {
                co_return cursor::make_cursor(
                    resource_,
                    core::error_t(
                        core::error_code_t::other_error,
                        std::pmr::string{"kafka: stream '" + stream + "' has no KAFKA_TOPIC/BOOTSTRAP_SERVERS",
                                         resource_}));
            }

            // Compile the INSERT once: find the SELECT's source (a known kafka object)
            // and compute the output schema for the union guard
            auto invalid = [&](const std::string& why) {
                return cursor::make_cursor(
                    resource_,
                    core::error_t(core::error_code_t::other_error,
                                  std::pmr::string{"kafka: INSERT INTO ... SELECT invalid: " + why, resource_}));
            };
            auto plan = kafka_parse_plan(resource_, insert_sql);
            if (plan.has_error()) {
                co_return invalid(plan.error().what.c_str());
            }
            const auto* agg =
                plan.value().sub_queries.empty() ? nullptr : kafka_find_aggregate(plan.value().sub_queries.back());
            if (!agg) {
                co_return invalid("SELECT has no source table");
            }
            const std::string source_relname = agg->relname().t;
            const auto src_it = registry_.find(source_relname);
            if (src_it == registry_.end()) {
                co_return invalid("unknown source '" + source_relname + "'");
            }
            auto out_columns =
                stream_output_schema(resource_, *agg, plan.value().parameters.get(), src_it->second.columns);

            // Union guard: the query's output must be positionally type-compatible with
            // the stream's schema, or the shared output topic gets incompatible records
            if (!columns_type_compatible(out_columns, sink_it->second.columns)) {
                co_return cursor::make_cursor(
                    resource_,
                    core::error_t(core::error_code_t::invalid_parameter,
                                  std::pmr::string{"kafka: INSERT INTO '" + stream +
                                                       "' SELECT output does not match the stream's schema",
                                                   resource_}));
            }

            // Exactly-once is inherited from the target stream's TRANSACTIONAL option
            bool transactional = false;
            if (const auto t = sink_opts.find("transactional"); t != sink_opts.end()) {
                std::string value = t->second;
                std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
                    return static_cast<char>(std::tolower(c));
                });
                transactional = (value == "true");
            }

            // An anonymous persistent query gets a unique internal name (own
            // registry_/streams_/__sources entry, consumer group + producer txn id)
            // The do-while skips any name a recovered query already holds
            std::string query_name;
            do {
                query_name = stream + "__insert__" + std::to_string(insert_query_seq_++);
            } while (registry_.count(query_name) != 0 || streams_.count(query_name) != 0);
            const std::string group_id = "otterstax_insert_" + query_name;

            // Register the query as a stream-like object whose OUT side is the target
            // stream's topic — launch_stream then drives it exactly like a CREATE
            // STREAM (parse as_select -> find source -> spawn a continuous worker)
            kafka_object_t obj;
            obj.op = kafka_op::create_stream;
            obj.columns = std::move(out_columns);
            obj.options["kafka_topic"] = out_topic->second;
            obj.options["bootstrap_servers"] = out_bootstrap->second;
            obj.options["group_id"] = group_id;
            obj.options["offset_reset"] = "earliest";
            if (transactional) {
                obj.options["transactional"] = "true";
            }
            obj.as_select = insert_sql;
            registry_[query_name] = std::move(obj);

            // Persist so a restart relaunches it (kind='insert'; topic/bootstrap = the
            // stream's OUT side; as_select carries the whole INSERT — the target stream
            // and the SELECT source are re-derived from it on recover())
            if (!sources_table_ensured_) {
                co_await ensure_sources_table();
                sources_table_ensured_ = true;
            }
            if (auto persisted = co_await persist_source_meta(query_name,
                                                              "insert",
                                                              out_topic->second,
                                                              out_bootstrap->second,
                                                              group_id,
                                                              "earliest",
                                                              transactional,
                                                              insert_sql);
                persisted && persisted->is_error()) {
                log_->error("kafka: persist meta for insert-query '{}' failed: {}",
                            query_name,
                            persisted->get_error().what.c_str());
            }

            // Launch the continuous worker (no-op unless start_pollers_) and remember it
            // under its target stream for DROP-time cleanup
            launch_stream(query_name);
            stream_insert_queries_[stream].push_back(query_name);
            log_->info("kafka: added INSERT INTO query '{}' into stream '{}' (source '{}', transactional={})",
                       query_name,
                       stream,
                       source_relname,
                       transactional);
            co_return cursor::make_cursor(resource_);
        } catch (const std::exception& e) {
            log_->error("KafkaManager::add_stream_insert caught exception: {}", e.what());
            co_return cursor::make_cursor(
                resource_,
                core::error_t(core::error_code_t::other_error, std::pmr::string{e.what(), resource_}));
        }
    }

    core::result_wrapper_t<kafka_producer_t*> KafkaManager::ensure_producer(const std::string& name,
                                                                            const std::string& bootstrap_servers,
                                                                            const std::string& topic) {
        OTX_ZONE_N("KafkaManager::ensure_producer");
        auto it = producers_.find(name);
        if (it == producers_.end()) {
            // Transactional producer: INSERT … VALUES commits the whole batch in one
            // Kafka transaction (a crash before commit aborts it — a read-committed
            // consumer never sees a partial INSERT). Stable txn id per object for
            // producer fencing. init_transactions() needs a reachable broker
            auto created = kafka_producer_t::create({bootstrap_servers, topic, "otterstax_insert_" + name});
            if (created.has_error()) {
                return created.error();
            }
            it = producers_.emplace(name, std::make_unique<kafka_producer_t>(std::move(created.value()))).first;
            log_->info("kafka: opened transactional producer for '{}' (topic '{}')", name, topic);
        }
        return it->second.get();
    }
} // namespace otterstax::kafka
