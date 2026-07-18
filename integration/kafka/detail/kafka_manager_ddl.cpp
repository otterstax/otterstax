// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "integration/kafka/kafka_manager.hpp"
#include "kafka_poller.hpp"
#include "kafka_producer.hpp"
#include "kafka_reader.hpp"
#include "kafka_stream.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/table/column_definition.hpp>

#include <algorithm>
#include <cctype>
#include <list>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace otterstax::kafka {
    using namespace components;
    using namespace detail;

    namespace {
        cursor::cursor_t_ptr
        error_cursor(std::pmr::memory_resource* resource, core::error_code_t code, const std::string& msg) {
            return cursor::make_cursor(resource, core::error_t(code, std::pmr::string{msg, resource}));
        }

        std::string cursor_error(const cursor::cursor_t_ptr& c) {
            return c ? std::string{c->get_error().what.c_str()} : std::string{"null cursor"};
        }

        // Report the outcome of a best-effort kafka.__sources row delete on DROP. The
        // delete is not load-bearing: the object is already gone from
        // registry_/pollers_, and recover() skips any orphan row whose backing table
        // no longer exists. On a fresh engine's FIRST teardown the engine's DELETE
        // validation can spuriously return table_not_exists for kafka.__sources even
        // though the row is removed (verified: kafka.__sources ends empty). Swallow
        // exactly that benign code; surface anything else at error.
        void log_forget_meta_error(const log_t& log, const std::string& name, const cursor::cursor_t_ptr& d) {
            if (!d || !d->is_error()) {
                return;
            }
            if (d->get_error().type == core::error_code_t::table_not_exists) {
                return;
            }
            log->error("kafka: delete __sources row for '{}' failed: {}", name, d->get_error().what.c_str());
        }

        core::result_wrapper_t<bool> parse_transactional(const std::optional<std::string>& value) {
            if (!value) {
                return false;
            }
            std::string v = *value;
            std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
            if (v == "true") {
                return true;
            }
            if (v == "false") {
                return false;
            }
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string("TRANSACTIONAL must be 'true' or 'false', got '" + *value + "'"));
        }

        std::vector<table::column_definition_t> offsets_columns() {
            std::vector<table::column_definition_t> cols;
            cols.emplace_back("partition_id",
                              types::complex_logical_type(types::logical_type::INTEGER)); // "partition" reserved
            cols.emplace_back("committed_offset", types::complex_logical_type(types::logical_type::BIGINT));
            return cols;
        }

        std::vector<table::column_definition_t> declared_columns(const kafka_node_ptr& node) {
            std::vector<table::column_definition_t> cols;
            cols.reserve(node->columns().size());
            for (const auto& col : node->columns()) {
                cols.emplace_back(col.name, col.type);
            }
            return cols;
        }
    } // namespace

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::ensure_sources_table_once() {
        if (!sources_table_ensured_) {
            co_await ensure_sources_table(); // already-exists (restart) is harmless
            sources_table_ensured_ = true;
        }
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::execute(session_hash_t id, kafka_node_ptr node) {
        OTX_ZONE_N("KafkaManager::execute");
        log_->debug("execute id {}: kafka op {} name {}", id, static_cast<int32_t>(node->op()), node->name());
        try {
            switch (node->op()) {
                case kafka_op::create_source:
                    co_return co_await handle_create_source(std::move(node));
                case kafka_op::create_stream:
                    co_return co_await handle_create_stream(std::move(node));
                case kafka_op::drop_source:
                    co_return co_await handle_drop_source(std::move(node));
                case kafka_op::drop_stream:
                    co_return co_await handle_drop_stream(std::move(node));
            }
            co_return error_cursor(resource_, core::error_code_t::other_error, "kafka: unhandled op");
        } catch (const std::exception& e) {
            log_->error("KafkaManager::execute caught exception: {}", e.what());
            co_return error_cursor(resource_, core::error_code_t::other_error, e.what());
        }
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_create_source(kafka_node_ptr node) {
        // A SOURCE is a user-visible materialized table: the poller ingests its topic
        // into it, and INSERT produces to the topic
        if (!database_ensured_) {
            if (auto db = co_await ensure_database(); !db || db->is_error()) {
                co_return error_cursor(resource_,
                                       core::error_code_t::schema_error,
                                       "kafka: ensure database failed: " + cursor_error(db));
            }
            database_ensured_ = true;
        }
        if (auto c = co_await create_table(node->name(), declared_columns(node)); !c || c->is_error()) {
            co_return error_cursor(resource_,
                                   core::error_code_t::schema_error,
                                   "kafka: create '" + node->name() + "' failed: " + cursor_error(c));
        }
        // A SOURCE consumes -> persist per-partition offsets
        if (auto c = co_await create_table(node->name() + "__offsets", offsets_columns()); !c || c->is_error()) {
            co_return error_cursor(resource_,
                                   core::error_code_t::schema_error,
                                   "kafka: create offsets for '" + node->name() + "' failed: " + cursor_error(c));
        }

        registry_[node->name()] = kafka_object_t{node->op(), node->columns(), node->options(), std::string{}};
        log_->info("kafka: registered source '{}'", node->name());

        // TRANSACTIONAL=true -> exactly-once ingestion; validated even broker-free
        auto txn = parse_transactional(node->option("TRANSACTIONAL"));
        if (txn.has_error()) {
            co_return error_cursor(resource_,
                                   core::error_code_t::invalid_parameter,
                                   "kafka: source '" + node->name() + "' " + txn.error().what.c_str());
        }
        const bool transactional = txn.value();

        // Persist options so a restart relaunches the poller (columns re-read from the
        // catalog, not stored)
        co_await ensure_sources_table_once();
        if (auto p = co_await persist_source_meta(node->name(),
                                                  "source",
                                                  node->option("KAFKA_TOPIC").value_or(""),
                                                  node->option("BOOTSTRAP_SERVERS").value_or(""),
                                                  node->option("GROUP_ID").value_or("otterstax_" + node->name()),
                                                  node->option("OFFSET_RESET").value_or("earliest"),
                                                  transactional,
                                                  /*as_select*/ "");
            p && p->is_error()) {
            log_->error("kafka: persist meta for '{}' failed: {}", node->name(), p->get_error().what.c_str());
        }
        launch_source_poller(node->name(), transactional);
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_create_stream(kafka_node_ptr node) {
        // STREAM is a continuous query (no user table); its schema is the SELECT's
        // output columns. Any parse/schema failure becomes a DDL error
        auto invalid = [&](const std::string& why) {
            return error_cursor(resource_, core::error_code_t::other_error, "kafka: stream SELECT invalid: " + why);
        };
        auto plan = kafka_parse_plan(resource_, node->as_select());
        if (plan.has_error()) {
            co_return invalid(plan.error().what.c_str());
        }
        const auto* agg =
            plan.value().sub_queries.empty() ? nullptr : kafka_find_aggregate(plan.value().sub_queries.back());
        if (!agg) {
            co_return invalid("SELECT has no aggregate/source table");
        }
        const auto src_it = registry_.find(agg->relname().t);
        if (src_it == registry_.end()) {
            co_return invalid("unknown source '" + agg->relname().t + "'");
        }
        auto stream_columns =
            stream_output_schema(resource_, *agg, plan.value().parameters.get(), src_it->second.columns);

        registry_[node->name()] = kafka_object_t{node->op(), stream_columns, node->options(), node->as_select()};
        log_->info("kafka: registered stream '{}' ({} output columns)", node->name(), stream_columns.size());

        auto txn = parse_transactional(node->option("TRANSACTIONAL"));
        if (txn.has_error()) {
            co_return error_cursor(resource_,
                                   core::error_code_t::invalid_parameter,
                                   "kafka: stream '" + node->name() + "' " + txn.error().what.c_str());
        }

        // Persist so a restart relaunches it (source re-derived from as_select)
        co_await ensure_sources_table_once();
        if (auto p = co_await persist_source_meta(node->name(),
                                                  "stream",
                                                  node->option("KAFKA_TOPIC").value_or(""),
                                                  node->option("BOOTSTRAP_SERVERS").value_or(""),
                                                  node->option("GROUP_ID").value_or("otterstax_stream_" + node->name()),
                                                  node->option("OFFSET_RESET").value_or("earliest"),
                                                  txn.value(),
                                                  node->as_select());
            p && p->is_error()) {
            log_->error("kafka: persist meta for stream '{}' failed: {}", node->name(), p->get_error().what.c_str());
        }
        launch_stream(node->name());
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_drop_source(kafka_node_ptr node) {
        pollers_.erase(node->name()); // stop the ingestion poller before dropping its table

        auto drop = logical_plan::make_node_drop_collection(resource_);
        logical_plan::node_ptr plan_node =
            sql::transform::maybe_wrap_with_catalog_resolve_table(resource_,
                                                                  std::string{KAFKA_DATABASE_NAME},
                                                                  node->name(),
                                                                  drop);
        auto cursor = co_await send_plan(
            logical_plan::execution_plan_t{resource_, plan_node, logical_plan::make_parameter_node(resource_)});

        auto drop_offsets = logical_plan::make_node_drop_collection(resource_);
        logical_plan::node_ptr offsets_node =
            sql::transform::maybe_wrap_with_catalog_resolve_table(resource_,
                                                                  std::string{KAFKA_DATABASE_NAME},
                                                                  node->name() + "__offsets",
                                                                  drop_offsets);
        if (auto off = co_await send_plan(logical_plan::execution_plan_t{resource_,
                                                                         offsets_node,
                                                                         logical_plan::make_parameter_node(resource_)});
            off && off->is_error()) {
            log_->debug("kafka: drop offsets table for '{}' (non-fatal): {}",
                        node->name(),
                        off->get_error().what.c_str());
        }

        registry_.erase(node->name());
        // Drop its persisted __sources row (best-effort bookkeeping — a failure does
        // not fail the DROP; the backing table is already gone). log_forget_meta_error
        // deliberately swallows the engine's spurious first-teardown table_not_exists.
        log_forget_meta_error(log_, node->name(), co_await delete_source_meta(node->name()));
        if (cursor && cursor->is_error() && !node->if_exists()) {
            co_return cursor;
        }
        log_->info("kafka: dropped '{}'", node->name());
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_drop_stream(kafka_node_ptr node) {
        streams_.erase(node->name()); // stop + join the continuous-query worker
        registry_.erase(node->name());

        // Stop + forget every INSERT INTO query that fed this stream (each has its own
        // worker/registry/__sources entry), so recover() won't relaunch into a gone stream
        if (auto iq = stream_insert_queries_.find(node->name()); iq != stream_insert_queries_.end()) {
            for (const auto& qn : iq->second) {
                streams_.erase(qn);
                registry_.erase(qn);
                if (auto d = co_await delete_source_meta(qn); d && d->is_error()) {
                    log_->error("kafka: delete __sources row for insert-query '{}' failed: {}",
                                qn,
                                d->get_error().what.c_str());
                }
            }
            stream_insert_queries_.erase(iq);
        }
        if (auto d = co_await delete_source_meta(node->name()); d && d->is_error()) {
            log_->error("kafka: delete __sources row for stream '{}' failed: {}",
                        node->name(),
                        d->get_error().what.c_str());
        }
        log_->info("kafka: dropped stream '{}'", node->name());
        co_return cursor::make_cursor(resource_);
    }
} // namespace otterstax::kafka
