// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "integration/kafka/kafka_manager.hpp"
#include "kafka_poller.hpp"
#include "kafka_producer.hpp"
#include "kafka_reader.hpp"
#include "kafka_stream.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/logical_plan/param_storage.hpp>
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

        // The engine's answer to a DDL on a relation that is not there
        bool is_not_found(const cursor::cursor_t_ptr& c) {
            if (!c || !c->is_error()) {
                return false;
            }
            const auto code = c->get_error().type;
            return code == core::error_code_t::table_not_exists || code == core::error_code_t::database_not_exists;
        }

        core::result_wrapper_t<bool> parse_transactional(std::pmr::memory_resource* resource,
                                                         const std::optional<std::string>& value) {
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
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"TRANSACTIONAL must be 'true' or 'false', got '" + *value + "'", resource});
        }

        // A registered object's stored TRANSACTIONAL option (validated at CREATE, so
        // only the spelling of 'true' varies)
        bool option_is_true(const std::unordered_map<std::string, std::string>& options, const char* key) {
            const auto it = options.find(key);
            if (it == options.end()) {
                return false;
            }
            std::string value = it->second;
            std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
            return value == "true";
        }

        // Index of the first output column without a name: its rows could not be
        // keyed in the produced JSON, so the stream must not be created over it
        std::optional<std::size_t> unnamed_column(const std::vector<kafka_column_t>& columns) {
            for (std::size_t i = 0; i < columns.size(); ++i) {
                if (columns[i].name.empty()) {
                    return i;
                }
            }
            return std::nullopt;
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
        OTX_ZONE_N("KafkaManager::ensure_sources_table_once");
        if (!sources_table_ensured_) {
            auto c = co_await ensure_sources_table(); // IF NOT EXISTS: a restart finds it already there
            if (!c || c->is_error()) {
                co_return error_cursor(resource_,
                                       c ? c->get_error().type : core::error_code_t::other_error,
                                       "kafka: ensure kafka.__sources failed: " + cursor_error(c));
            }
            sources_table_ensured_ = true;
        }
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::drop_source_tables(const std::string& name) {
        OTX_ZONE_N("KafkaManager::drop_source_tables");
        auto main = co_await drop_table(name);
        auto offsets = co_await drop_table(name + "__offsets");
        auto null_reply = [&](const char* step) {
            return error_cursor(resource_,
                                core::error_code_t::other_error,
                                std::string{"kafka: "} + step + ": null cursor");
        };
        if (!main || main->is_error()) {
            co_return main ? main : null_reply("drop");
        }
        if (!offsets || (offsets->is_error() && !is_not_found(offsets))) {
            co_return offsets ? offsets : null_reply("drop offsets");
        }
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::execute(session_hash_t id, kafka_node_ptr node) {
        OTX_ZONE_N("KafkaManager::execute");
        log_->debug("execute id {}: kafka op {} name {}", id, static_cast<int32_t>(node->op()), node->name());
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
        co_return error_cursor(resource_, core::error_code_t::unimplemented_yet, "kafka: unhandled op");
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_create_source(kafka_node_ptr node) {
        OTX_ZONE_N("KafkaManager::handle_create_source");
        // Every option is validated before the first side effect, so a rejected
        // statement leaves no table, registry entry or metadata row behind
        auto txn = parse_transactional(resource_, node->option("TRANSACTIONAL"));
        if (txn.has_error()) {
            co_return error_cursor(resource_,
                                   core::error_code_t::invalid_parameter,
                                   "kafka: source '" + node->name() + "' " + txn.error().what.c_str());
        }
        const bool transactional = txn.value(); // TRANSACTIONAL=true -> exactly-once ingestion

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
        // The metadata table comes first: a source that cannot be persisted would
        // silently vanish on restart, so it is not created at all
        if (auto s = co_await ensure_sources_table_once(); !s || s->is_error()) {
            co_return s ? s : error_cursor(resource_, core::error_code_t::other_error, "kafka: null cursor");
        }
        if (auto c = co_await create_table(node->name(), declared_columns(node), /*if_not_exists*/ false);
            !c || c->is_error()) {
            co_return error_cursor(resource_,
                                   core::error_code_t::schema_error,
                                   "kafka: create '" + node->name() + "' failed: " + cursor_error(c));
        }
        // A SOURCE consumes -> persist per-partition offsets. From here on a failure
        // rolls the created tables back so nothing half-created survives
        auto rollback = [&] { return drop_source_tables(node->name()); };
        if (auto c = co_await create_table(node->name() + "__offsets", offsets_columns(), /*if_not_exists*/ false);
            !c || c->is_error()) {
            const std::string why = "kafka: create offsets for '" + node->name() + "' failed: " + cursor_error(c);
            if (auto d = co_await rollback(); !d || d->is_error()) {
                log_->error("kafka: rollback of '{}' after a failed create: {}", node->name(), cursor_error(d));
            }
            co_return error_cursor(resource_, core::error_code_t::schema_error, why);
        }
        // Persist options so a restart relaunches the poller (columns re-read from the
        // catalog, not stored)
        if (auto p = co_await persist_source_meta(node->name(),
                                                  "source",
                                                  node->option("KAFKA_TOPIC").value_or(""),
                                                  node->option("BOOTSTRAP_SERVERS").value_or(""),
                                                  node->option("GROUP_ID").value_or("otterstax_" + node->name()),
                                                  node->option("OFFSET_RESET").value_or("earliest"),
                                                  transactional,
                                                  /*as_select*/ "");
            !p || p->is_error()) {
            const std::string why = "kafka: persist metadata for '" + node->name() + "' failed: " + cursor_error(p);
            if (auto d = co_await rollback(); !d || d->is_error()) {
                log_->error("kafka: rollback of '{}' after a failed persist: {}", node->name(), cursor_error(d));
            }
            co_return error_cursor(resource_, p ? p->get_error().type : core::error_code_t::other_error, why);
        }

        registry_[node->name()] = kafka_object_t{node->op(), node->columns(), node->options(), std::string{}};
        log_->info("kafka: registered source '{}'", node->name());
        launch_source_poller(node->name(), transactional);
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_create_stream(kafka_node_ptr node) {
        OTX_ZONE_N("KafkaManager::handle_create_stream");
        // Options are validated before the object is registered or persisted
        auto txn = parse_transactional(resource_, node->option("TRANSACTIONAL"));
        if (txn.has_error()) {
            co_return error_cursor(resource_,
                                   core::error_code_t::invalid_parameter,
                                   "kafka: stream '" + node->name() + "' " + txn.error().what.c_str());
        }

        // STREAM is a continuous query (no user table); its schema is the SELECT's
        // output columns. Any parse/schema failure becomes a DDL error
        auto invalid = [&](const std::string& why) {
            return error_cursor(resource_,
                                core::error_code_t::invalid_parameter,
                                "kafka: stream SELECT invalid: " + why);
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
        if (const auto unnamed = unnamed_column(stream_columns)) {
            co_return invalid("output column " + std::to_string(*unnamed + 1) +
                              " has no name; alias it so the produced JSON can be keyed");
        }

        // Persist so a restart relaunches it (source re-derived from as_select); a
        // stream that cannot be persisted is not created
        if (auto s = co_await ensure_sources_table_once(); !s || s->is_error()) {
            co_return s ? s : error_cursor(resource_, core::error_code_t::other_error, "kafka: null cursor");
        }
        if (auto p = co_await persist_source_meta(node->name(),
                                                  "stream",
                                                  node->option("KAFKA_TOPIC").value_or(""),
                                                  node->option("BOOTSTRAP_SERVERS").value_or(""),
                                                  node->option("GROUP_ID").value_or("otterstax_stream_" + node->name()),
                                                  node->option("OFFSET_RESET").value_or("earliest"),
                                                  txn.value(),
                                                  node->as_select());
            !p || p->is_error()) {
            co_return error_cursor(resource_,
                                   p ? p->get_error().type : core::error_code_t::other_error,
                                   "kafka: persist metadata for stream '" + node->name() +
                                       "' failed: " + cursor_error(p));
        }

        registry_[node->name()] = kafka_object_t{node->op(), stream_columns, node->options(), node->as_select()};
        log_->info("kafka: registered stream '{}' ({} output columns)", node->name(), stream_columns.size());
        launch_stream(node->name());
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_drop_source(kafka_node_ptr node) {
        OTX_ZONE_N("KafkaManager::handle_drop_source");
        // Stop ingestion before the tables go; it is relaunched if the drop fails
        const bool had_poller = pollers_.erase(node->name()) > 0;

        auto dropped = co_await drop_source_tables(node->name());
        const bool absent = is_not_found(dropped);
        if ((!dropped || dropped->is_error()) && !(absent && node->if_exists())) {
            // The source still exists (or its state is unknown): keep it registered
            // and ingesting; IF EXISTS only forgives a missing table, never a failure
            if (had_poller) {
                if (const auto it = registry_.find(node->name()); it != registry_.end()) {
                    launch_source_poller(node->name(), option_is_true(it->second.options, "transactional"));
                }
            }
            co_return dropped ? dropped
                              : error_cursor(resource_, core::error_code_t::other_error, "kafka: drop: null cursor");
        }

        // The tables are gone (or were never there): forget the object everywhere
        registry_.erase(node->name());
        producers_.erase(node->name());
        if (auto d = co_await delete_source_meta(node->name()); !d || d->is_error()) {
            // A stale row makes recover() skip the object (no backing table), so the
            // DROP itself stands; the inconsistency is reported
            log_->error("kafka: delete __sources row for '{}' failed: {}", node->name(), cursor_error(d));
        }
        log_->info("kafka: dropped '{}'", node->name());
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_drop_stream(kafka_node_ptr node) {
        OTX_ZONE_N("KafkaManager::handle_drop_stream");
        // A stream has no table: the registry is the only existence check
        if (registry_.find(node->name()) == registry_.end()) {
            if (node->if_exists()) {
                co_return cursor::make_cursor(resource_);
            }
            co_return error_cursor(resource_,
                                   core::error_code_t::do_not_exists,
                                   "kafka: stream '" + node->name() + "' does not exist");
        }
        streams_.erase(node->name()); // stop + join the continuous-query worker
        registry_.erase(node->name());
        producers_.erase(node->name());

        // Stop + forget every INSERT INTO query that fed this stream (each has its own
        // worker/registry/__sources entry), so recover() won't relaunch into a gone stream
        if (auto iq = stream_insert_queries_.find(node->name()); iq != stream_insert_queries_.end()) {
            for (const auto& qn : iq->second) {
                streams_.erase(qn);
                registry_.erase(qn);
                if (auto d = co_await delete_source_meta(qn); !d || d->is_error()) {
                    log_->error("kafka: delete __sources row for insert-query '{}' failed: {}", qn, cursor_error(d));
                }
            }
            stream_insert_queries_.erase(iq);
        }
        if (auto d = co_await delete_source_meta(node->name()); !d || d->is_error()) {
            log_->error("kafka: delete __sources row for stream '{}' failed: {}", node->name(), cursor_error(d));
        }
        log_->info("kafka: dropped stream '{}'", node->name());
        co_return cursor::make_cursor(resource_);
    }
} // namespace otterstax::kafka
