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

#include <components/catalog/catalog_codes.hpp>
#include <components/logical_plan/node_aggregate.hpp>

#include <components/table/column_definition.hpp>

#include <algorithm>
#include <cctype>
#include <list>
#include <map>
#include <memory_resource>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace otterstax::kafka {
    using namespace components;
    using namespace detail;

    namespace {
        // Drive a dispatcher reply to completion on this (non-loop) thread
        cursor::cursor_t_ptr drive(actor_zeta::unique_future<cursor::cursor_t_ptr> future) {
            while (!future.is_ready()) {
                std::this_thread::sleep_for(ENGINE_POLL_STEP);
            }
            return std::move(future).take_ready();
        }

        // `name` as SQL text, every part double-quoted (an object name is a Kafka
        // grammar identifier; a quote would be doubled all the same)
        std::string quoted(const qualified_name_t& name) {
            std::string out;
            for (const auto* part : {&name.database.t, &name.schema.t, &name.collection.t}) {
                if (!out.empty()) {
                    out += '.';
                }
                out += '"';
                for (char c : *part) {
                    out += c;
                    if (c == '"') {
                        out += '"';
                    }
                }
                out += '"';
            }
            return out;
        }

        std::string literal(const std::string& text) {
            std::string out{"'"};
            for (char c : text) {
                out += c;
                if (c == '\'') {
                    out += '\'';
                }
            }
            return out + "'";
        }

        // The columns of `table` as the engine describes SELECT * FROM it: typed
        // from the catalog, so an empty table answers as a populated one. A VIEW
        // answers its body's columns; a missing table the engine's own code
        core::result_wrapper_t<std::vector<kafka_column_t>> describe_table(actor_zeta::address_t dispatcher_address,
                                                                           std::pmr::memory_resource* resource,
                                                                           const qualified_name_t& table) {
            OTX_ZONE_N("kafka::describe_table");
            auto plan = kafka_parse_plan(resource, "SELECT * FROM " + quoted(table) + ";");
            if (plan.has_error()) {
                return plan.error();
            }
            plan.value().explain = logical_plan::explain_type::describe;
            return described_columns(resource,
                                     drive(kafka_execute(dispatcher_address, resource, std::move(plan.value()))));
        }

        // pg_class.relkind of the local `table` (database.schema.relname), asked of
        // the catalog: what tells a base table from a VIEW — the describe cannot,
        // since the engine answers a VIEW with its body's columns
        core::result_wrapper_t<char> relation_kind(actor_zeta::address_t dispatcher_address,
                                                   std::pmr::memory_resource* resource,
                                                   const qualified_name_t& table) {
            OTX_ZONE_N("kafka::relation_kind");
            auto cursor = drive(kafka_query(
                dispatcher_address,
                resource,
                "SELECT c.relkind FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
                "JOIN pg_catalog.pg_database d ON n.nspdatabase = d.oid WHERE d.datconnection = '' AND d.datname = " +
                    literal(table.database.t) + " AND n.nspname = " + literal(table.schema.t) +
                    " AND c.relname = " + literal(table.collection.t) + ";"));
            if (!cursor) {
                return core::error_t(core::error_code_t::internal_error,
                                     std::pmr::string{"kafka: the engine answered no cursor", resource});
            }
            if (cursor->is_error()) {
                return core::error_t(cursor->get_error().type,
                                     std::pmr::string{cursor->get_error().what.c_str(), resource});
            }
            if (cursor->size() == 0) {
                return core::error_t(core::error_code_t::table_not_exists,
                                     std::pmr::string{"pg_class has no row for " + table.to_string(), resource});
            }
            auto kind = cursor->get<std::string_view>(0, 0);
            if (kind.has_error() || kind.value().empty()) {
                return core::error_t(core::error_code_t::internal_error,
                                     std::pmr::string{"pg_class carries no relkind for " + table.to_string(), resource});
            }
            return kind.value().front();
        }

        // The columns the SELECT of a STREAM / INSERT INTO query `sql` produces
        // over the columns of its source (describe_stream, driven)
        core::result_wrapper_t<std::vector<kafka_column_t>>
        recovered_output(actor_zeta::address_t dispatcher_address,
                         std::pmr::memory_resource* resource,
                         const std::string& sql,
                         const std::vector<kafka_column_t>& source_columns) {
            OTX_ZONE_N("kafka::recovered_output");
            auto plan = kafka_parse_plan(resource, sql);
            if (plan.has_error()) {
                return plan.error();
            }
            auto stream_src = plan.value().sub_queries.empty()
                                  ? std::nullopt
                                  : kafka_stream_source(resource, plan.value().sub_queries.back());
            if (!stream_src) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"the SELECT has no source table", resource});
            }
            return described_columns(resource,
                                     drive(describe_stream(dispatcher_address,
                                                           resource,
                                                           source_columns,
                                                           stream_src->operators,
                                                           plan.value().parameters)));
        }

        // Stored per-partition offsets for table-seek resume; empty -> broker-group resume
        std::map<int32_t, int64_t> read_committed_offsets(actor_zeta::address_t dispatcher_address,
                                                          std::pmr::memory_resource* resource,
                                                          const qualified_name_t& offsets_table) {
            const std::string sql = "SELECT partition_id, committed_offset FROM " + quoted(offsets_table) + ";";
            return parse_offsets(drive(kafka_query(dispatcher_address, resource, sql)));
        }
    } // namespace

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::persist_source_meta(const std::string& name,
                                                                                      const std::string& kind,
                                                                                      const std::string& topic,
                                                                                      const std::string& bootstrap,
                                                                                      const std::string& group_id,
                                                                                      const std::string& offset_reset,
                                                                                      bool transactional,
                                                                                      const std::string& as_select) {
        // Insert the row via node_insert (a SQL INSERT string through kafka_query
        // crashes the engine on this table)
        std::pmr::vector<types::complex_logical_type> col_types(resource_);
        auto str_col = [&](const char* alias) {
            types::complex_logical_type t(types::logical_type::STRING_LITERAL);
            t.set_alias(alias);
            return t;
        };
        col_types.push_back(str_col("name"));
        col_types.push_back(str_col("kind"));
        col_types.push_back(str_col("topic"));
        col_types.push_back(str_col("bootstrap"));
        col_types.push_back(str_col("group_id"));
        col_types.push_back(str_col("offset_reset"));
        col_types.push_back(str_col("transactional")); // 'true'/'false' string (no BOOLEAN — see storage_plans)
        col_types.push_back(str_col("as_select"));

        vector::data_chunk_t chunk(resource_, col_types, 1);
        chunk.set_value(0, 0, std::string_view{name});
        chunk.set_value(1, 0, std::string_view{kind});
        chunk.set_value(2, 0, std::string_view{topic});
        chunk.set_value(3, 0, std::string_view{bootstrap});
        chunk.set_value(4, 0, std::string_view{group_id});
        chunk.set_value(5, 0, std::string_view{offset_reset});
        chunk.set_value(6, 0, std::string_view{transactional ? "true" : "false"});
        chunk.set_value(7, 0, std::string_view{as_select});
        chunk.set_cardinality(1);
        return kafka_insert(dispatcher_address_,
                            resource_,
                            kafka_service_table_name(KAFKA_SOURCES_TABLE),
                            std::move(chunk));
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::delete_source_meta(const std::string& name) {
        return kafka_delete_where_eq(dispatcher_address_,
                                     resource_,
                                     kafka_service_table_name(KAFKA_SOURCES_TABLE),
                                     "name",
                                     name);
    }

    void KafkaManager::recover() {
        OTX_ZONE_N("KafkaManager::recover");
        // The schemas and the metadata table exist from the start: `kafka.x` names
        // the schema main.kafka whether or not a SOURCE was ever created
        for (auto& plan : storage_plans()) {
            if (auto c = drive(send_plan(std::move(plan))); !c || c->is_error()) {
                log_->error("kafka: recover: ensure federation.kafka.__sources failed: {}",
                            c ? c->get_error().what.c_str() : "null cursor");
                return;
            }
        }
        storage_ensured_ = true;
        if (!start_pollers_) {
            return; // broker-free unit tests don't run pollers, nothing to relaunch
        }
        auto cursor = drive(
            kafka_query(dispatcher_address_,
                        resource_,
                        "SELECT name, kind, topic, bootstrap, group_id, offset_reset, transactional, as_select FROM " +
                            quoted(kafka_service_table_name(KAFKA_SOURCES_TABLE)) + ";"));
        if (!cursor || cursor->is_error()) {
            log_->error("kafka: recover: reading federation.kafka.__sources failed: {}",
                        cursor ? cursor->get_error().what.c_str() : "null cursor");
            return;
        }

        // Snapshot every persisted object, then recover in TWO passes — sources
        // first (a stream needs its source already in registry_ to relaunch)
        struct meta_t {
            std::string name, kind, topic, bootstrap, group_id, offset_reset, as_select;
            bool transactional;
        };
        std::vector<meta_t> metas;
        // The result is a batch of <=1024-row chunks: every chunk is read (a
        // registry past the first chunk is not lost), each through its own row
        // space rather than a per-cell chunk scan
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                auto text = [&](std::uint64_t col) {
                    return std::string{chunk.get_value<std::string_view>(col, row)};
                };
                metas.push_back(
                    meta_t{text(0), text(1), text(2), text(3), text(4), text(5), text(7), text(6) == "true"});
            }
        }

        auto make_options = [](const meta_t& m) {
            std::unordered_map<std::string, std::string> o;
            o["kafka_topic"] = m.topic; // stream: the OUT topic/bootstrap
            o["bootstrap_servers"] = m.bootstrap;
            o["group_id"] = m.group_id;
            o["offset_reset"] = m.offset_reset;
            if (m.transactional) {
                o["transactional"] = "true";
            }
            return o;
        };

        // Every name with a __sources row, and the sources whose backing table is
        // gone (a DROP SOURCE whose row delete failed). A stream or INSERT INTO
        // query whose source is one of those, or has no row at all, is dangling:
        // its row is deleted rather than skipped on every restart (and silently
        // re-attached to an unrelated source re-created under the name)
        std::unordered_set<std::string> persisted;
        for (const auto& m : metas) {
            persisted.insert(m.name);
        }
        std::unordered_set<std::string> dead;
        auto is_dead = [&](const std::string& object) {
            return dead.count(object) != 0 || persisted.count(object) == 0;
        };
        auto delete_dangling = [&](const char* what, const std::string& object, const std::string& reason) {
            if (auto d = drive(delete_source_meta(object)); !d || d->is_error()) {
                log_->error("kafka: recover {} '{}': {}; deleting its __sources row failed: {}",
                            what,
                            object,
                            reason,
                            d ? d->get_error().what.c_str() : "null cursor");
                return;
            }
            log_->warn("kafka: recover {} '{}': {}; deleted its __sources row", what, object, reason);
        };

        // Pass 1 — SOURCEs. Columns aren't persisted: re-read from the backing
        // table through the engine's describe, which types the result from the
        // catalog rather than from row data, so an empty table has the same schema
        // as a populated one
        for (const auto& m : metas) {
            if (m.kind != "source") {
                continue;
            }
            const auto table = kafka_table_name(m.name);
            auto read = describe_table(dispatcher_address_, resource_, table);
            if (read.has_error()) {
                // Only a missing backing table (a dropped source whose __sources row
                // lingers) is a skip; any other engine failure is reported as such
                const auto code = read.error().type;
                if (code == core::error_code_t::table_not_exists || code == core::error_code_t::schema_not_exists ||
                    code == core::error_code_t::database_not_exists) {
                    log_->warn("kafka: recover source '{}': backing table is gone, skipping", m.name);
                    dead.insert(m.name);
                } else {
                    log_->error("kafka: recover source '{}': describe failed (code {}): {}; skipping",
                                m.name,
                                static_cast<int32_t>(code),
                                read.error().what.c_str());
                }
                continue;
            }
            auto kind = relation_kind(dispatcher_address_, resource_, table);
            if (kind.has_error()) {
                log_->error("kafka: recover source '{}': pg_catalog lookup failed (code {}): {}; skipping",
                            m.name,
                            static_cast<int32_t>(kind.error().type),
                            kind.error().what.c_str());
                continue;
            }
            if (kind.value() == catalog::relkind::view || kind.value() == catalog::relkind::materialized_view) {
                // A SOURCE is backed by a base table the poller inserts into; a VIEW
                // under its name cannot be ingested into
                log_->error("kafka: recover source '{}': backing relation is a VIEW, not a table; skipping", m.name);
                continue;
            }
            auto columns = std::move(read.value());
            if (columns.empty()) {
                log_->error("kafka: recover source '{}': backing table has no columns, skipping", m.name);
                continue;
            }
            register_object(m.name, kafka_object_t{kafka_op::create_source,
                                                   std::move(columns),
                                                   make_options(m),
                                                   std::string{},
                                                   std::string{}});
            launch_source_poller(m.name, m.transactional);
            log_->info("kafka: recovered source '{}' (transactional={})", m.name, m.transactional);
        }

        // Pass 2 — STREAMs (recompute the output schema over the source's columns).
        // A stream can read another stream, so streams are recovered in rounds
        // until none becomes resolvable: the order of the __sources rows does not
        // decide whether a stream comes back
        struct pending_stream_t {
            const meta_t* meta;
            std::string source;
        };
        std::vector<pending_stream_t> pending;
        for (const auto& m : metas) {
            if (m.kind != "stream") {
                continue;
            }
            auto plan = kafka_parse_plan(resource_, m.as_select);
            if (plan.has_error()) {
                log_->warn("kafka: recover stream '{}': {}, skipping", m.name, plan.error().what.c_str());
                continue;
            }
            const auto* agg =
                plan.value().sub_queries.empty() ? nullptr : kafka_find_aggregate(plan.value().sub_queries.back());
            if (!agg) {
                log_->warn("kafka: recover stream '{}': SELECT has no aggregate/source, skipping", m.name);
                continue;
            }
            pending.push_back(pending_stream_t{&m, agg->target().collection.t});
        }
        for (bool progress = true; progress;) {
            progress = false;
            for (auto it = pending.begin(); it != pending.end();) {
                const auto src_it = registry_.find(it->source);
                if (src_it == registry_.end()) {
                    ++it;
                    continue;
                }
                const meta_t& m = *it->meta;
                auto stream_columns = recovered_output(dispatcher_address_, resource_, m.as_select, src_it->second.columns);
                if (stream_columns.has_error()) {
                    log_->error("kafka: recover stream '{}': describe failed (code {}): {}; skipping",
                                m.name,
                                static_cast<int32_t>(stream_columns.error().type),
                                stream_columns.error().what.c_str());
                    it = pending.erase(it);
                    continue;
                }
                register_object(m.name, kafka_object_t{kafka_op::create_stream,
                                                       std::move(stream_columns.value()),
                                                       make_options(m),
                                                       m.as_select,
                                                       it->source});
                launch_stream(m.name);
                log_->info("kafka: recovered stream '{}' (transactional={})", m.name, m.transactional);
                it = pending.erase(it);
                progress = true;
            }
        }
        // What is left reads an object that did not come back. Dead when that
        // object is gone (transitively: a stream over a dangling stream dangles too)
        for (bool grew = true; grew;) {
            grew = false;
            for (const auto& p : pending) {
                if (dead.count(p.meta->name) == 0 && is_dead(p.source)) {
                    dead.insert(p.meta->name);
                    grew = true;
                }
            }
        }
        for (const auto& p : pending) {
            if (dead.count(p.meta->name) != 0) {
                delete_dangling("stream", p.meta->name, "source '" + p.source + "' is gone");
            } else {
                log_->warn("kafka: recover stream '{}': source '{}' was not recovered, skipping",
                           p.meta->name,
                           p.source);
            }
        }

        // Pass 3 — INSERT INTO queries. Target stream + SELECT source are re-derived
        // from the persisted INSERT SQL; launched like a stream
        for (const auto& m : metas) {
            if (m.kind != "insert") {
                continue;
            }
            auto plan = kafka_parse_plan(resource_, m.as_select);
            if (plan.has_error()) {
                log_->warn("kafka: recover insert-query '{}': {}, skipping", m.name, plan.error().what.c_str());
                continue;
            }
            if (plan.value().sub_queries.empty()) {
                log_->warn("kafka: recover insert-query '{}': compiled to an empty plan, skipping", m.name);
                continue;
            }
            auto write = kafka_write_target(plan.value().sub_queries.back());
            if (!write) {
                log_->warn("kafka: recover insert-query '{}': no kafka target, skipping", m.name);
                continue;
            }
            const std::string sink = write->relname();
            const auto* agg = kafka_find_aggregate(plan.value().sub_queries.back());
            if (!agg) {
                log_->warn("kafka: recover insert-query '{}': no source table, skipping", m.name);
                continue;
            }
            const std::string source = agg->target().collection.t;
            if (is_dead(sink)) {
                delete_dangling("insert-query", m.name, "target stream '" + sink + "' is gone");
                continue;
            }
            const auto src_it = registry_.find(source);
            if (src_it == registry_.end()) {
                if (is_dead(source)) {
                    delete_dangling("insert-query", m.name, "source '" + source + "' is gone");
                } else {
                    log_->warn("kafka: recover insert-query '{}': source '{}' was not recovered, skipping",
                               m.name,
                               source);
                }
                continue;
            }
            auto out_columns = recovered_output(dispatcher_address_, resource_, m.as_select, src_it->second.columns);
            if (out_columns.has_error()) {
                log_->error("kafka: recover insert-query '{}': describe failed (code {}): {}; skipping",
                            m.name,
                            static_cast<int32_t>(out_columns.error().type),
                            out_columns.error().what.c_str());
                continue;
            }
            register_object(m.name, kafka_object_t{kafka_op::create_stream,
                                                   std::move(out_columns.value()),
                                                   make_options(m),
                                                   m.as_select,
                                                   source});
            stream_insert_queries_[sink].push_back(m.name);
            launch_stream(m.name);
            log_->info("kafka: recovered INSERT INTO query '{}' into stream '{}' (transactional={})",
                       m.name,
                       sink,
                       m.transactional);
        }
    }

    void KafkaManager::launch_source_poller(const std::string& name, bool transactional) {
        OTX_ZONE_N("KafkaManager::launch_source_poller");
        if (!start_pollers_) {
            return;
        }
        auto it = registry_.find(name);
        if (it == registry_.end()) {
            return;
        }
        const auto& opts = it->second.options;
        const auto topic = opts.find("kafka_topic");
        const auto bootstrap = opts.find("bootstrap_servers");
        if (topic == opts.end() || bootstrap == opts.end()) {
            log_->warn("kafka: source '{}' has no KAFKA_TOPIC/BOOTSTRAP_SERVERS — no poller", name);
            return;
        }
        const auto gid = opts.find("group_id");
        const auto ores = opts.find("offset_reset");
        consumer_config_t cfg{bootstrap->second,
                              topic->second,
                              gid != opts.end() ? gid->second : ("otterstax_" + name),
                              ores != opts.end() ? ores->second : "earliest",
                              transactional ? read_committed_offsets(dispatcher_address_,
                                                                     resource_,
                                                                     kafka_offsets_table_name(name))
                                            : std::map<int32_t, int64_t>{}};
        auto consumer = kafka_consumer_t::create(std::move(cfg));
        if (consumer.has_error()) {
            log_->error("kafka: poller consumer for '{}' failed: {}", name, consumer.error().what.c_str());
            return;
        }
        pollers_[name] = std::make_unique<kafka_poller_t>(dispatcher_address_,
                                                          resource_,
                                                          kafka_table_name(name),
                                                          kafka_offsets_table_name(name),
                                                          it->second.columns,
                                                          std::move(consumer.value()),
                                                          transactional);
        log_->info("kafka: started poller for source '{}' (topic '{}')", name, topic->second);
    }

    void KafkaManager::launch_stream(const std::string& name) {
        OTX_ZONE_N("KafkaManager::launch_stream");
        if (!start_pollers_) {
            return;
        }
        auto it = registry_.find(name);
        if (it == registry_.end()) {
            return;
        }
        auto start_failed = [&](const std::string& why) {
            log_->error("kafka: stream start failed for '{}': {}", name, why);
        };
        // Compile the captured SELECT once; extract the source table + operators
        auto plan = kafka_parse_plan(resource_, it->second.as_select);
        if (plan.has_error()) {
            return start_failed(plan.error().what.c_str());
        }
        if (plan.value().sub_queries.empty()) {
            return start_failed("stream SELECT compiled to an empty plan");
        }
        auto stream_src = kafka_stream_source(resource_, plan.value().sub_queries.back());
        if (!stream_src) {
            return start_failed("stream SELECT has no source table");
        }
        // The source must already be in registry_ (recover() does sources first)
        auto src_it = registry_.find(stream_src->source_relname);
        if (src_it == registry_.end()) {
            return start_failed("unknown source '" + stream_src->source_relname + "'");
        }
        const auto& src_opts = src_it->second.options;
        const auto src_topic = src_opts.find("kafka_topic");
        const auto src_bootstrap = src_opts.find("bootstrap_servers");
        if (src_topic == src_opts.end() || src_bootstrap == src_opts.end()) {
            return start_failed("source '" + stream_src->source_relname + "' has no KAFKA_TOPIC/BOOTSTRAP_SERVERS");
        }
        const auto& opts = it->second.options;
        const auto out_topic = opts.find("kafka_topic");
        const auto out_bootstrap = opts.find("bootstrap_servers");
        if (out_topic == opts.end() || out_bootstrap == opts.end()) {
            return start_failed("stream has no KAFKA_TOPIC/BOOTSTRAP_SERVERS");
        }
        const auto gid = opts.find("group_id");
        const std::string group_id = gid != opts.end() ? gid->second : ("otterstax_stream_" + name);
        const auto ores = opts.find("offset_reset");
        const std::string offset_reset = ores != opts.end() ? ores->second : "earliest";
        // TRANSACTIONAL already validated at CREATE STREAM (DDL); 'true' -> a stable
        // transactional id (exactly-once producer). Value stored verbatim, so fold
        std::string transactional_id;
        if (const auto t = opts.find("transactional"); t != opts.end()) {
            std::string value = t->second;
            std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
            if (value == "true") {
                transactional_id = "otterstax_stream_" + name;
            }
        }

        auto consumer =
            kafka_consumer_t::create({src_bootstrap->second, src_topic->second, group_id, offset_reset, {}});
        if (consumer.has_error()) {
            return start_failed(std::string{"consumer failed: "} + consumer.error().what.c_str());
        }
        auto producer = kafka_producer_t::create({out_bootstrap->second, out_topic->second, transactional_id});
        if (producer.has_error()) {
            return start_failed(std::string{"producer failed: "} + producer.error().what.c_str());
        }
        streams_[name] = std::make_unique<kafka_stream_t>(dispatcher_address_,
                                                          resource_,
                                                          stream_transform_t{src_it->second.columns,
                                                                             std::move(stream_src->operators),
                                                                             plan.value().parameters,
                                                                             it->second.columns},
                                                          std::move(consumer.value()),
                                                          std::move(producer.value()));
        log_->info("kafka: started stream '{}' ({} -> {})", name, src_topic->second, out_topic->second);
    }
} // namespace otterstax::kafka
