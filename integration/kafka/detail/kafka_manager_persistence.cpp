// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "integration/kafka/kafka_manager.hpp"
#include "kafka_const.hpp"
#include "kafka_poller.hpp"
#include "kafka_producer.hpp"
#include "kafka_reader.hpp"
#include "kafka_stream.hpp"
#include "otterbrix/operators/schema_probe.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

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

        // pg_class identity of database.relation, asked of the engine: the namespace
        // by name, then the relation by name within it. This is what tells a base
        // table from a VIEW — the column probe cannot, since the engine answers a
        // VIEW with its body's columns
        core::result_wrapper_t<schema_probe::relation_t> probe_relation_kind(actor_zeta::address_t dispatcher_address,
                                                                             std::pmr::memory_resource* resource,
                                                                             const std::string& database,
                                                                             const std::string& relation) {
            OTX_ZONE_N("kafka::probe_relation_kind");
            auto namespace_probe = schema_probe::make_namespace_probe(resource, database);
            auto namespace_oid = schema_probe::read_namespace_oid(
                namespace_probe,
                drive(kafka_execute(dispatcher_address, resource, namespace_probe.execution_plan(resource))),
                resource);
            if (namespace_oid.has_error()) {
                return namespace_oid.convert_error<schema_probe::relation_t>();
            }
            auto relation_probe = schema_probe::make_relation_probe(resource, database, relation);
            return schema_probe::read_relation(
                relation_probe,
                drive(kafka_execute(dispatcher_address, resource, relation_probe.execution_plan(resource))),
                namespace_oid.value(),
                resource);
        }

        // Stored per-partition offsets for table-seek resume; empty -> broker-group resume
        core::result_wrapper_t<std::map<int32_t, int64_t>> read_committed_offsets(actor_zeta::address_t dispatcher_address,
                                                          std::pmr::memory_resource* resource,
                                                          const std::string& database,
                                                          const std::string& offsets_table) {
            const std::string sql =
                "SELECT partition_id, committed_offset FROM " + database + "." + offsets_table + ";";
            return parse_offsets(drive(kafka_query(dispatcher_address, resource, sql)));
        }
    } // namespace

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::ensure_sources_table() {
        std::vector<table::column_definition_t> cols;
        cols.emplace_back("name", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        cols.emplace_back("kind", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        cols.emplace_back("topic", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        cols.emplace_back("bootstrap", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        cols.emplace_back("group_id", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        cols.emplace_back("offset_reset", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        cols.emplace_back("transactional", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        cols.emplace_back("as_select", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        return create_table("__sources", std::move(cols), /*if_not_exists*/ true);
    }

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
        col_types.push_back(str_col("transactional")); // 'true'/'false' string (no BOOLEAN — see ensure_sources_table)
        col_types.push_back(str_col("as_select"));

        vector::data_chunk_t chunk(resource_, col_types, 1);
        chunk.set_value(0, 0, types::logical_value_t(resource_, name));
        chunk.set_value(1, 0, types::logical_value_t(resource_, kind));
        chunk.set_value(2, 0, types::logical_value_t(resource_, topic));
        chunk.set_value(3, 0, types::logical_value_t(resource_, bootstrap));
        chunk.set_value(4, 0, types::logical_value_t(resource_, group_id));
        chunk.set_value(5, 0, types::logical_value_t(resource_, offset_reset));
        chunk.set_value(6, 0, types::logical_value_t(resource_, std::string(transactional ? "true" : "false")));
        chunk.set_value(7, 0, types::logical_value_t(resource_, as_select));
        chunk.set_cardinality(1);
        return kafka_insert(dispatcher_address_,
                            resource_,
                            std::string{KAFKA_DATABASE_NAME},
                            "__sources",
                            std::move(chunk));
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::delete_source_meta(const std::string& name) {
        return kafka_delete_where_eq(dispatcher_address_,
                                     resource_,
                                     std::string{KAFKA_DATABASE_NAME},
                                     "__sources",
                                     "name",
                                     name);
    }

    core::error_t KafkaManager::recover() {
        OTX_ZONE_N("KafkaManager::recover");
        if (!start_pollers_) {
            return core::error_t::no_error();
        }
        auto failure = [&](core::error_code_t code, const std::string& what) {
            return core::error_t(code, std::pmr::string{"kafka recovery: " + what, resource_});
        };
        // Only an absent registry is a first start. Preserve other engine errors.
        const std::string kafka_db{KAFKA_DATABASE_NAME};
        auto cursor = drive(
            kafka_query(dispatcher_address_,
                        resource_,
                        "SELECT name, kind, topic, bootstrap, group_id, offset_reset, transactional, as_select FROM " +
                            kafka_db + ".__sources;"));
        if (!cursor) {
            return failure(core::error_code_t::other_error, "no reply when reading __sources");
        }
        if (cursor->is_error()) {
            const auto code = cursor->get_error().type;
            if (code == core::error_code_t::table_not_exists || code == core::error_code_t::database_not_exists) {
                return core::error_t::no_error();
            }
            return failure(code, cursor->get_error().what.c_str());
        }
        sources_table_ensured_ = true;

        // Snapshot every persisted object, then recover in three passes — sources
        // first (a stream needs its source already in registry_ to relaunch)
        struct meta_t {
            std::string name, kind, topic, bootstrap, group_id, offset_reset, as_select;
            bool transactional;
        };
        std::vector<meta_t> metas;
        std::unordered_map<std::string, bool> names;
        // The result is a batch of <=1024-row chunks: every chunk is read (a
        // registry past the first chunk is not lost), each through its own row
        // space rather than a per-cell chunk scan
        for (const auto& chunk : cursor->chunks()) {
            const auto column_types = chunk.types();
            if (column_types.size() != 8 ||
                std::any_of(column_types.begin(), column_types.end(), [](const auto& column) {
                    return column.type() != types::logical_type::STRING_LITERAL;
                })) {
                return failure(core::error_code_t::data_corruption, "__sources must contain eight text columns");
            }
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                for (std::size_t col = 0; col < column_types.size(); ++col) {
                    if (chunk.data[col].is_null(row)) {
                        return failure(core::error_code_t::data_corruption, "NULL field in __sources");
                    }
                }
                auto text = [&](std::uint64_t col) {
                    return std::string{chunk.value(col, row).value<std::string_view>()};
                };
                auto meta = meta_t{text(0), text(1), text(2), text(3), text(4), text(5), text(7), text(6) == "true"};
                if (meta.name.empty() || !names.emplace(meta.name, true).second ||
                    (meta.kind != "source" && meta.kind != "stream" && meta.kind != "insert") ||
                    (text(6) != "true" && text(6) != "false")) {
                    return failure(core::error_code_t::data_corruption, "invalid or duplicate object in __sources");
                }
                metas.push_back(std::move(meta));
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

        // Pass 1 — SOURCEs. Columns aren't persisted: re-read from the backing
        // table through the engine's schema reply. An empty table has the same
        // schema as a populated one.
        for (const auto& m : metas) {
            if (m.kind != "source") {
                continue;
            }
            auto probe = schema_probe::make_table_probe(resource_, kafka_db, m.name);
            auto schema_cursor = drive(kafka_execute(dispatcher_address_, resource_, probe.execution_plan(resource_)));
            auto read = schema_probe::read_columns(probe, schema_cursor, resource_);
            if (read.has_error()) {
                // Only a missing backing table (a dropped source whose __sources row
                // lingers) is a skip; any other engine failure is reported as such
                const auto code = read.error().type;
                if (code == core::error_code_t::table_not_exists || code == core::error_code_t::database_not_exists) {
                    log_->warn("kafka: recover source '{}': backing table is gone, skipping", m.name);
                } else {
                    return failure(code, "source '" + m.name + "': " + read.error().what.c_str());
                }
                continue;
            }
            auto identity = probe_relation_kind(dispatcher_address_, resource_, kafka_db, m.name);
            if (identity.has_error()) {
                return failure(identity.error().type, "source '" + m.name + "': " + identity.error().what.c_str());
            }
            if (identity.value().relkind == catalog::relkind::view ||
                identity.value().relkind == catalog::relkind::materialized_view) {
                // A SOURCE is backed by a base table the poller inserts into; a VIEW
                // under its name cannot be ingested into
                return failure(core::error_code_t::schema_error, "source '" + m.name + "' is backed by a VIEW");
            }
            std::vector<kafka_column_t> columns;
            for (const auto& col : read.value().columns) {
                columns.push_back(kafka_column_t{col.has_alias() ? std::string{col.alias()} : std::string{}, col});
            }
            if (columns.empty()) {
                return failure(core::error_code_t::schema_error, "source '" + m.name + "' has no columns");
            }
            registry_[m.name] =
                kafka_object_t{kafka_op::create_source, std::move(columns), make_options(m), std::string{}};
            if (auto started = launch_source_poller(m.name, m.transactional); started.contains_error()) {
                return started;
            }
            log_->info("kafka: recovered source '{}' (transactional={})", m.name, m.transactional);
        }

        // Pass 2 — STREAMs (source now in registry_; recompute the output schema)
        for (const auto& m : metas) {
            if (m.kind != "stream") {
                continue;
            }
            auto plan = kafka_parse_plan(resource_, m.as_select);
            if (plan.has_error()) {
                return failure(plan.error().type, "stream '" + m.name + "': " + plan.error().what.c_str());
            }
            const auto* agg =
                plan.value().sub_queries.empty() ? nullptr : kafka_find_aggregate(plan.value().sub_queries.back());
            if (!agg) {
                return failure(core::error_code_t::schema_error, "stream '" + m.name + "' SELECT has no source");
            }
            const auto src_it = registry_.find(agg->relname().t);
            if (src_it == registry_.end()) {
                return failure(core::error_code_t::schema_error, "stream '" + m.name + "' has an unknown source");
            }
            auto stream_columns =
                stream_output_schema(resource_, *agg, plan.value().parameters.get(), src_it->second.columns);
            registry_[m.name] =
                kafka_object_t{kafka_op::create_stream, std::move(stream_columns), make_options(m), m.as_select};
            if (auto started = launch_stream(m.name); started.contains_error()) {
                return started;
            }
            log_->info("kafka: recovered stream '{}' (transactional={})", m.name, m.transactional);
        }

        // Pass 3 — INSERT INTO queries. Target stream + SELECT source are re-derived
        // from the persisted INSERT SQL; launched like a stream
        for (const auto& m : metas) {
            if (m.kind != "insert") {
                continue;
            }
            auto plan = kafka_parse_plan(resource_, m.as_select);
            if (plan.has_error()) {
                return failure(plan.error().type, "insert-query '" + m.name + "': " + plan.error().what.c_str());
            }
            if (plan.value().sub_queries.empty()) {
                return failure(core::error_code_t::schema_error, "insert-query '" + m.name + "' has an empty plan");
            }
            auto write = kafka_write_target(plan.value().sub_queries.back());
            if (!write) {
                return failure(core::error_code_t::schema_error, "insert-query '" + m.name + "' has no Kafka target");
            }
            const std::string sink = write->relname;
            const auto* agg = kafka_find_aggregate(plan.value().sub_queries.back());
            if (!agg) {
                return failure(core::error_code_t::schema_error, "insert-query '" + m.name + "' has no source");
            }
            const auto src_it = registry_.find(agg->relname().t);
            if (src_it == registry_.end()) {
                return failure(core::error_code_t::schema_error, "insert-query '" + m.name + "' has an unknown source");
            }
            auto out_columns =
                stream_output_schema(resource_, *agg, plan.value().parameters.get(), src_it->second.columns);
            registry_[m.name] =
                kafka_object_t{kafka_op::create_stream, std::move(out_columns), make_options(m), m.as_select};
            stream_insert_queries_[sink].push_back(m.name);
            if (auto started = launch_stream(m.name); started.contains_error()) {
                return started;
            }
            log_->info("kafka: recovered INSERT INTO query '{}' into stream '{}' (transactional={})",
                       m.name,
                       sink,
                       m.transactional);
        }
        return core::error_t::no_error();
    }

    core::error_t KafkaManager::launch_source_poller(const std::string& name, bool transactional) {
        OTX_ZONE_N("KafkaManager::launch_source_poller");
        if (!start_pollers_) {
            return core::error_t::no_error();
        }
        auto failure = [&](core::error_code_t code, const std::string& what) {
            return core::error_t(code, std::pmr::string{"kafka source '" + name + "': " + what, resource_});
        };
        auto it = registry_.find(name);
        if (it == registry_.end()) {
            return failure(core::error_code_t::invalid_parameter, "source is not registered");
        }
        const auto& opts = it->second.options;
        const auto topic = opts.find("kafka_topic");
        const auto bootstrap = opts.find("bootstrap_servers");
        if (topic == opts.end() || bootstrap == opts.end()) {
            return failure(core::error_code_t::invalid_parameter, "missing KAFKA_TOPIC/BOOTSTRAP_SERVERS");
        }
        const auto gid = opts.find("group_id");
        const auto ores = opts.find("offset_reset");
        std::map<int32_t, int64_t> committed_offsets;
        if (transactional) {
            auto stored = read_committed_offsets(dispatcher_address_, resource_,
                                                  std::string{KAFKA_DATABASE_NAME}, name + "__offsets");
            if (stored.has_error()) {
                return failure(stored.error().type, "reading committed offsets failed: " + std::string{stored.error().what});
            }
            committed_offsets = std::move(stored.value());
        }
        consumer_config_t cfg{bootstrap->second,
                              topic->second,
                              gid != opts.end() ? gid->second : ("otterstax_" + name),
                              ores != opts.end() ? ores->second : "earliest",
                              std::move(committed_offsets), destinations_};
        auto consumer = kafka_consumer_t::create(std::move(cfg));
        if (consumer.has_error()) {
            return failure(consumer.error().type, consumer.error().what.c_str());
        }
        pollers_[name] = std::make_unique<kafka_poller_t>(dispatcher_address_,
                                                          resource_,
                                                          std::string{KAFKA_DATABASE_NAME},
                                                          name,
                                                          it->second.columns,
                                                          std::move(consumer.value()),
                                                          transactional);
        log_->info("kafka: started poller for source '{}' (topic '{}')", name, topic->second);
        return core::error_t::no_error();
    }

    core::error_t KafkaManager::launch_stream(const std::string& name) {
        OTX_ZONE_N("KafkaManager::launch_stream");
        if (!start_pollers_) {
            return core::error_t::no_error();
        }
        auto start_failed = [&](const std::string& why) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"kafka stream '" + name + "': " + why, resource_});
        };
        auto it = registry_.find(name);
        if (it == registry_.end()) {
            return start_failed("stream is not registered");
        }
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
            kafka_consumer_t::create({src_bootstrap->second, src_topic->second, group_id, offset_reset, {}, destinations_});
        if (consumer.has_error()) {
            return start_failed(std::string{"consumer failed: "} + consumer.error().what.c_str());
        }
        auto producer = kafka_producer_t::create({out_bootstrap->second, out_topic->second, transactional_id, destinations_});
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
        return core::error_t::no_error();
    }
} // namespace otterstax::kafka
