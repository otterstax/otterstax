// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "integration/kafka/kafka_manager.hpp"
#include "kafka_poller.hpp"
#include "kafka_producer.hpp"
#include "kafka_reader.hpp"
#include "kafka_stream.hpp"
#include "utility/logger.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/table/column_definition.hpp>

#include <algorithm>
#include <cctype>
#include <list>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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

        // A RESTRICT drop the engine refused because a normal dependent (a VIEW, a
        // foreign key) reads the table. The engine words it as PostgreSQL does:
        //   cannot drop table kafka.s because other objects depend on it
        //   DETAIL: object with oid N depends on table kafka.s
        //   HINT: Use DROP ... CASCADE to drop the dependent objects too.
        // and reports the first such dependent only. Answers N, or nullopt when the
        // error is not that refusal
        std::optional<std::string> restrict_blocking_oid(const cursor::cursor_t_ptr& c) {
            if (!c || !c->is_error() || c->get_error().type != core::error_code_t::other_error) {
                return std::nullopt;
            }
            const std::string what{c->get_error().what.c_str()};
            static constexpr std::string_view marker{"DETAIL: object with oid "};
            const auto at = what.find(marker);
            if (what.find("because other objects depend on it") == std::string::npos || at == std::string::npos) {
                return std::nullopt;
            }
            const auto first = at + marker.size();
            auto last = first;
            while (last < what.size() && std::isdigit(static_cast<unsigned char>(what[last]))) {
                ++last;
            }
            if (last == first) {
                return std::nullopt;
            }
            return what.substr(first, last - first);
        }

        // pg_class.relkind spelled the way a DROP error names the object
        std::string relkind_word(char kind) {
            switch (kind) {
                case catalog::relkind::view:
                    return "view";
                case catalog::relkind::materialized_view:
                    return "materialized view";
                case catalog::relkind::regular:
                    return "table";
                case catalog::relkind::index:
                    return "index";
                default:
                    return "relation";
            }
        }

        // "stream 'x', insert query 'y__insert__1' depend on it"
        template<typename Reader>
        std::string describe_readers(const std::vector<Reader>& readers) {
            std::string out;
            for (std::size_t i = 0; i < readers.size(); ++i) {
                if (i > 0) {
                    out += ", ";
                }
                out += readers[i].insert_query ? "insert query '" : "stream '";
                out += readers[i].name;
                out += "'";
            }
            out += readers.size() == 1 ? " depends on it" : " depend on it";
            return out;
        }

        // The INSERT INTO queries owned by `stream` (the queries whose sink it is)
        const std::vector<std::string>*
        insert_queries_of(const std::unordered_map<std::string, std::vector<std::string>>& insert_queries,
                          const std::string& stream) {
            const auto it = insert_queries.find(stream);
            return it == insert_queries.end() ? nullptr : &it->second;
        }

        bool is_insert_query(const std::unordered_map<std::string, std::vector<std::string>>& insert_queries,
                             const std::string& name) {
            for (const auto& [sink, queries] : insert_queries) {
                if (std::find(queries.begin(), queries.end(), name) != queries.end()) {
                    return true;
                }
            }
            return false;
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

    actor_zeta::unique_future<cursor::cursor_t_ptr>
    KafkaManager::drop_source_tables(const std::string& name, catalog::drop_behavior_t behavior) {
        // KafkaManager's loop interleaves its coroutines on one thread, so the zone
        // closes before the first suspension (CLAUDE.md, "Tracy zones in coroutines")
        {
            OTX_ZONE_N("KafkaManager::drop_source_tables");
            log_->debug("kafka: dropping the tables of source '{}' ({})",
                        name,
                        behavior == catalog::drop_behavior_t::cascade_ ? "CASCADE" : "RESTRICT");
        }
        auto main = co_await drop_table(name, behavior);
        if (!main) {
            co_return error_cursor(resource_, core::error_code_t::other_error, "kafka: drop: null cursor");
        }
        if (main->is_error() && !is_not_found(main)) {
            // Refused (RESTRICT under a dependent) or failed: the source stands, so
            // its offsets table must stand too
            co_return main;
        }
        // __offsets is internal and nothing user-visible should read it; CASCADE so
        // that a VIEW someone put over it anyway cannot strand a source whose
        // backing table is already gone
        auto offsets = co_await drop_table(name + "__offsets", catalog::drop_behavior_t::cascade_);
        if (!offsets || (offsets->is_error() && !is_not_found(offsets))) {
            // The source is gone either way; a leftover offsets table is removed by
            // a later DROP SOURCE IF EXISTS of the name (or fails its next CREATE)
            log_->error("kafka: drop of kafka.{}__offsets failed: {}", name, cursor_error(offsets));
        }
        co_return main;
    }

    std::vector<KafkaManager::reader_t> KafkaManager::blocking_readers(const std::string& name) const {
        OTX_ZONE_N("KafkaManager::blocking_readers");
        // The object's own set: itself, plus the INSERT INTO queries a STREAM owns
        std::unordered_set<std::string> own{name};
        if (const auto* queries = insert_queries_of(stream_insert_queries_, name)) {
            own.insert(queries->begin(), queries->end());
        }
        std::vector<reader_t> readers;
        for (const auto& [object, binding] : registry_) {
            if (!binding.source.empty() && own.count(binding.source) != 0 && own.count(object) == 0) {
                readers.push_back(reader_t{object, is_insert_query(stream_insert_queries_, object)});
            }
        }
        std::sort(readers.begin(), readers.end(), [](const reader_t& a, const reader_t& b) {
            return a.name < b.name;
        });
        return readers;
    }

    std::vector<std::string> KafkaManager::cascade_closure(const std::string& name) const {
        OTX_ZONE_N("KafkaManager::cascade_closure");
        std::vector<std::string> taken;
        std::unordered_set<std::string> seen;
        auto take = [&](const std::string& object) {
            if (seen.insert(object).second) {
                taken.push_back(object);
            }
            if (const auto* queries = insert_queries_of(stream_insert_queries_, object)) {
                for (const auto& query : *queries) {
                    if (seen.insert(query).second) {
                        taken.push_back(query);
                    }
                }
            }
        };
        take(name);
        for (std::size_t i = 0; i < taken.size(); ++i) {
            for (const auto& [object, binding] : registry_) {
                if (binding.source == taken[i]) {
                    take(object);
                }
            }
        }
        // Only stream-like objects are stopped and forgotten here; a SOURCE (the
        // seed of DROP SOURCE) keeps its own table/poller handling
        std::vector<std::string> stream_like;
        for (auto& object : taken) {
            const auto it = registry_.find(object);
            if (it != registry_.end() && it->second.op == kafka_op::create_stream) {
                stream_like.push_back(std::move(object));
            }
        }
        return stream_like;
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr>
    KafkaManager::forget_stream_objects(std::vector<std::string> names) {
        {
            OTX_ZONE_N("KafkaManager::forget_stream_objects");
            for (const auto& object : names) {
                streams_.erase(object); // stop + join the continuous-query worker
                registry_.erase(object);
                producers_.erase(object);
                // As a sink: its INSERT INTO queries are in `names` too (cascade_closure)
                stream_insert_queries_.erase(object);
                // As an INSERT INTO query: off its sink's list
                for (auto& [sink, queries] : stream_insert_queries_) {
                    queries.erase(std::remove(queries.begin(), queries.end(), object), queries.end());
                }
            }
        }
        // Every row goes, so recover() relaunches none of them
        for (const auto& object : names) {
            if (auto d = co_await delete_source_meta(object); !d || d->is_error()) {
                log_->error("kafka: delete __sources row for '{}' failed: {}", object, cursor_error(d));
            }
            log_->info("kafka: dropped '{}'", object);
        }
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr>
    KafkaManager::describe_restrict_refusal(const std::string& name, cursor::cursor_t_ptr refused) {
        std::optional<std::string> oid;
        std::string prefix;
        {
            OTX_ZONE_N("KafkaManager::describe_restrict_refusal");
            oid = restrict_blocking_oid(refused);
            prefix = "kafka: cannot drop source '" + name + "': ";
        }
        auto fallback = [&] {
            return error_cursor(resource_, refused->get_error().type, prefix + cursor_error(refused));
        };
        if (!oid) {
            co_return fallback();
        }
        // The oid is digits parsed off the engine's own message, so it is spelled
        // into the lookup as a number; no name reaches SQL text
        auto relation = co_await kafka_query(dispatcher_address_,
                                             resource_,
                                             "SELECT relname, relnamespace, relkind FROM pg_catalog.pg_class "
                                             "WHERE oid = " + *oid + ";");
        if (!relation || relation->is_error() || relation->size() == 0) {
            co_return fallback();
        }
        const auto relname_col = relation->column_index("relname");
        const auto namespace_col = relation->column_index("relnamespace");
        const auto kind_col = relation->column_index("relkind");
        if (relname_col.has_error() || namespace_col.has_error() || kind_col.has_error()) {
            co_return fallback();
        }
        const std::string relname{relation->value(relname_col.value(), 0).value<std::string_view>()};
        // Copied out at once: value() answers a temporary the view would outlive
        const std::string kind{relation->value(kind_col.value(), 0).value<std::string_view>()};
        const auto namespace_oid = relation->value(namespace_col.value(), 0).value<std::uint32_t>();
        auto nsp = co_await kafka_query(dispatcher_address_,
                                        resource_,
                                        "SELECT nspname FROM pg_catalog.pg_namespace WHERE oid = " +
                                            std::to_string(namespace_oid) + ";");
        std::string qualified = relname;
        if (nsp && !nsp->is_error() && nsp->size() > 0) {
            if (const auto nspname_col = nsp->column_index("nspname"); !nspname_col.has_error()) {
                qualified = std::string{nsp->value(nspname_col.value(), 0).value<std::string_view>()} + "." + relname;
            }
        }
        co_return error_cursor(resource_,
                               refused->get_error().type,
                               prefix + relkind_word(kind.empty() ? '\0' : kind.front()) + " " + qualified +
                                   " depends on it (and possibly other objects); use DROP SOURCE " + name +
                                   " CASCADE");
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
        auto rollback = [&] { return drop_source_tables(node->name(), catalog::drop_behavior_t::cascade_); };
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

        registry_[node->name()] =
            kafka_object_t{node->op(), node->columns(), node->options(), std::string{}, std::string{}};
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
        const auto src_it = registry_.find(agg->target().collection.t);
        if (src_it == registry_.end()) {
            co_return invalid("unknown source '" + agg->target().collection.t + "'");
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

        registry_[node->name()] = kafka_object_t{node->op(),
                                                 stream_columns,
                                                 node->options(),
                                                 node->as_select(),
                                                 agg->target().collection.t};
        log_->info("kafka: registered stream '{}' ({} output columns)", node->name(), stream_columns.size());
        launch_stream(node->name());
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_drop_source(kafka_node_ptr node) {
        const std::string name = node->name();
        const bool cascade = node->cascade();
        bool had_poller = false;
        {
            OTX_ZONE_N("KafkaManager::handle_drop_source");
            // DROP SOURCE names a source, never a stream or INSERT INTO query: a
            // stream has no backing table, so the absent-table path below would
            // otherwise forget it (and, through cascade_closure, its readers)
            if (const auto it = registry_.find(name); it != registry_.end() && it->second.op != kafka_op::create_source) {
                co_return error_cursor(resource_,
                                       core::error_code_t::invalid_parameter,
                                       "kafka: '" + name + "' is a stream, not a source; use DROP STREAM");
            }
            // RESTRICT asks the Kafka side first, before anything is stopped: a
            // stream or INSERT INTO query reading the source keeps it
            if (!cascade) {
                if (const auto readers = blocking_readers(name); !readers.empty()) {
                    co_return error_cursor(resource_,
                                           core::error_code_t::other_error,
                                           "kafka: cannot drop source '" + name + "': " + describe_readers(readers) +
                                               "; use DROP SOURCE " + name + " CASCADE");
                }
            }
            // Stop ingestion before the tables go; it is relaunched if the drop fails
            had_poller = pollers_.erase(name) > 0;
        }

        // The backing table first: until it is gone nothing else is touched, so a
        // refused or failed drop leaves the source exactly as it was
        auto dropped = co_await drop_source_tables(name,
                                                   cascade ? catalog::drop_behavior_t::cascade_
                                                           : catalog::drop_behavior_t::restrict_);
        const bool absent = is_not_found(dropped);
        if (dropped->is_error() && !(absent && node->if_exists())) {
            // The source still exists (or its state is unknown): keep it registered
            // and ingesting; IF EXISTS only forgives a missing table, never a failure
            if (had_poller) {
                if (const auto it = registry_.find(name); it != registry_.end()) {
                    launch_source_poller(name, option_is_true(it->second.options, "transactional"));
                }
            }
            if (!cascade && restrict_blocking_oid(dropped)) {
                co_return co_await describe_restrict_refusal(name, std::move(dropped));
            }
            if (absent) {
                co_return dropped; // the engine's own "does not exist"
            }
            // Any other refusal or failure (wording not recognised, an engine error):
            // the engine's code and message, under the same prefix
            co_return error_cursor(resource_,
                                   dropped->get_error().type,
                                   "kafka: cannot drop source '" + name + "': " + cursor_error(dropped));
        }

        // The tables are gone (or were never there). CASCADE takes every stream and
        // INSERT INTO query reading the source, transitively; under RESTRICT there
        // were none when asked (one created while the drop ran goes too: its source
        // no longer exists). In-memory steps first, then the __sources rows
        if (auto readers = cascade_closure(name); !readers.empty()) {
            co_await forget_stream_objects(std::move(readers));
        }
        registry_.erase(name);
        producers_.erase(name);
        // A concurrent DROP of the same source that was refused may have
        // relaunched the poller while this one awaited the engine
        pollers_.erase(name);
        if (auto d = co_await delete_source_meta(name); !d || d->is_error()) {
            // A stale row makes recover() skip the object (no backing table), so the
            // DROP itself stands; the inconsistency is reported
            log_->error("kafka: delete __sources row for '{}' failed: {}", name, cursor_error(d));
        }
        log_->info("kafka: dropped '{}'", name);
        co_return cursor::make_cursor(resource_);
    }

    actor_zeta::unique_future<cursor::cursor_t_ptr> KafkaManager::handle_drop_stream(kafka_node_ptr node) {
        const std::string name = node->name();
        std::vector<std::string> dropped;
        {
            OTX_ZONE_N("KafkaManager::handle_drop_stream");
            // A stream has no table: the registry is the only existence check
            const auto it = registry_.find(name);
            if (it == registry_.end()) {
                if (node->if_exists()) {
                    co_return cursor::make_cursor(resource_);
                }
                co_return error_cursor(resource_,
                                       core::error_code_t::do_not_exists,
                                       "kafka: stream '" + name + "' does not exist");
            }
            if (it->second.op != kafka_op::create_stream) {
                co_return error_cursor(resource_,
                                       core::error_code_t::invalid_parameter,
                                       "kafka: '" + name + "' is a source, not a stream; use DROP SOURCE");
            }
            // A stream's own INSERT INTO queries (it is their sink) go with it in
            // both modes; RESTRICT refuses only over a stream or INSERT INTO query
            // that reads FROM it (a stream's output topic can be another's source)
            if (!node->cascade()) {
                if (const auto readers = blocking_readers(name); !readers.empty()) {
                    co_return error_cursor(resource_,
                                           core::error_code_t::other_error,
                                           "kafka: cannot drop stream '" + name + "': " + describe_readers(readers) +
                                               "; use DROP STREAM " + name + " CASCADE");
                }
            }
            dropped = cascade_closure(name);
        }
        // Stops every worker, then deletes every __sources row, so recover() won't
        // relaunch anything into (or out of) a gone stream
        co_await forget_stream_objects(std::move(dropped));
        log_->info("kafka: dropped stream '{}'", name);
        co_return cursor::make_cursor(resource_);
    }
} // namespace otterstax::kafka
