// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <catch2/catch_all.hpp>

#include <components/cursor/cursor.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/types/types.hpp>
#include <services/dispatcher/enrich_logical_plan.hpp>

#include "drive_future.hpp"
#include "integration/kafka/detail/kafka_poller.hpp"
#include "integration/kafka/detail/kafka_reader.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/otterbrix/otterbrix_engine.hpp"
#include "otterbrix/config.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/parser.hpp"
#include "utility/tsan_helper.hpp"

#include <actor-zeta.hpp>
#include <algorithm>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <map>
#include <memory_resource>
#include <string>

using otterstax::kafka::kafka_node_ptr;
using otterstax::kafka::kafka_node_t;
using otterstax::kafka::kafka_op;
using otterstax::kafka::KafkaManager;

namespace {
    kafka_node_ptr parse_kafka(GreenplumParser& parser, const std::string& sql) {
        auto parsed = parser.parse(sql);
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        // Every statement handed here is kafka DDL: the parser extension lowers it
        // to a kafka_node_t, the only node_type::unused root the parser produces
        auto& root = data->otterbrix_params->node;
        if (!root || root->type() != components::logical_plan::node_type::unused) {
            return kafka_node_ptr{};
        }
        return kafka_node_ptr{static_cast<kafka_node_t*>(root.get())}; // shares ownership before `data` drops
    }

    // One JSON row per id in [first, first + count): {"id": <i>, "name": "n<i>"}
    std::vector<std::string> id_name_payloads(std::int64_t first, std::size_t count) {
        std::vector<std::string> payloads;
        payloads.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            const auto id = first + static_cast<std::int64_t>(i);
            payloads.push_back("{\"id\":" + std::to_string(id) + ",\"name\":\"n" + std::to_string(id) + "\"}");
        }
        return payloads;
    }
} // namespace

TEST_CASE("kafka dry: CREATE/DROP SOURCE materialises tables, STREAM does not") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_dry");
    auto cfg = make_create_config("/tmp/otterstax_kafka_dry");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    // Probe a table's schema via a plain SELECT: a LIMIT plan (even LIMIT 1)
    // short-circuits over an empty table and returns a bare cursor (no
    // type_data, zero-column chunk), while an unlimited scan fills type_data
    // on the empty result. Tables stay empty in this dry test, so the full
    // scan is free. is_error() on the returned cursor == table absent
    auto probe = [&](const std::string& qualified) {
        return drive_until_ready(otterstax::kafka::detail::kafka_query(engine->engine_dispatcher_address(),
                                                                       resource,
                                                                       "SELECT * FROM " + qualified + ";"));
    };
    auto run_kafka = [&](kafka_node_ptr node) {
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(node));
        return drive_until_ready(std::move(fut));
    };
    // True iff kafka.__sources currently holds a row for `name` (the persisted
    // registry each CREATE writes and each DROP must remove)
    auto sources_has = [&](const std::string& name) {
        auto cursor = drive_until_ready(otterstax::kafka::detail::kafka_query(engine->engine_dispatcher_address(),
                                                                              resource,
                                                                              "SELECT name FROM kafka.__sources;"));
        if (!cursor || cursor->is_error()) {
            return false;
        }
        // cursor->value() spans the <=1024-row chunks of the result
        for (std::uint64_t row = 0; row < cursor->size(); ++row) {
            if (std::string{cursor->value(0, row).value<std::string_view>()} == name) {
                return true;
            }
        }
        return false;
    };

    // CREATE SOURCE: the parser produces a kafka_node_t and the manager creates
    // kafka.s with the declared columns
    auto src = parse_kafka(parser,
                           "CREATE SOURCE s (id INT, name VARCHAR) "
                           "WITH (KAFKA_TOPIC='t', VALUE_FORMAT='JSON', BOOTSTRAP_SERVERS='localhost:9092');");
    REQUIRE(src);
    REQUIRE(src->op() == kafka_op::create_source);
    REQUIRE(src->name() == "s");
    REQUIRE(src->columns().size() == 2);
    {
        auto cursor = run_kafka(std::move(src));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }
    {
        auto schema = probe("kafka.s");
        REQUIRE(schema);
        REQUIRE_FALSE(schema->is_error());
        REQUIRE(schema->type_data().size() == 2);
    }
    {
        // a SOURCE also gets a per-partition offsets table
        auto offsets = probe("kafka.s__offsets");
        REQUIRE(offsets);
        REQUIRE_FALSE(offsets->is_error());
        REQUIRE(offsets->type_data().size() == 2); // (partition, committed_offset)
    }

    // CREATE STREAM: registered only — no backing table (its state table is
    // designed with the streaming runtime later)
    auto stream = parse_kafka(parser,
                              "CREATE STREAM st WITH (KAFKA_TOPIC='ot', VALUE_FORMAT='JSON') "
                              "AS SELECT * FROM kafka.s;");
    REQUIRE(stream);
    REQUIRE(stream->op() == kafka_op::create_stream);
    REQUIRE_FALSE(stream->as_select().empty());
    {
        auto cursor = run_kafka(std::move(stream));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }
    REQUIRE(probe("kafka.st")->is_error()); // a stream creates no table

    // Both objects persisted a kafka.__sources row (source + stream)
    REQUIRE(sources_has("s"));
    REQUIRE(sources_has("st"));

    // DROP SOURCE removes the backing table AND its __sources row
    auto drop = parse_kafka(parser, "DROP SOURCE s;");
    REQUIRE(drop);
    REQUIRE(drop->op() == kafka_op::drop_source);
    {
        auto cursor = run_kafka(std::move(drop));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }
    REQUIRE(probe("kafka.s")->is_error()); // table gone
    REQUIRE_FALSE(sources_has("s"));       // no stale registry row for recover()
    REQUIRE(sources_has("st"));            // the stream's row is untouched

    // DROP STREAM removes its __sources row too (a stream has no backing table)
    auto drop_st = parse_kafka(parser, "DROP STREAM st;");
    REQUIRE(drop_st);
    REQUIRE(drop_st->op() == kafka_op::drop_stream);
    {
        auto cursor = run_kafka(std::move(drop_st));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }
    REQUIRE_FALSE(sources_has("st"));
}

TEST_CASE("kafka insert-query: INSERT INTO stream SELECT registers + persists, DROP cleans up") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_insert");
    auto cfg = make_create_config("/tmp/otterstax_kafka_insert");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    auto run_kafka = [&](kafka_node_ptr node) {
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(node));
        return drive_until_ready(std::move(fut));
    };
    auto run_insert = [&](const std::string& stream, const std::string& sql) {
        auto [_, fut] = actor_zeta::send(kafka_mgr->address(),
                                         &KafkaManager::add_stream_insert,
                                         session_hash_t{1},
                                         std::string{stream},
                                         std::string{sql});
        return drive_until_ready(std::move(fut));
    };
    // Count kafka.__sources rows of a given kind (source / stream / insert)
    auto kind_count = [&](const std::string& kind) {
        auto cursor =
            drive_until_ready(otterstax::kafka::detail::kafka_query(engine->engine_dispatcher_address(),
                                                                    resource,
                                                                    "SELECT name, kind FROM kafka.__sources;"));
        int n = 0;
        if (cursor && !cursor->is_error()) {
            // cursor->value() spans the <=1024-row chunks of the result
            for (std::uint64_t row = 0; row < cursor->size(); ++row) {
                if (std::string{cursor->value(1, row).value<std::string_view>()} == kind) {
                    ++n;
                }
            }
        }
        return n;
    };

    // Two sources with matching schemas + a stream over the first
    REQUIRE_FALSE(run_kafka(parse_kafka(parser,
                                        "CREATE SOURCE a (id INT, val INT) "
                                        "WITH (KAFKA_TOPIC='ta', BOOTSTRAP_SERVERS='localhost:9092');"))
                      ->is_error());
    REQUIRE_FALSE(run_kafka(parse_kafka(parser,
                                        "CREATE SOURCE b (id INT, val INT) "
                                        "WITH (KAFKA_TOPIC='tb', BOOTSTRAP_SERVERS='localhost:9092');"))
                      ->is_error());
    REQUIRE_FALSE(run_kafka(parse_kafka(parser,
                                        "CREATE STREAM merged WITH (KAFKA_TOPIC='om', "
                                        "BOOTSTRAP_SERVERS='localhost:9092') AS SELECT id, val FROM kafka.a;"))
                      ->is_error());
    REQUIRE(kind_count("insert") == 0);

    // Fan-in a second source into the stream: succeeds + persists a kind='insert' row
    {
        auto cursor = run_insert("merged", "INSERT INTO kafka.merged SELECT id, val FROM kafka.b;");
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }
    REQUIRE(kind_count("insert") == 1);

    // Reject INSERT ... SELECT into a SOURCE (continuous fan-in is stream-only)
    REQUIRE(run_insert("a", "INSERT INTO kafka.a SELECT id, val FROM kafka.b;")->is_error());
    // Reject a schema mismatch (1 projected column vs the stream's 2)
    REQUIRE(run_insert("merged", "INSERT INTO kafka.merged SELECT id FROM kafka.b;")->is_error());
    REQUIRE(kind_count("insert") == 1); // neither rejection persisted a row

    // DROP STREAM removes the stream's row AND its INSERT INTO query's row
    REQUIRE_FALSE(run_kafka(parse_kafka(parser, "DROP STREAM merged;"))->is_error());
    REQUIRE(kind_count("insert") == 0);
    REQUIRE(kind_count("stream") == 0);
}

TEST_CASE("kafka json: batch -> data_chunk drops malformed/incomplete rows") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    std::vector<otterstax::kafka::kafka_column_t> columns = {
        {"id", complex_logical_type(logical_type::INTEGER)},
        {"name", complex_logical_type(logical_type::STRING_LITERAL)},
    };

    auto chunk = otterstax::kafka::detail::json_to_chunk(resource,
                                                         columns,
                                                         {
                                                             R"({"id": 1, "name": "alice"})", // valid
                                                             R"({"id": 2, "name": "bob"})",   // valid
                                                             R"({"id": 3})",                // missing 'name' -> dropped
                                                             R"({"id": "x", "name": "c"})", // id wrong type -> dropped
                                                             R"(not json)",                 // malformed -> dropped
                                                         });

    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 2); // only the two fully-valid rows
}

TEST_CASE("kafka ingest: a json batch is inserted into the source table") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_ingest");
    auto cfg = make_create_config("/tmp/otterstax_kafka_ingest");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    auto src = parse_kafka(parser,
                           "CREATE SOURCE s (id INT, name VARCHAR) "
                           "WITH (KAFKA_TOPIC='t', VALUE_FORMAT='JSON', BOOTSTRAP_SERVERS='localhost:9092');");
    REQUIRE(src);
    auto columns = src->columns(); // copy before the node is moved into the manager
    {
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    auto chunk = otterstax::kafka::detail::json_to_chunk(resource,
                                                         columns,
                                                         {R"({"id": 1, "name": "a"})", R"({"id": 2, "name": "b"})"});
    REQUIRE(chunk.size() == 2);
    {
        auto cursor = drive_until_ready(otterstax::kafka::detail::kafka_insert(engine->engine_dispatcher_address(),
                                                                               resource,
                                                                               "kafka",
                                                                               "s",
                                                                               std::move(chunk)));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    auto rows = drive_until_ready(
        otterstax::kafka::detail::kafka_query(engine->engine_dispatcher_address(), resource, "SELECT * FROM kafka.s;"));
    REQUIRE(rows);
    REQUIRE_FALSE(rows->is_error());
    REQUIRE(rows->size() == 2);
}

TEST_CASE("kafka offsets: write_offsets / parse_offsets roundtrip with max-per-partition") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_offsets");
    auto cfg = make_create_config("/tmp/otterstax_kafka_offsets");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    auto src = parse_kafka(parser,
                           "CREATE SOURCE s (id INT, name VARCHAR) "
                           "WITH (KAFKA_TOPIC='t', VALUE_FORMAT='JSON', BOOTSTRAP_SERVERS='localhost:9092');");
    REQUIRE(src);
    {
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    auto addr = engine->engine_dispatcher_address();
    // Read offsets via kafka_query — the exact path the poller uses on startup
    auto read = [&] {
        return otterstax::kafka::detail::parse_offsets(drive_until_ready(
            otterstax::kafka::detail::kafka_query(addr,
                                                  resource,
                                                  "SELECT partition_id, committed_offset FROM kafka.s__offsets;")));
    };

    {
        auto cur = drive_until_ready(
            otterstax::kafka::detail::write_offsets(addr, resource, "kafka", "s__offsets", {{0, 100}, {1, 250}}));
        REQUIRE(cur);
        REQUIRE_FALSE(cur->is_error());
    }
    {
        auto offsets = read();
        REQUIRE(offsets.size() == 2);
        REQUIRE(offsets.at(0) == 100);
        REQUIRE(offsets.at(1) == 250);
    }

    // Appending a higher offset for partition 0 wins; partition 1 unchanged
    {
        auto cur = drive_until_ready(
            otterstax::kafka::detail::write_offsets(addr, resource, "kafka", "s__offsets", {{0, 180}}));
        REQUIRE(cur);
        REQUIRE_FALSE(cur->is_error());
    }
    {
        auto offsets = read();
        REQUIRE(offsets.at(0) == 180);
        REQUIRE(offsets.at(1) == 250);
    }
}

TEST_CASE("kafka poller: CREATE SOURCE starts a poller, DROP/teardown join cleanly") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_poller");
    auto cfg = make_create_config("/tmp/otterstax_kafka_poller");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();

    GreenplumParser parser(resource);
    auto kafka_mgr =
        actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address(), /*start_pollers=*/true);

    auto run_kafka = [&](kafka_node_ptr node) {
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(node));
        return drive_until_ready(std::move(fut));
    };

    auto src = parse_kafka(parser,
                           "CREATE SOURCE s (id INT, name VARCHAR) "
                           "WITH (KAFKA_TOPIC='t', VALUE_FORMAT='JSON', BOOTSTRAP_SERVERS='127.0.0.1:9092');");
    REQUIRE(src);
    {
        auto cursor = run_kafka(std::move(src));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    // DROP stops + joins the poller for s
    auto drop = parse_kafka(parser, "DROP SOURCE s;");
    REQUIRE(drop);
    {
        auto cursor = run_kafka(std::move(drop));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    // A leftover poller (s2, never dropped) must join cleanly when kafka_mgr is
    // destroyed at scope exit — reaching the end of the test is the check
    auto src2 = parse_kafka(parser,
                            "CREATE SOURCE s2 (id INT) "
                            "WITH (KAFKA_TOPIC='t2', VALUE_FORMAT='JSON', BOOTSTRAP_SERVERS='127.0.0.1:9092');");
    REQUIRE(src2);
    {
        auto cursor = run_kafka(std::move(src2));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }
    SUCCEED("poller lifecycle completed without hang");
}

TEST_CASE("kafka stream node-swap: aggregate(empty)+raw_data applies the SELECT") {
    namespace lp = components::logical_plan;
    namespace ty = components::types;

    std::filesystem::remove_all("/tmp/otterstax_kafka_swap");
    auto cfg = make_create_config("/tmp/otterstax_kafka_swap");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    // Create kafka.s so `SELECT ... FROM kafka.s` resolves at parse time
    {
        auto src = parse_kafka(
            parser,
            "CREATE SOURCE s (id INT, name VARCHAR) WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');");
        REQUIRE(src);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    std::vector<otterstax::kafka::kafka_column_t> cols{
        {"id", ty::complex_logical_type(ty::logical_type::INTEGER)},
        {"name", ty::complex_logical_type(ty::logical_type::STRING_LITERAL)}};
    std::vector<std::string> payloads;
    for (int i = 1; i <= 5; ++i) {
        payloads.push_back("{\"id\":" + std::to_string(i) + ",\"name\":\"name_" + std::to_string(i) + "\"}");
    }
    // The stream widens INTEGER columns to BIGINT (kafka_stream_t) so a parsed
    // `WHERE id > <literal>` (BIGINT param) filters over the raw_data batch; build
    // the batch the same way here
    auto stream_batch = [&] {
        auto wcols = cols;
        for (auto& c : wcols) {
            if (c.type.type() == ty::logical_type::INTEGER) {
                c.type = ty::complex_logical_type(ty::logical_type::BIGINT);
            }
        }
        return otterstax::kafka::detail::json_to_chunk(resource, wcols, payloads);
    };

    // Passthrough: aggregate(empty)+[raw_data] returns the batch unchanged
    {
        auto agg = lp::make_node_aggregate(resource, {}, {});
        agg->append_child(lp::make_node_raw_data(resource, stream_batch()));
        auto cursor = drive_until_ready(otterstax::kafka::detail::kafka_execute(
            addr,
            resource,
            lp::execution_plan_t{resource, agg, lp::make_parameter_node(resource)}));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        CHECK(cursor->size() == 5);
    }
    // kafka_stream_source extracts the SELECT's source table + RE-HOMED operators
    // (empty relname) so the parsed operator chain applies to the raw_data batch
    {
        auto plan = otterstax::kafka::detail::kafka_parse_plan(resource, "SELECT id, name FROM kafka.s WHERE id > 2;");
        REQUIRE_FALSE(plan.has_error());
        REQUIRE_FALSE(plan.value().sub_queries.empty());
        auto stream_src = otterstax::kafka::kafka_stream_source(resource, plan.value().sub_queries.back());
        REQUIRE(stream_src);
        CHECK(stream_src->source_relname == "s");
        // The SELECT body is three operator children: $match (the WHERE), $group —
        // which carries the SELECT list, since the transformer routes every projected
        // expression there — and an EMPTY $select, the node the validator moves that
        // list into once the query turns out not to be grouped. All three are carried:
        // without the $select the group stays a real reduction that folds the batch
        // into one row, without the $group there is no projection left to apply
        REQUIRE(stream_src->operators.size() == 3);
        CHECK(stream_src->operators[0]->type() == lp::node_type::match_t);
        CHECK(stream_src->operators[1]->type() == lp::node_type::group_t);
        CHECK(stream_src->operators[2]->type() == lp::node_type::select_t);
        CHECK(stream_src->operators[1]->expressions().size() == 2); // id, name
        // Every carried operator is re-homed: one still naming `kafka.s` would both
        // miss the raw_data rows and make each batch resolve a table it never reads
        CHECK(static_cast<const lp::node_match_t*>(stream_src->operators[0].get())->relname().empty());
        CHECK(static_cast<const lp::node_group_t*>(stream_src->operators[1].get())->relname().empty());
        CHECK(static_cast<const lp::node_select_t*>(stream_src->operators[2].get())->relname().t.empty());

        auto agg = lp::make_node_aggregate(resource, {}, {});
        agg->append_child(
            lp::make_node_raw_data(resource, otterstax::kafka::detail::json_to_chunk(resource, cols, payloads)));
        for (const auto& op : stream_src->operators) {
            agg->append_child(lp::node_ptr(op));
        }
        auto cursor = drive_until_ready(
            otterstax::kafka::detail::kafka_execute(addr,
                                                    resource,
                                                    lp::execution_plan_t{resource, agg, plan.value().parameters}));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        CHECK(cursor->size() == 3); // id 3,4,5 survive WHERE id > 2 (operators re-homed)
        // ...and the SELECT list survived the swap: the projected rows carry exactly
        // the two declared columns, under their names — what the stream's write-path
        // guard (chunk_matches_columns) checks before a batch is produced
        auto serialized = otterstax::kafka::detail::chunk_to_json(resource, cursor->chunks());
        REQUIRE_FALSE(serialized.has_error());
        const auto& rows = serialized.value();
        REQUIRE(rows.size() == 3);
        CHECK(rows[0] == R"({"id":3,"name":"name_3"})");
        CHECK(rows[2] == R"({"id":5,"name":"name_5"})");
    }
}

TEST_CASE("kafka stream node-swap: the ORDER BY / LIMIT / HAVING tail is re-homed too") {
    namespace lp = components::logical_plan;
    namespace ty = components::types;

    std::filesystem::remove_all("/tmp/otterstax_kafka_swap_tail");
    auto cfg = make_create_config("/tmp/otterstax_kafka_swap_tail");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    // kafka.s has to exist for `FROM kafka.s` to resolve at parse time
    {
        auto src = parse_kafka(
            parser,
            "CREATE SOURCE s (id INT, name VARCHAR) WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');");
        REQUIRE(src);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    const std::vector<otterstax::kafka::kafka_column_t> cols{
        {"id", ty::complex_logical_type(ty::logical_type::INTEGER)},
        {"name", ty::complex_logical_type(ty::logical_type::STRING_LITERAL)}};

    struct swap_result_t {
        std::vector<lp::node_type> operators;
        std::vector<std::string> resolved_tables;
        std::vector<std::string> rows;
    };

    // Compile `sql`, swap its operators over ONE batch of `payloads`, and run that plan.
    // `resolved_tables` is what the engine looks up in the catalog for the swapped plan:
    // the executor runs register_plan_targets on EVERY execute, and target_names_of names
    // a sort_t / limit_t / having_t exactly like a match/group/select. The batch's rows
    // come from the raw_data child, so a table named anywhere in this plan is a per-batch
    // resolve of a relation the plan never reads
    auto swap_and_run = [&](const std::string& sql, const std::vector<std::string>& payloads) {
        auto plan = otterstax::kafka::detail::kafka_parse_plan(resource, sql);
        REQUIRE_FALSE(plan.has_error());
        REQUIRE_FALSE(plan.value().sub_queries.empty());
        auto stream_src = otterstax::kafka::kafka_stream_source(resource, plan.value().sub_queries.back());
        REQUIRE(stream_src);
        CHECK(stream_src->source_relname == "s");

        swap_result_t out;
        auto agg = lp::make_node_aggregate(resource, {}, {});
        agg->append_child(
            lp::make_node_raw_data(resource, otterstax::kafka::detail::json_to_chunk(resource, cols, payloads)));
        for (const auto& op : stream_src->operators) {
            out.operators.push_back(op->type());
            agg->append_child(lp::node_ptr(op));
        }

        lp::catalog_resolves_t resolves;
        services::catalog_resolve::register_plan_targets(resource, agg.get(), &resolves);
        if (resolves.tables) {
            for (const auto& entry : resolves.tables->entries()) {
                out.resolved_tables.push_back(entry.dbname + "." + entry.relname);
            }
        }

        auto cursor = drive_until_ready(
            otterstax::kafka::detail::kafka_execute(addr,
                                                    resource,
                                                    lp::execution_plan_t{resource, agg, plan.value().parameters}));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        auto serialized = otterstax::kafka::detail::chunk_to_json(resource, cursor->chunks());
        REQUIRE_FALSE(serialized.has_error());
        out.rows = std::move(serialized.value());
        return out;
    };

    // ORDER BY: the sort rides along re-homed, and still orders the batch it is swapped over
    {
        auto out = swap_and_run("SELECT id, name FROM kafka.s ORDER BY id DESC;", id_name_payloads(1, 5));
        REQUIRE(out.operators.size() == 3);
        CHECK(out.operators[0] == lp::node_type::group_t);
        CHECK(out.operators[1] == lp::node_type::sort_t);
        CHECK(out.operators[2] == lp::node_type::select_t);
        CAPTURE(out.resolved_tables);
        CHECK(out.resolved_tables.empty());
        REQUIRE(out.rows.size() == 5);
        CHECK(out.rows[0] == R"({"id":5,"name":"n5"})");
        CHECK(out.rows[4] == R"({"id":1,"name":"n1"})");
    }

    // LIMIT: the window is the node's own, so a re-homed limit still cuts the batch
    {
        auto out = swap_and_run("SELECT id, name FROM kafka.s LIMIT 2;", id_name_payloads(1, 5));
        REQUIRE(out.operators.size() == 3);
        CHECK(out.operators[0] == lp::node_type::group_t);
        CHECK(out.operators[1] == lp::node_type::select_t);
        CHECK(out.operators[2] == lp::node_type::limit_t);
        CAPTURE(out.resolved_tables);
        CHECK(out.resolved_tables.empty());
        REQUIRE(out.rows.size() == 2);
        CHECK(out.rows[0] == R"({"id":1,"name":"n1"})");
        CHECK(out.rows[1] == R"({"id":2,"name":"n2"})");
    }

    // HAVING: the predicate is the node's own expression list, and the group it filters
    // is the one re-homed beside it — the pair keeps reducing and filtering the batch.
    // The aggregate the predicate reads is a hidden helper the transformer appends to the
    // group (the SELECT list names none of its own), so the surviving row must carry the
    // projected column ALONE: a helper that leaked into the output would show up here
    {
        const std::vector<std::string> payloads{R"({"id":1,"name":"a"})",
                                                R"({"id":2,"name":"a"})",
                                                R"({"id":3,"name":"b"})"};
        auto out = swap_and_run("SELECT name FROM kafka.s GROUP BY name HAVING COUNT(*) > 1;", payloads);
        REQUIRE(out.operators.size() == 3);
        CHECK(out.operators[0] == lp::node_type::group_t);
        CHECK(out.operators[1] == lp::node_type::having_t);
        CHECK(out.operators[2] == lp::node_type::select_t);
        CAPTURE(out.resolved_tables);
        CHECK(out.resolved_tables.empty());
        REQUIRE(out.rows.size() == 1); // 'b' has a single row and does not survive HAVING
        CHECK(out.rows[0] == R"({"name":"a"})");
    }
}

TEST_CASE("kafka txn: BEGIN/INSERT/INSERT/COMMIT on one session is atomic; ROLLBACK reverts") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_txn");
    auto cfg = make_create_config("/tmp/otterstax_kafka_txn");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    // CREATE SOURCE s -> kafka.s (id, name) + kafka.s__offsets (partition_id, committed_offset)
    {
        auto src = parse_kafka(parser,
                               "CREATE SOURCE s (id INT, name VARCHAR) "
                               "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');");
        REQUIRE(src);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    namespace ses = components::session;
    auto on_session = [&](ses::session_id_t s, const std::string& sql) {
        auto cur = drive_until_ready(otterstax::kafka::detail::kafka_query_session(addr, resource, s, sql));
        REQUIRE(cur);
        return cur;
    };
    // Row count seen by a FRESH session (its own snapshot / autocommit).
    auto count = [&](const std::string& qualified) {
        auto cur = drive_until_ready(otterstax::kafka::detail::kafka_query(engine->engine_dispatcher_address(),
                                                                           resource,
                                                                           "SELECT * FROM " + qualified + ";"));
        REQUIRE(cur);
        REQUIRE_FALSE(cur->is_error());
        return cur->size();
    };

    // COMMIT: data + offset land together, invisible until COMMIT
    {
        auto txn = ses::session_id_t::generate_uid();
        REQUIRE_FALSE(on_session(txn, "BEGIN;")->is_error());
        REQUIRE_FALSE(on_session(txn, "INSERT INTO kafka.s (id, name) VALUES (1, 'a');")->is_error());
        REQUIRE_FALSE(on_session(txn, "INSERT INTO kafka.s__offsets (partition_id, committed_offset) VALUES (0, 100);")
                          ->is_error());

        // Mid-txn: a fresh session must NOT see the uncommitted rows — proves the
        // session txn genuinely spans the calls (not per-statement autocommit)
        CHECK(count("kafka.s") == 0);
        CHECK(count("kafka.s__offsets") == 0);

        REQUIRE_FALSE(on_session(txn, "COMMIT;")->is_error());

        // After COMMIT both inserts are visible — committed atomically
        CHECK(count("kafka.s") == 1);
        CHECK(count("kafka.s__offsets") == 1);
    }

    // ROLLBACK: an aborted batch leaves the tables unchanged
    {
        auto txn = ses::session_id_t::generate_uid();
        REQUIRE_FALSE(on_session(txn, "BEGIN;")->is_error());
        REQUIRE_FALSE(on_session(txn, "INSERT INTO kafka.s (id, name) VALUES (2, 'b');")->is_error());
        REQUIRE_FALSE(on_session(txn, "ROLLBACK;")->is_error());

        CHECK(count("kafka.s") == 1); // still just the one committed row
    }
}

TEST_CASE("kafka eos: node-based insert+offsets on one session are atomic; ROLLBACK reverts") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_eos");
    auto cfg = make_create_config("/tmp/otterstax_kafka_eos");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, addr);

    auto src = parse_kafka(parser,
                           "CREATE SOURCE s (id INT, name VARCHAR) "
                           "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');");
    REQUIRE(src);
    auto columns = src->columns(); // copy before the node is moved into the manager
    {
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    namespace ses = components::session;
    namespace kd = otterstax::kafka::detail;
    auto on_session = [&](ses::session_id_t s, const std::string& sql) {
        auto cur = drive_until_ready(kd::kafka_query_session(addr, resource, s, sql));
        REQUIRE(cur);
        return cur;
    };
    auto count = [&](const std::string& qualified) {
        auto cur = drive_until_ready(kd::kafka_query(addr, resource, "SELECT * FROM " + qualified + ";"));
        REQUIRE(cur);
        REQUIRE_FALSE(cur->is_error());
        return cur->size();
    };

    // COMMIT: data (node_insert) + offsets land together, invisible until COMMIT
    {
        auto txn = ses::session_id_t::generate_uid();
        REQUIRE_FALSE(on_session(txn, "BEGIN;")->is_error());

        auto chunk = kd::json_to_chunk(resource, columns, {R"({"id": 1, "name": "a"})", R"({"id": 2, "name": "b"})"});
        REQUIRE(chunk.size() == 2);
        REQUIRE_FALSE(drive_until_ready(kd::kafka_insert_session(addr, resource, txn, "kafka", "s", std::move(chunk)))
                          ->is_error());
        REQUIRE_FALSE(drive_until_ready(
                          kd::write_offsets_session(addr, resource, txn, "kafka", "s__offsets", {{0, 100}, {1, 250}}))
                          ->is_error());

        // Mid-txn: a fresh session must NOT see the uncommitted rows — proves the
        // node-based helpers genuinely run inside the session txn.
        CHECK(count("kafka.s") == 0);
        CHECK(count("kafka.s__offsets") == 0);

        REQUIRE_FALSE(on_session(txn, "COMMIT;")->is_error());

        CHECK(count("kafka.s") == 2);
        CHECK(count("kafka.s__offsets") == 2); // one row per partition
    }

    // ROLLBACK: an aborted batch leaves both tables unchanged
    {
        auto txn = ses::session_id_t::generate_uid();
        REQUIRE_FALSE(on_session(txn, "BEGIN;")->is_error());

        auto chunk = kd::json_to_chunk(resource, columns, {R"({"id": 3, "name": "c"})"});
        REQUIRE(chunk.size() == 1);
        REQUIRE_FALSE(drive_until_ready(kd::kafka_insert_session(addr, resource, txn, "kafka", "s", std::move(chunk)))
                          ->is_error());
        REQUIRE_FALSE(
            drive_until_ready(kd::write_offsets_session(addr, resource, txn, "kafka", "s__offsets", {{0, 300}}))
                ->is_error());

        REQUIRE_FALSE(on_session(txn, "ROLLBACK;")->is_error());

        CHECK(count("kafka.s") == 2);          // still the two committed rows
        CHECK(count("kafka.s__offsets") == 2); // offsets unchanged
    }
}

TEST_CASE("kafka schema guard: chunk_matches_columns accepts a matching chunk, rejects mismatches") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;
    using components::types::complex_logical_type;
    using components::types::logical_type;
    namespace k = otterstax::kafka;

    std::vector<k::kafka_column_t> declared = {{"id", complex_logical_type(logical_type::INTEGER)},
                                               {"name", complex_logical_type(logical_type::STRING_LITERAL)}};

    // Matching: built from the declared columns -> every row round-trips
    {
        std::pmr::vector<components::vector::data_chunk_t> chunks(resource);

        chunks.push_back(
            k::detail::json_to_chunk(resource, declared, {R"({"id":1,"name":"a"})", R"({"id":2,"name":"b"})"}));

        REQUIRE(chunks.front().size() == 2);

        CHECK(k::detail::chunk_matches_columns(resource, chunks, declared));
    }
    // Wrong type, same names + count: id holds a string -> won't re-ingest as INT
    {
        std::vector<k::kafka_column_t> wrong = {{"id", complex_logical_type(logical_type::STRING_LITERAL)},
                                                {"name", complex_logical_type(logical_type::STRING_LITERAL)}};
        std::pmr::vector<components::vector::data_chunk_t> chunks(resource);

        chunks.push_back(k::detail::json_to_chunk(resource, wrong, {R"({"id":"x","name":"a"})"}));

        REQUIRE(chunks.front().size() == 1);

        CHECK_FALSE(k::detail::chunk_matches_columns(resource, chunks, declared));
    }
    // Wrong column count: only id present.
    {
        std::vector<k::kafka_column_t> partial = {{"id", complex_logical_type(logical_type::INTEGER)}};
        std::pmr::vector<components::vector::data_chunk_t> chunks(resource);

        chunks.push_back(k::detail::json_to_chunk(resource, partial, {R"({"id":1})"}));

        REQUIRE(chunks.front().size() == 1);

        CHECK_FALSE(k::detail::chunk_matches_columns(resource, chunks, declared));
    }
}

TEST_CASE("kafka produce: a schema-mismatched INSERT is rejected, nothing produced") {
    namespace lp = components::logical_plan;
    namespace ty = components::types;

    std::filesystem::remove_all("/tmp/otterstax_kafka_badinsert");
    auto cfg = make_create_config("/tmp/otterstax_kafka_badinsert");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    // CREATE SOURCE s (id INT, name VARCHAR) -> registry carries the declared columns
    {
        auto src = parse_kafka(parser,
                               "CREATE SOURCE s (id INT, name VARCHAR) "
                               "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');");
        REQUIRE(src);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    auto produce = [&](lp::node_ptr source) {
        auto [_, fut] = actor_zeta::send(kafka_mgr->address(),
                                         &KafkaManager::produce,
                                         session_hash_t{2},
                                         std::string{"s"},
                                         std::move(source));
        return drive_until_ready(std::move(fut));
    };

    // Wrong type: id is a string -> can't re-ingest as INT
    {
        std::vector<otterstax::kafka::kafka_column_t> wrong{
            {"id", ty::complex_logical_type(ty::logical_type::STRING_LITERAL)},
            {"name", ty::complex_logical_type(ty::logical_type::STRING_LITERAL)}};
        auto bad = otterstax::kafka::detail::json_to_chunk(resource, wrong, {R"({"id":"x","name":"a"})"});
        REQUIRE(bad.size() == 1);
        auto cur = produce(lp::make_node_raw_data(resource, std::move(bad)));
        REQUIRE(cur);
        CHECK(cur->is_error()); // rejected before producing -> no broker needed
    }

    // Wrong column set: only id present
    {
        std::vector<otterstax::kafka::kafka_column_t> partial{
            {"id", ty::complex_logical_type(ty::logical_type::INTEGER)}};
        auto bad = otterstax::kafka::detail::json_to_chunk(resource, partial, {R"({"id":1})"});
        REQUIRE(bad.size() == 1);
        auto cur = produce(lp::make_node_raw_data(resource, std::move(bad)));
        REQUIRE(cur);
        CHECK(cur->is_error());
    }
}

TEST_CASE("kafka stream write: SELECT output schema is derived; INSERT routes to the stream") {
    namespace ty = components::types;
    std::filesystem::remove_all("/tmp/otterstax_kafka_streamwrite");
    auto cfg = make_create_config("/tmp/otterstax_kafka_streamwrite");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    auto run = [&](kafka_node_ptr node) {
        REQUIRE(node);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(node));
        auto cur = drive_until_ready(std::move(fut));
        REQUIRE(cur);
        REQUIRE_FALSE(cur->is_error());
    };
    run(parse_kafka(
        parser,
        "CREATE SOURCE s (id INT, name VARCHAR) WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');"));
    // create_stream probes this SELECT for its schema; the stream registering without
    // error means the probe resolved and its columns are populated
    run(parse_kafka(parser,
                    "CREATE STREAM st WITH (KAFKA_TOPIC='ot', VALUE_FORMAT='JSON', "
                    "BOOTSTRAP_SERVERS='localhost:9092') AS SELECT id, name FROM kafka.s;"));

    // (a) Output-schema computation: apply the SELECT's projection to the source's
    // declared schema, no data (schema_utils::aggregate_filter_schema via
    // stream_output_schema). Covers projection, single-column, and SELECT *
    {
        const std::vector<otterstax::kafka::kafka_column_t> source_cols{
            {"id", ty::complex_logical_type(ty::logical_type::INTEGER)},
            {"name", ty::complex_logical_type(ty::logical_type::STRING_LITERAL)}};
        auto schema_of = [&](const std::string& sql) {
            auto plan = otterstax::kafka::detail::kafka_parse_plan(resource, sql);
            REQUIRE_FALSE(plan.has_error());
            REQUIRE_FALSE(plan.value().sub_queries.empty());
            const auto* agg = otterstax::kafka::kafka_find_aggregate(plan.value().sub_queries.back());
            REQUIRE(agg != nullptr);
            return otterstax::kafka::detail::stream_output_schema(resource,
                                                                  *agg,
                                                                  plan.value().parameters.get(),
                                                                  source_cols);
        };

        auto proj = schema_of("SELECT id, name FROM kafka.s WHERE id > 2;");
        REQUIRE(proj.size() == 2);
        CHECK(proj[0].name == "id");
        CHECK(proj[1].name == "name");

        auto one = schema_of("SELECT id FROM kafka.s;");
        REQUIRE(one.size() == 1);
        CHECK(one[0].name == "id");

        CHECK(schema_of("SELECT * FROM kafka.s;").size() == 2); // SELECT * -> all source columns
    }

    // (b) INSERT INTO kafka.st is detected as a write to the stream (-> produce())
    {
        auto parsed = parser.parse("INSERT INTO kafka.st (id, name) VALUES (1, 'a');");
        REQUIRE_FALSE(parsed.has_error());
        auto write = otterstax::kafka::kafka_write_target(parsed.value()->otterbrix_params->node);
        REQUIRE(write.has_value());
        CHECK(write->relname == "st");
    }
}

// A COUNT in a STREAM's SELECT. The engine's count kernel answers UBIGINT
// (components/compute/kernels/aggregate.cpp:503,508 — output_type::fixed(UBIGINT),
// count_finalize at :401 writing through output.data<uint64_t>()), so the schema the
// stream is declared under has to name that type: the declared columns are what every
// produced batch is checked against and what a consumer of the topic is promised.
// The produced JSON then has to carry the number — a column type the writer had no
// case for used to be serialized as `null`, so the count reached the topic as a
// missing value with nothing logged and nothing failed.
TEST_CASE("kafka stream: a COUNT column is declared and produced as the type the engine computes") {
    namespace lp = components::logical_plan;
    namespace ty = components::types;
    namespace kd = otterstax::kafka::detail;

    std::filesystem::remove_all("/tmp/otterstax_kafka_count");
    auto cfg = make_create_config("/tmp/otterstax_kafka_count");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, addr);
    {
        auto src = parse_kafka(
            parser,
            "CREATE SOURCE s (id INT, name VARCHAR) WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');");
        REQUIRE(src);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    const std::vector<otterstax::kafka::kafka_column_t> source_cols{
        {"id", ty::complex_logical_type(ty::logical_type::INTEGER)},
        {"name", ty::complex_logical_type(ty::logical_type::STRING_LITERAL)}};
    auto plan = kd::kafka_parse_plan(resource, "SELECT name, COUNT(*) AS cnt FROM kafka.s GROUP BY name;");
    REQUIRE_FALSE(plan.has_error());
    REQUIRE_FALSE(plan.value().sub_queries.empty());
    const auto* agg = otterstax::kafka::kafka_find_aggregate(plan.value().sub_queries.back());
    REQUIRE(agg != nullptr);

    // What CREATE STREAM registers the stream's output columns as
    const auto declared = kd::stream_output_schema(resource, *agg, plan.value().parameters.get(), source_cols);
    REQUIRE(declared.size() == 2);
    REQUIRE(declared[0].name == "name");
    REQUIRE(declared[1].name == "cnt");

    // What the engine puts in the chunk, over the very node-swap the stream worker
    // runs per batch: aggregate(empty) + [raw_data(batch), <re-homed operators>]
    auto stream_src = otterstax::kafka::kafka_stream_source(resource, plan.value().sub_queries.back());
    REQUIRE(stream_src);
    auto swapped = lp::make_node_aggregate(resource, {}, {});
    swapped->append_child(lp::make_node_raw_data(
        resource,
        kd::json_to_chunk(
            resource,
            source_cols,
            {R"({"id":1,"name":"a"})", R"({"id":2,"name":"a"})", R"({"id":3,"name":"b"})"})));
    for (const auto& op : stream_src->operators) {
        swapped->append_child(lp::node_ptr(op));
    }
    auto cursor = drive_until_ready(
        kd::kafka_execute(addr, resource, lp::execution_plan_t{resource, swapped, plan.value().parameters}));
    REQUIRE(cursor);
    INFO("transform: " << (cursor->is_error() ? cursor->get_error().what.c_str() : "ok"));
    REQUIRE_FALSE(cursor->is_error());
    REQUIRE(cursor->chunks().size() == 1);
    const auto& chunk = cursor->chunks().front();
    REQUIRE(chunk.column_count() == 2);
    REQUIRE(chunk.size() == 2); // one row per distinct name

    // The declared type IS the executed type, column by column: the schema the
    // stream promises and the data it carries are one answer, not two
    for (std::uint64_t col = 0; col < chunk.column_count(); ++col) {
        INFO("column " << declared[col].name);
        CHECK(declared[col].type.type() == chunk.data[col].type().type());
    }
    CHECK(declared[1].type.type() == ty::logical_type::UBIGINT);

    // ...and the count reaches the topic as its number
    auto serialized = kd::chunk_to_json(resource, cursor->chunks());
    REQUIRE_FALSE(serialized.has_error());
    const auto& rows = serialized.value();
    REQUIRE(rows.size() == 2);
    // GROUP BY fixes no row order, so the two objects are matched as a set
    CHECK(std::count(rows.begin(), rows.end(), std::string{R"({"name":"a","cnt":2})"}) == 1);
    CHECK(std::count(rows.begin(), rows.end(), std::string{R"({"name":"b","cnt":1})"}) == 1);
    // ...and the write-path guard accepts what the transform produced: the rows
    // round-trip through the declared columns, which is what lets them be published
    CHECK(kd::chunk_matches_columns(resource, cursor->chunks(), declared));
}

TEST_CASE("kafka json: out-of-range numbers are rejected without throwing, JSON null becomes NULL") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    std::vector<otterstax::kafka::kafka_column_t> columns = {
        {"id", complex_logical_type(logical_type::INTEGER)},
        {"big", complex_logical_type(logical_type::BIGINT)},
        {"name", complex_logical_type(logical_type::STRING_LITERAL)},
    };

    auto chunk = otterstax::kafka::detail::json_to_chunk(
        resource,
        columns,
        {
            R"({"id": 1, "big": 9223372036854775807, "name": "a"})",           // valid (INT64_MAX)
            R"({"id": 3000000000, "big": 1, "name": "b"})",                    // id above int32 -> dropped
            R"({"id": 1, "big": 18446744073709551615, "name": "c"})",          // uint64 > INT64_MAX -> dropped
            R"({"id": -2147483648, "big": -9223372036854775808, "name": "d"})", // valid (lower bounds)
            R"({"id": 1.5, "big": 1, "name": "e"})",                           // fractional into INTEGER -> dropped
            R"({"id": null, "big": 5, "name": null})",                         // JSON null -> SQL NULL, row kept
        });

    REQUIRE(chunk.column_count() == 3);
    REQUIRE(chunk.size() == 3);
    CHECK(chunk.value(1, 0).value<int64_t>() == std::numeric_limits<int64_t>::max());
    CHECK(chunk.value(0, 1).value<int32_t>() == std::numeric_limits<int32_t>::min());
    CHECK(chunk.value(1, 1).value<int64_t>() == std::numeric_limits<int64_t>::min());
    CHECK(chunk.is_null(0, 2));
    CHECK_FALSE(chunk.is_null(1, 2));
    CHECK(chunk.value(1, 2).value<int64_t>() == 5);
    CHECK(chunk.is_null(2, 2));
}

TEST_CASE("kafka schema guard: an alias-less chunk and a mismatching second chunk are rejected") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;
    using components::types::complex_logical_type;
    using components::types::logical_type;
    namespace k = otterstax::kafka;

    std::vector<k::kafka_column_t> declared = {{"id", complex_logical_type(logical_type::INTEGER)},
                                               {"name", complex_logical_type(logical_type::STRING_LITERAL)}};

    // Two chunks: the first round-trips, the second holds a string in `id`
    {
        std::vector<k::kafka_column_t> wrong = {{"id", complex_logical_type(logical_type::STRING_LITERAL)},
                                                {"name", complex_logical_type(logical_type::STRING_LITERAL)}};
        std::pmr::vector<components::vector::data_chunk_t> chunks(resource);
        chunks.push_back(k::detail::json_to_chunk(resource, declared, {R"({"id":1,"name":"a"})"}));
        chunks.push_back(k::detail::json_to_chunk(resource, wrong, {R"({"id":"x","name":"b"})"}));
        REQUIRE(chunks[0].size() == 1);
        REQUIRE(chunks[1].size() == 1);
        CHECK_FALSE(k::detail::chunk_matches_columns(resource, chunks, declared));
    }
    // Right types and count, but the columns carry no alias: the serialized keys
    // cannot name a declared column, so nothing round-trips
    {
        std::pmr::vector<complex_logical_type> types(resource);
        types.emplace_back(logical_type::INTEGER);
        types.emplace_back(logical_type::STRING_LITERAL);
        components::vector::data_chunk_t bare(resource, types, 1);
        bare.set_value(0, 0, components::types::logical_value_t(resource, int32_t{1}));
        bare.set_value(1, 0, components::types::logical_value_t(resource, std::string{"a"}));
        bare.set_cardinality(1);
        std::pmr::vector<components::vector::data_chunk_t> chunks(resource);
        chunks.push_back(std::move(bare));
        CHECK_FALSE(k::detail::chunk_matches_columns(resource, chunks, declared));
    }
}

TEST_CASE("kafka json: chunk_to_json over more than 1024 rows keeps every row in order") {
    std::pmr::monotonic_buffer_resource arena(std::pmr::new_delete_resource());
    auto* resource = &arena;
    using components::types::complex_logical_type;
    using components::types::logical_type;
    namespace k = otterstax::kafka;

    std::vector<k::kafka_column_t> columns = {{"id", complex_logical_type(logical_type::BIGINT)},
                                              {"name", complex_logical_type(logical_type::STRING_LITERAL)}};
    // A cursor batch: a full 1024-row chunk followed by a 500-row tail
    std::pmr::vector<components::vector::data_chunk_t> chunks(resource);
    chunks.push_back(k::detail::json_to_chunk(resource, columns, id_name_payloads(0, 1024)));
    chunks.push_back(k::detail::json_to_chunk(resource, columns, id_name_payloads(1024, 500)));
    REQUIRE(chunks[0].size() == 1024);
    REQUIRE(chunks[1].size() == 500);

    auto serialized = k::detail::chunk_to_json(resource, chunks);
    REQUIRE_FALSE(serialized.has_error());
    const auto& out = serialized.value();
    REQUIRE(out.size() == 1524);
    for (std::size_t i = 0; i < out.size(); ++i) {
        const std::string expected_id = "\"id\":" + std::to_string(i) + ",";
        REQUIRE(out[i].find(expected_id) != std::string::npos);
    }
    CHECK(k::detail::chunk_matches_columns(resource, chunks, columns));
}

TEST_CASE("kafka offsets: parse_offsets spans a result of more than 1024 rows") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_offsets_multi");
    auto cfg = make_create_config("/tmp/otterstax_kafka_offsets_multi");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, addr);
    {
        auto src = parse_kafka(parser,
                               "CREATE SOURCE s (id INT, name VARCHAR) "
                               "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');");
        REQUIRE(src);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }

    namespace kd = otterstax::kafka::detail;
    auto write = [&](int32_t first_partition, int32_t count, int64_t offset) {
        std::map<int32_t, int64_t> offsets;
        for (int32_t p = first_partition; p < first_partition + count; ++p) {
            offsets[p] = offset;
        }
        auto cur = drive_until_ready(kd::write_offsets(addr, resource, "kafka", "s__offsets", offsets));
        REQUIRE(cur);
        REQUIRE_FALSE(cur->is_error());
    };
    // 1800 rows over 1200 partitions: partitions 0..599 appear twice, the later
    // (higher) offset must win even though it lives in a later chunk
    write(0, 600, 10);
    write(600, 600, 20);
    write(0, 600, 30);

    auto rows = drive_until_ready(
        kd::kafka_query(addr, resource, "SELECT partition_id, committed_offset FROM kafka.s__offsets;"));
    REQUIRE(rows);
    REQUIRE_FALSE(rows->is_error());
    REQUIRE(rows->size() == 1800);
    REQUIRE(rows->chunks().size() > 1);

    auto offsets = kd::parse_offsets(rows);
    REQUIRE(offsets.size() == 1200);
    CHECK(offsets.at(0) == 30);
    CHECK(offsets.at(599) == 30);
    CHECK(offsets.at(600) == 20);
    CHECK(offsets.at(1199) == 20);
}

TEST_CASE("kafka create: an invalid TRANSACTIONAL option is rejected before any side effect") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_validate");
    auto cfg = make_create_config("/tmp/otterstax_kafka_validate");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, addr);

    auto run_kafka = [&](kafka_node_ptr node) {
        REQUIRE(node);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(node));
        return drive_until_ready(std::move(fut));
    };
    auto probe = [&](const std::string& qualified) {
        return drive_until_ready(
            otterstax::kafka::detail::kafka_query(addr, resource, "SELECT * FROM " + qualified + ";"));
    };
    auto sources_has = [&](const std::string& name) {
        auto cursor = drive_until_ready(
            otterstax::kafka::detail::kafka_query(addr, resource, "SELECT name FROM kafka.__sources;"));
        if (!cursor || cursor->is_error()) {
            return false;
        }
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                if (std::string{chunk.value(0, row).value<std::string_view>()} == name) {
                    return true;
                }
            }
        }
        return false;
    };

    // SOURCE: the option is validated before the backing tables are created
    {
        auto cursor = run_kafka(parse_kafka(parser,
                                            "CREATE SOURCE bad (id INT) WITH (KAFKA_TOPIC='t', "
                                            "BOOTSTRAP_SERVERS='localhost:9092', TRANSACTIONAL='maybe');"));
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::invalid_parameter);
    }
    CHECK(probe("kafka.bad")->is_error());
    CHECK(probe("kafka.bad__offsets")->is_error());
    CHECK_FALSE(sources_has("bad"));

    // STREAM: validated before the object is registered or persisted
    REQUIRE_FALSE(run_kafka(parse_kafka(parser,
                                        "CREATE SOURCE s (id INT) "
                                        "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');"))
                      ->is_error());
    {
        auto cursor = run_kafka(parse_kafka(parser,
                                            "CREATE STREAM st WITH (KAFKA_TOPIC='ot', TRANSACTIONAL='x') "
                                            "AS SELECT id FROM kafka.s;"));
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::invalid_parameter);
    }
    CHECK_FALSE(sources_has("st"));
    {
        // Not registered: a continuous INSERT into it is an unknown-object error
        auto [_, fut] = actor_zeta::send(kafka_mgr->address(),
                                         &KafkaManager::add_stream_insert,
                                         session_hash_t{2},
                                         std::string{"st"},
                                         std::string{"INSERT INTO kafka.st SELECT id FROM kafka.s;"});
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        CHECK(std::string{cursor->get_error().what.c_str()}.find("unknown object") != std::string::npos);
    }
}

TEST_CASE("kafka create: a failed __sources write fails CREATE SOURCE and rolls its tables back") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_persist_fail");
    auto cfg = make_create_config("/tmp/otterstax_kafka_persist_fail");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    namespace kd = otterstax::kafka::detail;
    // A pre-existing kafka.__sources with the wrong shape: the manager's metadata
    // insert (8 columns) cannot land in it, so persisting the source must fail
    REQUIRE_FALSE(drive_until_ready(kd::kafka_query(addr, resource, "CREATE DATABASE kafka;"))->is_error());
    REQUIRE_FALSE(
        drive_until_ready(kd::kafka_query(addr, resource, "CREATE TABLE kafka.__sources (x INT);"))->is_error());

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, addr);
    auto probe = [&](const std::string& qualified) {
        return drive_until_ready(kd::kafka_query(addr, resource, "SELECT * FROM " + qualified + ";"));
    };

    auto src = parse_kafka(parser,
                           "CREATE SOURCE s (id INT, name VARCHAR) "
                           "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');");
    REQUIRE(src);
    {
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
    }
    // Nothing half-created survives the failure: no backing tables, no registration
    CHECK(probe("kafka.s")->is_error());
    CHECK(probe("kafka.s__offsets")->is_error());
    {
        namespace lp = components::logical_plan;
        namespace ty = components::types;
        std::vector<otterstax::kafka::kafka_column_t> cols{{"id", ty::complex_logical_type(ty::logical_type::INTEGER)}};
        auto chunk = kd::json_to_chunk(resource, cols, {R"({"id":1})"});
        auto [_, fut] = actor_zeta::send(kafka_mgr->address(),
                                         &KafkaManager::produce,
                                         session_hash_t{2},
                                         std::string{"s"},
                                         lp::node_ptr(lp::make_node_raw_data(resource, std::move(chunk))));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        CHECK(std::string{cursor->get_error().what.c_str()}.find("unknown object") != std::string::npos);
    }
}

TEST_CASE("kafka drop: DROP SOURCE removes the offsets table; a missing source errors unless IF EXISTS") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_drop");
    auto cfg = make_create_config("/tmp/otterstax_kafka_drop");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, addr);

    auto run_kafka = [&](kafka_node_ptr node) {
        REQUIRE(node);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(node));
        return drive_until_ready(std::move(fut));
    };
    auto probe = [&](const std::string& qualified) {
        return drive_until_ready(
            otterstax::kafka::detail::kafka_query(addr, resource, "SELECT * FROM " + qualified + ";"));
    };

    REQUIRE_FALSE(run_kafka(parse_kafka(parser,
                                        "CREATE SOURCE s (id INT, name VARCHAR) "
                                        "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');"))
                      ->is_error());
    REQUIRE_FALSE(probe("kafka.s")->is_error());
    REQUIRE_FALSE(probe("kafka.s__offsets")->is_error());

    REQUIRE_FALSE(run_kafka(parse_kafka(parser, "DROP SOURCE s;"))->is_error());
    CHECK(probe("kafka.s")->is_error());
    CHECK(probe("kafka.s__offsets")->is_error()); // the offsets table goes with its source

    // Dropped -> unknown to the write path
    {
        namespace lp = components::logical_plan;
        namespace ty = components::types;
        std::vector<otterstax::kafka::kafka_column_t> cols{
            {"id", ty::complex_logical_type(ty::logical_type::INTEGER)},
            {"name", ty::complex_logical_type(ty::logical_type::STRING_LITERAL)}};
        auto chunk = otterstax::kafka::detail::json_to_chunk(resource, cols, {R"({"id":1,"name":"a"})"});
        auto [_, fut] = actor_zeta::send(kafka_mgr->address(),
                                         &KafkaManager::produce,
                                         session_hash_t{2},
                                         std::string{"s"},
                                         lp::node_ptr(lp::make_node_raw_data(resource, std::move(chunk))));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        CHECK(std::string{cursor->get_error().what.c_str()}.find("unknown object") != std::string::npos);
    }

    // A second DROP of the same name is an error; IF EXISTS makes it a no-op
    {
        auto cursor = run_kafka(parse_kafka(parser, "DROP SOURCE s;"));
        REQUIRE(cursor);
        CHECK(cursor->is_error());
    }
    {
        auto cursor = run_kafka(parse_kafka(parser, "DROP SOURCE IF EXISTS s;"));
        REQUIRE(cursor);
        CHECK_FALSE(cursor->is_error());
    }
}

TEST_CASE("kafka poller: an at-least-once batch whose insert fails reports the engine error") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_alo");
    auto cfg = make_create_config("/tmp/otterstax_kafka_alo");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, addr);
    auto src = parse_kafka(parser,
                           "CREATE SOURCE s (id INT, name VARCHAR) "
                           "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');");
    REQUIRE(src);
    auto columns = src->columns();
    {
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        REQUIRE_FALSE(drive_until_ready(std::move(fut))->is_error());
    }

    namespace kd = otterstax::kafka::detail;
    auto payloads = std::vector<std::string>{R"({"id": 1, "name": "a"})", R"({"id": 2, "name": "b"})"};
    // The batch's table is gone: the error must reach the caller (it decides not
    // to commit the consumer positions), not vanish
    {
        auto error = kd::ingest_at_least_once(addr,
                                              resource,
                                              "kafka",
                                              "missing",
                                              kd::json_to_chunk(resource, columns, payloads));
        CHECK(error.contains_error());
    }
    // The same batch into the real table lands
    {
        auto error =
            kd::ingest_at_least_once(addr, resource, "kafka", "s", kd::json_to_chunk(resource, columns, payloads));
        CHECK_FALSE(error.contains_error());
    }
    auto rows = drive_until_ready(kd::kafka_query(addr, resource, "SELECT * FROM kafka.s;"));
    REQUIRE(rows);
    REQUIRE_FALSE(rows->is_error());
    CHECK(rows->size() == 2);
}

TEST_CASE("kafka poller: a failed exactly-once batch is rolled back and the session stays usable") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_eos_fail");
    auto cfg = make_create_config("/tmp/otterstax_kafka_eos_fail");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, addr);
    auto src = parse_kafka(parser,
                           "CREATE SOURCE s (id INT, name VARCHAR) "
                           "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092', TRANSACTIONAL='true');");
    REQUIRE(src);
    auto columns = src->columns();
    {
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(src));
        REQUIRE_FALSE(drive_until_ready(std::move(fut))->is_error());
    }

    namespace kd = otterstax::kafka::detail;
    namespace ses = components::session;
    auto count = [&](const std::string& qualified) {
        auto cur = drive_until_ready(kd::kafka_query(addr, resource, "SELECT * FROM " + qualified + ";"));
        REQUIRE(cur);
        REQUIRE_FALSE(cur->is_error());
        return cur->size();
    };
    auto payloads = std::vector<std::string>{R"({"id": 1, "name": "a"})"};

    // An insert into a missing table inside BEGIN fails, ROLLBACK succeeds, and the
    // same session runs the next transaction to COMMIT
    {
        auto txn = ses::session_id_t::generate_uid();
        REQUIRE_FALSE(drive_until_ready(kd::kafka_query_session(addr, resource, txn, "BEGIN;"))->is_error());
        auto failed = drive_until_ready(kd::kafka_insert_session(addr,
                                                                 resource,
                                                                 txn,
                                                                 "kafka",
                                                                 "missing",
                                                                 kd::json_to_chunk(resource, columns, payloads)));
        REQUIRE(failed);
        CHECK(failed->is_error());
        REQUIRE_FALSE(drive_until_ready(kd::kafka_query_session(addr, resource, txn, "ROLLBACK;"))->is_error());

        REQUIRE_FALSE(drive_until_ready(kd::kafka_query_session(addr, resource, txn, "BEGIN;"))->is_error());
        REQUIRE_FALSE(drive_until_ready(kd::kafka_insert_session(addr,
                                                                 resource,
                                                                 txn,
                                                                 "kafka",
                                                                 "s",
                                                                 kd::json_to_chunk(resource, columns, payloads)))
                          ->is_error());
        REQUIRE_FALSE(drive_until_ready(kd::kafka_query_session(addr, resource, txn, "COMMIT;"))->is_error());
        CHECK(count("kafka.s") == 1);
    }

    // The poller's batch step: a failing batch returns the engine error and leaves
    // both tables untouched; a good batch lands data + offsets atomically
    {
        auto error = kd::ingest_transactional(addr,
                                              resource,
                                              "kafka",
                                              "missing",
                                              {{0, 10}},
                                              kd::json_to_chunk(resource, columns, payloads));
        CHECK(error.contains_error());
        CHECK(count("kafka.s") == 1);
        CHECK(count("kafka.s__offsets") == 0);
    }
    {
        auto error = kd::ingest_transactional(addr,
                                              resource,
                                              "kafka",
                                              "s",
                                              {{0, 10}},
                                              kd::json_to_chunk(resource, columns, payloads));
        CHECK_FALSE(error.contains_error());
        CHECK(count("kafka.s") == 2);
        CHECK(count("kafka.s__offsets") == 1);
    }
}

TEST_CASE("kafka recover: __sources rows past the first chunk are replayed, a missing backing table is skipped") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_recover_multi");
    auto cfg = make_create_config("/tmp/otterstax_kafka_recover_multi");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();
    namespace kd = otterstax::kafka::detail;
    namespace lp = components::logical_plan;
    namespace ty = components::types;

    GreenplumParser parser(resource);
    // The registry in this process: creates the objects and persists their rows
    auto creator = actor_zeta::spawn<KafkaManager>(resource, addr);
    auto create_source = [&](const std::string& name) {
        auto node = parse_kafka(parser,
                                "CREATE SOURCE " + name +
                                    " (id INT, name VARCHAR) "
                                    "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='127.0.0.1:9092');");
        REQUIRE(node);
        auto [_, fut] =
            actor_zeta::send(creator->address(), &KafkaManager::execute, session_hash_t{1}, std::move(node));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    };
    create_source("first");

    // 1030 persisted sources whose backing tables never existed, so the real
    // source created afterwards sits beyond the first 1024-row chunk of __sources
    auto ghost_rows = [&](std::size_t first, std::size_t count) {
        std::pmr::vector<ty::complex_logical_type> col_types(resource);
        for (const char* alias :
             {"name", "kind", "topic", "bootstrap", "group_id", "offset_reset", "transactional", "as_select"}) {
            ty::complex_logical_type t(ty::logical_type::STRING_LITERAL);
            t.set_alias(alias);
            col_types.push_back(std::move(t));
        }
        components::vector::data_chunk_t chunk(resource, col_types, count);
        for (std::size_t row = 0; row < count; ++row) {
            const std::string values[8] = {"ghost_" + std::to_string(first + row),
                                           "source",
                                           "t",
                                           "127.0.0.1:9092",
                                           "g",
                                           "earliest",
                                           "false",
                                           ""};
            for (std::size_t col = 0; col < 8; ++col) {
                chunk.set_value(col, row, ty::logical_value_t(resource, values[col]));
            }
        }
        chunk.set_cardinality(count);
        auto cursor = drive_until_ready(kd::kafka_insert(addr, resource, "kafka", "__sources", std::move(chunk)));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    };
    ghost_rows(0, 515);
    ghost_rows(515, 515);
    create_source("last");
    {
        auto rows = drive_until_ready(kd::kafka_query(addr, resource, "SELECT name FROM kafka.__sources;"));
        REQUIRE(rows);
        REQUIRE_FALSE(rows->is_error());
        REQUIRE(rows->size() == 1032);
        REQUIRE(rows->chunks().size() > 1);
    }

    // A restarted manager: nothing in its registry until recover() replays __sources
    auto restarted = actor_zeta::spawn<KafkaManager>(resource, addr, /*start_pollers=*/true);
    auto produce_mismatch = [&](const std::string& name) {
        std::vector<otterstax::kafka::kafka_column_t> partial{
            {"id", ty::complex_logical_type(ty::logical_type::INTEGER)}};
        auto chunk = kd::json_to_chunk(resource, partial, {R"({"id":1})"});
        auto [_, fut] = actor_zeta::send(restarted->address(),
                                         &KafkaManager::produce,
                                         session_hash_t{2},
                                         std::string{name},
                                         lp::node_ptr(lp::make_node_raw_data(resource, std::move(chunk))));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        return std::string{cursor->get_error().what.c_str()};
    };
    CHECK(produce_mismatch("last").find("unknown object") != std::string::npos);

    restarted->recover();

    // Both real sources are registered again (a produce reaches the schema guard),
    // the ghosts were skipped, and the object past row 1024 was not lost
    CHECK(produce_mismatch("first").find("does not match its declared schema") != std::string::npos);
    CHECK(produce_mismatch("last").find("does not match its declared schema") != std::string::npos);
    CHECK(produce_mismatch("ghost_1029").find("unknown object") != std::string::npos);
}

TEST_CASE("kafka recover: a source's columns are read from the catalog, for an empty and a populated table") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_recover_columns");
    auto cfg = make_create_config("/tmp/otterstax_kafka_recover_columns");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();
    namespace kd = otterstax::kafka::detail;
    namespace lp = components::logical_plan;
    namespace ty = components::types;
    const std::string kafka_db{otterstax::kafka::KAFKA_DATABASE_NAME};

    GreenplumParser parser(resource);
    auto run_kafka = [&](auto& manager, const std::string& sql) {
        auto node = parse_kafka(parser, sql);
        REQUIRE(node);
        auto [_, fut] =
            actor_zeta::send(manager->address(), &KafkaManager::execute, session_hash_t{1}, std::move(node));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        return cursor;
    };
    const std::string with = " WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='127.0.0.1:9092');";

    // The registry of the previous process: two sources of one shape, one left
    // empty and one filled, so the recovered schema cannot come from row data
    auto creator = actor_zeta::spawn<KafkaManager>(resource, addr);
    REQUIRE_FALSE(run_kafka(creator, "CREATE SOURCE empty_src (id INT, name VARCHAR, w DOUBLE)" + with)->is_error());
    auto full = parse_kafka(parser, "CREATE SOURCE full_src (id INT, name VARCHAR, w DOUBLE)" + with);
    REQUIRE(full);
    auto full_columns = full->columns(); // copy before the node is moved into the manager
    {
        auto [_, fut] =
            actor_zeta::send(creator->address(), &KafkaManager::execute, session_hash_t{1}, std::move(full));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }
    {
        auto chunk = kd::json_to_chunk(resource,
                                       full_columns,
                                       {R"({"id":1,"name":"a","w":1.5})",
                                        R"({"id":2,"name":"b","w":2.5})",
                                        R"({"id":3,"name":"c","w":3.5})"});
        REQUIRE(chunk.size() == 3);
        auto cursor = drive_until_ready(kd::kafka_insert(addr, resource, kafka_db, "full_src", std::move(chunk)));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
    }
    auto count = [&](const std::string& relname) {
        auto cursor =
            drive_until_ready(kd::kafka_query(addr, resource, "SELECT * FROM " + kafka_db + "." + relname + ";"));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        return cursor->size();
    };
    REQUIRE(count("empty_src") == 0);
    REQUIRE(count("full_src") == 3);

    // A restarted manager knows neither source until recover() replays __sources
    auto restarted = actor_zeta::spawn<KafkaManager>(resource, addr, /*start_pollers=*/true);
    auto produce_error = [&](const std::string& name) {
        std::vector<otterstax::kafka::kafka_column_t> partial{
            {"id", ty::complex_logical_type(ty::logical_type::INTEGER)}};
        auto chunk = kd::json_to_chunk(resource, partial, {R"({"id":1})"});
        auto [_, fut] = actor_zeta::send(restarted->address(),
                                         &KafkaManager::produce,
                                         session_hash_t{2},
                                         std::string{name},
                                         lp::node_ptr(lp::make_node_raw_data(resource, std::move(chunk))));
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        return std::string{cursor->get_error().what.c_str()};
    };
    CHECK(produce_error("empty_src").find("unknown object") != std::string::npos);
    CHECK(produce_error("full_src").find("unknown object") != std::string::npos);

    restarted->recover();

    // Both are registered again with a non-empty schema (a one-column batch is
    // refused by the schema guard, not by the registry lookup)
    CHECK(produce_error("empty_src").find("does not match its declared schema") != std::string::npos);
    CHECK(produce_error("full_src").find("does not match its declared schema") != std::string::npos);
    CHECK(count("full_src") == 3); // the probe reads the plan, never the rows

    // Names and types: a continuous INSERT into a stream over a freshly declared
    // reference source of the same shape passes the union guard only when the
    // recovered columns are positionally type-identical to (INTEGER, STRING, DOUBLE)
    REQUIRE_FALSE(run_kafka(restarted, "CREATE SOURCE same_ref (id INT, name VARCHAR, w DOUBLE)" + with)->is_error());
    REQUIRE_FALSE(run_kafka(restarted,
                            "CREATE STREAM same_st WITH (KAFKA_TOPIC='o', BOOTSTRAP_SERVERS='127.0.0.1:9092') "
                            "AS SELECT id, name, w FROM kafka.same_ref;")
                      ->is_error());
    auto insert_into = [&](const std::string& sql) {
        auto [_, fut] = actor_zeta::send(restarted->address(),
                                         &KafkaManager::add_stream_insert,
                                         session_hash_t{3},
                                         std::string{"same_st"},
                                         std::string{sql});
        auto cursor = drive_until_ready(std::move(fut));
        REQUIRE(cursor);
        return cursor;
    };
    CHECK_FALSE(insert_into("INSERT INTO kafka.same_st SELECT id, name, w FROM kafka.empty_src;")->is_error());
    CHECK_FALSE(insert_into("INSERT INTO kafka.same_st SELECT id, name, w FROM kafka.full_src;")->is_error());
    {
        // The recovered types are concrete: the same columns in another order are
        // (DOUBLE, STRING, INTEGER) and no longer match the stream
        auto cursor = insert_into("INSERT INTO kafka.same_st SELECT w, name, id FROM kafka.full_src;");
        REQUIRE(cursor->is_error());
        CHECK(std::string{cursor->get_error().what.c_str()}.find("does not match the stream's schema") !=
              std::string::npos);
    }
    {
        // A recovered column set is complete: dropping one column fails the count
        auto cursor = insert_into("INSERT INTO kafka.same_st SELECT id, name FROM kafka.empty_src;");
        REQUIRE(cursor->is_error());
        CHECK(std::string{cursor->get_error().what.c_str()}.find("does not match the stream's schema") !=
              std::string::npos);
    }
}

// A VIEW over a source's backing table does not hold the source back. Records
// the current outcome: DROP SOURCE neither cascades to the view nor is refused
// by it; the view is left behind, fails while the table is gone and reads again
// once a source of the same name is created.
TEST_CASE("kafka drop: DROP SOURCE with a dependent VIEW") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_drop_view");
    auto cfg = make_create_config("/tmp/otterstax_kafka_drop_view");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();
    auto addr = engine->engine_dispatcher_address();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, addr);

    auto run_kafka = [&](kafka_node_ptr node) {
        REQUIRE(node);
        auto [_, fut] =
            actor_zeta::send(kafka_mgr->address(), &KafkaManager::execute, session_hash_t{1}, std::move(node));
        return drive_until_ready(std::move(fut));
    };
    auto query = [&](const std::string& sql) {
        return drive_until_ready(otterstax::kafka::detail::kafka_query(addr, resource, sql));
    };
    auto require_error =
        [](const components::cursor::cursor_t_ptr& cursor, core::error_code_t code, const std::string& what) {
            REQUIRE(cursor);
            REQUIRE(cursor->is_error());
            CHECK(cursor->get_error().type == code);
            CHECK(std::string{cursor->get_error().what.c_str()} == what);
        };
    const std::string create_source =
        "CREATE SOURCE s (id INT, name VARCHAR) WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092');";

    REQUIRE_FALSE(run_kafka(parse_kafka(parser, create_source))->is_error());
    REQUIRE_FALSE(query("CREATE VIEW kafka.v AS SELECT id, name FROM kafka.s;")->is_error());
    {
        auto rows = query("SELECT * FROM kafka.v;");
        REQUIRE_FALSE(rows->is_error());
        CHECK(rows->size() == 0);
        CHECK(rows->type_data().size() == 2);
    }

    REQUIRE_FALSE(run_kafka(parse_kafka(parser, "DROP SOURCE s;"))->is_error());
    require_error(query("SELECT * FROM kafka.s;"),
                  core::error_code_t::table_not_exists,
                  "collection does not exist: kafka.s");
    require_error(query("SELECT * FROM kafka.s__offsets;"),
                  core::error_code_t::table_not_exists,
                  "collection does not exist: kafka.s__offsets");
    {
        auto sources = query("SELECT name FROM kafka.__sources;");
        REQUIRE_FALSE(sources->is_error());
        CHECK(sources->size() == 0);
    }
    require_error(query("SELECT * FROM kafka.v;"),
                  core::error_code_t::table_not_exists,
                  "collection does not exist: kafka.s");
    {
        namespace lp = components::logical_plan;
        namespace ty = components::types;
        std::vector<otterstax::kafka::kafka_column_t> cols{
            {"id", ty::complex_logical_type(ty::logical_type::INTEGER)},
            {"name", ty::complex_logical_type(ty::logical_type::STRING_LITERAL)}};
        auto chunk = otterstax::kafka::detail::json_to_chunk(resource, cols, {R"({"id":1,"name":"a"})"});
        auto [_, fut] = actor_zeta::send(kafka_mgr->address(),
                                         &KafkaManager::produce,
                                         session_hash_t{2},
                                         std::string{"s"},
                                         lp::node_ptr(lp::make_node_raw_data(resource, std::move(chunk))));
        require_error(drive_until_ready(std::move(fut)),
                      core::error_code_t::do_not_exists,
                      "kafka: INSERT into unknown object 's'");
    }
    CHECK_FALSE(run_kafka(parse_kafka(parser, "DROP SOURCE IF EXISTS s;"))->is_error());

    REQUIRE_FALSE(run_kafka(parse_kafka(parser, create_source))->is_error());
    {
        auto rows = query("SELECT * FROM kafka.v;");
        REQUIRE_FALSE(rows->is_error());
        CHECK(rows->size() == 0);
        CHECK(rows->type_data().size() == 2);
    }
    CHECK_FALSE(query("DROP VIEW kafka.v;")->is_error());
}
