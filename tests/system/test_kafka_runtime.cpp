// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include <catch2/catch_all.hpp>

#include <components/cursor/cursor.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/types/types.hpp>

#include "drive_future.hpp"
#include "integration/kafka/detail/kafka_reader.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "integration/otterbrix/otterbrix_engine.hpp"
#include "otterbrix/config.hpp"
#include "otterbrix/operators/execute_plan.hpp"
#include "otterbrix/parser/parser.hpp"
#include "utility/tsan_helper.hpp"

#include <actor-zeta.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <filesystem>
#include <map>

using otterstax::kafka::kafka_node_ptr;
using otterstax::kafka::kafka_node_t;
using otterstax::kafka::kafka_op;
using otterstax::kafka::KafkaManager;

namespace {
    kafka_node_ptr parse_kafka(GreenplumParser& parser, const std::string& sql) {
        auto parsed = parser.parse(sql);
        REQUIRE_FALSE(parsed.has_error());
        auto data = std::move(parsed.value());
        auto* kn = dynamic_cast<kafka_node_t*>(data->otterbrix_params->node.get());
        return kn ? kafka_node_ptr{kn} : kafka_node_ptr{}; // shares ownership before `data` drops
    }
} // namespace

TEST_CASE("kafka dry: CREATE/DROP SOURCE materialises tables, STREAM does not") {
    std::filesystem::remove_all("/tmp/otterstax_kafka_dry");
    auto cfg = make_create_config("/tmp/otterstax_kafka_dry");
    auto engine = db::make_otterbrix_engine(cfg);
    auto* resource = engine->dispatcher()->resource();

    GreenplumParser parser(resource);
    auto kafka_mgr = actor_zeta::spawn<KafkaManager>(resource, engine->engine_dispatcher_address());

    // Probe a table's schema via a plain SELECT: since b2-rc-2 a LIMIT plan
    // (even LIMIT 1) short-circuits over an empty table and returns a bare
    // cursor (no type_data, zero-column chunk), while an unlimited scan fills
    // type_data on the empty result. Tables stay empty in this dry test, so
    // the full scan is free. is_error() on the returned cursor == table absent
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
        // b1/b2: cursor->value() spans the <=1024-row chunks of the result
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
            // b1/b2: cursor->value() spans the <=1024-row chunks of the result
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
    auto* resource = std::pmr::get_default_resource();
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
    // (empty relname) so the parsed match+select chains over the raw_data batch
    {
        auto plan = otterstax::kafka::detail::kafka_parse_plan(resource, "SELECT id, name FROM kafka.s WHERE id > 2;");
        REQUIRE_FALSE(plan.has_error());
        REQUIRE_FALSE(plan.value().sub_queries.empty());
        auto stream_src = otterstax::kafka::kafka_stream_source(resource, plan.value().sub_queries.back());
        REQUIRE(stream_src);
        CHECK(stream_src->source_relname == "s");
        CHECK(stream_src->operators.size() == 2); // $match + $select

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
    auto* resource = std::pmr::get_default_resource();
    using components::types::complex_logical_type;
    using components::types::logical_type;
    namespace k = otterstax::kafka;

    std::vector<k::kafka_column_t> declared = {{"id", complex_logical_type(logical_type::INTEGER)},
                                               {"name", complex_logical_type(logical_type::STRING_LITERAL)}};

    // Matching: built from the declared columns -> every row round-trips
    {
        auto chunk = k::detail::json_to_chunk(resource, declared, {R"({"id":1,"name":"a"})", R"({"id":2,"name":"b"})"});
        REQUIRE(chunk.size() == 2);
        CHECK(k::detail::chunk_matches_columns(resource, chunk, declared));
    }
    // Wrong type, same names + count: id holds a string -> won't re-ingest as INT
    {
        std::vector<k::kafka_column_t> wrong = {{"id", complex_logical_type(logical_type::STRING_LITERAL)},
                                                {"name", complex_logical_type(logical_type::STRING_LITERAL)}};
        auto chunk = k::detail::json_to_chunk(resource, wrong, {R"({"id":"x","name":"a"})"});
        REQUIRE(chunk.size() == 1);
        CHECK_FALSE(k::detail::chunk_matches_columns(resource, chunk, declared));
    }
    // Wrong column count: only id present.
    {
        std::vector<k::kafka_column_t> partial = {{"id", complex_logical_type(logical_type::INTEGER)}};
        auto chunk = k::detail::json_to_chunk(resource, partial, {R"({"id":1})"});
        REQUIRE(chunk.size() == 1);
        CHECK_FALSE(k::detail::chunk_matches_columns(resource, chunk, declared));
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
