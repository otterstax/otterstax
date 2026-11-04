// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The Kafka routes through query_session_t: the objects live in the schema
// main.kafka, and an INSERT is a write to a Kafka object when the last part of
// its name is a registered object and the name, completed with the session's
// place (current_database(), current_schema()), is main.kafka.<object>. Every
// other INSERT is the engine's. No broker: the Kafka write is told by the Kafka
// manager's own refusal of rows that do not match the object's columns, which
// comes before any producer is opened.

#include "../system/session_stack.hpp"

#include <catch2/catch_all.hpp>

#include <string>

using otterstax::test::run_session_or_fail;
using otterstax::test::run_session_sql_payload;
using otterstax::test::session_stack;
using otterstax::test::with_session_stack;

namespace {

    // The answer of `sql`: the Kafka manager's (its prefix) or the engine's.
    std::string answer_of(const session_stack& s, session_hash_t id, const std::string& sql) {
        auto r = run_session_sql_payload(s, id, sql);
        REQUIRE(r.has_error());
        return std::string{r.error().what.c_str()};
    }

    bool is_kafka_write(const std::string& what) { return what.starts_with("Kafka write failed: "); }

} // namespace

TEST_CASE("session kafka: an INSERT names a Kafka object by main.kafka and the session's place") {
    with_session_stack("/tmp/test_session_kafka_routes", [](session_stack s) {
        run_session_or_fail(s,
                            5100,
                            "CREATE SOURCE s (id INT, name VARCHAR) "
                            "WITH (KAFKA_TOPIC='t', BOOTSTRAP_SERVERS='localhost:9092')");
        // The backing table is main.kafka.s: `kafka.s` from the default place.
        run_session_or_fail(s, 5101, "SELECT * FROM kafka.s");
        run_session_or_fail(s, 5102, "SELECT * FROM main.kafka.s");

        // `kafka.s` and `main.kafka.s` are the object; one column of two is
        // the Kafka manager's refusal, not the engine's.
        CHECK(is_kafka_write(answer_of(s, 5103, "INSERT INTO kafka.s (id) VALUES (1)")));
        CHECK(is_kafka_write(answer_of(s, 5104, "INSERT INTO main.kafka.s (id) VALUES (1)")));

        // `s` from main.public is main.public.s: the engine's, which has none.
        CHECK_FALSE(is_kafka_write(answer_of(s, 5105, "INSERT INTO s (id) VALUES (1)")));
        // A name that is no registered object goes to the engine whatever it is.
        CHECK_FALSE(is_kafka_write(answer_of(s, 5106, "INSERT INTO kafka.nothing (id) VALUES (1)")));

        // From the place main.kafka the bare name is the object.
        run_session_or_fail(s, 5107, "SET search_path = 'main.kafka'");
        CHECK(is_kafka_write(answer_of(s, 5108, "INSERT INTO s (id) VALUES (1)")));
        // ... and from another database `kafka.s` is that database's.
        run_session_or_fail(s, 5109, "CREATE DATABASE other");
        run_session_or_fail(s, 5110, "SET search_path = 'other.public'");
        CHECK_FALSE(is_kafka_write(answer_of(s, 5111, "INSERT INTO kafka.s (id) VALUES (1)")));
    });
}
