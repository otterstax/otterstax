// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax

#include <catch2/catch_all.hpp>

#include "../mock/aliases.hpp"
#include "drive_future.hpp"
#include "integration/kafka/detail/kafka_reader.hpp"
#include "integration/kafka/kafka_manager.hpp"
#include "../mock/kafka_destinations.hpp"
#include "integration/otterbrix/otterbrix_engine.hpp"
#include "otterbrix/config.hpp"
#include "otterbrix/parser/parser.hpp"
#include "tests/temporary_directory.hpp"

namespace {
    using otterstax::kafka::KafkaManager;
    namespace kd = otterstax::kafka::detail;

    components::cursor::cursor_t_ptr execute(KafkaManager& manager, std::pmr::memory_resource* resource,
                                             const std::string& sql) {
        GreenplumParser parser(resource, no_aliases());
        auto parsed = parser.parse(sql);
        REQUIRE_FALSE(parsed.has_error());
        auto root = parsed.value()->otterbrix_params->node;
        auto node = otterstax::kafka::kafka_node_ptr{static_cast<otterstax::kafka::kafka_node_t*>(root.get())};
        auto [_, future] = actor_zeta::send(manager.address(), &KafkaManager::execute,
                                           session_hash_t{1}, std::move(node));
        auto cursor = drive_until_ready(std::move(future));
        REQUIRE(cursor);
        return cursor;
    }

    components::cursor::cursor_t_ptr create_source(KafkaManager& manager,
                                                   std::pmr::memory_resource* resource,
                                                   const std::string& offset_reset = "earliest") {
        GreenplumParser parser(resource, no_aliases());
        auto parsed = parser.parse("CREATE SOURCE recovery_src (id INT) WITH "
                                   "(KAFKA_TOPIC='recovery', BOOTSTRAP_SERVERS='127.0.0.1:9092', "
                                   "TRANSACTIONAL=true, OFFSET_RESET='" + offset_reset + "');");
        REQUIRE_FALSE(parsed.has_error());
        auto root = parsed.value()->otterbrix_params->node;
        REQUIRE(root->type() == components::logical_plan::node_type::unused);
        auto node = otterstax::kafka::kafka_node_ptr{static_cast<otterstax::kafka::kafka_node_t*>(root.get())};
        auto [_, future] = actor_zeta::send(manager.address(), &KafkaManager::execute,
                                           session_hash_t{1}, std::move(node));
        auto cursor = drive_until_ready(std::move(future));
        REQUIRE(cursor);
        return cursor;
    }
}

TEST_CASE("kafka CREATE failure cannot remove a preexisting offsets relation") {
    otterstax::test::temporary_directory directory("otterstax-kafka-offset-collision");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto address = engine->engine_dispatcher_address();
    const auto query = [&](const std::string& sql) {
        auto result = drive_until_ready(kd::kafka_query(address, resource, sql));
        REQUIRE(result);
        REQUIRE_FALSE(result->is_error());
        return result;
    };
    query("CREATE DATABASE kafka;");
    query("CREATE TABLE kafka.recovery_src__offsets (owned_value INT);");
    query("INSERT INTO kafka.recovery_src__offsets (owned_value) VALUES (42);");
    auto manager = actor_zeta::spawn<KafkaManager>(resource, address, false, fixture_kafka_destinations());
    REQUIRE(create_source(*manager, resource)->is_error());
    auto preserved = query("SELECT owned_value FROM kafka.recovery_src__offsets;");
    REQUIRE(preserved->size() == 1);
    CHECK(preserved->value(0, 0).value<int32_t>() == 42);
    auto absent = drive_until_ready(kd::kafka_query(address, resource, "SELECT * FROM kafka.recovery_src;"));
    REQUIRE(absent);
    REQUIRE(absent->is_error());
    CHECK(query("SELECT * FROM kafka.__sources;")->size() == 0);
}

TEST_CASE("kafka DDL rejects duplicate names and mismatched DROP kinds without losing metadata") {
    otterstax::test::temporary_directory directory("otterstax-kafka-object-identity");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto address = engine->engine_dispatcher_address();
    auto manager = actor_zeta::spawn<KafkaManager>(resource, address, false, fixture_kafka_destinations());
    REQUIRE_FALSE(create_source(*manager, resource)->is_error());
    const std::string stream = "CREATE STREAM recovery_stream WITH (KAFKA_TOPIC='output', "
        "BOOTSTRAP_SERVERS='127.0.0.1:9092') AS SELECT id FROM kafka.recovery_src;";
    REQUIRE_FALSE(execute(*manager, resource, stream)->is_error());
    REQUIRE(execute(*manager, resource, stream)->is_error());
    REQUIRE(create_source(*manager, resource)->is_error());
    REQUIRE(execute(*manager, resource, "DROP STREAM recovery_src;")->is_error());
    REQUIRE(execute(*manager, resource, "DROP SOURCE recovery_stream;")->is_error());
    auto metadata = drive_until_ready(kd::kafka_query(address, resource, "SELECT * FROM kafka.__sources;"));
    REQUIRE(metadata);
    REQUIRE_FALSE(metadata->is_error());
    CHECK(metadata->size() == 2);
    REQUIRE_FALSE(execute(*manager, resource, "DROP STREAM recovery_stream;")->is_error());
    REQUIRE_FALSE(execute(*manager, resource, "DROP SOURCE recovery_src;")->is_error());
}

TEST_CASE("kafka recovery: an absent registry succeeds but a corrupt registry fails startup") {
    otterstax::test::temporary_directory directory("otterstax-kafka-corrupt-registry");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto address = engine->engine_dispatcher_address();
    auto recovering = actor_zeta::spawn<KafkaManager>(resource, address, true, fixture_kafka_destinations());
    REQUIRE_FALSE(recovering->recover().contains_error());
    auto creator = actor_zeta::spawn<KafkaManager>(resource, address, false, fixture_kafka_destinations());
    REQUIRE_FALSE(create_source(*creator, resource)->is_error());
    const auto corrupt = GENERATE(
        "UPDATE kafka.__sources SET transactional = 'invalid';",
        "UPDATE kafka.__sources SET kind = 'unknown';",
        "UPDATE kafka.__sources SET topic = NULL;",
        "INSERT INTO kafka.__sources SELECT * FROM kafka.__sources;",
        "ALTER TABLE kafka.__sources DROP COLUMN topic;");
    auto changed = drive_until_ready(kd::kafka_query(address, resource, corrupt));
    REQUIRE(changed);
    REQUIRE_FALSE(changed->is_error());
    const auto recovered = recovering->recover();
    INFO(recovered.what);
    REQUIRE(recovered.contains_error());
}

TEST_CASE("kafka DROP SOURCE cannot remove ordinary tables absent from its registry") {
    otterstax::test::temporary_directory directory("otterstax-kafka-drop-ownership");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto address = engine->engine_dispatcher_address();
    auto manager = actor_zeta::spawn<KafkaManager>(resource, address, false, fixture_kafka_destinations());
    const auto query = [&](const std::string& sql) {
        auto result = drive_until_ready(kd::kafka_query(address, resource, sql));
        REQUIRE(result);
        REQUIRE_FALSE(result->is_error());
        return result;
    };
    query("CREATE DATABASE kafka;");
    for (const auto* relation : {"ordinary", "ordinary__offsets"}) {
        query("CREATE TABLE kafka." + std::string{relation} + " (owned_value INT);");
        query("INSERT INTO kafka." + std::string{relation} + " (owned_value) VALUES (42);");
    }
    REQUIRE(execute(*manager, resource, "DROP SOURCE ordinary;")->is_error());
    REQUIRE_FALSE(execute(*manager, resource, "DROP SOURCE IF EXISTS ordinary;")->is_error());
    for (const auto* relation : {"ordinary", "ordinary__offsets"}) {
        auto preserved = query("SELECT owned_value FROM kafka." + std::string{relation} + ";");
        REQUIRE(preserved->size() == 1);
        CHECK(preserved->value(0, 0).value<int32_t>() == 42);
    }
}

TEST_CASE("kafka recovery: a missing transactional checkpoint is an error") {
    otterstax::test::temporary_directory directory("otterstax-kafka-missing-checkpoint");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto address = engine->engine_dispatcher_address();
    auto creator = actor_zeta::spawn<KafkaManager>(resource, address, false, fixture_kafka_destinations());
    REQUIRE_FALSE(create_source(*creator, resource)->is_error());
    auto dropped = drive_until_ready(kd::kafka_query(address, resource, "DROP TABLE kafka.recovery_src__offsets;"));
    REQUIRE(dropped);
    REQUIRE_FALSE(dropped->is_error());
    auto recovering = actor_zeta::spawn<KafkaManager>(resource, address, true, fixture_kafka_destinations());
    const auto recovered = recovering->recover();
    INFO(recovered.what);
    REQUIRE(recovered.contains_error());
    CHECK(recovered.type == core::error_code_t::table_not_exists);
    CHECK(std::string{recovered.what}.find("committed offsets") != std::string::npos);
}

TEST_CASE("kafka CREATE: consumer startup failure reaches the caller and cleans up the source") {
    otterstax::test::temporary_directory directory("otterstax-kafka-start-failure");
    auto engine = db::make_otterbrix_engine(make_create_config(directory.path));
    auto* resource = engine->dispatcher()->resource();
    const auto address = engine->engine_dispatcher_address();
    auto manager = actor_zeta::spawn<KafkaManager>(resource, address, true, fixture_kafka_destinations());
    auto created = create_source(*manager, resource, "invalid-offset-policy");
    REQUIRE(created->is_error());
    CHECK(std::string{created->get_error().what}.find("auto.offset.reset") != std::string::npos);
    for (const auto* relation : {"recovery_src", "recovery_src__offsets"}) {
        auto absent = drive_until_ready(kd::kafka_query(address, resource, "SELECT * FROM kafka." + std::string{relation} + ";"));
        REQUIRE(absent);
        REQUIRE(absent->is_error());
        CHECK(absent->get_error().type == core::error_code_t::table_not_exists);
    }
    auto metadata = drive_until_ready(kd::kafka_query(address, resource, "SELECT * FROM kafka.__sources;"));
    REQUIRE(metadata);
    REQUIRE_FALSE(metadata->is_error());
    CHECK(metadata->size() == 0);
}
