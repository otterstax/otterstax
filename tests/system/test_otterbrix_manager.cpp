// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// OtterbrixManager and the DML-shape rules it applies (types/otterbrix.hpp),
// driven against the mock data manager so every branch is reachable without a
// backend: the affected-row carrier for a DML that ran remotely, the rule that
// tells a no-RETURNING DML apart from everything else, the count captured at
// substitution time, the engine's verdicts on create_table, and an exception
// raised at the engine boundary.

#include "integration/otterbrix/otterbrix_manager.hpp"
#include "otterbrix/parser/parser.hpp"
#include "test_helpers.hpp"
#include "types/otterbrix.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"

#include <actor-zeta.hpp>
#include <catch2/catch_all.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <memory>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::data_chunk_t;
using components::vector::DEFAULT_VECTOR_CAPACITY;

namespace {

    std::pmr::vector<complex_logical_type> no_columns(std::pmr::memory_resource* resource) {
        return std::pmr::vector<complex_logical_type>{resource};
    }

    // A statement whose plan is a raw-data node; only its remote_affected_rows
    // matters to the paths under test.
    OtterbrixStatementPtr statement_with_remote_count(std::pmr::memory_resource* resource, size_t affected) {
        auto statement = std::make_unique<OtterbrixStatement>(
            std::pmr::vector<std::pmr::vector<external_entry_t>>{resource},
            components::logical_plan::make_parameter_node(resource),
            components::logical_plan::make_node_raw_data(resource, data_chunk_t{resource, no_columns(resource), 0}),
            0);
        statement->remote_affected_rows = affected;
        return statement;
    }

    using otterstax::test::wait_until_ready;

    components::cursor::cursor_t_ptr run_execute(db::OtterbrixManager& manager, OtterbrixStatementPtr statement) {
        auto [needs_sched, future] = actor_zeta::send(manager.address(),
                                                      &db::OtterbrixManager::execute,
                                                      session_hash_t{1},
                                                      std::move(statement));
        wait_until_ready(future);
        return std::move(future).take_ready();
    }

    core::result_wrapper_t<bool> run_create_table(db::OtterbrixManager& manager, data_chunk_t chunk) {
        auto [needs_sched, future] = actor_zeta::send(manager.address(),
                                                      &db::OtterbrixManager::create_table,
                                                      session_hash_t{1},
                                                      std::string{"filedb"},
                                                      std::string{"people"},
                                                      std::move(chunk));
        wait_until_ready(future);
        return std::move(future).take_ready();
    }

    // A data manager whose create_database / insert_data answer with the given
    // engine verdicts, so create_table's handling of them is observable.
    class verdict_data_manager final : public SimpleMockOtterbrixManager {
    public:
        verdict_data_manager(mock_config config, core::error_code_t on_create_database, core::error_code_t on_insert)
            : SimpleMockOtterbrixManager(config)
            , on_create_database_(on_create_database)
            , on_insert_(on_insert) {}

        components::cursor::cursor_t_ptr create_database(const std::string&) override {
            return verdict(on_create_database_, "create_database verdict");
        }

        components::cursor::cursor_t_ptr insert_data(const std::string&,
                                                     const std::string&,
                                                     std::vector<components::table::column_definition_t>,
                                                     data_chunk_t) override {
            return verdict(on_insert_, "insert_data verdict");
        }

    private:
        components::cursor::cursor_t_ptr verdict(core::error_code_t code, const char* what) {
            if (code == core::error_code_t::none) {
                return components::cursor::make_cursor(resource());
            }
            return components::cursor::make_cursor(resource(), core::error_t(code, std::pmr::string{what, resource()}));
        }

        core::error_code_t on_create_database_;
        core::error_code_t on_insert_;
    };

    core::result_wrapper_t<bool> run_register_external_database(db::OtterbrixManager& manager,
                                                                const std::string& uid) {
        auto [needs_sched, future] =
            actor_zeta::send(manager.address(), &db::OtterbrixManager::register_external_database, uid);
        wait_until_ready(future);
        return std::move(future).take_ready();
    }

    // A data manager whose engine already holds every database: CREATE
    // DATABASE answers database_already_exists, as the real engine does for a
    // uid's mirror database restored from a previous run's data dir.
    class existing_database_data_manager final : public SimpleMockOtterbrixManager {
    public:
        explicit existing_database_data_manager(mock_config config)
            : SimpleMockOtterbrixManager(config) {}

        components::cursor::cursor_t_ptr execute_sql(const std::string&) override {
            return components::cursor::make_cursor(
                resource(),
                core::error_t(core::error_code_t::database_already_exists,
                              std::pmr::string{"database already exists", resource()}));
        }
    };

    core::result_wrapper_t<bool> run_drop_external_table(db::OtterbrixManager& manager, qualified_name_t name) {
        auto [needs_sched, future] =
            actor_zeta::send(manager.address(), &db::OtterbrixManager::drop_external_table, std::move(name));
        wait_until_ready(future);
        return std::move(future).take_ready();
    }

    // A data manager that keeps every statement handed to execute_sql, so the
    // exact engine-side statement a handler emits is checkable.
    class recording_data_manager final : public SimpleMockOtterbrixManager {
    public:
        explicit recording_data_manager(mock_config config)
            : SimpleMockOtterbrixManager(config) {}

        components::cursor::cursor_t_ptr execute_sql(const std::string& query) override {
            statements.push_back(query);
            return components::cursor::make_cursor(resource());
        }

        std::vector<std::string> statements;
    };

    // A data manager whose engine refuses every statement with the given verdict.
    class refusing_data_manager final : public SimpleMockOtterbrixManager {
    public:
        refusing_data_manager(mock_config config, core::error_code_t code)
            : SimpleMockOtterbrixManager(config)
            , code_(code) {}

        components::cursor::cursor_t_ptr execute_sql(const std::string&) override {
            return components::cursor::make_cursor(
                resource(),
                core::error_t(code_, std::pmr::string{"engine verdict", resource()}));
        }

    private:
        core::error_code_t code_;
    };

    data_chunk_t two_named_columns(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> types(resource);
        types.emplace_back(logical_type::INTEGER);
        types.back().set_alias("id");
        types.emplace_back(logical_type::STRING_LITERAL);
        types.back().set_alias("name");
        data_chunk_t chunk{resource, types, 1};
        chunk.set_value(0, 0, components::types::logical_value_t(resource, static_cast<int32_t>(1)));
        chunk.set_value(1, 0, components::types::logical_value_t(resource, std::string{"one"}));
        chunk.set_cardinality(1);
        return chunk;
    }

} // namespace

TEST_CASE("make_affected_count_carrier: windows of at most DEFAULT_VECTOR_CAPACITY rows, never zero chunks") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());

    SECTION("zero affected rows is one empty chunk") {
        auto carrier = make_affected_count_carrier(&arena, 0);
        REQUIRE(carrier.size() == 1);
        REQUIRE(carrier.front().size() == 0);
        REQUIRE(carrier.front().column_count() == 0);
    }
    SECTION("exactly one capacity fits in one chunk") {
        auto carrier = make_affected_count_carrier(&arena, DEFAULT_VECTOR_CAPACITY);
        REQUIRE(carrier.size() == 1);
        REQUIRE(carrier.front().size() == DEFAULT_VECTOR_CAPACITY);
    }
    SECTION("one row over capacity spills into a second chunk") {
        auto carrier = make_affected_count_carrier(&arena, DEFAULT_VECTOR_CAPACITY + 1);
        REQUIRE(carrier.size() == 2);
        REQUIRE(carrier[0].size() == DEFAULT_VECTOR_CAPACITY);
        REQUIRE(carrier[1].size() == 1);
        for (const auto& chunk : carrier) {
            REQUIRE(chunk.column_count() == 0);
            REQUIRE(chunk.capacity() <= DEFAULT_VECTOR_CAPACITY);
        }
    }
}

TEST_CASE("OtterbrixManager::execute: a remote DML count above one chunk's capacity arrives whole") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = &arena}));
    constexpr size_t affected = 2500;

    auto cursor = run_execute(*manager, statement_with_remote_count(&arena, affected));

    REQUIRE(cursor);
    REQUIRE_FALSE(cursor->is_error());
    REQUIRE(cursor->size() == affected);
    REQUIRE(cursor->column_count() == 0);
    REQUIRE(cursor->chunks().size() == 3);
    for (const auto& chunk : cursor->chunks()) {
        REQUIRE(chunk.size() <= DEFAULT_VECTOR_CAPACITY);
        REQUIRE(chunk.column_count() == 0);
    }
}

TEST_CASE("OtterbrixManager::execute: a remote DML affecting nothing is one empty carrier chunk") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = &arena}));

    auto cursor = run_execute(*manager, statement_with_remote_count(&arena, 0));

    REQUIRE(cursor);
    REQUIRE_FALSE(cursor->is_error());
    REQUIRE(cursor->size() == 0);
    REQUIRE(cursor->chunks().size() == 1);
    REQUIRE(cursor->column_count() == 0);
}

TEST_CASE("OtterbrixManager::execute: a column-less result of a SELECT plan is a schema_error cursor") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    // return_empty: the engine answers every plan with a chunk of no columns.
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = &arena, .return_empty = true}));

    SECTION("an aggregate root owes columns") {
        auto statement = std::make_unique<OtterbrixStatement>(
            std::pmr::vector<std::pmr::vector<external_entry_t>>{&arena},
            components::logical_plan::make_parameter_node(&arena),
            components::logical_plan::make_node_aggregate(&arena, core::dbname_t{"db"}, core::relname_t{"t"}),
            0);
        auto cursor = run_execute(*manager, std::move(statement));
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        REQUIRE(cursor->get_error().type == core::error_code_t::schema_error);
        REQUIRE(std::string{cursor->get_error().what.c_str()}.find("SELECT") != std::string::npos);
        REQUIRE(std::string{cursor->get_error().what.c_str()}.find("without columns") != std::string::npos);
    }
    SECTION("an inlined backend slice does not") {
        auto cursor = run_execute(*manager, statement_with_remote_count(&arena, OtterbrixStatement::no_remote_dml));
        REQUIRE(cursor);
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == 0);
        REQUIRE(cursor->column_count() == 0);
    }
}

TEST_CASE("OtterbrixManager::execute: a multi-chunk result keeps every row's values") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    constexpr size_t rows = 2500;
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = &arena}, rows));

    // The remote-count path is off; the plan itself is irrelevant to the mock's
    // multi-chunk mode.
    auto cursor = run_execute(*manager, statement_with_remote_count(&arena, OtterbrixStatement::no_remote_dml));

    REQUIRE(cursor);
    REQUIRE_FALSE(cursor->is_error());
    REQUIRE(cursor->size() == rows);
    REQUIRE(cursor->column_count() == 2);
    REQUIRE(cursor->chunks().size() == 3);
    // Rows on both sides of a chunk boundary and the very last one.
    for (size_t row : {size_t{0}, size_t{1023}, size_t{1024}, size_t{2047}, size_t{2048}, rows - 1}) {
        INFO("row " << row);
        REQUIRE(cursor->value(0, row).value<int32_t>() == SimpleMockOtterbrixManager::multi_chunk_id(row));
        REQUIRE(std::string{cursor->value(1, row).value<std::string_view>()} ==
                SimpleMockOtterbrixManager::multi_chunk_name(row));
    }
}

TEST_CASE("OtterbrixManager::execute: an exception at the engine boundary is an error cursor") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<SimpleMockOtterbrixManager>(
            mock_config{.resource = &arena, .can_throw = true, .error_message = "engine_boom"}));

    auto cursor = run_execute(*manager, statement_with_remote_count(&arena, OtterbrixStatement::no_remote_dml));

    REQUIRE(cursor);
    REQUIRE(cursor->is_error());
    REQUIRE(std::string{cursor->get_error().what.c_str()}.find("engine_boom") != std::string::npos);
}

TEST_CASE("dml_without_returning: only INSERT/UPDATE/DELETE without RETURNING qualify") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    GreenplumParser parser(&arena);
    auto plan_of = [&](const char* sql) {
        auto parsed = parser.parse(sql);
        INFO(sql << " -> " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value()->otterbrix_params->node);
    };

    REQUIRE(dml_without_returning(plan_of("INSERT INTO db.t (id) VALUES (1);")));
    REQUIRE(dml_without_returning(plan_of("UPDATE db.t SET id = 2 WHERE id = 1;")));
    REQUIRE(dml_without_returning(plan_of("DELETE FROM db.t WHERE id = 1;")));

    REQUIRE_FALSE(dml_without_returning(plan_of("INSERT INTO db.t (id) VALUES (1) RETURNING id;")));
    REQUIRE_FALSE(dml_without_returning(plan_of("UPDATE db.t SET id = 2 WHERE id = 1 RETURNING id;")));
    REQUIRE_FALSE(dml_without_returning(plan_of("DELETE FROM db.t WHERE id = 1 RETURNING id;")));

    REQUIRE_FALSE(dml_without_returning(plan_of("SELECT id FROM db.t;")));
    REQUIRE_FALSE(dml_without_returning(components::logical_plan::node_ptr{}));
    // A bare DML node (no resolve wrapper) is the same statement.
    REQUIRE(dml_without_returning(components::logical_plan::make_node_insert(&arena)));
}

TEST_CASE("row_producing_statement: SELECT and RETURNING DML answer rows and nothing else does") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    GreenplumParser parser(&arena);
    auto plan_of = [&](const char* sql) {
        auto parsed = parser.parse(sql);
        INFO(sql << " -> " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value()->otterbrix_params->node);
    };
    auto kind_of = [&](const char* sql) {
        const char* kind = row_producing_statement(plan_of(sql));
        return kind ? std::string{kind} : std::string{};
    };

    REQUIRE(kind_of("SELECT id FROM db.t;") == "SELECT");
    REQUIRE(kind_of("SELECT id FROM db.t WHERE id > 1 ORDER BY id LIMIT 3;") == "SELECT");
    REQUIRE(kind_of("SELECT count(*) FROM db.t;") == "SELECT");
    REQUIRE(kind_of("SELECT a.id FROM db.a JOIN db.b ON a.id = b.id;") == "SELECT");
    REQUIRE(kind_of("SELECT id FROM db.t UNION SELECT id FROM db.u;") == "SELECT");
    REQUIRE(kind_of("INSERT INTO db.t (id) VALUES (1) RETURNING id;") == "INSERT ... RETURNING");
    REQUIRE(kind_of("UPDATE db.t SET id = 2 WHERE id = 1 RETURNING id;") == "UPDATE ... RETURNING");
    REQUIRE(kind_of("DELETE FROM db.t WHERE id = 1 RETURNING id;") == "DELETE ... RETURNING");

    REQUIRE(kind_of("INSERT INTO db.t (id) VALUES (1);").empty());
    REQUIRE(kind_of("UPDATE db.t SET id = 2 WHERE id = 1;").empty());
    REQUIRE(kind_of("DELETE FROM db.t WHERE id = 1;").empty());
    REQUIRE(kind_of("CREATE DATABASE db;").empty());
    REQUIRE(kind_of("CREATE TABLE db.t (id INT);").empty());
    REQUIRE(kind_of("DROP TABLE db.t;").empty());

    // A backend slice inlined as raw data and a missing plan answer no rows here:
    // the shape of a fetched slice is the translator's contract, not the engine's.
    REQUIRE(row_producing_statement(components::logical_plan::make_node_raw_data(
                &arena, data_chunk_t{&arena, no_columns(&arena), 0})) == nullptr);
    REQUIRE(row_producing_statement(components::logical_plan::node_ptr{}) == nullptr);
    // A bare aggregate node (no resolve wrapper) is the same statement.
    REQUIRE(std::string{row_producing_statement(components::logical_plan::make_node_aggregate(
                &arena, core::dbname_t{"db"}, core::relname_t{"t"}))} == "SELECT");
}

TEST_CASE("capture_remote_dml_count: records a column-less DML result and nothing else") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto statement = statement_with_remote_count(&arena, OtterbrixStatement::no_remote_dml);
    data_chunk_t count_only{&arena, no_columns(&arena), 7};
    count_only.set_cardinality(7);

    SECTION("a DML node with a count carrier") {
        components::logical_plan::node_ptr node = components::logical_plan::make_node_insert(&arena);
        capture_remote_dml_count(*statement, node, count_only);
        REQUIRE(statement->remote_affected_rows == 7);
    }
    SECTION("a SELECT slot is data, not an affected count") {
        components::logical_plan::node_ptr node =
            components::logical_plan::make_node_aggregate(&arena, core::dbname_t{"db"}, core::relname_t{"t"});
        capture_remote_dml_count(*statement, node, count_only);
        REQUIRE(statement->remote_affected_rows == OtterbrixStatement::no_remote_dml);
    }
    SECTION("a DML slot carrying columns is rows, not a count") {
        components::logical_plan::node_ptr node = components::logical_plan::make_node_insert(&arena);
        auto rows = two_named_columns(&arena);
        capture_remote_dml_count(*statement, node, rows);
        REQUIRE(statement->remote_affected_rows == OtterbrixStatement::no_remote_dml);
    }
    SECTION("an empty slot") {
        capture_remote_dml_count(*statement, components::logical_plan::node_ptr{}, count_only);
        REQUIRE(statement->remote_affected_rows == OtterbrixStatement::no_remote_dml);
    }
}

// The uid's name is guarded against user DDL, so a database the engine already
// holds under it is this connection's mirror from a previous run: reused, and
// answered as not created.
TEST_CASE("OtterbrixManager::register_external_database: an existing database is reused as the mirror") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<existing_database_data_manager>(mock_config{.resource = &arena}));

    auto registered = run_register_external_database(*manager, "shop");

    REQUIRE_FALSE(registered.has_error());
    REQUIRE(registered.value() == false);
}

// Any other engine verdict on the database is the failure it names.
TEST_CASE("OtterbrixManager::register_external_database: another engine verdict is returned with its code") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<refusing_data_manager>(mock_config{.resource = &arena}, core::error_code_t::io_error));

    auto registered = run_register_external_database(*manager, "shop");

    REQUIRE(registered.has_error());
    REQUIRE(registered.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{registered.error().what.c_str()}.find("shop") != std::string::npos);
}

TEST_CASE("OtterbrixManager::register_external_database: a fresh database is created") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = &arena}));

    auto registered = run_register_external_database(*manager, "shop");

    REQUIRE_FALSE(registered.has_error());
    REQUIRE(registered.value() == true);
}

TEST_CASE("OtterbrixManager::create_table: the engine's verdict on the database is returned with its code") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<verdict_data_manager>(mock_config{.resource = &arena},
                                               core::error_code_t::database_not_exists,
                                               core::error_code_t::none));

    auto created = run_create_table(*manager, two_named_columns(&arena));

    REQUIRE(created.has_error());
    REQUIRE(created.error().type == core::error_code_t::database_not_exists);
    REQUIRE(std::string{created.error().what.c_str()}.find("create_database verdict") != std::string::npos);
}

TEST_CASE("OtterbrixManager::create_table: the engine's verdict on the insert is returned with its code") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<verdict_data_manager>(mock_config{.resource = &arena},
                                               core::error_code_t::none,
                                               core::error_code_t::table_already_exists));

    auto created = run_create_table(*manager, two_named_columns(&arena));

    REQUIRE(created.has_error());
    REQUIRE(created.error().type == core::error_code_t::table_already_exists);
    REQUIRE(std::string{created.error().what.c_str()}.find("insert_data verdict") != std::string::npos);
}

TEST_CASE("OtterbrixManager::create_table: a nameless column cannot define a table") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = &arena}));

    std::pmr::vector<complex_logical_type> types(&arena);
    types.emplace_back(logical_type::INTEGER);
    types.back().set_alias("id");
    types.emplace_back(logical_type::DOUBLE);
    data_chunk_t chunk{&arena, types, 0};

    auto created = run_create_table(*manager, std::move(chunk));

    REQUIRE(created.has_error());
    REQUIRE(created.error().type == core::error_code_t::invalid_parameter);
    REQUIRE(std::string{created.error().what.c_str()}.find("column 1") != std::string::npos);
}

TEST_CASE("OtterbrixManager::create_table: a well-formed chunk is accepted") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = &arena}));

    auto created = run_create_table(*manager, two_named_columns(&arena));

    REQUIRE_FALSE(created.has_error());
    REQUIRE(created.value() == true);
}

// The drop names exactly what register_external_table created: the uid is the
// engine database, the remaining qualifiers fold into one encoded collection
// name, and both are quoted in the engine dialect.
TEST_CASE("OtterbrixManager::drop_external_table: drops the encoded engine collection of the table") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto engine = std::make_unique<recording_data_manager>(mock_config{.resource = &arena});
    auto* recorder = engine.get();
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(&arena, std::move(engine));

    auto dropped = run_drop_external_table(*manager, qualified_name_t("shop", "pgdb", "public", "orders"));

    REQUIRE_FALSE(dropped.has_error());
    REQUIRE(dropped.value() == true);
    REQUIRE(recorder->statements == std::vector<std::string>{"DROP TABLE \"shop\".\"pgdb:public:orders\""});
}

TEST_CASE("OtterbrixManager::drop_external_table: the engine's verdict is returned with its code") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    auto manager = actor_zeta::spawn<db::OtterbrixManager>(
        &arena,
        std::make_unique<refusing_data_manager>(mock_config{.resource = &arena},
                                                core::error_code_t::table_not_exists));

    auto dropped = run_drop_external_table(*manager, qualified_name_t("shop", "pgdb", "public", "orders"));

    REQUIRE(dropped.has_error());
    REQUIRE(dropped.error().type == core::error_code_t::table_not_exists);
    REQUIRE(std::string{dropped.error().what.c_str()}.find("pgdb.orders") != std::string::npos);
}
