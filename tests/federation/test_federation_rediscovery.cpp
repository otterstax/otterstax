// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The session's recovery flow over a real engine: a statement fails because the metadata does not
// hold a remote table (or holds a stale schema) → the catalog's rediscovery API names what to
// discover → the discovery reply is upserted → the statement is retried once.

#include "federation_fixture.hpp"

#include "catalog/remote_rediscovery.hpp"

using namespace fedtest;
namespace cat = otterstax::catalog;

namespace {

    qualified_name_t written(const std::string& a, const std::string& b, const std::string& c) {
        // a.b.c as the engine reads it: database.schema.table.
        return qualified_name_t{core::dbname_t{a}, core::schema_t{b}, core::relname_t{c}};
    }

    // Steps 2-5 of the flow (catalog/remote_rediscovery.hpp) with a stub for the backend's
    // discover. Answers the retried statement's cursor, or the first one when nothing was retried.
    template<typename Discover>
    components::cursor::cursor_t_ptr run_with_rediscovery(engine_t& engine,
                                                          const std::string& sql,
                                                          const std::vector<qualified_name_t>& names,
                                                          Discover discover) {
        auto cursor = engine.sql(sql);
        if (!cursor->is_error()) {
            return cursor;
        }
        auto targets = cat::rediscovery_targets(cursor->get_error(), names, fed::registry());
        if (targets.empty()) {
            return cursor;
        }
        for (const auto& target : targets) {
            auto plans = cat::make_rediscovery_plans(engine.resource(), target, discover(target));
            if (plans.has_error()) {
                return cursor;
            }
            engine.apply(std::move(plans.value()));
        }
        return engine.sql(sql);
    }

} // namespace

TEST_CASE("federation rediscovery: a table created on the backend after startup is found by one retry") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    const std::vector<complex_logical_type> fresh_columns{column(logical_type::BIGINT, "Id"),
                                                          column(logical_type::STRING_LITERAL, "Name")};
    backend->set_table("Fresh",
                       canned_table_t{fresh_columns, {{std::int64_t{1}, std::string{"one"}}}, std::nullopt, {}});
    install({server("Sales", backend)});
    auto engine = open_engine("rediscover_new");
    engine->bootstrap();

    int discoveries = 0;
    cat::rediscovery_target_t asked{};
    auto discover = [&](const cat::rediscovery_target_t& target) -> core::result_wrapper_t<fed::discovered_tables_t> {
        ++discoveries;
        asked = target;
        return tables({discovered("Sales", "bill", "", "Fresh", fresh_columns)});
    };

    auto cursor = run_with_rediscovery(*engine,
                                       "SELECT id, name FROM sales.bill.fresh;",
                                       {written("sales", "bill", "fresh")},
                                       discover);
    INFO(error_of(cursor));
    REQUIRE(cursor->is_success());
    CHECK(rows_of(cursor) == text_rows_t{{"1", "one"}});
    CHECK(discoveries == 1);
    CHECK(asked.type == backend_type_t::MySQL);
    CHECK(asked.discovery_scope.unique_identifier.t == "Sales");
    CHECK(asked.discovery_scope.database.t == "bill");
    CHECK(asked.discovery_scope.schema.t.empty());
    CHECK(asked.discovery_scope.collection.t == "fresh");

    // The metadata now holds it: the next statement needs no rediscovery.
    backend->script("SELECT COUNT(*) AS `count` FROM `bill`.`Fresh`;",
                    canned_table_t{{column(logical_type::BIGINT, "count")}, {{std::int64_t{1}}}, std::nullopt, {}});
    auto again = run_with_rediscovery(*engine,
                                      "SELECT count(*) FROM sales.bill.fresh;",
                                      {written("sales", "bill", "fresh")},
                                      discover);
    INFO(error_of(again));
    REQUIRE(again->is_success());
    CHECK(rows_of(again) == text_rows_t{{"1"}});
    CHECK(discoveries == 1);
}

TEST_CASE("federation rediscovery: a statement is retried once only") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    install({server("Sales", backend)});
    auto engine = open_engine("rediscover_once");
    engine->bootstrap();

    // The backend does not have the table either: the discovery reply is an error, nothing is retried.
    int discoveries = 0;
    auto missing = [&](const cat::rediscovery_target_t&) -> core::result_wrapper_t<fed::discovered_tables_t> {
        ++discoveries;
        return core::error_t{core::error_code_t::table_not_exists, std::pmr::string{"Table 'bill.ghost' doesn't exist"}};
    };
    auto cursor = run_with_rediscovery(*engine,
                                       "SELECT * FROM sales.bill.ghost;",
                                       {written("sales", "bill", "ghost")},
                                       missing);
    REQUIRE(cursor->is_error());
    CHECK(std::string{cursor->get_error().what}.find("does not exist") != std::string::npos);
    CHECK(discoveries == 1);

    // An empty reply is "not found" too.
    auto target = cat::rediscovery_targets(cursor->get_error(), std::vector<qualified_name_t>{written("sales", "bill", "ghost")}, fed::registry());
    REQUIRE(target.size() == 1);
    auto none = cat::make_rediscovery_plans(engine->resource(), target.front(), tables({}));
    REQUIRE(none.has_error());
    CHECK(none.error().type == core::error_code_t::table_not_exists);
}

TEST_CASE("federation rediscovery: a changed remote schema is detected at scan and rediscovered") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    const std::vector<complex_logical_type> old_columns{column(logical_type::BIGINT, "id"),
                                                        column(logical_type::BIGINT, "price")};
    const std::vector<complex_logical_type> new_columns{column(logical_type::BIGINT, "id"),
                                                        column(logical_type::STRING_LITERAL, "price")};
    // The backend already answers the new types.
    backend->set_table("goods",
                       canned_table_t{new_columns, {{std::int64_t{1}, std::string{"9.99"}}}, std::nullopt, {}});
    install({server("Sales", backend)});
    auto engine = open_engine("rediscover_stale");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("Sales", "bill", ""),
                   tables({discovered("Sales", "bill", "", "goods", old_columns)}));

    auto stale = engine->sql("SELECT id, price FROM sales.bill.goods;");
    REQUIRE(stale->is_error());
    CHECK(stale->get_error().type == core::error_code_t::schema_error);
    // The scan's refusal is a stale remote schema: the table is a rediscovery target.
    const std::vector<qualified_name_t> goods{written("sales", "bill", "goods")};
    CHECK(cat::rediscovery_targets(stale->get_error(), goods, fed::registry()).size() == 1);

    auto discover = [&](const cat::rediscovery_target_t&) -> core::result_wrapper_t<fed::discovered_tables_t> {
        return tables({discovered("Sales", "bill", "", "goods", new_columns)});
    };
    auto cursor = run_with_rediscovery(*engine,
                                       "SELECT id, price FROM sales.bill.goods;",
                                       {written("sales", "bill", "goods")},
                                       discover);
    INFO(error_of(cursor));
    REQUIRE(cursor->is_success());
    CHECK(rows_of(cursor) == text_rows_t{{"1", "9.99"}});
}

// A pushed query block whose answer has another number of columns than the block declares is a
// changed remote table too: rediscovered, and the retry reads the backend's columns.
TEST_CASE("federation rediscovery: a pushed query answering another number of columns is rediscovered") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    const std::vector<complex_logical_type> old_columns{column(logical_type::BIGINT, "id"),
                                                        column(logical_type::BIGINT, "price")};
    const std::vector<complex_logical_type> new_columns{column(logical_type::BIGINT, "id"),
                                                        column(logical_type::BIGINT, "price"),
                                                        column(logical_type::STRING_LITERAL, "note")};
    // The backend already answers the new columns, whatever the statement projects.
    backend->set_table("goods",
                       canned_table_t{new_columns, {{std::int64_t{1}, std::int64_t{7}, std::string{"n"}}}, std::nullopt, {}});
    install({server("Sales", backend)});
    auto engine = open_engine("rediscover_pushed_count");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("Sales", "bill", ""),
                   tables({discovered("Sales", "bill", "", "goods", old_columns)}));

    const std::string sql = "SELECT * FROM sales.bill.goods WHERE id = 1;";
    auto stale = engine->sql(sql);
    REQUIRE(stale->is_error());
    CHECK(stale->get_error().type == core::error_code_t::schema_error);
    // The block was pushed whole (its WHERE went to the backend), not read through the table's scan.
    const auto statements = backend->sql_log();
    REQUIRE_FALSE(statements.empty());
    CHECK(statements.back().find("WHERE") != std::string::npos);

    auto discover = [&](const cat::rediscovery_target_t&) -> core::result_wrapper_t<fed::discovered_tables_t> {
        return tables({discovered("Sales", "bill", "", "goods", new_columns)});
    };
    auto cursor = run_with_rediscovery(*engine, sql, {written("sales", "bill", "goods")}, discover);
    INFO(error_of(cursor));
    REQUIRE(cursor->is_success());
    CHECK(rows_of(cursor) == text_rows_t{{"1", "7", "n"}});
}

TEST_CASE("federation rediscovery: only missing or stale remote names are rediscovered") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::PostgreSQL);
    install({server("pg", backend)});
    const auto& registry = fed::registry();
    const std::vector<qualified_name_t> names{
        written("pg", "shop", "orders"),
        qualified_name_t{core::uid_t{"pg"}, core::dbname_t{"db"}, core::schema_t{"shop"}, core::relname_t{"orders"}},
        written("local", "x", "t"),
        qualified_name_t{core::dbname_t{"localdb"}, core::relname_t{"t"}}};
    const core::error_t missing{core::error_code_t::table_not_exists, std::pmr::string{"relation \"t\" does not exist"}};

    // The two spellings of one PostgreSQL table are one target; local names are none.
    auto targets = cat::rediscovery_targets(missing, names, registry);
    REQUIRE(targets.size() == 1);
    CHECK(targets.front().type == backend_type_t::PostgreSQL);
    CHECK(targets.front().key == fed::table_key_t{"pg", "", "shop", "orders"});
    CHECK(targets.front().discovery_scope.schema.t == "shop");
    CHECK(targets.front().discovery_scope.collection.t == "orders");

    // A missing database is a missing relation too.
    CHECK(cat::rediscovery_targets(core::error_t{core::error_code_t::database_not_exists,
                                                 std::pmr::string{"database \"pg\" does not exist"}},
                                   names,
                                   registry)
              .size() == 1);
    // Errors that are not about a name are never retried.
    for (const auto& other : {core::error_t{core::error_code_t::invalid_parameter, std::pmr::string{"division by zero"}},
                              core::error_t{core::error_code_t::connection_closed, std::pmr::string{"backend down"}},
                              core::error_t{core::error_code_t::schema_error, std::pmr::string{"another schema error"}}}) {
        CHECK(cat::rediscovery_targets(other, names, registry).empty());
    }
    // A discovery reply that is an error keeps its code.
    auto refused = cat::make_rediscovery_plans(std::pmr::get_default_resource(),
                                               targets.front(),
                                               core::error_t{core::error_code_t::io_error, std::pmr::string{"no route"}});
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::io_error);
}
