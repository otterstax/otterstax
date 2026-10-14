// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

// The federation inside a real engine: remote names resolved by decide over federation.remote_columns,
// a query block over one remote table pushed to the backend as one statement by the pushdown rule,
// rows read by the storage's make_scan from a fake backend where the engine computes on top
// (a JOIN, a statement the generator cannot spell).

#include "federation_fixture.hpp"

#include "otterbrix/federation/remote_storage.hpp"
#include "otterbrix/translators/input/pg_to_chunk.hpp"

#include <components/catalog/system_table_schemas.hpp>
#include <components/types/logical_value.hpp>

#include <libpq-fe.h>

#include <cstring>

using namespace fedtest;

namespace {

    // The MySQL connection `Sales` holds `Bill`.`Orders` (ID, Amount, Note) — spelled as the backend
    // spells it; the engine binds sales.bill.orders (id, amount, note).
    std::vector<complex_logical_type> orders_columns() {
        return {column(logical_type::BIGINT, "ID"),
                column(logical_type::BIGINT, "Amount"),
                column(logical_type::STRING_LITERAL, "Note")};
    }

    canned_table_t orders_rows() {
        return canned_table_t{orders_columns(),
                              {{std::int64_t{1}, std::int64_t{100}, std::string{"a"}},
                               {std::int64_t{2}, std::int64_t{200}, std::string{"b"}},
                               {std::int64_t{3}, std::int64_t{300}, std::string{"a"}}},
                              std::nullopt,
                              {}};
    }

    // `Orders` has the primary key (ID).
    struct sales_t {
        std::shared_ptr<fake_backend_t> backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
        std::unique_ptr<engine_t> engine;

        explicit sales_t(const std::string& leaf, std::vector<fed::server_t> others = {}) {
            backend->set_table("Orders", orders_rows());
            others.insert(others.begin(), server("Sales", backend));
            install(std::move(others));
            engine = open_engine(leaf);
            engine->bootstrap();
            engine->upsert(backend_type_t::MySQL,
                           scope("Sales", "Bill", ""),
                           tables({discovered("Sales", "Bill", "", "Orders", orders_columns(), {"ID"})}));
        }

        // loc.ids holding `ids`: a sub-query the generator cannot push, so the engine picks the rows.
        void local_ids(const std::vector<int>& ids) {
            REQUIRE(engine->sql("CREATE DATABASE loc;")->is_success());
            REQUIRE(engine->sql("CREATE TABLE loc.ids (id BIGINT);")->is_success());
            for (int id : ids) {
                REQUIRE(engine->sql("INSERT INTO loc.ids (id) VALUES (" + std::to_string(id) + ");")->is_success());
            }
        }
    };

    // The column types of a result, in order.
    std::vector<logical_type> types_of(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<logical_type> out;
        if (!cursor->chunks().empty()) {
            for (const auto& type : cursor->chunks().front().types()) {
                out.push_back(type.type());
            }
        }
        return out;
    }

    bool mentions(const components::cursor::cursor_t_ptr& cursor, const std::string& text) {
        return cursor->is_error() && std::string{cursor->get_error().what}.find(text) != std::string::npos;
    }

} // namespace

TEST_CASE("federation engine: the metadata bootstrap is idempotent") {
    install({});
    auto engine = open_engine("bootstrap");
    engine->bootstrap();
    engine->bootstrap();
    for (const char* table : {"remote_tables", "remote_columns"}) {
        auto cursor = engine->sql(std::string{"SELECT * FROM federation."} + table + ";");
        INFO(table << ": " << error_of(cursor));
        REQUIRE(cursor->is_success());
        CHECK(cursor->size() == 0);
    }
}

TEST_CASE("federation engine: an alias that names another backend kind loses the rows of the old kind") {
    install({});
    auto engine = open_engine("server_kind_change");
    engine->bootstrap();
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "id")};
    engine->upsert(backend_type_t::MySQL,
                   scope("sales", "bill", ""),
                   tables({discovered("sales", "bill", "", "orders", columns, {"id"})}));
    // Another connection's rows stay.
    engine->upsert(backend_type_t::MySQL,
                   scope("other", "bill", ""),
                   tables({discovered("other", "bill", "", "orders", columns)}));

    // config.yaml now opens `sales` as a PostgreSQL connection: a PostgreSQL discovery is keyed by
    // schema and would never replace the MySQL rows keyed by database.
    engine->apply(
        otterstax::catalog::make_other_kind_removal(engine->resource(), "Sales", backend_type_t::PostgreSQL));
    for (const char* table : {"remote_tables", "remote_columns"}) {
        auto rows = engine->sql(std::string{"SELECT uid, backend FROM federation."} + table + ";");
        INFO(table << ": " << error_of(rows));
        REQUIRE(rows->is_success());
        CHECK(rows_of(rows) == text_rows_t{{"other", "mysql"}});
    }
}

TEST_CASE("federation engine: a remote name resolves through decide and reads through make_scan") {
    sales_t sales("resolve_and_scan");

    auto four = sales.engine->sql("SELECT id, amount, note FROM sales.bill.public.orders;");
    INFO(error_of(four));
    REQUIRE(four->is_success());
    CHECK(sorted_rows_of(four) == text_rows_t{{"1", "100", "a"}, {"2", "200", "b"}, {"3", "300", "a"}});
    CHECK(column_names(four) == std::vector<std::string>{"id", "amount", "note"});
    // A projection of every column is a query block: pushed as one statement.
    CHECK(sales.backend->sql_log() ==
          std::vector<std::string>{"SELECT `ID`, `Amount`, `Note` FROM `Bill`.`Orders`;"});

    // uid.db.table: the database slot is the uid. An unquoted uid in another case folds to it.
    auto three = sales.engine->sql("SELECT * FROM SALES.bill.Orders;");
    INFO(error_of(three));
    REQUIRE(three->is_success());
    CHECK(sorted_rows_of(three) == text_rows_t{{"1", "100", "a"}, {"2", "200", "b"}, {"3", "300", "a"}});
    CHECK(sales.backend->scans() == 2);
    // SELECT * reads the table through the storage's scan.
    CHECK(sales.backend->sql_log().back() == "SELECT `ID`, `Amount`, `Note` FROM `Bill`.`Orders`");
}

TEST_CASE("federation engine: a query block over one remote table is pushed to the backend as one statement") {
    sales_t sales("sql_on_top");
    const auto bigint = column(logical_type::BIGINT, "ID");

    const std::string filter_sql = "SELECT `ID` FROM `Bill`.`Orders` WHERE `Amount` > 150 ORDER BY `ID` IS NULL ASC, `ID` ASC;";
    sales.backend->script(filter_sql, canned_table_t{{bigint}, {{std::int64_t{2}}, {std::int64_t{3}}}, std::nullopt, {}});
    auto filtered = sales.engine->sql("SELECT id FROM sales.bill.orders WHERE amount > 150 ORDER BY id;");
    INFO(error_of(filtered));
    REQUIRE(filtered->is_success());
    CHECK(rows_of(filtered) == text_rows_t{{"2"}, {"3"}});
    CHECK(column_names(filtered) == std::vector<std::string>{"id"});
    CHECK(sales.backend->sql_log().back() == filter_sql);

    // The answer is held to the engine's result types: COUNT(*) UBIGINT, SUM of BIGINT BIGINT — what
    // MySQL answers (BIGINT, and DECIMAL as text) is converted.
    const std::string totals_sql =
        "SELECT COUNT(*) AS `n`, SUM(`Amount`) AS `total` FROM `Bill`.`Orders` WHERE `Amount` >= 200;";
    sales.backend->script(totals_sql,
                          canned_table_t{{column(logical_type::BIGINT, "n"), column(logical_type::STRING_LITERAL, "total")},
                                         {{std::int64_t{2}, std::string{"500"}}},
                                         std::nullopt,
                                         {}});
    auto totals = sales.engine->sql("SELECT count(*) AS n, sum(amount) AS total FROM sales.bill.orders WHERE amount >= 200;");
    INFO(error_of(totals));
    REQUIRE(totals->is_success());
    CHECK(rows_of(totals) == text_rows_t{{"2", "500"}});
    CHECK(types_of(totals) == std::vector<logical_type>{logical_type::UBIGINT, logical_type::BIGINT});
    CHECK(sales.backend->sql_log().back() == totals_sql);

    const std::string grouped_sql = "SELECT `Note`, COUNT(*) AS `n`, MAX(`Amount`) AS `top` FROM `Bill`.`Orders` "
                                    "GROUP BY `Note` ORDER BY `Note` IS NULL ASC, `Note` ASC;";
    sales.backend->script(grouped_sql,
                          canned_table_t{{column(logical_type::STRING_LITERAL, "Note"),
                                          column(logical_type::BIGINT, "n"),
                                          column(logical_type::BIGINT, "top")},
                                         {{std::string{"a"}, std::int64_t{2}, std::int64_t{300}},
                                          {std::string{"b"}, std::int64_t{1}, std::int64_t{200}}},
                                         std::nullopt,
                                         {}});
    auto grouped = sales.engine->sql(
        "SELECT note, count(*) AS n, max(amount) AS top FROM sales.bill.orders GROUP BY note ORDER BY note;");
    INFO(error_of(grouped));
    REQUIRE(grouped->is_success());
    CHECK(rows_of(grouped) == text_rows_t{{"a", "2", "300"}, {"b", "1", "200"}});
    CHECK(column_names(grouped) == std::vector<std::string>{"note", "n", "top"});
    CHECK(sales.backend->sql_log().back() == grouped_sql);

    const std::string top_sql =
        "SELECT `ID`, (`Amount` * 2) AS `doubled` FROM `Bill`.`Orders` ORDER BY `Amount` IS NULL DESC, `Amount` DESC LIMIT 2;";
    sales.backend->script(top_sql,
                          canned_table_t{{bigint, column(logical_type::BIGINT, "doubled")},
                                         {{std::int64_t{3}, std::int64_t{600}}, {std::int64_t{2}, std::int64_t{400}}},
                                         std::nullopt,
                                         {}});
    auto top = sales.engine->sql("SELECT id, amount * 2 AS doubled FROM sales.bill.orders ORDER BY amount DESC LIMIT 2;");
    INFO(error_of(top));
    REQUIRE(top->is_success());
    CHECK(rows_of(top) == text_rows_t{{"3", "600"}, {"2", "400"}});
    CHECK(sales.backend->sql_log().back() == top_sql);
    CHECK(sales.backend->scans() == 4);
}

TEST_CASE("federation engine: a remote table joins a local table") {
    sales_t sales("join_local");
    REQUIRE(sales.engine->sql("CREATE DATABASE shopdb;")->is_success());
    REQUIRE(sales.engine->sql("CREATE TABLE shopdb.customers (id BIGINT, name TEXT);")->is_success());
    REQUIRE(sales.engine->sql("INSERT INTO shopdb.customers (id, name) VALUES (1, 'ann'), (3, 'bob'), (5, 'cid');")
                ->is_success());

    auto joined = sales.engine->sql("SELECT o.id, o.amount, c.name FROM sales.bill.orders AS o "
                                    "JOIN shopdb.customers AS c ON o.id = c.id;");
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_rows_of(joined) == text_rows_t{{"1", "100", "ann"}, {"3", "300", "bob"}});

    // The local table needs nothing from the host: a statement of local names never reaches decide.
    auto local = sales.engine->sql("SELECT count(*) FROM shopdb.customers;");
    REQUIRE(local->is_success());
    CHECK(rows_of(local) == text_rows_t{{"3"}});
    CHECK(sales.backend->scans() == 1);
}

TEST_CASE("federation engine: a wide remote result reaches the engine in chunks within its bound") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    canned_table_t wide{{column(logical_type::BIGINT, "n")}, {}, std::nullopt, {}};
    for (std::int64_t i = 0; i < 2500; ++i) {
        wide.rows.push_back({i});
    }
    backend->set_table("numbers", wide);
    install({server("Sales", backend)});
    auto engine = open_engine("wide");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("Sales", "bill", "", "numbers"),
                   tables({discovered("Sales", "bill", "", "numbers", {column(logical_type::BIGINT, "n")})}));

    backend->script("SELECT COUNT(*) AS `count`, SUM(`n`) AS `sum` FROM `bill`.`numbers`;",
                    canned_table_t{{column(logical_type::BIGINT, "count"), column(logical_type::STRING_LITERAL, "sum")},
                                   {{std::int64_t{2500}, std::string{"3123750"}}},
                                   std::nullopt,
                                   {}});
    auto counted = engine->sql("SELECT count(*), sum(n) FROM sales.bill.numbers;");
    INFO(error_of(counted));
    REQUIRE(counted->is_success());
    CHECK(rows_of(counted) == text_rows_t{{"2500", "3123750"}});

    auto all = engine->sql("SELECT n FROM sales.bill.numbers;");
    INFO(error_of(all));
    REQUIRE(all->is_success());
    CHECK(all->size() == 2500);
    for (const auto& chunk : all->chunks()) {
        CHECK(chunk.size() <= components::vector::DEFAULT_VECTOR_CAPACITY);
    }
}

TEST_CASE("federation engine: a name the federation does not answer for does not exist") {
    sales_t sales("not_mine");

    // A known uid, a table the metadata does not hold.
    auto unknown_table = sales.engine->sql("SELECT * FROM sales.bill.invoices;");
    INFO(error_of(unknown_table));
    CHECK(mentions(unknown_table, "does not exist"));

    // A first qualifier that is no registered uid, in both shapes.
    auto no_uid = sales.engine->sql("SELECT * FROM nosuch.bill.orders;");
    INFO(error_of(no_uid));
    CHECK(mentions(no_uid, "does not exist"));
    auto no_uid_four = sales.engine->sql("SELECT * FROM nosuch.bill.public.orders;");
    INFO(error_of(no_uid_four));
    CHECK(mentions(no_uid_four, "does not exist"));

    // database.table is a local name: the uid is not a database of the engine.
    auto two_parts = sales.engine->sql("SELECT * FROM sales.orders;");
    INFO(error_of(two_parts));
    CHECK(mentions(two_parts, "does not exist"));

    CHECK(sales.backend->scans() == 0);
}

TEST_CASE("federation engine: the metadata upsert is idempotent and follows a changed remote schema") {
    sales_t sales("upsert");

    // The same reply again: the rows are replaced, not doubled.
    sales.engine->upsert(backend_type_t::MySQL,
                         scope("Sales", "Bill", ""),
                         tables({discovered("Sales", "Bill", "", "Orders", orders_columns(), {"ID"})}));
    auto counts = sales.engine->sql("SELECT count(*) FROM federation.remote_columns;");
    REQUIRE(counts->is_success());
    CHECK(rows_of(counts) == text_rows_t{{"3"}});
    auto listed = sales.engine->sql(
        "SELECT uid, database_name, schema_name, table_name, remote_database, remote_table, backend "
        "FROM federation.remote_tables;");
    REQUIRE(listed->is_success());
    CHECK(rows_of(listed) == text_rows_t{{"sales", "bill", "", "orders", "Bill", "Orders", "mysql"}});
    auto described = sales.engine->sql("SELECT column_name, remote_column, column_type, ordinal, key_position "
                                       "FROM federation.remote_columns ORDER BY ordinal;");
    REQUIRE(described->is_success());
    CHECK(rows_of(described) == text_rows_t{{"id", "ID", "int8", "1", "1"},
                                            {"amount", "Amount", "int8", "2", "0"},
                                            {"note", "Note", "text", "3", "0"}});

    // The backend changed the table: Note dropped, Status added. One-table rediscovery reply.
    const std::vector<complex_logical_type> changed{column(logical_type::BIGINT, "ID"),
                                                    column(logical_type::BIGINT, "Amount"),
                                                    column(logical_type::STRING_LITERAL, "Status")};
    sales.backend->set_table("Orders",
                             canned_table_t{changed,
                                            {{std::int64_t{1}, std::int64_t{100}, std::string{"open"}}},
                                            std::nullopt,
                                            {}});
    sales.engine->upsert(backend_type_t::MySQL,
                         scope("Sales", "bill", "", "orders"),
                         tables({discovered("Sales", "Bill", "", "Orders", changed)}));
    sales.backend->script("SELECT `ID`, `Status` FROM `Bill`.`Orders`;",
                          canned_table_t{{column(logical_type::BIGINT, "ID"), column(logical_type::STRING_LITERAL, "Status")},
                                         {{std::int64_t{1}, std::string{"open"}}},
                                         std::nullopt,
                                         {}});
    auto status = sales.engine->sql("SELECT id, status FROM sales.bill.orders;");
    INFO(error_of(status));
    REQUIRE(status->is_success());
    CHECK(rows_of(status) == text_rows_t{{"1", "open"}});
    CHECK(sales.engine->sql("SELECT note FROM sales.bill.orders;")->is_error());
    auto recount = sales.engine->sql("SELECT count(*) FROM federation.remote_columns;");
    REQUIRE(recount->is_success());
    CHECK(rows_of(recount) == text_rows_t{{"3"}});

    // A whole-database reply that no longer holds Orders: its rows go, Items' arrive.
    sales.engine->upsert(backend_type_t::MySQL,
                         scope("Sales", "Bill", ""),
                         tables({discovered("Sales", "Bill", "", "Items", {column(logical_type::BIGINT, "sku")})}));
    CHECK(mentions(sales.engine->sql("SELECT * FROM sales.bill.orders;"), "does not exist"));
    auto items = sales.engine->sql("SELECT table_name FROM federation.remote_tables;");
    REQUIRE(items->is_success());
    CHECK(rows_of(items) == text_rows_t{{"items"}});

    // A one-table reply without the table: its rows go.
    sales.engine->upsert(backend_type_t::MySQL, scope("Sales", "bill", "", "items"), tables({}));
    auto none = sales.engine->sql("SELECT count(*) FROM federation.remote_columns;");
    REQUIRE(none->is_success());
    CHECK(rows_of(none) == text_rows_t{{"0"}});
}

TEST_CASE("federation engine: a discovery reply the metadata cannot record is refused") {
    auto* resource = std::pmr::get_default_resource();
    auto nameless = otterstax::catalog::make_discovery_upsert(
        resource,
        backend_type_t::MySQL,
        scope("Sales", "bill", ""),
        tables({discovered("Sales", "bill", "", "t", {complex_logical_type{logical_type::BIGINT}})}));
    REQUIRE(nameless.has_error());
    CHECK(nameless.error().type == core::error_code_t::schema_error);

    auto folded = otterstax::catalog::make_discovery_upsert(
        resource,
        backend_type_t::MySQL,
        scope("Sales", "bill", ""),
        tables({discovered("Sales",
                           "bill",
                           "",
                           "t",
                           {column(logical_type::BIGINT, "Id"), column(logical_type::BIGINT, "ID")})}));
    REQUIRE(folded.has_error());
    CHECK(folded.error().type == core::error_code_t::schema_error);

    // A column type the engine's type spec cannot carry.
    auto untyped = otterstax::catalog::make_discovery_upsert(
        resource,
        backend_type_t::MySQL,
        scope("Sales", "bill", ""),
        tables({discovered("Sales", "bill", "", "t", {column(logical_type::INVALID, "a")})}));
    REQUIRE(untyped.has_error());
    CHECK(untyped.error().type == core::error_code_t::conversion_failure);
}

TEST_CASE("federation engine: column_type holds every discovered type in the engine's type spec") {
    auto* resource = std::pmr::get_default_resource();
    std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "id"),
                                              column(logical_type::STRING_LITERAL, "name"),
                                              column(logical_type::HUGEINT, "big"),
                                              column(logical_type::UBIGINT, "bits"),
                                              column(logical_type::BOOLEAN, "flag")};
    auto decimal = complex_logical_type::create_decimal(resource, 18, 4);
    REQUIRE_FALSE(decimal.has_error());
    columns.push_back(decimal.value());
    columns.back().set_alias("price");
    columns.push_back(complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER}, "tags"));
    columns.push_back(complex_logical_type::create_array(complex_logical_type{logical_type::DOUBLE}, 3, "point"));
    columns.push_back(complex_logical_type::create_map(resource,
                                                       complex_logical_type{logical_type::STRING_LITERAL},
                                                       complex_logical_type{logical_type::BIGINT},
                                                       "attrs"));
    std::pmr::vector<complex_logical_type> fields{resource};
    fields.emplace_back(logical_type::INTEGER, "x");
    fields.emplace_back(logical_type::STRING_LITERAL, "odd, (name): here");
    columns.push_back(complex_logical_type::create_struct("pair_t", fields, "pair"));
    std::vector<components::types::logical_value_t> entries;
    for (std::int32_t i = 0; i < 2; ++i) {
        components::types::logical_value_t entry{resource, i};
        entry.set_alias(i == 0 ? "bronze" : "gold");
        entries.push_back(entry);
    }
    columns.push_back(complex_logical_type::create_enum("tier_t", entries, "tier"));

    install({});
    auto engine = open_engine("column_types");
    engine->bootstrap();
    engine->upsert(backend_type_t::ClickHouse,
                   scope("ch", "ev", ""),
                   tables({discovered("ch", "ev", "", "wide", columns)}));
    auto stored = engine->sql("SELECT column_type FROM federation.remote_columns ORDER BY ordinal;");
    INFO(error_of(stored));
    REQUIRE(stored->is_success());
    const auto texts = rows_of(stored);
    REQUIRE(texts.size() == columns.size());
    CHECK(texts[0] == std::vector<std::string>{"int8"});
    CHECK(texts[1] == std::vector<std::string>{"text"});
    CHECK(texts[5] == std::vector<std::string>{"numeric(18,4)"});
    for (std::size_t i = 0; i < columns.size(); ++i) {
        INFO(texts[i][0]);
        auto decoded = components::catalog::decode_type_spec(resource, texts[i][0]);
        REQUIRE_FALSE(decoded.has_error());
        auto type = std::move(decoded.value());
        type.set_alias(columns[i].alias());
        CHECK(type == columns[i]);
    }
}

TEST_CASE("federation engine: of two remote tables differing only in case the first is kept") {
    install({});
    auto engine = open_engine("twins");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("Sales", "bill", ""),
                   tables({discovered("Sales", "bill", "", "T", {column(logical_type::BIGINT, "a")}),
                           discovered("Sales", "bill", "", "t", {column(logical_type::BIGINT, "a")})}));
    auto kept = engine->sql("SELECT table_name, remote_table FROM federation.remote_tables;");
    INFO(error_of(kept));
    REQUIRE(kept->is_success());
    CHECK(rows_of(kept) == text_rows_t{{"t", "T"}});
}

TEST_CASE("federation engine: writes into a remote table run on the backend") {
    auto ch = std::make_shared<fake_backend_t>(backend_type_t::ClickHouse);
    ch->set_table("events", canned_table_t{{column(logical_type::BIGINT, "id")}, {{std::int64_t{1}}}, std::nullopt, {}});
    sales_t sales("writes", {server("ch", ch)});
    sales.engine->upsert(backend_type_t::ClickHouse,
                         scope("ch", "ev", ""),
                         tables({discovered("ch", "ev", "", "events", {column(logical_type::BIGINT, "id")})}));

    // One statement each, the count the backend reports for it.
    auto insert = sales.engine->sql("INSERT INTO sales.bill.orders (id, amount, note) VALUES (4, 400, 'c');");
    INFO(error_of(insert));
    REQUIRE(insert->is_success());
    CHECK(insert->affected_rows() == 1);
    auto update = sales.engine->sql("UPDATE sales.bill.orders SET amount = 1 WHERE note = 'a';");
    INFO(error_of(update));
    REQUIRE(update->is_success());
    CHECK(update->affected_rows() == 2);
    auto del = sales.engine->sql("DELETE FROM sales.bill.orders;");
    INFO(error_of(del));
    REQUIRE(del->is_success());
    CHECK(del->affected_rows() == 4);
    CHECK(sales.backend->rows("Orders").empty());
    CHECK(sales.backend->write_log() ==
          std::vector<std::string>{"INSERT INTO `Bill`.`Orders` (`ID`, `Amount`, `Note`) VALUES (4, 400, 'c');",
                                   "UPDATE `Bill`.`Orders` SET `Amount` = 1 WHERE `Note` = 'a';",
                                   "DELETE FROM `Bill`.`Orders`;"});
    CHECK(sales.backend->scans() == 0);

    // A backend refusal is the statement's error.
    sales.backend->fail_writes(core::error_code_t::io_error);
    auto refused = sales.engine->sql("DELETE FROM sales.bill.orders WHERE id = 1;");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::io_error);
    CHECK(error_of(refused) == "fake backend refuses the write");

    // ClickHouse: a mutation the generator spells is pushed, synchronous (the next read sees it);
    // one that needs the engine to pick the rows (a sub-query over a local table) cannot change a
    // row by its number.
    auto ch_update = sales.engine->sql("UPDATE ch.ev.events SET id = 2 WHERE id = 1;");
    INFO(error_of(ch_update));
    REQUIRE(ch_update->is_success());
    CHECK(ch_update->affected_rows() == 0);
    CHECK(ch->write_log() ==
          std::vector<std::string>{
              "ALTER TABLE `ev`.`events` UPDATE `id` = 2 WHERE `id` = 1 SETTINGS mutations_sync = 2;"});
    CHECK(ch->rows("events") == std::vector<row_t>{{std::int64_t{2}}});
    sales.local_ids({1});
    auto ch_delete = sales.engine->sql("DELETE FROM ch.ev.events WHERE id IN (SELECT id FROM loc.ids);");
    INFO(error_of(ch_delete));
    REQUIRE(ch_delete->is_error());
    CHECK(error_of(ch_delete) == "ClickHouse table \"ch.ev.events\" cannot change a row by its number");
}

TEST_CASE("federation engine: INSERT SELECT from a local table writes the rows into the remote table") {
    sales_t sales("insert_select");
    REQUIRE(sales.engine->sql("CREATE DATABASE loc;")->is_success());
    REQUIRE(sales.engine->sql("CREATE TABLE loc.t (id BIGINT, name TEXT);")->is_success());
    REQUIRE(sales.engine->sql("INSERT INTO loc.t (id, name) VALUES (5, 'x'), (6, 'it''s');")->is_success());
    auto inserted = sales.engine->sql("INSERT INTO sales.bill.orders (id, amount, note) SELECT id, 7, name FROM loc.t;");
    INFO(error_of(inserted));
    REQUIRE(inserted->is_success());
    CHECK(inserted->affected_rows() == 2);
    CHECK(sales.backend->write_log() ==
          std::vector<std::string>{"INSERT INTO `Bill`.`Orders` (`ID`, `Amount`, `Note`) VALUES (5, 7, 'x'), (6, 7, 'it''s')"});
    CHECK(sales.backend->rows("Orders").size() == 5);
}

TEST_CASE("federation engine: a row change the backend cannot run whole goes row by row by its primary key") {
    sales_t sales("row_by_row");
    sales.local_ids({1, 3});
    auto updated = sales.engine->sql("UPDATE sales.bill.orders SET amount = 7 WHERE id IN (SELECT id FROM loc.ids);");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 2);
    CHECK(sales.backend->write_log() ==
          std::vector<std::string>{"UPDATE `Bill`.`Orders` SET `Amount` = 7 WHERE `ID` = 1",
                                   "UPDATE `Bill`.`Orders` SET `Amount` = 7 WHERE `ID` = 3"});
    CHECK(sales.backend->rows("Orders") ==
          std::vector<row_t>{{std::int64_t{1}, std::int64_t{7}, std::string{"a"}},
                             {std::int64_t{2}, std::int64_t{200}, std::string{"b"}},
                             {std::int64_t{3}, std::int64_t{7}, std::string{"a"}}});

    // A SET that changes nothing still names the row (and counts it, as the engine does).
    sales.backend->clear_logs();
    auto unchanged = sales.engine->sql("UPDATE sales.bill.orders SET amount = 7 WHERE id IN (SELECT id FROM loc.ids);");
    INFO(error_of(unchanged));
    REQUIRE(unchanged->is_success());
    CHECK(unchanged->affected_rows() == 2);
    CHECK(sales.backend->write_log() == std::vector<std::string>{"UPDATE `Bill`.`Orders` SET `ID` = 1 WHERE `ID` = 1",
                                                                 "UPDATE `Bill`.`Orders` SET `ID` = 3 WHERE `ID` = 3"});

    sales.backend->clear_logs();
    auto deleted = sales.engine->sql("DELETE FROM sales.bill.orders WHERE id IN (SELECT id FROM loc.ids);");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 2);
    CHECK(sales.backend->write_log() == std::vector<std::string>{"DELETE FROM `Bill`.`Orders` WHERE `ID` = 1",
                                                                 "DELETE FROM `Bill`.`Orders` WHERE `ID` = 3"});
    CHECK(sales.backend->rows("Orders") == std::vector<row_t>{{std::int64_t{2}, std::int64_t{200}, std::string{"b"}}});
}

TEST_CASE("federation engine: a row change names the row by its key whatever the other columns hold") {
    // Rows 1 and 2 are equal but for the key, with a NULL and a DOUBLE that has no exact decimal
    // literal (0.1): a match on the values would hit both rows, or neither.
    const std::vector<complex_logical_type> columns{column(logical_type::BIGINT, "ID"),
                                                    column(logical_type::DOUBLE, "Price"),
                                                    column(logical_type::STRING_LITERAL, "Note")};
    sales_t sales("row_by_key");
    sales.backend->set_table("Items",
                             canned_table_t{columns,
                                            {{std::int64_t{1}, 0.1, null_t{}},
                                             {std::int64_t{2}, 0.1, null_t{}},
                                             {std::int64_t{3}, 2.5, std::string{"x"}}},
                                            std::nullopt,
                                            {}});
    sales.engine->upsert(backend_type_t::MySQL,
                         scope("Sales", "Bill", "", "Items"),
                         tables({discovered("Sales", "Bill", "", "Items", columns, {"ID"})}));
    sales.local_ids({2});

    auto updated = sales.engine->sql("UPDATE sales.bill.items SET price = 9 WHERE id IN (SELECT id FROM loc.ids);");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 1);
    CHECK(sales.backend->write_log() == std::vector<std::string>{"UPDATE `Bill`.`Items` SET `Price` = 9 WHERE `ID` = 2"});
    CHECK(sales.backend->rows("Items") == std::vector<row_t>{{std::int64_t{1}, 0.1, null_t{}},
                                                             {std::int64_t{2}, 9.0, null_t{}},
                                                             {std::int64_t{3}, 2.5, std::string{"x"}}});

    sales.backend->clear_logs();
    auto deleted = sales.engine->sql("DELETE FROM sales.bill.items WHERE id IN (SELECT id FROM loc.ids);");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 1);
    CHECK(sales.backend->write_log() == std::vector<std::string>{"DELETE FROM `Bill`.`Items` WHERE `ID` = 2"});
    CHECK(sales.backend->rows("Items").size() == 2);
}

TEST_CASE("federation engine: a composite primary key names a row by every key column") {
    const std::vector<complex_logical_type> columns{column(logical_type::STRING_LITERAL, "Region"),
                                                    column(logical_type::BIGINT, "Seq"),
                                                    column(logical_type::BIGINT, "Qty")};
    sales_t sales("row_by_composite_key");
    sales.backend->set_table("Stock",
                             canned_table_t{columns,
                                            {{std::string{"eu"}, std::int64_t{1}, std::int64_t{5}},
                                             {std::string{"us"}, std::int64_t{1}, std::int64_t{5}},
                                             {std::string{"eu"}, std::int64_t{2}, std::int64_t{5}}},
                                            std::nullopt,
                                            {}});
    // The key is (Seq, Region): its order is the key's, not the table's.
    sales.engine->upsert(backend_type_t::MySQL,
                         scope("Sales", "Bill", "", "Stock"),
                         tables({discovered("Sales", "Bill", "", "Stock", columns, {"Seq", "Region"})}));
    sales.local_ids({1});
    auto updated = sales.engine->sql(
        "UPDATE sales.bill.stock SET qty = 0 WHERE seq IN (SELECT id FROM loc.ids) AND region = 'eu';");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 1);
    CHECK(sales.backend->write_log() ==
          std::vector<std::string>{"UPDATE `Bill`.`Stock` SET `Qty` = 0 WHERE `Seq` = 1 AND `Region` = 'eu'"});
    CHECK(sales.backend->rows("Stock") == std::vector<row_t>{{std::string{"eu"}, std::int64_t{1}, std::int64_t{0}},
                                                             {std::string{"us"}, std::int64_t{1}, std::int64_t{5}},
                                                             {std::string{"eu"}, std::int64_t{2}, std::int64_t{5}}});
}

TEST_CASE("federation engine: a table without a primary key refuses a row change the engine picks") {
    sales_t sales("row_without_key");
    sales.engine->upsert(backend_type_t::MySQL,
                         scope("Sales", "Bill", "", "Orders"),
                         tables({discovered("Sales", "Bill", "", "Orders", orders_columns())}));
    sales.local_ids({1});
    for (const char* statement : {"UPDATE sales.bill.orders SET amount = 7 WHERE id IN (SELECT id FROM loc.ids);",
                                  "DELETE FROM sales.bill.orders WHERE id IN (SELECT id FROM loc.ids);"}) {
        auto refused = sales.engine->sql(statement);
        INFO(statement << ": " << error_of(refused));
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == core::error_code_t::unimplemented_yet);
        CHECK(error_of(refused) ==
              "remote table \"sales.bill.orders\" has no primary key: a row the engine picked cannot be named on "
              "the backend");
    }
    CHECK(sales.backend->write_log().empty());
    // What the generator writes whole still runs: it needs no key.
    auto pushed = sales.engine->sql("DELETE FROM sales.bill.orders WHERE id = 2;");
    INFO(error_of(pushed));
    REQUIRE(pushed->is_success());
    CHECK(pushed->affected_rows() == 1);
}

TEST_CASE("federation engine: a floating-point primary key does not name a row") {
    const std::vector<complex_logical_type> columns{column(logical_type::DOUBLE, "K"),
                                                    column(logical_type::BIGINT, "V")};
    sales_t sales("row_by_double_key");
    sales.backend->set_table("Measures", canned_table_t{columns, {{0.1, std::int64_t{1}}}, std::nullopt, {}});
    sales.engine->upsert(backend_type_t::MySQL,
                         scope("Sales", "Bill", "", "Measures"),
                         tables({discovered("Sales", "Bill", "", "Measures", columns, {"K"})}));
    sales.local_ids({1});
    auto refused = sales.engine->sql("DELETE FROM sales.bill.measures WHERE v IN (SELECT id FROM loc.ids);");
    INFO(error_of(refused));
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::unimplemented_yet);
    CHECK(error_of(refused) == "primary key column \"k\" of remote table \"sales.bill.measures\" has a type whose "
                               "value does not name a row exactly");
    CHECK(sales.backend->write_log().empty());
}

// A DECIMAL primary key names its row exactly: its literal is its exact text (12.30), which the
// backend compares as an exact number, so the row the engine picked is changed and no other — the
// neighbour 12.31 too, and 12.3 written as a DOUBLE would name it only by luck.
TEST_CASE("federation engine: a DECIMAL primary key names its row exactly") {
    const std::vector<complex_logical_type> columns{
        complex_logical_type::create_decimal(std::pmr::get_default_resource(), 10, 2, "Code").value(),
        column(logical_type::BIGINT, "V")};
    sales_t sales("row_by_decimal_key");
    sales.backend->set_table(
        "Prices",
        canned_table_t{columns, {{std::string{"12.30"}, std::int64_t{1}}, {std::string{"12.31"}, std::int64_t{2}}}, std::nullopt, {}});
    sales.engine->upsert(backend_type_t::MySQL,
                         scope("Sales", "Bill", "", "Prices"),
                         tables({discovered("Sales", "Bill", "", "Prices", columns, {"Code"})}));
    sales.local_ids({1});
    auto updated = sales.engine->sql("UPDATE sales.bill.prices SET v = 7 WHERE v IN (SELECT id FROM loc.ids);");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 1);
    CHECK(sales.backend->write_log() == std::vector<std::string>{"UPDATE `Bill`.`Prices` SET `V` = 7 WHERE `Code` = 12.30"});
    CHECK(sales.backend->rows("Prices") ==
          std::vector<row_t>{{std::string{"12.30"}, std::int64_t{7}}, {std::string{"12.31"}, std::int64_t{2}}});

    sales.backend->clear_logs();
    auto deleted = sales.engine->sql("DELETE FROM sales.bill.prices WHERE v IN (SELECT id FROM loc.ids) OR v = 7;");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 1);
    CHECK(sales.backend->write_log() == std::vector<std::string>{"DELETE FROM `Bill`.`Prices` WHERE `Code` = 12.30"});
    CHECK(sales.backend->rows("Prices") == std::vector<row_t>{{std::string{"12.31"}, std::int64_t{2}}});
}

// DECIMAL values compared and summed by the engine itself (the JOIN with a local table runs in the
// engine over the remote table's scan): `price > 50` keeps 50.01 and drops 50.00 and 49.99, and the
// sum of 0.10 and 0.20 is 0.30 to the cent — a DOUBLE would answer 0.30000000000000004.
TEST_CASE("federation engine: DECIMAL values are compared and summed exactly by the engine") {
    const std::vector<complex_logical_type> columns{
        column(logical_type::BIGINT, "ID"),
        complex_logical_type::create_decimal(std::pmr::get_default_resource(), 10, 2, "Price").value()};
    sales_t sales("decimal_in_engine");
    sales.backend->set_table("Goods",
                             canned_table_t{columns,
                                            {{std::int64_t{1}, std::string{"49.99"}},
                                             {std::int64_t{2}, std::string{"50.00"}},
                                             {std::int64_t{3}, std::string{"50.01"}},
                                             {std::int64_t{4}, std::string{"0.10"}},
                                             {std::int64_t{5}, std::string{"0.20"}}},
                                            std::nullopt,
                                            {}});
    sales.engine->upsert(backend_type_t::MySQL,
                         scope("Sales", "Bill", "", "Goods"),
                         tables({discovered("Sales", "Bill", "", "Goods", columns, {"ID"})}));
    sales.local_ids({1, 2, 3, 4, 5});

    auto above = sales.engine->sql("SELECT g.id, g.price FROM sales.bill.goods AS g JOIN loc.ids AS i ON g.id = i.id "
                                   "WHERE g.price > 50;");
    INFO(error_of(above));
    REQUIRE(above->is_success());
    CHECK(rows_of(above) == text_rows_t{{"3", "50.01"}});
    REQUIRE(!above->chunks().empty());
    CHECK(above->chunks().front().types()[1].type() == logical_type::DECIMAL);

    auto summed = sales.engine->sql("SELECT sum(g.price) AS s FROM sales.bill.goods AS g JOIN loc.ids AS i "
                                    "ON g.id = i.id WHERE g.id > 3;");
    INFO(error_of(summed));
    REQUIRE(summed->is_success());
    CHECK(rows_of(summed) == text_rows_t{{"0.30"}});
    CHECK(sales.backend->write_log().empty());
}

TEST_CASE("federation engine: a row change that changes no row on the backend fails the statement") {
    sales_t sales("row_gone");
    sales.local_ids({1, 3});
    // Row 3 is deleted on the backend after the engine read it.
    sales.backend->before_next_write([](std::map<std::string, canned_table_t>& tables) {
        std::erase_if(tables["Orders"].rows, [](const row_t& row) { return std::get<std::int64_t>(row[0]) == 3; });
    });
    auto updated = sales.engine->sql("UPDATE sales.bill.orders SET amount = 7 WHERE id IN (SELECT id FROM loc.ids);");
    REQUIRE(updated->is_error());
    CHECK(updated->get_error().type == core::error_code_t::write_conflict);
    CHECK(error_of(updated).find("changed 0 rows: no row holds the key it was read with any more (deleted on the "
                                 "backend, or its key changed, after it was read)") != std::string::npos);
    // The statement before it ran, in the backend's autocommit.
    CHECK(sales.backend->write_log().size() == 2);
    CHECK(sales.backend->rows("Orders") == std::vector<row_t>{{std::int64_t{1}, std::int64_t{7}, std::string{"a"}},
                                                              {std::int64_t{2}, std::int64_t{200}, std::string{"b"}}});
}

TEST_CASE("federation engine: a non-key column changed on the backend after the read is overwritten") {
    // Only the key names the row: there is no check of the other columns, so a change the backend
    // took between the engine's read and its write is lost — not a write_conflict.
    sales_t sales("lost_update");
    sales.local_ids({1});
    sales.backend->before_next_write([](std::map<std::string, canned_table_t>& tables) {
        for (auto& row : tables["Orders"].rows) {
            if (std::get<std::int64_t>(row[0]) == 1) {
                row[1] = std::int64_t{500};
            }
        }
    });
    auto updated =
        sales.engine->sql("UPDATE sales.bill.orders SET amount = amount + 1 WHERE id IN (SELECT id FROM loc.ids);");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 1);
    // The value computed from the stale read (100 + 1) replaced the backend's 500.
    CHECK(sales.backend->rows("Orders").front() == row_t{std::int64_t{1}, std::int64_t{101}, std::string{"a"}});
}

TEST_CASE("federation engine: a remote table is read inside an explicit transaction") {
    sales_t sales("explicit_txn");
    sales.backend->script("SELECT COUNT(*) AS `count` FROM `Bill`.`Orders`;",
                          canned_table_t{{column(logical_type::BIGINT, "count")}, {{std::int64_t{3}}}, std::nullopt, {}});
    const components::session::session_id_t session;
    REQUIRE(sales.engine->sql(session, "BEGIN;")->is_success());
    auto read = sales.engine->sql(session, "SELECT count(*) FROM sales.bill.orders;");
    INFO(error_of(read));
    REQUIRE(read->is_success());
    CHECK(rows_of(read) == text_rows_t{{"3"}});
    // A write inside the block runs on the backend at once (the backend has no part in the block).
    auto insert = sales.engine->sql(session, "INSERT INTO sales.bill.orders (id, amount, note) VALUES (9, 9, 'z');");
    INFO(error_of(insert));
    CHECK(insert->is_success());
    REQUIRE(sales.engine->sql(session, "ROLLBACK;")->is_success());
    CHECK(sales.backend->write_log().size() == 1);
}

TEST_CASE("federation engine: a backend error reaches the cursor unchanged") {
    sales_t sales("backend_error");
    sales.backend->set_table("Orders",
                             canned_table_t{orders_columns(), {}, core::error_code_t::connection_closed, "backend down"});
    auto failed = sales.engine->sql("SELECT * FROM sales.bill.orders;");
    REQUIRE(failed->is_error());
    CHECK(failed->get_error().type == core::error_code_t::connection_closed);
    CHECK(error_of(failed) == "backend down");
}

TEST_CASE("federation engine: an empty remote table answers no rows") {
    sales_t sales("empty_remote");
    sales.backend->set_table("Orders", canned_table_t{orders_columns(), {}, std::nullopt, {}});
    sales.backend->script("SELECT `ID` FROM `Bill`.`Orders`;",
                          canned_table_t{{column(logical_type::BIGINT, "ID")}, {}, std::nullopt, {}});
    sales.backend->script("SELECT COUNT(*) AS `count` FROM `Bill`.`Orders`;",
                          canned_table_t{{column(logical_type::BIGINT, "count")}, {{std::int64_t{0}}}, std::nullopt, {}});
    auto empty = sales.engine->sql("SELECT id FROM sales.bill.orders;");
    INFO(error_of(empty));
    REQUIRE(empty->is_success());
    CHECK(empty->size() == 0);
    // The pushed block answered no row: its columns stay, typed as the engine typed them.
    CHECK(column_names(empty) == std::vector<std::string>{"id"});
    CHECK(types_of(empty) == std::vector<logical_type>{logical_type::BIGINT});
    CHECK(sales.backend->sql_log().back() == "SELECT `ID` FROM `Bill`.`Orders`;");
    auto counted = sales.engine->sql("SELECT count(*) FROM sales.bill.orders;");
    REQUIRE(counted->is_success());
    CHECK(rows_of(counted) == text_rows_t{{"0"}});
}

TEST_CASE("federation engine: the fetches of two remote tables overlap") {
    auto backend = std::make_shared<fake_backend_t>(backend_type_t::MySQL);
    backend->set_table("Orders", orders_rows());
    backend->set_table("Refunds",
                       canned_table_t{{column(logical_type::BIGINT, "order_id"), column(logical_type::BIGINT, "back")},
                                      {{std::int64_t{1}, std::int64_t{10}}, {std::int64_t{3}, std::int64_t{30}}},
                                      std::nullopt,
                                      {}});
    // Neither answers before both scans were asked for: a serial open would wait 5 s and fail below.
    backend->answer_asynchronously(2, std::chrono::milliseconds(20));
    install({server("Sales", backend)});
    auto engine = open_engine("overlap");
    engine->bootstrap();
    engine->upsert(backend_type_t::MySQL,
                   scope("Sales", "Bill", ""),
                   tables({discovered("Sales", "Bill", "", "Orders", orders_columns()),
                           discovered("Sales",
                                      "Bill",
                                      "",
                                      "Refunds",
                                      {column(logical_type::BIGINT, "order_id"), column(logical_type::BIGINT, "back")})}));

    // A FULL JOIN, which MySQL has none of: the engine runs it over a scan of each table (a JOIN
    // MySQL can say is one remote statement, test_federation_joins.cpp).
    auto joined = engine->sql("SELECT o.id, r.back FROM sales.bill.orders AS o "
                              "FULL JOIN sales.bill.refunds AS r ON o.id = r.order_id;");
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_rows_of(joined) == text_rows_t{{"1", "10"}, {"2", "NULL"}, {"3", "30"}});
    CHECK(backend->scans() == 2);
    CHECK(backend->peak_in_flight() == 2);
    CHECK_FALSE(backend->answer_deadline_missed());
}

TEST_CASE("federation engine: concurrent statements resolve remote names from several threads") {
    sales_t sales("concurrent");
    sales.backend->answer_asynchronously(0, std::chrono::milliseconds(1));
    REQUIRE(sales.engine->sql("CREATE DATABASE shopdb;")->is_success());
    REQUIRE(sales.engine->sql("CREATE TABLE shopdb.customers (id BIGINT, name TEXT);")->is_success());
    REQUIRE(sales.engine->sql("INSERT INTO shopdb.customers (id, name) VALUES (1, 'ann'), (2, 'bob');")->is_success());
    sales.backend->script("SELECT COUNT(*) AS `count` FROM `Bill`.`Orders` WHERE `Amount` > 100;",
                          canned_table_t{{column(logical_type::BIGINT, "count")}, {{std::int64_t{2}}}, std::nullopt, {}});

    constexpr int threads = 8;
    constexpr int statements = 12;
    std::atomic<int> succeeded{0};
    std::vector<std::string> failures(threads);
    std::vector<std::thread> pool;
    for (int t = 0; t < threads; ++t) {
        pool.emplace_back([&, t] {
            for (int s = 0; s < statements; ++s) {
                const bool join = (t + s) % 2 == 0;
                auto cursor = join ? sales.engine->sql("SELECT o.id, c.name FROM sales.bill.orders AS o "
                                                       "JOIN shopdb.customers AS c ON o.id = c.id;")
                                   : sales.engine->sql("SELECT count(*) FROM sales.bill.orders WHERE amount > 100;");
                const auto expected = join ? text_rows_t{{"1", "ann"}, {"2", "bob"}} : text_rows_t{{"2"}};
                if (cursor->is_success() && sorted_rows_of(cursor) == expected) {
                    succeeded.fetch_add(1);
                } else {
                    failures[t] = error_of(cursor);
                }
            }
        });
    }
    for (auto& thread : pool) {
        thread.join();
    }
    for (const auto& failure : failures) {
        CHECK(failure.empty());
    }
    CHECK(succeeded.load() == threads * statements);
    CHECK(sales.backend->scans() == threads * statements);
}

namespace {

    // A PostgreSQL backend whose answers go through the real translator: tuples in text form, as
    // libpq hands them over, made a chunk by tsl::pg_to_chunk — the reader the PostgreSQL manager's
    // execute uses. tsl::pg_to_struct of the same result is the discovery reply.
    PGresult* make_pg_result(const std::vector<std::pair<const char*, unsigned int>>& columns,
                             const std::vector<std::vector<const char*>>& rows) {
        PGresult* result = PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK);
        std::vector<PGresAttDesc> attrs(columns.size());
        for (std::size_t c = 0; c < columns.size(); ++c) {
            attrs[c] = PGresAttDesc{const_cast<char*>(columns[c].first), 0, 0, 0, columns[c].second, -1, -1};
        }
        PQsetResultAttrs(result, static_cast<int>(attrs.size()), attrs.data());
        for (std::size_t r = 0; r < rows.size(); ++r) {
            for (std::size_t c = 0; c < columns.size(); ++c) {
                PQsetvalue(result,
                           static_cast<int>(r),
                           static_cast<int>(c),
                           const_cast<char*>(rows[r][c]),
                           static_cast<int>(std::strlen(rows[r][c])));
            }
        }
        return result;
    }

    constexpr unsigned int int8_oid = 20;
    constexpr unsigned int text_oid = 25;

    const std::vector<std::pair<const char*, unsigned int>> pg_columns{{"id", int8_oid}, {"AMOUNT", int8_oid}, {"Label", text_oid}};
    const std::vector<std::vector<const char*>> pg_rows{{"7", "70", "x"}, {"8", "80", "y"}};

    class pg_backend_t final : public fed::backend_t {
    public:
        // What a statement answers instead of the table's rows.
        void script(const std::string& sql,
                    std::vector<std::pair<const char*, unsigned int>> columns,
                    std::vector<std::vector<const char*>> rows) {
            std::lock_guard guard(mutex_);
            scripted_[sql] = {std::move(columns), std::move(rows)};
        }

        actor_zeta::unique_future<core::error_t>
        scan(std::pmr::memory_resource* resource, fed::scan_request_t request, std::shared_ptr<fed::scan_rows_t> rows) override {
            {
                std::lock_guard guard(mutex_);
                sql_log_.push_back(request.sql);
            }
            actor_zeta::promise<core::error_t> promise(resource);
            auto future = promise.get_future();
            auto columns = pg_columns;
            auto answer = pg_rows;
            {
                std::lock_guard guard(mutex_);
                if (auto it = scripted_.find(request.sql); it != scripted_.end()) {
                    columns = it->second.first;
                    answer = it->second.second;
                }
            }
            PGresult* result = make_pg_result(columns, answer);
            auto chunk = tsl::pg_to_chunk(&resource_, result);
            PQclear(result);
            if (chunk.has_error()) {
                promise.set_value(core::error_t{chunk.error().type, std::pmr::string{chunk.error().what.c_str(), &resource_}});
                return future;
            }
            rows->chunks.push_back(std::move(chunk.value()));
            promise.set_value(core::error_t::no_error());
            return future;
        }

        actor_zeta::unique_future<fed::write_result_t> write(std::pmr::memory_resource* resource,
                                                             fed::write_request_t) override {
            actor_zeta::promise<fed::write_result_t> promise(resource);
            auto future = promise.get_future();
            promise.set_value(fed::write_result_t{std::uint64_t{0}});
            return future;
        }

        core::result_wrapper_t<fed::discovered_tables_t> discover(const qualified_name_t&) override {
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"pg backend has no discovery", &resource_}};
        }

        core::error_t execute_ddl(const std::string&, const std::string&) override {
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"pg backend runs no DDL", &resource_}};
        }

        std::vector<std::string> sql_log() const {
            std::lock_guard guard(mutex_);
            return sql_log_;
        }

    private:
        std::pmr::synchronized_pool_resource resource_;
        mutable std::mutex mutex_;
        std::vector<std::string> sql_log_;
        std::map<std::string,
                 std::pair<std::vector<std::pair<const char*, unsigned int>>, std::vector<std::vector<const char*>>>>
            scripted_;
    };

} // namespace

TEST_CASE("federation engine: PostgreSQL rows are typed by the input translator the discovery used") {
    auto backend = std::make_shared<pg_backend_t>();
    install({fed::server_t{"pg", backend_type_t::PostgreSQL, backend}});
    auto engine = open_engine("pg_translator");
    engine->bootstrap();

    PGresult* probe = make_pg_result(pg_columns, {});
    const auto schema = tsl::pg_to_struct(std::pmr::get_default_resource(), probe);
    PQclear(probe);
    std::vector<complex_logical_type> discovered_columns(schema.child_types().begin(), schema.child_types().end());
    engine->upsert(backend_type_t::PostgreSQL,
                   scope("pg", "shopdb", "Shop"),
                   tables({discovered("pg", "shopdb", "Shop", "Orders", discovered_columns)}));

    // pg.shop.orders: the database slot is the uid, the schema keys the table.
    auto read = engine->sql("SELECT id, amount, label FROM pg.shop.orders ORDER BY id;");
    INFO(error_of(read));
    REQUIRE(read->is_success());
    CHECK(rows_of(read) == text_rows_t{{"7", "70", "x"}, {"8", "80", "y"}});
    CHECK(backend->sql_log() ==
          std::vector<std::string>{R"(SELECT "id", "AMOUNT", "Label" FROM "Shop"."Orders" ORDER BY "id" ASC NULLS LAST;)"});

    // PostgreSQL answers SUM of int8 as numeric (read as DOUBLE); the result is the engine's BIGINT.
    backend->script(R"(SELECT SUM("AMOUNT") AS "sum" FROM "Shop"."Orders";)", {{"sum", 1700}}, {{"150"}});
    auto summed = engine->sql("SELECT sum(amount) FROM pg.anydb.shop.orders;");
    INFO(error_of(summed));
    REQUIRE(summed->is_success());
    CHECK(rows_of(summed) == text_rows_t{{"150"}});
    CHECK(types_of(summed) == std::vector<logical_type>{logical_type::BIGINT});
}

TEST_CASE("federation engine: a leftover mirror database of an older OtterStax does not capture a remote name") {
    sales_t sales("uid_database");
    // What an older OtterStax left behind (the server drops it at startup, backends::publish_federation_metadata): an engine
    // database named after the uid, holding mirrors under encoded names `<db>:<schema>:<table>`.
    REQUIRE(sales.engine->sql("CREATE DATABASE sales;")->is_success());
    REQUIRE(sales.engine->sql("CREATE TABLE sales.\"bill::orders\" (id BIGINT);")->is_success());
    sales.backend->script("SELECT `ID` FROM `Bill`.`Orders` ORDER BY `ID` IS NULL ASC, `ID` ASC;",
                          canned_table_t{{column(logical_type::BIGINT, "ID")},
                                         {{std::int64_t{1}}, {std::int64_t{2}}, {std::int64_t{3}}},
                                         std::nullopt,
                                         {}});

    auto three = sales.engine->sql("SELECT id FROM sales.bill.orders ORDER BY id;");
    INFO(error_of(three));
    REQUIRE(three->is_success());
    CHECK(rows_of(three) == text_rows_t{{"1"}, {"2"}, {"3"}});
    auto four = sales.engine->sql("SELECT id FROM sales.bill.public.orders ORDER BY id;");
    INFO(error_of(four));
    REQUIRE(four->is_success());
    CHECK(rows_of(four) == text_rows_t{{"1"}, {"2"}, {"3"}});

    // An unknown remote table under the uid is now refused as a schema slot under an existing
    // database (invalid_parameter, "... does not exist ..."), which the rediscovery classifier reads.
    auto unknown = sales.engine->sql("SELECT * FROM sales.bill.invoices;");
    INFO(error_of(unknown));
    REQUIRE(unknown->is_error());
    CHECK(unknown->get_error().type == core::error_code_t::invalid_parameter);
    CHECK(std::string{unknown->get_error().what}.find("does not exist") != std::string::npos);
}

// otterbrix resolves a local table by (database, name) alone, the uid and schema slots ignored
// (components/physical_plan/operators/operator_resolve_table.cpp: the pg_class probe takes
// entry.dbname + entry.relname), and the host is asked only for names the catalog left unresolved
// (services/collection/executor.cpp, resolve_external_names_). So a local `bill.orders` answers
// `sales.bill.public.orders` — silently, with local rows — and a local `sales.orders` captures
// `sales.bill.orders`, which is then refused for its schema slot. Hidden until otterbrix stops
// matching a name that carries a uid or schema slot against the local catalog.
TEST_CASE("federation engine: a local table never answers a uid-qualified name", "[.engine-defect-uid-capture]") {
    sales_t sales("uid_capture");
    REQUIRE(sales.engine->sql("CREATE DATABASE sales;")->is_success());
    REQUIRE(sales.engine->sql("CREATE TABLE sales.orders (id BIGINT);")->is_success());
    REQUIRE(sales.engine->sql("INSERT INTO sales.orders (id) VALUES (42);")->is_success());
    REQUIRE(sales.engine->sql("CREATE DATABASE bill;")->is_success());
    REQUIRE(sales.engine->sql("CREATE TABLE bill.orders (id BIGINT);")->is_success());
    REQUIRE(sales.engine->sql("INSERT INTO bill.orders (id) VALUES (77);")->is_success());

    auto four = sales.engine->sql("SELECT id FROM sales.bill.public.orders ORDER BY id;");
    INFO(error_of(four));
    REQUIRE(four->is_success());
    CHECK(rows_of(four) == text_rows_t{{"1"}, {"2"}, {"3"}});
    auto three = sales.engine->sql("SELECT id FROM sales.bill.orders ORDER BY id;");
    INFO(error_of(three));
    REQUIRE(three->is_success());
    CHECK(rows_of(three) == text_rows_t{{"1"}, {"2"}, {"3"}});
}
