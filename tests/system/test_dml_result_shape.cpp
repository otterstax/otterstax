// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Shape of the payload a LOCAL DML statement returns.
//
// The engine reports a DML's affected-row count as a carrier batch: chunks of
// <=DEFAULT_VECTOR_CAPACITY rows whose cardinality IS the count. Whatever the
// engine puts into that carrier's columns is never initialised data, so the
// payload handed to the frontends must be column-less — every wire protocol
// turns a zero-column payload into an OK/CommandComplete with the count.
//
// A STRING column is mandatory in the fixture. On an all-numeric table an
// uninitialised column reads back as plausible garbage and the test passes
// while the bug is live; on a string column the column count is the checkable
// invariant, without a sanitizer.
//
// The 0-row DELETE case covers the batch invariant: an empty result is one
// empty chunk, never no chunks, because column_count() reads chunks.front().

#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

using otterstax::test::engine_row_count;
using otterstax::test::execute_scheduler_prepared;
using otterstax::test::execute_scheduler_statement;
using otterstax::test::prepare_scheduler_sql;
using otterstax::test::run_scheduler_prepared;
using otterstax::test::run_scheduler_sql;
using otterstax::test::run_scheduler_sql_payload;
using otterstax::test::scheduler_stack;
using otterstax::test::seed_people_bulk;
using otterstax::test::with_scheduler_stack;

namespace {

    // Fresh db.table with one bigint and one STRING column, `rows` rows.
    void seed_people(const scheduler_stack& s, session_hash_t& id, size_t rows) {
        std::string err;
        const bool created_db = run_scheduler_sql(s, id++, "CREATE DATABASE dmldb;", err);
        INFO("CREATE DATABASE: " << err);
        REQUIRE(created_db);
        const bool created_tbl = run_scheduler_sql(s, id++, "CREATE TABLE dmldb.people (id bigint, name string);", err);
        INFO("CREATE TABLE: " << err);
        REQUIRE(created_tbl);
        for (size_t i = 0; i < rows; ++i) {
            const std::string sql = "INSERT INTO dmldb.people (id, name) VALUES (" + std::to_string(i + 1) +
                                    ", 'name_" + std::to_string(i) + "');";
            const bool inserted = run_scheduler_sql(s, id++, sql, err);
            INFO("INSERT " << i << ": " << err);
            REQUIRE(inserted);
        }
    }

    // More rows than one DEFAULT_VECTOR_CAPACITY chunk holds, so the affected
    // count has to travel as several carrier chunks.
    constexpr size_t kMultiChunkRows = 2500;

} // namespace

TEST_CASE("local DELETE returns an affected-count carrier, not table columns") {
    with_scheduler_stack("/tmp/test_dml_shape_delete", [](scheduler_stack s) {
        session_hash_t id = 7100;
        seed_people(s, id, 4);

        auto r = run_scheduler_sql_payload(s, id++, "DELETE FROM dmldb.people WHERE id <= 3;");
        INFO("DELETE error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        auto payload = std::move(r.value());

        // The invariant every caller depends on: a batch always holds >= 1 chunk,
        // so front()/column_count() are defined.
        REQUIRE_FALSE(payload.chunks.empty());
        // The count survives...
        REQUIRE(payload.size() == 3);
        // ...but no columns ride along: the table's (id, name) must not reach
        // the frontend as three rows of never-written memory.
        REQUIRE(payload.column_count() == 0);
    });
}

TEST_CASE("local DELETE affecting zero rows still yields a well-formed payload") {
    with_scheduler_stack("/tmp/test_dml_shape_delete_zero", [](scheduler_stack s) {
        session_hash_t id = 7200;
        seed_people(s, id, 2);

        auto r = run_scheduler_sql_payload(s, id++, "DELETE FROM dmldb.people WHERE id = 999;");
        INFO("DELETE error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        auto payload = std::move(r.value());

        // chunks.front() must be safe to call even when nothing was deleted.
        REQUIRE_FALSE(payload.chunks.empty());
        REQUIRE(payload.size() == 0);
        REQUIRE(payload.column_count() == 0);
        REQUIRE(payload.empty());
    });
}

TEST_CASE("local INSERT and UPDATE return the same affected-count shape as DELETE") {
    with_scheduler_stack("/tmp/test_dml_shape_insert_update", [](scheduler_stack s) {
        session_hash_t id = 7300;
        seed_people(s, id, 2);

        auto ins = run_scheduler_sql_payload(s, id++, "INSERT INTO dmldb.people (id, name) VALUES (98, 'ninety8');");
        INFO("INSERT error: " << ins.error().what.c_str());
        REQUIRE_FALSE(ins.has_error());
        auto ins_payload = std::move(ins.value());
        REQUIRE_FALSE(ins_payload.chunks.empty());
        REQUIRE(ins_payload.column_count() == 0);
        REQUIRE(ins_payload.size() == 1);

        auto upd = run_scheduler_sql_payload(s, id++, "UPDATE dmldb.people SET name = 'renamed' WHERE id = 98;");
        INFO("UPDATE error: " << upd.error().what.c_str());
        REQUIRE_FALSE(upd.has_error());
        auto upd_payload = std::move(upd.value());
        REQUIRE_FALSE(upd_payload.chunks.empty());
        REQUIRE(upd_payload.column_count() == 0);
        REQUIRE(upd_payload.size() == 1);
    });
}

TEST_CASE("local DELETE spanning several chunks returns the whole count as a column-less carrier") {
    with_scheduler_stack("/tmp/test_dml_shape_delete_multichunk", [](scheduler_stack s) {
        session_hash_t id = 7600;
        seed_people_bulk(s, id, kMultiChunkRows);
        REQUIRE(engine_row_count(s, "dmldb", "people") == kMultiChunkRows);

        auto r = run_scheduler_sql_payload(s, id++, "DELETE FROM dmldb.people WHERE id > 0;");
        INFO("DELETE error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        auto payload = std::move(r.value());

        // The count spans chunks and no chunk carries columns.
        REQUIRE(payload.size() == kMultiChunkRows);
        REQUIRE(payload.column_count() == 0);
        REQUIRE(payload.chunks.size() >= 3);
        for (const auto& chunk : payload.chunks) {
            REQUIRE(chunk.column_count() == 0);
        }
        REQUIRE(engine_row_count(s, "dmldb", "people") == 0);
    });
}

TEST_CASE("local UPDATE spanning several chunks returns the whole count as a column-less carrier") {
    with_scheduler_stack("/tmp/test_dml_shape_update_multichunk", [](scheduler_stack s) {
        session_hash_t id = 7700;
        seed_people_bulk(s, id, kMultiChunkRows);

        auto r = run_scheduler_sql_payload(s, id++, "UPDATE dmldb.people SET name = 'renamed' WHERE id > 0;");
        INFO("UPDATE error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        auto payload = std::move(r.value());

        REQUIRE(payload.size() == kMultiChunkRows);
        REQUIRE(payload.column_count() == 0);
        REQUIRE(payload.chunks.size() >= 3);
        for (const auto& chunk : payload.chunks) {
            REQUIRE(chunk.column_count() == 0);
        }
        REQUIRE(engine_row_count(s, "dmldb", "people") == kMultiChunkRows);
    });
}

TEST_CASE("a purely local statement survives the prepare/execute path") {
    // Backend classification for a statement without external nodes is written
    // on the parsed data before the map entry is built from it, so Execute finds
    // the statement classified Otterbrix. The simple-query route classifies in a
    // different place; only the two-phase route exercises this one.
    with_scheduler_stack("/tmp/test_dml_shape_prepared", [](scheduler_stack s) {
        session_hash_t id = 7500;
        seed_people(s, id, 3);

        std::string prepare_err;
        auto r = run_scheduler_prepared(s, id++, "SELECT id, name FROM dmldb.people", prepare_err);
        INFO("prepare error: " << prepare_err);
        INFO("execute error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        auto payload = std::move(r.value());
        REQUIRE(payload.size() == 3);
        REQUIRE(payload.column_count() == 2);

        // DML through the same route: the count still has to come back.
        auto d = run_scheduler_prepared(s, id++, "DELETE FROM dmldb.people WHERE id = 1", prepare_err);
        INFO("prepare error (DELETE): " << prepare_err);
        INFO("execute error (DELETE): " << d.error().what.c_str());
        REQUIRE_FALSE(d.has_error());
        auto deleted = std::move(d.value());
        REQUIRE(deleted.column_count() == 0);
        REQUIRE(deleted.size() == 1);
    });
}

TEST_CASE("a plain SELECT is unaffected — the DML guard must not eat real columns") {
    // Negative control. The guard keys off the DML node type plus an empty
    // returning() list, so anything that legitimately produces rows has to come
    // back with its columns intact.
    with_scheduler_stack("/tmp/test_dml_shape_select", [](scheduler_stack s) {
        session_hash_t id = 7400;
        seed_people(s, id, 3);

        auto r = run_scheduler_sql_payload(s, id++, "SELECT id, name FROM dmldb.people;");
        INFO("SELECT error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        auto payload = std::move(r.value());

        REQUIRE_FALSE(payload.chunks.empty());
        REQUIRE(payload.size() == 3);
        REQUIRE(payload.column_count() == 2);
    });
}

// The transformer lowers EXPLAIN to the inner statement's plan and keeps the
// explain mode on a plan this pipeline discards, so an accepted EXPLAIN would
// run the inner statement. The refusal must hold on the full Scheduler path,
// not only in the parser: the rows are still there afterwards.
TEST_CASE("EXPLAIN DELETE is refused by the Scheduler and deletes nothing") {
    with_scheduler_stack("/tmp/test_dml_shape_explain_delete", [](scheduler_stack s) {
        session_hash_t id = 7500;
        seed_people(s, id, 3);

        auto r = run_scheduler_sql_payload(s, id++, "EXPLAIN DELETE FROM dmldb.people WHERE id <= 2;");
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::unimplemented_yet);

        REQUIRE(engine_row_count(s, "dmldb", "people") == 3);
    });
}

// ── Constraints, constraint DDL and prepared parameters ───────────────────────
// Driven through the Scheduler the way a frontend sends them, on the simple
// route and on prepare + execute. Each case records the outcome of every
// statement — its affected count or result shape, or its whole refusal — and
// what the table holds afterwards.
//
// A constraint is enforced by the engine, out of the catalog lookups the plan
// carries (OtterbrixStatement::catalog_resolves). A statement with `$n`
// placeholders reaches them one step later than the others: they are takeable
// only once Bind has filled every parameter, so the Worker takes them off the
// plan finalize() answers and stamps them on the statement it then executes.
// The FOREIGN KEY case below drives a constraint down both routes.

namespace {

    using parameters_t = std::pmr::vector<components::types::logical_value_t>;

    // The statement ran: `rows` is the payload's row count (a DML's affected
    // count), `columns` its column count.
    void require_ran(const core::result_wrapper_t<session_payload>& r, size_t rows, size_t columns) {
        INFO("error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().size() == rows);
        REQUIRE(r.value().column_count() == columns);
    }

    void require_refused(const core::result_wrapper_t<session_payload>& r,
                         core::error_code_t code,
                         const std::string& what) {
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == code);
        REQUIRE(std::string{r.error().what.c_str()} == what);
    }

    // prepare_schema of a statement whose result schema stays unresolved until
    // Bind: no columns, `parameters` placeholders.
    void require_prepared(const core::result_wrapper_t<session_payload>& r, size_t parameters) {
        INFO("prepare error: " << r.error().what.c_str());
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().column_count() == 0);
        REQUIRE(r.value().parameter_count == parameters);
    }

    // The bigint first column of every row of the payload, sorted.
    std::vector<std::int64_t> ids_of(const session_payload& payload) {
        std::vector<std::int64_t> ids;
        for (const auto& chunk : payload.chunks) {
            for (size_t row = 0; row < chunk.size(); ++row) {
                ids.push_back(chunk.value(0, row).value<std::int64_t>());
            }
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

} // namespace

TEST_CASE("constraints: CHECK on INSERT and UPDATE through the Scheduler") {
    with_scheduler_stack("/tmp/test_dml_constraint_check", [](scheduler_stack s) {
        session_hash_t id = 8100;
        std::string prepare_err;
        require_ran(run_scheduler_sql_payload(s, id++, "CREATE DATABASE cdb;"), 0, 0);
        require_ran(
            run_scheduler_sql_payload(s, id++, "CREATE TABLE cdb.checked (id bigint, qty bigint CHECK (qty > 0));"),
            0,
            0);
        require_ran(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.checked (id, qty) VALUES (1, 5);"), 1, 0);

        // The column CHECK is enforced: the violating INSERT is refused on the
        // simple route and on prepare + execute, and so is the violating UPDATE.
        // A column constraint is declared under no name, and the refusal names
        // it as it was declared — empty.
        const std::string unnamed = "Otterbrix execution failed: CHECK constraint \"\" violated";
        require_refused(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.checked (id, qty) VALUES (2, -1);"),
                        core::error_code_t::other_error,
                        unnamed);
        require_refused(
            run_scheduler_prepared(s, id++, "INSERT INTO cdb.checked (id, qty) VALUES (3, -2)", prepare_err),
            core::error_code_t::other_error,
            unnamed);
        require_refused(run_scheduler_sql_payload(s, id++, "UPDATE cdb.checked SET qty = -5 WHERE id = 1;"),
                        core::error_code_t::other_error,
                        unnamed);

        // Nothing a refusal touched reached the table: the one accepted row
        // stands, unchanged.
        auto kept = run_scheduler_sql_payload(s, id++, "SELECT id, qty FROM cdb.checked;");
        require_ran(kept, 1, 2);
        REQUIRE(ids_of(kept.value()) == std::vector<std::int64_t>{1});
        require_ran(run_scheduler_sql_payload(s, id++, "SELECT id FROM cdb.checked WHERE qty < 0;"), 0, 1);

        // A table-level CHECK is enforced the same way, under the name it was
        // declared with.
        require_ran(run_scheduler_sql_payload(
                        s,
                        id++,
                        "CREATE TABLE cdb.checked2 (id bigint, qty bigint, CONSTRAINT qty_pos CHECK (qty > 0));"),
                    0,
                    0);
        require_refused(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.checked2 (id, qty) VALUES (1, -1);"),
                        core::error_code_t::other_error,
                        "Otterbrix execution failed: CHECK constraint \"qty_pos\" violated");
        require_ran(run_scheduler_sql_payload(s, id++, "SELECT id, qty FROM cdb.checked2;"), 0, 2);
    });
}

TEST_CASE("constraints: PRIMARY KEY duplicate through the Scheduler") {
    with_scheduler_stack("/tmp/test_dml_constraint_pk", [](scheduler_stack s) {
        session_hash_t id = 8200;
        std::string prepare_err;
        require_ran(run_scheduler_sql_payload(s, id++, "CREATE DATABASE cdb;"), 0, 0);
        require_ran(
            run_scheduler_sql_payload(s, id++, "CREATE TABLE cdb.keyed (id bigint PRIMARY KEY, name string);"),
            0,
            0);
        require_ran(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.keyed (id, name) VALUES (1, 'a');"), 1, 0);

        // The PRIMARY KEY is enforced: a duplicate key is refused on the simple
        // route, on prepare + execute, and inside one multi-row INSERT — where
        // the duplicate is of a row of the same statement, not of a stored one.
        const std::string duplicate = "Otterbrix execution failed: UNIQUE constraint violated: key already exists";
        require_refused(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.keyed (id, name) VALUES (1, 'b');"),
                        core::error_code_t::other_error,
                        duplicate);
        require_refused(run_scheduler_prepared(s, id++, "INSERT INTO cdb.keyed (id, name) VALUES (1, 'c')", prepare_err),
                        core::error_code_t::other_error,
                        duplicate);
        require_refused(
            run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.keyed (id, name) VALUES (2, 'x'), (2, 'y');"),
            core::error_code_t::other_error,
            "Otterbrix execution failed: UNIQUE constraint violated: duplicate key within write batch");
        auto keys = run_scheduler_sql_payload(s, id++, "SELECT id, name FROM cdb.keyed;");
        require_ran(keys, 1, 2);
        REQUIRE(ids_of(keys.value()) == std::vector<std::int64_t>{1});
    });
}

TEST_CASE("constraints: FOREIGN KEY on INSERT through the simple and the prepared path") {
    with_scheduler_stack("/tmp/test_dml_constraint_fk_insert", [](scheduler_stack s) {
        session_hash_t id = 8300;
        std::string prepare_err;
        require_ran(run_scheduler_sql_payload(s, id++, "CREATE DATABASE cdb;"), 0, 0);
        require_ran(
            run_scheduler_sql_payload(s, id++, "CREATE TABLE cdb.parent (id bigint PRIMARY KEY, name string);"),
            0,
            0);
        require_ran(run_scheduler_sql_payload(
                        s,
                        id++,
                        "CREATE TABLE cdb.child (id bigint, parent_id bigint REFERENCES cdb.parent (id));"),
                    0,
                    0);
        require_ran(
            run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.parent (id, name) VALUES (1, 'p1'), (2, 'p2');"),
            2,
            0);
        require_ran(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.child (id, parent_id) VALUES (10, 1);"), 1, 0);

        // The FOREIGN KEY is enforced on INSERT: a child naming a parent that is
        // not there is refused on the simple route and on prepare + execute.
        const std::string missing_parent =
            "Otterbrix execution failed: FK constraint violated: referenced row not found in parent table";
        require_refused(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.child (id, parent_id) VALUES (11, 999);"),
                        core::error_code_t::other_error,
                        missing_parent);
        require_refused(
            run_scheduler_prepared(s, id++, "INSERT INTO cdb.child (id, parent_id) VALUES (12, 998)", prepare_err),
            core::error_code_t::other_error,
            missing_parent);

        // A statement with placeholders is refused the same way. Its catalog
        // lookups — the parent table among them — are takeable only once Bind
        // has filled every parameter, so they come off the plan finalize()
        // answers, and the engine validates this INSERT against the same
        // FOREIGN KEY as the two above.
        const session_hash_t orphan_stmt = id++;
        require_prepared(prepare_scheduler_sql(s, orphan_stmt, "INSERT INTO cdb.child (id, parent_id) VALUES ($1, $2)"),
                         2);
        parameters_t orphan{s.resource};
        orphan.emplace_back(s.resource, std::int64_t{13});
        orphan.emplace_back(s.resource, std::int64_t{997});
        require_refused(execute_scheduler_prepared(s, orphan_stmt, std::move(orphan)),
                        core::error_code_t::other_error,
                        missing_parent);

        // The lookups belong to the prepare, not to the process: the same text
        // prepared and executed again — the route an extended-protocol client
        // takes to run a statement twice, since one execute consumes the entry —
        // is validated against them again, neither lost nor doubled.
        const session_hash_t again_stmt = id++;
        require_prepared(prepare_scheduler_sql(s, again_stmt, "INSERT INTO cdb.child (id, parent_id) VALUES ($1, $2)"),
                         2);
        parameters_t again{s.resource};
        again.emplace_back(s.resource, std::int64_t{14});
        again.emplace_back(s.resource, std::int64_t{996});
        require_refused(execute_scheduler_prepared(s, again_stmt, std::move(again)),
                        core::error_code_t::other_error,
                        missing_parent);

        // And a parameterized row naming a parent that IS there is stored: the
        // constraint is enforced, not the statement shape refused.
        const session_hash_t bound_stmt = id++;
        require_prepared(prepare_scheduler_sql(s, bound_stmt, "INSERT INTO cdb.child (id, parent_id) VALUES ($1, $2)"),
                         2);
        parameters_t bound{s.resource};
        bound.emplace_back(s.resource, std::int64_t{15});
        bound.emplace_back(s.resource, std::int64_t{2});
        require_ran(execute_scheduler_prepared(s, bound_stmt, std::move(bound)), 1, 0);

        auto children = run_scheduler_sql_payload(s, id++, "SELECT id, parent_id FROM cdb.child;");
        require_ran(children, 2, 2);
        REQUIRE(ids_of(children.value()) == std::vector<std::int64_t>{10, 15});
    });
}

TEST_CASE("constraints: FOREIGN KEY on a referencing DELETE through the simple and the prepared path") {
    with_scheduler_stack("/tmp/test_dml_constraint_fk_delete", [](scheduler_stack s) {
        session_hash_t id = 8400;
        std::string prepare_err;
        require_ran(run_scheduler_sql_payload(s, id++, "CREATE DATABASE cdb;"), 0, 0);
        require_ran(
            run_scheduler_sql_payload(s, id++, "CREATE TABLE cdb.parent (id bigint PRIMARY KEY, name string);"),
            0,
            0);
        require_ran(run_scheduler_sql_payload(
                        s,
                        id++,
                        "CREATE TABLE cdb.child (id bigint, parent_id bigint REFERENCES cdb.parent (id));"),
                    0,
                    0);
        require_ran(run_scheduler_sql_payload(s,
                                              id++,
                                              "INSERT INTO cdb.parent (id, name) VALUES (1, 'p1'), (2, 'p2'), (3, 'p3');"),
                    3,
                    0);
        require_ran(
            run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.child (id, parent_id) VALUES (10, 1), (20, 2);"),
            2,
            0);

        // The FOREIGN KEY is enforced on the referenced side too: deleting a
        // parent a child still references is refused on both routes. The parent
        // no child references is deleted.
        const std::string still_referenced =
            "Otterbrix execution failed: FK constraint violated: child rows reference deleted parent row";
        require_refused(run_scheduler_sql_payload(s, id++, "DELETE FROM cdb.parent WHERE id = 1;"),
                        core::error_code_t::invalid_constraint,
                        still_referenced);
        require_refused(run_scheduler_prepared(s, id++, "DELETE FROM cdb.parent WHERE id = 2", prepare_err),
                        core::error_code_t::invalid_constraint,
                        still_referenced);
        require_ran(run_scheduler_sql_payload(s, id++, "DELETE FROM cdb.parent WHERE id = 3;"), 1, 0);
        auto parents = run_scheduler_sql_payload(s, id++, "SELECT id FROM cdb.parent;");
        require_ran(parents, 2, 1);
        REQUIRE(ids_of(parents.value()) == std::vector<std::int64_t>{1, 2});
        auto children = run_scheduler_sql_payload(s, id++, "SELECT id FROM cdb.child;");
        require_ran(children, 2, 1);
        REQUIRE(ids_of(children.value()) == std::vector<std::int64_t>{10, 20});
    });
}

TEST_CASE("constraint DDL: REFERENCES to a table that does not exist") {
    with_scheduler_stack("/tmp/test_dml_constraint_ddl_refs", [](scheduler_stack s) {
        session_hash_t id = 8500;
        require_ran(run_scheduler_sql_payload(s, id++, "CREATE DATABASE cdb;"), 0, 0);

        // The REFERENCES target is resolved while the CREATE is planned, so a
        // parent relation that does not exist refuses the statement. An unnamed
        // column constraint is named in the refusal by its kind.
        require_refused(
            run_scheduler_sql_payload(s,
                                      id++,
                                      "CREATE TABLE cdb.orphan (id bigint, parent_id bigint REFERENCES cdb.nope (id));"),
            core::error_code_t::invalid_constraint,
            "Otterbrix execution failed: FOREIGN KEY constraint: referenced relation \"cdb.nope\" does not exist");
        // The refusal left nothing behind: the table it named is not there.
        REQUIRE(run_scheduler_sql_payload(s, id++, "SELECT id FROM cdb.orphan;").has_error());

        // The same for a table-level FOREIGN KEY naming a missing database — it
        // carries the name it was declared under.
        require_refused(
            run_scheduler_sql_payload(s,
                                      id++,
                                      "CREATE TABLE cdb.orphan2 (id bigint, parent_id bigint, "
                                      "CONSTRAINT orphan2_fk FOREIGN KEY (parent_id) REFERENCES nodb.nope (id));"),
            core::error_code_t::invalid_constraint,
            "Otterbrix execution failed: constraint \"orphan2_fk\": referenced relation \"nodb.nope\" does not exist");
        REQUIRE(run_scheduler_sql_payload(s, id++, "SELECT id FROM cdb.orphan2;").has_error());
    });
}

TEST_CASE("constraint DDL: ALTER TABLE ADD CONSTRAINT and DROP CONSTRAINT") {
    with_scheduler_stack("/tmp/test_dml_constraint_ddl_alter", [](scheduler_stack s) {
        session_hash_t id = 8600;
        const std::string violation = "Otterbrix execution failed: CHECK constraint \"qty_positive\" violated";
        require_ran(run_scheduler_sql_payload(s, id++, "CREATE DATABASE cdb;"), 0, 0);
        require_ran(run_scheduler_sql_payload(s, id++, "CREATE TABLE cdb.t (id bigint, qty bigint);"), 0, 0);
        require_ran(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.t (id, qty) VALUES (1, 5);"), 1, 0);

        // A CHECK added by ALTER TABLE is enforced from then on.
        require_ran(run_scheduler_sql_payload(s, id++, "ALTER TABLE cdb.t ADD CONSTRAINT qty_positive CHECK (qty > 0);"),
                    0,
                    0);
        require_refused(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.t (id, qty) VALUES (2, -1);"),
                        core::error_code_t::other_error,
                        violation);

        // DROP CONSTRAINT takes it off again: the INSERT refused a moment ago is
        // accepted now.
        require_ran(run_scheduler_sql_payload(s, id++, "ALTER TABLE cdb.t DROP CONSTRAINT qty_positive;"), 0, 0);
        require_ran(run_scheduler_sql_payload(s, id++, "INSERT INTO cdb.t (id, qty) VALUES (3, -1);"), 1, 0);

        // A PRIMARY KEY over the rows the table already holds is accepted. A
        // FOREIGN KEY to a table that does not exist is refused, as it is in
        // CREATE TABLE, and so is the drop of a constraint never declared.
        require_ran(run_scheduler_sql_payload(s, id++, "ALTER TABLE cdb.t ADD CONSTRAINT t_pk PRIMARY KEY (id);"), 0, 0);
        require_refused(
            run_scheduler_sql_payload(s,
                                      id++,
                                      "ALTER TABLE cdb.t ADD CONSTRAINT t_fk FOREIGN KEY (qty) REFERENCES cdb.nope (id);"),
            core::error_code_t::invalid_constraint,
            "Otterbrix execution failed: constraint \"t_fk\": referenced relation \"cdb.nope\" does not exist");
        require_refused(run_scheduler_sql_payload(s, id++, "ALTER TABLE cdb.t DROP CONSTRAINT nope;"),
                        core::error_code_t::invalid_constraint,
                        "Otterbrix execution failed: constraint \"nope\" of relation \"t\" does not exist");
        auto rows = run_scheduler_sql_payload(s, id++, "SELECT id, qty FROM cdb.t;");
        require_ran(rows, 2, 2);
        REQUIRE(ids_of(rows.value()) == std::vector<std::int64_t>{1, 3});
    });
}

TEST_CASE("prepared matrix: WHERE and LIMIT and IN parameters") {
    with_scheduler_stack("/tmp/test_dml_prepared_select", [](scheduler_stack s) {
        session_hash_t id = 8700;
        seed_people(s, id, 3);

        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, "SELECT id, name FROM dmldb.people WHERE id = $1"), 1);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{2});
            auto r = execute_scheduler_prepared(s, stmt, std::move(p));
            require_ran(r, 1, 2);
            REQUIRE(ids_of(r.value()) == std::vector<std::int64_t>{2});
        }
        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, "SELECT id, name FROM dmldb.people LIMIT $1"), 1);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{2});
            require_ran(execute_scheduler_prepared(s, stmt, std::move(p)), 2, 2);
        }
        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, "SELECT id, name FROM dmldb.people WHERE id IN ($1, $2)"),
                             2);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{1});
            p.emplace_back(s.resource, std::int64_t{3});
            auto r = execute_scheduler_prepared(s, stmt, std::move(p));
            require_ran(r, 2, 2);
            REQUIRE(ids_of(r.value()) == std::vector<std::int64_t>{1, 3});
        }
    });
}

TEST_CASE("prepared matrix: INSERT and UPDATE and DELETE parameters") {
    with_scheduler_stack("/tmp/test_dml_prepared_dml", [](scheduler_stack s) {
        session_hash_t id = 8800;
        seed_people(s, id, 3);

        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, "INSERT INTO dmldb.people (id, name) VALUES ($1, $2)"), 2);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{10});
            p.emplace_back(s.resource, std::string{"ten"});
            require_ran(execute_scheduler_prepared(s, stmt, std::move(p)), 1, 0);
        }
        {
            auto r = run_scheduler_sql_payload(s, id++, "SELECT id, name FROM dmldb.people WHERE id = 10;");
            require_ran(r, 1, 2);
            REQUIRE(std::string{r.value().chunks.front().value(1, 0).value<std::string_view>()} == "ten");
        }
        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, "UPDATE dmldb.people SET name = $1 WHERE id = $2"), 2);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::string{"renamed"});
            p.emplace_back(s.resource, std::int64_t{10});
            require_ran(execute_scheduler_prepared(s, stmt, std::move(p)), 1, 0);
        }
        {
            auto r = run_scheduler_sql_payload(s, id++, "SELECT name FROM dmldb.people WHERE id = 10;");
            require_ran(r, 1, 1);
            REQUIRE(std::string{r.value().chunks.front().value(0, 0).value<std::string_view>()} == "renamed");
        }
        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, "DELETE FROM dmldb.people WHERE id = $1"), 1);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{10});
            require_ran(execute_scheduler_prepared(s, stmt, std::move(p)), 1, 0);
        }
        auto remaining = run_scheduler_sql_payload(s, id++, "SELECT id FROM dmldb.people;");
        require_ran(remaining, 3, 1);
        REQUIRE(ids_of(remaining.value()) == std::vector<std::int64_t>{1, 2, 3});
    });
}

TEST_CASE("prepared matrix: a NULL parameter") {
    with_scheduler_stack("/tmp/test_dml_prepared_null", [](scheduler_stack s) {
        session_hash_t id = 8900;
        seed_people(s, id, 3);

        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, "INSERT INTO dmldb.people (id, name) VALUES ($1, $2)"), 2);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{20});
            p.emplace_back(s.resource, components::types::logical_type::NA);
            require_ran(execute_scheduler_prepared(s, stmt, std::move(p)), 1, 0);
        }
        auto nulls = run_scheduler_sql_payload(s, id++, "SELECT id FROM dmldb.people WHERE name IS NULL;");
        require_ran(nulls, 1, 1);
        REQUIRE(ids_of(nulls.value()) == std::vector<std::int64_t>{20});
        require_ran(run_scheduler_sql_payload(s, id++, "SELECT id FROM dmldb.people;"), 4, 1);
    });
}

// `id = NULL` matches no row, so the answer is an empty result that keeps its
// `id` column, the way PostgreSQL answers with an empty result and a
// RowDescription. A scan that short-circuits the comparison into a drain chunk
// without types leaves the Worker refusing the column-less result as a
// schema_error instead.
TEST_CASE("prepared matrix: WHERE id = $1 with a NULL parameter answers no rows", "[engine-defect-nullparam]") {
    with_scheduler_stack("/tmp/test_dml_prepared_null_where", [](scheduler_stack s) {
        session_hash_t id = 8950;
        seed_people(s, id, 3);

        const session_hash_t stmt = id++;
        require_prepared(prepare_scheduler_sql(s, stmt, "SELECT id FROM dmldb.people WHERE id = $1"), 1);
        parameters_t p{s.resource};
        p.emplace_back(s.resource, components::types::logical_type::NA);
        auto r = execute_scheduler_prepared(s, stmt, std::move(p));
        require_ran(r, 0, 1);
        REQUIRE(r.value().chunks.front().types()[0].alias() == "id");
    });
}

// A parameterized statement cannot be validated by the engine: the plan-only
// pass refuses a plan that still carries `$n` ("unbound parameter in
// expression") and the binder holds every parameter until finalize. Its result
// is computed instead from the columns the engine answers for the relations the
// statement reads — one `SELECT * FROM db.rel LIMIT 0` probe each, which no
// placeholder reaches — with the plan's own projection resolved against them.
// So a named column carries the type it will arrive under, and `*`, which the
// transformer leaves a passthrough naming nothing, is expanded into the table's
// columns — what PostgreSQL does out of its catalog at parse time. The
// extended-protocol frontends hand that out as the row shape known before Bind
// and hold the executed result to it.
TEST_CASE("prepared matrix: a parameterized SELECT is prepared from the engine's own columns") {
    with_scheduler_stack("/tmp/test_dml_prepared_projection", [](scheduler_stack s) {
        session_hash_t id = 8960;
        seed_people(s, id, 3);

        auto named = prepare_scheduler_sql(s, id++, "SELECT id, name FROM dmldb.people WHERE id = $1");
        require_prepared(named, 1);
        const auto& schema = named.value().schema;
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 2);
        REQUIRE(schema.child_types()[0].alias() == "id");
        REQUIRE(schema.child_types()[1].alias() == "name");
        // Named AND typed: the placeholder sits in the WHERE, and the column
        // types come from the table, not from a value.
        INFO("described id: " << static_cast<int>(schema.child_types()[0].type())
                              << ", name: " << static_cast<int>(schema.child_types()[1].type()));
        REQUIRE(schema.child_types()[0].type() == components::types::logical_type::BIGINT);
        REQUIRE(schema.child_types()[1].type() == components::types::logical_type::STRING_LITERAL);

        // The control: the same statement without a placeholder is validated by
        // the engine, which stamps the very same types.
        auto exact = prepare_scheduler_sql(s, id++, "SELECT id, name FROM dmldb.people WHERE id = 1");
        INFO("prepare error: " << exact.error().what.c_str());
        REQUIRE_FALSE(exact.has_error());
        REQUIRE(exact.value().schema.child_types().size() == 2);
        REQUIRE(exact.value().schema.child_types()[0].type() == components::types::logical_type::BIGINT);
        REQUIRE(exact.value().schema.child_types()[1].type() == components::types::logical_type::STRING_LITERAL);

        // `SELECT *` names no column on the plan, so the whole probed schema is
        // the answer: the table's columns, in order, typed. It used to be a
        // shape of no columns, which every Execute of a described statement was
        // refused under.
        const session_hash_t star_stmt = id++;
        auto star = prepare_scheduler_sql(s, star_stmt, "SELECT * FROM dmldb.people WHERE id = $1");
        require_prepared(star, 1);
        const auto& star_schema = star.value().schema;
        REQUIRE(star_schema.type() == components::types::logical_type::STRUCT);
        INFO("described star columns: " << star_schema.child_types().size());
        REQUIRE(star_schema.child_types().size() == 2);
        REQUIRE(star_schema.child_types()[0].alias() == "id");
        REQUIRE(star_schema.child_types()[1].alias() == "name");
        REQUIRE(star_schema.child_types()[0].type() == components::types::logical_type::BIGINT);
        REQUIRE(star_schema.child_types()[1].type() == components::types::logical_type::STRING_LITERAL);

        // ...and the execution answers exactly that, column for column: there
        // is nothing left for the frontend's check to refuse.
        parameters_t p{s.resource};
        p.emplace_back(s.resource, std::int64_t{2});
        auto ran = execute_scheduler_prepared(s, star_stmt, std::move(p));
        require_ran(ran, 1, 2);
        const auto executed = ran.value().chunks.front().types();
        REQUIRE(executed.size() == star_schema.child_types().size());
        for (size_t i = 0; i < executed.size(); ++i) {
            INFO("column " << i << ": described " << static_cast<int>(star_schema.child_types()[i].type())
                           << ", executed " << static_cast<int>(executed[i].type()));
            REQUIRE(executed[i].type() == star_schema.child_types()[i].type());
        }

        // An expression over the placeholder takes its first operand's type —
        // the column the probe answered for — and the execution answers the
        // same, so there is nothing for the frontend's check to refuse here
        // either.
        const session_hash_t shift_stmt = id++;
        auto shifted =
            prepare_scheduler_sql(s, shift_stmt, "SELECT id + $1 AS shifted FROM dmldb.people WHERE id = 2");
        require_prepared(shifted, 1);
        REQUIRE(shifted.value().schema.child_types().size() == 1);
        INFO("described shifted: " << static_cast<int>(shifted.value().schema.child_types()[0].type()));
        REQUIRE(shifted.value().schema.child_types()[0].type() == components::types::logical_type::BIGINT);
        parameters_t sp{s.resource};
        sp.emplace_back(s.resource, std::int64_t{10});
        auto shift_ran = execute_scheduler_prepared(s, shift_stmt, std::move(sp));
        require_ran(shift_ran, 1, 1);
        REQUIRE(shift_ran.value().chunks.front().types()[0].type() ==
                shifted.value().schema.child_types()[0].type());

        // A DML answers an affected count, which has no result schema —
        // placeholders or not.
        auto dml = prepare_scheduler_sql(s, id++, "DELETE FROM dmldb.people WHERE id = $1");
        require_prepared(dml, 1);
        REQUIRE(dml.value().schema.type() != components::types::logical_type::STRUCT);
    });
}

// What the probe path still cannot answer, and therefore still leaves to the
// frontend's check on the executed result (`same_wire_shape` → 0A000 "cached
// plan must not change result type"): the type of an expression over `$n`,
// which would have to come from the value the binder holds, and the width of a
// JOIN, whose schema is merged BY NAME while the engine answers both key
// columns. Both are described as something the execution does not match, which
// is exactly what that check is for.
TEST_CASE("prepared matrix: what a parameterized prepare still cannot describe") {
    with_scheduler_stack("/tmp/test_dml_prepared_undescribable", [](scheduler_stack s) {
        session_hash_t id = 8980;
        seed_people(s, id, 3);
        std::string err;
        const bool created = run_scheduler_sql(s, id++, "CREATE TABLE dmldb.tags (id bigint, name string);", err);
        INFO("CREATE TABLE tags: " << err);
        REQUIRE(created);
        const bool tagged =
            run_scheduler_sql(s, id++, "INSERT INTO dmldb.tags (id, name) VALUES (1, 'x'), (2, 'y');", err);
        INFO("INSERT tags: " << err);
        REQUIRE(tagged);

        // A CALL over a column is a column of the answer under its own name,
        // and its type is none this computation can name — the engine's
        // kernels are not consulted, only the input columns the probe answered
        // — so it stays NA while the execution answers a real type. (An
        // arithmetic expression is different: it takes its first operand's
        // type, which is right, and the case above pins that.)
        const session_hash_t expr_stmt = id++;
        auto expr = prepare_scheduler_sql(s, expr_stmt, "SELECT length(name) AS n FROM dmldb.people WHERE id = $1");
        require_prepared(expr, 1);
        REQUIRE(expr.value().schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(expr.value().schema.child_types().size() == 1);
        INFO("described call: " << static_cast<int>(expr.value().schema.child_types()[0].type()));
        REQUIRE(expr.value().schema.child_types()[0].type() == components::types::logical_type::NA);

        parameters_t ep{s.resource};
        ep.emplace_back(s.resource, std::int64_t{2});
        auto expr_ran = execute_scheduler_prepared(s, expr_stmt, std::move(ep));
        INFO("call execute error: " << expr_ran.error().what.c_str());
        REQUIRE_FALSE(expr_ran.has_error());
        INFO("executed call: " << static_cast<int>(expr_ran.value().chunks.front().types()[0].type()));
        REQUIRE(expr_ran.value().chunks.front().types()[0].type() != components::types::logical_type::NA);

        // A JOIN of two relations sharing a column name: the merge keeps one
        // `id` and one `name`, the engine answers all four columns.
        const session_hash_t join_stmt = id++;
        auto joined = prepare_scheduler_sql(
            s,
            join_stmt,
            "SELECT * FROM dmldb.people JOIN dmldb.tags ON people.id = tags.id WHERE people.id = $1");
        require_prepared(joined, 1);
        REQUIRE(joined.value().schema.type() == components::types::logical_type::STRUCT);
        INFO("described join columns: " << joined.value().schema.child_types().size());
        REQUIRE(joined.value().schema.child_types().size() == 2);

        parameters_t jp{s.resource};
        jp.emplace_back(s.resource, std::int64_t{2});
        auto join_ran = execute_scheduler_prepared(s, join_stmt, std::move(jp));
        INFO("join execute error: " << join_ran.error().what.c_str());
        REQUIRE_FALSE(join_ran.has_error());
        INFO("executed join columns: " << join_ran.value().column_count());
        REQUIRE(join_ran.value().column_count() == 4);
        REQUIRE(join_ran.value().column_count() != joined.value().schema.child_types().size());
    });
}

// A set operation answers rows as much as an aggregate does, so it is described
// like any other query — never as "this statement returns no rows". Its columns
// are its FIRST branch's, which is what SQL names the result after, and the
// frontends hold the executed result to that description as usual. Before this,
// every UNION — with a placeholder or without — answered an unresolved schema,
// and the only way a frontend can send that is NoData: the answer a client
// cannot recover from, since it states there are no rows and lib/pq then reads a
// row of zero values instead of raising.
TEST_CASE("prepared matrix: a set operation is described from its first branch") {
    with_scheduler_stack("/tmp/test_dml_prepared_setop", [](scheduler_stack s) {
        session_hash_t id = 9200;
        seed_people(s, id, 3);
        std::string err;
        const bool created = run_scheduler_sql(s, id++, "CREATE TABLE dmldb.tags (id bigint, name string);", err);
        INFO("CREATE TABLE tags: " << err);
        REQUIRE(created);
        const bool tagged =
            run_scheduler_sql(s, id++, "INSERT INTO dmldb.tags (id, name) VALUES (1, 'x'), (2, 'y');", err);
        INFO("INSERT tags: " << err);
        REQUIRE(tagged);

        // Without a placeholder the engine's plan validation is no help either:
        // that pass stamps the aggregate consumer, not a union root, so this is
        // computed from the branch as well.
        auto plain = prepare_scheduler_sql(s, id++, "SELECT id FROM dmldb.people UNION ALL SELECT id FROM dmldb.tags");
        INFO("prepare error: " << plain.error().what.c_str());
        REQUIRE_FALSE(plain.has_error());
        REQUIRE(plain.value().schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(plain.value().schema.child_types().size() == 1);
        INFO("described union column: " << static_cast<int>(plain.value().schema.child_types()[0].type()));
        REQUIRE(plain.value().schema.child_types()[0].has_alias());
        REQUIRE(plain.value().schema.child_types()[0].alias() == "id");
        REQUIRE(plain.value().schema.child_types()[0].type() == components::types::logical_type::BIGINT);

        // With one, executed under that very description: same width, same type.
        const session_hash_t stmt = id++;
        auto parameterized = prepare_scheduler_sql(
            s, stmt, "SELECT id FROM dmldb.people WHERE id = $1 UNION ALL SELECT id FROM dmldb.tags");
        require_prepared(parameterized, 1);
        REQUIRE(parameterized.value().schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(parameterized.value().schema.child_types().size() == 1);
        const auto described = parameterized.value().schema.child_types()[0];
        REQUIRE(described.alias() == "id");
        REQUIRE(described.type() == components::types::logical_type::BIGINT);

        parameters_t p{s.resource};
        p.emplace_back(s.resource, std::int64_t{2});
        auto ran = execute_scheduler_prepared(s, stmt, std::move(p));
        INFO("execute error: " << ran.error().what.c_str());
        REQUIRE_FALSE(ran.has_error());
        REQUIRE(ran.value().column_count() == 1);
        INFO("executed union column: " << static_cast<int>(ran.value().chunks.front().types()[0].type()));
        REQUIRE(ran.value().chunks.front().types()[0].type() == described.type());

        // INTERSECT / EXCEPT never reach the prepare at all — the engine's
        // transformer refuses them — so they raise no schema question.
        auto intersect =
            prepare_scheduler_sql(s, id++, "SELECT id FROM dmldb.people INTERSECT SELECT id FROM dmldb.tags");
        REQUIRE(intersect.has_error());
    });
}

TEST_CASE("prepared matrix: a wrong number of parameters") {
    with_scheduler_stack("/tmp/test_dml_prepared_count", [](scheduler_stack s) {
        session_hash_t id = 9000;
        seed_people(s, id, 3);
        const std::string sql = "SELECT id, name FROM dmldb.people WHERE id = $1 AND name = $2";

        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, sql), 2);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{1});
            require_refused(execute_scheduler_prepared(s, stmt, std::move(p)),
                            core::error_code_t::sql_parse_error,
                            "Argument binding failed: Not all parameters were bound: $2");
        }
        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, sql), 2);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{1});
            p.emplace_back(s.resource, std::string{"name_0"});
            p.emplace_back(s.resource, std::int64_t{3});
            require_refused(execute_scheduler_prepared(s, stmt, std::move(p)),
                            core::error_code_t::sql_parse_error,
                            "Argument binding failed: Parameter with id=3 not found");
        }
        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, sql), 2);
            require_refused(execute_scheduler_prepared(s, stmt, parameters_t{s.resource}),
                            core::error_code_t::sql_parse_error,
                            "Argument binding failed: Not all parameters were bound: $1 $2");
        }
        {
            // An execute without Bind is refused by the Worker before the plan
            // reaches the engine.
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, sql), 2);
            require_refused(execute_scheduler_statement(s, stmt),
                            core::error_code_t::invalid_parameter,
                            "prepared statement executed without binding its 2 parameter(s)");
        }
        {
            const session_hash_t stmt = id++;
            require_prepared(prepare_scheduler_sql(s, stmt, sql), 2);
            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{1});
            p.emplace_back(s.resource, std::string{"name_0"});
            auto r = execute_scheduler_prepared(s, stmt, std::move(p));
            require_ran(r, 1, 2);
            REQUIRE(ids_of(r.value()) == std::vector<std::int64_t>{1});
        }
    });
}

TEST_CASE("session_payload: the accessors stay defined on an emptied batch") {
    // `chunks` is a public, mutable vector: a caller can clear it after the
    // constructor's batch check, so the accessors must not read front() blindly.
    session_payload payload{std::pmr::new_delete_resource()};
    REQUIRE(payload.column_count() == 0);
    payload.chunks.clear();
    REQUIRE(payload.column_count() == 0);
    REQUIRE(payload.size() == 0);
    REQUIRE(payload.empty());
}


// A DML with RETURNING answers ROWS, so it HAS a result schema and the prepare
// must describe it. NoData is not an available answer: it states that the
// statement returns no rows, and the clients that live by a statement's
// description believe it — lib/pq hands the caller a row of zero values instead
// of raising, so the rows are lost without an error.
//
// It is described from the PLAN. Describing it the way Describe(portal)
// describes a SELECT — by running it — would WRITE, and write a second time for
// a client that binds the portal again. So the prepare probes the TARGET
// RELATION for its columns (`SELECT * FROM db.rel LIMIT 0`, which reads no row)
// and resolves the RETURNING list against them. The row count asserted around
// every prepare below is what records that the description wrote nothing.
TEST_CASE("prepared matrix: a DML with RETURNING is described from its target columns") {
    with_scheduler_stack("/tmp/test_dml_returning_describe", [](scheduler_stack s) {
        session_hash_t id = 9300;
        seed_people(s, id, 3);
        REQUIRE(engine_row_count(s, "dmldb", "people") == 3);

        const auto bigint = components::types::logical_type::BIGINT;
        const auto text = components::types::logical_type::STRING_LITERAL;
        const auto structure = components::types::logical_type::STRUCT;

        // A RETURNING column list: the columns it names, under the names and the
        // types the target table carries them by.
        {
            const session_hash_t stmt = id++;
            auto prepared = prepare_scheduler_sql(
                s, stmt, "INSERT INTO dmldb.people (id, name) VALUES (60, 'sixty') RETURNING id, name");
            require_prepared(prepared, 0);
            const auto& schema = prepared.value().schema;
            REQUIRE(schema.type() == structure);
            REQUIRE(schema.child_types().size() == 2);
            REQUIRE(schema.child_types()[0].alias() == "id");
            REQUIRE(schema.child_types()[1].alias() == "name");
            REQUIRE(schema.child_types()[0].type() == bigint);
            REQUIRE(schema.child_types()[1].type() == text);

            // Describing wrote nothing: the row is still not in the table.
            REQUIRE(engine_row_count(s, "dmldb", "people") == 3);

            // ...and the execution answers exactly the shape that was described,
            // so the frontend's check on it has nothing to refuse.
            auto ran = execute_scheduler_statement(s, stmt);
            require_ran(ran, 1, 2);
            const auto executed = ran.value().chunks.front().types();
            REQUIRE(executed.size() == schema.child_types().size());
            for (size_t i = 0; i < executed.size(); ++i) {
                INFO("column " << i << ": described " << static_cast<int>(schema.child_types()[i].type())
                               << ", executed " << static_cast<int>(executed[i].type()));
                REQUIRE(executed[i].type() == schema.child_types()[i].type());
            }
            REQUIRE(engine_row_count(s, "dmldb", "people") == 4);
        }

        // RETURNING * is the target's own columns, in its own order — what the
        // engine expands it to. It names nothing on the plan, exactly as a
        // SELECT's star does, so it is the case a plan-only reading would answer
        // with no columns at all.
        {
            const session_hash_t stmt = id++;
            auto prepared =
                prepare_scheduler_sql(s, stmt, "INSERT INTO dmldb.people (id, name) VALUES (61, 'one') RETURNING *");
            require_prepared(prepared, 0);
            const auto& schema = prepared.value().schema;
            REQUIRE(schema.type() == structure);
            REQUIRE(schema.child_types().size() == 2);
            REQUIRE(schema.child_types()[0].alias() == "id");
            REQUIRE(schema.child_types()[1].alias() == "name");
            REQUIRE(schema.child_types()[0].type() == bigint);
            REQUIRE(schema.child_types()[1].type() == text);
            REQUIRE(engine_row_count(s, "dmldb", "people") == 4);

            auto ran = execute_scheduler_statement(s, stmt);
            require_ran(ran, 1, 2);
            REQUIRE(engine_row_count(s, "dmldb", "people") == 5);
        }

        // An output alias renames the column but does not retype it: the type is
        // the column the expression READS, the name the one it is projected by.
        {
            const session_hash_t stmt = id++;
            auto prepared =
                prepare_scheduler_sql(s, stmt, "INSERT INTO dmldb.people (id, name) VALUES (62, 'two') RETURNING id AS pk");
            require_prepared(prepared, 0);
            const auto& schema = prepared.value().schema;
            REQUIRE(schema.type() == structure);
            REQUIRE(schema.child_types().size() == 1);
            REQUIRE(schema.child_types()[0].alias() == "pk");
            REQUIRE(schema.child_types()[0].type() == bigint);
            REQUIRE(engine_row_count(s, "dmldb", "people") == 5);

            require_ran(execute_scheduler_statement(s, stmt), 1, 1);
            REQUIRE(engine_row_count(s, "dmldb", "people") == 6);
        }

        // UPDATE and DELETE are described the same way, and describing them
        // changes no row either.
        {
            const session_hash_t stmt = id++;
            auto prepared =
                prepare_scheduler_sql(s, stmt, "UPDATE dmldb.people SET name = 'renamed' WHERE id = 60 RETURNING id");
            require_prepared(prepared, 0);
            REQUIRE(prepared.value().schema.type() == structure);
            REQUIRE(prepared.value().schema.child_types().size() == 1);
            REQUIRE(prepared.value().schema.child_types()[0].alias() == "id");
            REQUIRE(prepared.value().schema.child_types()[0].type() == bigint);

            auto unchanged = run_scheduler_sql_payload(s, id++, "SELECT id FROM dmldb.people WHERE name = 'renamed';");
            require_ran(unchanged, 0, 1);

            require_ran(execute_scheduler_statement(s, stmt), 1, 1);
            auto renamed = run_scheduler_sql_payload(s, id++, "SELECT id FROM dmldb.people WHERE name = 'renamed';");
            require_ran(renamed, 1, 1);
            REQUIRE(ids_of(renamed.value()) == std::vector<std::int64_t>{60});
        }
        {
            const session_hash_t stmt = id++;
            auto prepared =
                prepare_scheduler_sql(s, stmt, "DELETE FROM dmldb.people WHERE id = 61 RETURNING id, name");
            require_prepared(prepared, 0);
            REQUIRE(prepared.value().schema.type() == structure);
            REQUIRE(prepared.value().schema.child_types().size() == 2);
            REQUIRE(engine_row_count(s, "dmldb", "people") == 6);

            require_ran(execute_scheduler_statement(s, stmt), 1, 2);
            REQUIRE(engine_row_count(s, "dmldb", "people") == 5);
        }

        // A placeholder changes none of it: the RETURNING columns come off the
        // plan and the target's own columns, neither of which a `$n` touches, so
        // the statement carries both its schema and its parameter count.
        {
            const session_hash_t stmt = id++;
            auto prepared = prepare_scheduler_sql(
                s, stmt, "INSERT INTO dmldb.people (id, name) VALUES ($1, $2) RETURNING id, name");
            require_prepared(prepared, 2);
            const auto& schema = prepared.value().schema;
            REQUIRE(schema.type() == structure);
            REQUIRE(schema.child_types().size() == 2);
            REQUIRE(schema.child_types()[0].type() == bigint);
            REQUIRE(schema.child_types()[1].type() == text);
            REQUIRE(engine_row_count(s, "dmldb", "people") == 5);

            parameters_t p{s.resource};
            p.emplace_back(s.resource, std::int64_t{70});
            p.emplace_back(s.resource, std::string{"seventy"});
            auto ran = execute_scheduler_prepared(s, stmt, std::move(p));
            require_ran(ran, 1, 2);
            const auto executed = ran.value().chunks.front().types();
            REQUIRE(executed.size() == 2);
            REQUIRE(executed[0].type() == schema.child_types()[0].type());
            REQUIRE(executed[1].type() == schema.child_types()[1].type());
            REQUIRE(engine_row_count(s, "dmldb", "people") == 6);
        }

        // The control: the same statements WITHOUT a RETURNING clause answer an
        // affected count, which has no result schema, and must keep answering
        // that — the guard is the RETURNING list, not the statement kind.
        auto plain = prepare_scheduler_sql(s, id++, "DELETE FROM dmldb.people WHERE id = 62");
        require_prepared(plain, 0);
        REQUIRE(plain.value().schema.type() != structure);
    });
}

// The dependency map a parameterized statement is described through carries one
// slot per DISTINCT relation: IDataManager::get_schema answers a vector of that
// many probes and the schema computation indexes into it by these numbers. A
// self-join is the shape where numbering by NODE instead diverges — two
// aggregates over one relation share a key and so one slot, while a node counter
// keeps counting, and a third relation is then handed an index one past the
// vector the map sized. That is a bounds assert in a debug build and a write
// past the end of the probe vector in a release one, where the slot then read
// back is missing and the prepare fails with "OtterBrix collection is missing in
// catalog". Two aggregates over one relation plus a third relation is the
// smallest statement that reaches it; a two-table JOIN never does.
//
// The JOIN ORDER is part of the case. The plan is left-deep — `A JOIN B JOIN C`
// is join(join(A, B), C) — and the walk is breadth-first, so its leaves are
// reached C, A, B. A node counter only runs past the end of the map when the
// REPEATED relation is reached before the last distinct one, so the repeat here
// is A and C and the third relation is B. Written the obvious way round,
// `people JOIN people JOIN tags` reaches its repeat last and stays in bounds
// under either numbering — it would pin nothing.
TEST_CASE("prepared matrix: a self join numbers one probe per relation not per node") {
    with_scheduler_stack("/tmp/test_dml_self_join_probes", [](scheduler_stack s) {
        session_hash_t id = 9400;
        seed_people(s, id, 3);
        std::string err;
        const bool created = run_scheduler_sql(s, id++, "CREATE TABLE dmldb.tags (id bigint, name string);", err);
        INFO("CREATE TABLE tags: " << err);
        REQUIRE(created);
        const bool tagged =
            run_scheduler_sql(s, id++, "INSERT INTO dmldb.tags (id, name) VALUES (1, 'x'), (2, 'y');", err);
        INFO("INSERT tags: " << err);
        REQUIRE(tagged);

        // people, then tags, then people again: three join leaves over two
        // relations, plus the root aggregate — and the repeat placed so the walk
        // reaches it before the last distinct relation (see above).
        const session_hash_t stmt = id++;
        auto prepared = prepare_scheduler_sql(s,
                                              stmt,
                                              "SELECT p1.id, t.name, p2.name FROM dmldb.people p1 "
                                              "JOIN dmldb.tags t ON t.id = p1.id "
                                              "JOIN dmldb.people p2 ON p2.id = p1.id WHERE p1.id = $1");
        require_prepared(prepared, 1);
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        // Merged by name across the three leaves, which is the JOIN width the
        // computation can answer; the engine answers every column of every leaf.
        INFO("described join columns: " << schema.child_types().size());
        REQUIRE(schema.child_types().size() == 2);
        REQUIRE(schema.child_types()[0].type() == components::types::logical_type::BIGINT);
        REQUIRE(schema.child_types()[1].type() == components::types::logical_type::STRING_LITERAL);

        parameters_t p{s.resource};
        p.emplace_back(s.resource, std::int64_t{1});
        auto ran = execute_scheduler_prepared(s, stmt, std::move(p));
        INFO("execute error: " << ran.error().what.c_str());
        REQUIRE_FALSE(ran.has_error());
        REQUIRE(ran.value().column_count() == 3);
    });
}
