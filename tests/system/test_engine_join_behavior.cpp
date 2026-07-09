#include "otterbrix/config.hpp"
#include <catch2/catch.hpp>
#include <iostream>
#include <otterbrix/otterbrix.hpp>

// a13 JOIN output keeps BOTH join key columns (id, x, id, y) — the computed
// federated schema dedups by name, and ChunkBatchReader keeps the first.
TEST_CASE("a13 join output keeps both join key columns") {
    auto cfg = make_create_config("/tmp/otterstax_join_probe");
    auto inst = otterbrix::make_otterbrix(cfg);
    auto* d = inst->dispatcher();
    auto run = [&](const char* q) {
        auto c = d->execute_sql(otterbrix::session_id_t(), q);
        std::cout << "Q: " << q << " -> err=" << (c ? c->is_error() : true);
        if (c && c->is_error())
            std::cout << " what=" << c->get_error().what.c_str();
        if (c && !c->is_error())
            std::cout << " rows=" << c->size();
        std::cout << "\n";
        return c;
    };
    run("CREATE DATABASE db;");
    run("CREATE TABLE db.a (id INT, x INT);");
    run("CREATE TABLE db.b (id INT, y INT);");
    run("INSERT INTO db.a (id, x) VALUES (1, 10), (2, 20);");
    run("INSERT INTO db.b (id, y) VALUES (1, 100), (2, 200);");
    auto g = run("SELECT a.id, COUNT(b.y) AS cnt, AVG(b.y) AS av FROM db.a JOIN db.b ON a.id = b.id GROUP BY a.id "
                 "ORDER BY cnt DESC;");
    if (g && !g->is_error()) {
        std::cout << "groupby type_data count=" << g->type_data().size() << " rows=" << g->size() << "\n";
    }
    auto c = run("SELECT * FROM db.a JOIN db.b ON a.id = b.id;");
    if (c && !c->is_error()) {
        std::cout << "type_data count=" << c->type_data().size() << " names:";
        for (auto& t : c->type_data()) std::cout << " '" << t.alias() << "'";
        std::cout << "\nchunk columns=" << c->chunks().front().data.size() << " cardinality=" << c->size()
                  << "\n";
    }
}

TEST_CASE("engine string group by on real tables") {
    auto cfg = make_create_config("/tmp/otterstax_strgrp_probe");
    auto inst = otterbrix::make_otterbrix(cfg);
    auto* d = inst->dispatcher();
    auto run = [&](const char* q) {
        auto c = d->execute_sql(otterbrix::session_id_t(), q);
        std::cout << "Q: " << q << " -> err=" << (c ? c->is_error() : true);
        if (c && c->is_error())
            std::cout << " what=" << c->get_error().what.c_str();
        if (c && !c->is_error())
            std::cout << " rows=" << c->size();
        std::cout << "\n";
        return c;
    };
    run("CREATE DATABASE sdb;");
    run("CREATE TABLE sdb.c (cid INT, cname TEXT);");
    run("CREATE TABLE sdb.p (pid INT, cid INT, price DOUBLE);");
    run("INSERT INTO sdb.c (cid, cname) VALUES (1, 'alpha'), (2, 'beta');");
    run("INSERT INTO sdb.p (pid, cid, price) VALUES (1, 1, 10.5), (2, 1, 20.5), (3, 2, 30.0);");
    run("SELECT c.cname, COUNT(p.pid) AS cnt, AVG(p.price) AS av FROM sdb.c c JOIN sdb.p p ON p.cid = c.cid GROUP BY "
        "c.cname ORDER BY cnt DESC;");
}

// The buggy operator_group_t fallback is NOT reachable from plain SQL: every
// SQL shape that would route into it (derived-table keys, joins of subqueries,
// expression keys) is rejected by the engine with a clean error before the
// group operator runs. The minimal reproduction therefore stays the
// programmatic node_data plan in test_mixed_plan_engine.cpp.
TEST_CASE("engine string group by via pure SQL fallback shapes") {
    auto cfg = make_create_config("/tmp/otterstax_sqlgrp_probe");
    auto inst = otterbrix::make_otterbrix(cfg);
    auto* d = inst->dispatcher();
    auto run = [&](const char* tag, const char* q) {
        auto c = d->execute_sql(otterbrix::session_id_t(), q);
        std::cout << "[" << tag << "] err=" << (c ? c->is_error() : true);
        if (c && c->is_error()) {
            std::cout << " what=" << c->get_error().what.c_str();
        }
        if (c && !c->is_error()) {
            std::cout << " rows=" << c->size();
        }
        std::cout << "\n";
        return c;
    };
    run("setup1", "CREATE DATABASE gdb;");
    run("setup2", "CREATE TABLE gdb.c (cid INT, cname TEXT);");
    run("setup3", "CREATE TABLE gdb.p (pid INT, cid INT, price DOUBLE);");
    run("setup4", "INSERT INTO gdb.c (cid, cname) VALUES (1, 'alpha'), (2, 'beta');");
    run("setup5", "INSERT INTO gdb.p (pid, cid, price) VALUES (1, 1, 10.5), (2, 1, 20.5), (3, 2, 30.0);");

    // Each shape must come back as a cursor (error or success) — never a crash.
    REQUIRE(run("derived", "SELECT t.cname, COUNT(*) AS cnt FROM (SELECT * FROM gdb.c) t GROUP BY t.cname;"));
    REQUIRE(run("derived_join",
                "SELECT t.cname, COUNT(s.pid) AS cnt, AVG(s.price) AS av "
                "FROM (SELECT * FROM gdb.c) t JOIN (SELECT * FROM gdb.p) s ON s.cid = t.cid "
                "GROUP BY t.cname ORDER BY cnt DESC;"));
    REQUIRE(run("coalesce_key",
                "SELECT COALESCE(c.cname, 'none') AS k, COUNT(*) AS cnt FROM gdb.c c "
                "GROUP BY COALESCE(c.cname, 'none');"));
}
