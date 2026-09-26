// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Scheduler::prepare_plan and Scheduler::execute_plan: the entry points a
// pre-built plan takes instead of SQL text. The Spark Connect frontend
// translates a DataFrame straight into a logical plan; AnalyzePlan answers
// df.schema through prepare_plan, ExecutePlan runs the plan through
// execute_plan. prepare_plan must DESCRIBE the plan and never run it — a schema
// request that executed its statement would read the backend's rows on every
// df.schema. Driven through the real Scheduler→Worker→catalog stack over a
// typed PostgreSQL mock connector that records its describe probes apart from
// its data queries; the plan is built by the real GreenplumParser, the parse
// the Worker applies to SQL text.
//
// spark.sql() is told apart the same way — without running anything: the
// frontend parses the statement (classify_sql), hands a query back to the
// client unrun and runs any other statement once. The plan shapes the frontend
// answers without translating them (that classification, a SqlCommand's text
// and result relation, a SQL relation without arguments, the empty
// LocalRelation, the name of an unsupported command) are pinned last.

#include "catalog/catalog_manager.hpp"
#include "frontend/spark_connect_server/service.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "otterbrix/parser/parser.hpp"
#include "scheduler_stack.hpp"

#include <catch2/catch_all.hpp>
#include <google/protobuf/util/message_differencer.h>
#include <libpq-fe.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using otterstax::test::await_session;
using otterstax::test::close_scheduler_statement;
using otterstax::test::engine_row_count;
using otterstax::test::execute_scheduler_statement;
using otterstax::test::init_fresh_test_otterbrix;
using otterstax::test::make_az_scheduler;
using otterstax::test::prepare_scheduler_sql;
using otterstax::test::run_scheduler_sql;
using otterstax::test::run_scheduler_sql_payload;
using otterstax::test::scheduler_stack;
using otterstax::test::worker_pool_size;

namespace {

    constexpr const char* kUid = "planpg";
    // Every column of the one remote table, in backend order: the projection
    // the mock answers the describe probe and the data query with.
    constexpr std::string_view kSql = "SELECT id, name, price FROM planpg.pgdb.public.items;";

    constexpr Oid kInt4Oid = 23;
    constexpr Oid kFloat8Oid = 701;
    constexpr Oid kVarcharOid = 1043;

    // The discovered table, in backend column order: (name, PostgreSQL type
    // oid, the logical type pg_to_struct / pg_to_chunk map it to).
    struct item_column {
        const char* name;
        Oid typid;
        components::types::logical_type type;
    };

    constexpr std::array<item_column, 3> kColumns{{
        {"id", kInt4Oid, components::types::logical_type::INTEGER},
        {"name", kVarcharOid, components::types::logical_type::STRING_LITERAL},
        {"price", kFloat8Oid, components::types::logical_type::DOUBLE},
    }};

    // One row of the backend table.
    struct item_row {
        int32_t id;
        std::string name;
        double price;

        bool operator==(const item_row&) const = default;
    };

    const std::array<item_row, 2> kRows{{{1, "widget", 9.5}, {2, "gadget", 19.25}}};

    // The wrap the Worker has the backend describe a statement with
    // (db::make_prepare_probe).
    constexpr std::string_view kProbePrefix = "SELECT * FROM (";

    // Shared with the connector through file-scope state (connector_factory is
    // a plain function pointer); every access is ordered by the future the
    // io-thread query settles.
    std::mutex g_mutex;
    std::vector<std::string> g_probe_queries;
    std::vector<std::string> g_data_queries;

    void reset_queries() {
        std::lock_guard guard(g_mutex);
        g_probe_queries.clear();
        g_data_queries.clear();
    }

    std::vector<std::string> probe_queries() {
        std::lock_guard guard(g_mutex);
        return g_probe_queries;
    }

    std::vector<std::string> data_queries() {
        std::lock_guard guard(g_mutex);
        return g_data_queries;
    }

    // The table's attributes and no tuples: what the backend answers the
    // catalog's discovery probe and the LIMIT-0 describe probe with.
    std::unique_ptr<PGresult, decltype(&PQclear)> make_header() {
        std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), &PQclear);
        std::array<PGresAttDesc, kColumns.size()> attrs{};
        for (std::size_t i = 0; i < kColumns.size(); ++i) {
            attrs[i].name = const_cast<char*>(kColumns[i].name);
            attrs[i].tableid = 0;
            attrs[i].columnid = 0;
            attrs[i].format = 0;
            attrs[i].typid = kColumns[i].typid;
            attrs[i].typlen = -1;
            attrs[i].atttypmod = -1;
        }
        PQsetResultAttrs(result.get(), static_cast<int>(attrs.size()), attrs.data());
        return result;
    }

    // Answers discovery and the describe probe with the table's columns and the
    // data query with kRows, recording the probes and the data queries apart.
    class recording_pg_connector final : public pg::IConnector {
    public:
        recording_pg_connector(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias)
            : resource_(resource)
            , params_(std::move(params))
            , alias_(std::move(alias)) {}

        pg::Status status() const noexcept override { return pg::Status::Connected; }
        pg::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        // The data path: the statement a remote SELECT executes.
        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<components::vector::data_chunk_t>(PGresult*)>) override {
            using components::types::complex_logical_type;
            using components::types::logical_value_t;
            {
                std::lock_guard guard(g_mutex);
                g_data_queries.emplace_back(query);
            }
            std::pmr::vector<complex_logical_type> fields(resource_);
            for (const auto& column : kColumns) {
                fields.emplace_back(column.type, column.name);
            }
            auto chunk = std::make_unique<components::vector::data_chunk_t>(resource_, fields, kRows.size());
            for (std::size_t row = 0; row < kRows.size(); ++row) {
                chunk->set_value(0, row, logical_value_t(resource_, kRows[row].id));
                chunk->set_value(1, row, logical_value_t(resource_, std::string_view{kRows[row].name}));
                chunk->set_value(2, row, logical_value_t(resource_, kRows[row].price));
            }
            chunk->set_cardinality(kRows.size());
            co_return std::move(chunk);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            co_return int64_t{0};
        }

        // Discovery (the pg_enum query lists no enums, the table's schema probe
        // answers its columns) and the describe probe, told apart by the wrap
        // describe puts around the statement.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                std::unique_ptr<PGresult, decltype(&PQclear)> empty(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                    &PQclear);
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(empty.get()));
            }
            if (query.substr(0, kProbePrefix.size()) == kProbePrefix) {
                std::lock_guard guard(g_mutex);
                g_probe_queries.emplace_back(query);
            }
            auto header = make_header();
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(header.get()));
        }

    private:
        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<pg::IConnector>
    recording_pg_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<recording_pg_connector>(resource, std::move(params), std::move(alias));
    }

    // Real engine, real parser, real catalog; the PostgreSQL connection is the
    // recording mock registered under kUid with its single table discovered
    // eagerly.
    class plan_stack_owner {
    public:
        // Member order is construction order: every actor is spawned after the
        // ones whose addresses it takes, and the pmr deleters carry the engine's
        // resource, so the members are initialised here rather than assigned.
        explicit plan_stack_owner(const std::string& data_dir)
            : data_dir_(data_dir)
            , otterbrix_(init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address()))
            , pg_conn_(std::make_unique<pg::ConnectorManager>(resource_,
                                                              catalog_->address(),
                                                              &recording_pg_factory,
                                                              /*pool_size*/ 1))
            , pg_mgr_(actor_zeta::spawn<db::PostgressManager>(resource_, pg_conn_.get()))
            , scheduler_(spawn_scheduler()) {}

        plan_stack_owner(const plan_stack_owner&) = delete;
        plan_stack_owner& operator=(const plan_stack_owner&) = delete;

        ~plan_stack_owner() {
            scheduler_.reset();
            pg_mgr_.reset();
            pg_conn_.reset();
            catalog_.reset();
            otb_mgr_.reset();
            az_scheduler_->stop();
            otterbrix_.reset();
            std::filesystem::remove_all(data_dir_);
        }

        scheduler_stack stack() const { return scheduler_stack{scheduler_->address(), otterbrix_, resource_}; }

    private:
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> spawn_scheduler() {
            catalog_->set_backend_managers(actor_zeta::address_t::empty_address(),
                                           pg_mgr_->address(),
                                           actor_zeta::address_t::empty_address());

            conn::api_server::PgConnectionParams params;
            params.alias = kUid;
            params.host = "localhost";
            params.port = "5432";
            params.username = "user";
            params.password = "pass";
            params.database = "pgdb";
            params.schema = "public";
            params.table = "items";
            auto added = pg_conn_->addConnection(params);
            INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());

            return actor_zeta::spawn<Scheduler>(resource_,
                                                az_scheduler_.get(),
                                                worker_pool_size(),
                                                &make_parser,
                                                actor_zeta::address_t::empty_address(), // sql
                                                pg_mgr_->address(),
                                                actor_zeta::address_t::empty_address(), // ch
                                                otb_mgr_->address(),
                                                catalog_->address(),
                                                actor_zeta::address_t::empty_address(),  // s3
                                                actor_zeta::address_t::empty_address()); // file
        }

        std::string data_dir_;
        db::otterbrix_engine_ptr otterbrix_;
        std::pmr::memory_resource* resource_{nullptr};
        std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otb_mgr_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<pg::ConnectorManager> pg_conn_;
        std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> pg_mgr_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
    };

    // The plan a pre-built-plan frontend hands the Scheduler, parsed the way
    // the Worker parses SQL text.
    ParsedQueryDataPtr parse_plan(GreenplumParser& parser, std::string_view sql) {
        auto parsed = parser.parse(std::string{sql});
        INFO("parse: " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(parsed.has_error());
        return std::move(parsed.value());
    }

    core::result_wrapper_t<session_payload>
    prepare_scheduler_plan(const scheduler_stack& s, session_hash_t id, ParsedQueryDataPtr plan) {
        auto [ns, fut] = actor_zeta::send(s.scheduler, &Scheduler::prepare_plan, id, std::move(plan));
        return await_session(std::move(fut), std::chrono::milliseconds(10000), s.resource);
    }

    core::result_wrapper_t<session_payload>
    execute_scheduler_plan(const scheduler_stack& s, session_hash_t id, ParsedQueryDataPtr plan) {
        auto [ns, fut] = actor_zeta::send(s.scheduler, &Scheduler::execute_plan, id, std::move(plan));
        return await_session(std::move(fut), std::chrono::milliseconds(10000), s.resource);
    }

    // The described schema is the STRUCT of the table's columns, in backend
    // order, with the discovered types.
    void require_item_schema(const components::types::complex_logical_type& schema) {
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == kColumns.size());
        for (std::size_t i = 0; i < kColumns.size(); ++i) {
            const auto& column = schema.child_types()[i];
            INFO("column " << i << " expected " << kColumns[i].name);
            REQUIRE(column.has_alias());
            REQUIRE(column.alias() == kColumns[i].name);
            REQUIRE(column.type() == kColumns[i].type);
        }
    }

    // Every row of a payload across its chunks, ordered by id.
    std::vector<item_row> rows_of(const session_payload& payload) {
        std::vector<item_row> rows;
        for (const auto& chunk : payload.chunks) {
            for (std::size_t row = 0; row < chunk.size(); ++row) {
                rows.push_back(item_row{chunk.value(0, row).value<int32_t>(),
                                        std::string{chunk.value(1, row).value<std::string_view>()},
                                        chunk.value(2, row).value<double>()});
            }
        }
        std::sort(rows.begin(), rows.end(), [](const item_row& a, const item_row& b) { return a.id < b.id; });
        return rows;
    }

    namespace sc = ::spark::connect;

// The SqlCommand / SQL fields a 3.5 client fills are deprecated in the protocol.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

    // The SqlCommand a 3.5 client sends: the statement and its arguments in the
    // flat fields.
    sc::SqlCommand flat_sql_command(const std::string& sql) {
        sc::SqlCommand command;
        command.set_sql(sql);
        (*command.mutable_args())["limit"].set_integer(5);
        command.add_pos_args()->set_string("widget");
        return command;
    }

    // The SQL relation Spark's own server rebuilds from those fields.
    sc::Relation flat_sql_relation(const std::string& sql) {
        sc::Relation relation;
        relation.mutable_sql()->set_query(sql);
        (*relation.mutable_sql()->mutable_args())["limit"].set_integer(5);
        relation.mutable_sql()->add_pos_args()->set_string("widget");
        return relation;
    }

#pragma GCC diagnostic pop

} // namespace

TEST_CASE("Scheduler::prepare_plan: a parsed remote SELECT is described and never executed") {
    plan_stack_owner owner("/tmp/test_prepare_plan_describe");
    auto s = owner.stack();

    // The same statement as SQL text through prepare_schema: what the plan's
    // description must equal.
    const session_hash_t by_sql = 9900;
    auto reference = prepare_scheduler_sql(s, by_sql, std::string{kSql});
    INFO("prepare_schema: " << (reference.has_error() ? reference.error().what.c_str() : "ok"));
    REQUIRE_FALSE(reference.has_error());
    require_item_schema(reference.value().schema);
    REQUIRE_FALSE(close_scheduler_statement(s, by_sql).has_error());

    reset_queries();
    GreenplumParser parser(s.resource);
    const session_hash_t by_plan = 9901;
    auto prepared = prepare_scheduler_plan(s, by_plan, parse_plan(parser, kSql));
    INFO("prepare_plan: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    REQUIRE(prepared.value().tag == T_SelectStmt);
    REQUIRE(prepared.value().parameter_count == 0);
    require_item_schema(prepared.value().schema);

    // Described by the backend's LIMIT-0 probe, never run: no row came back and
    // the data path was not taken.
    REQUIRE(prepared.value().empty());
    INFO("data queries: " << data_queries().size());
    REQUIRE(data_queries().empty());
    REQUIRE(probe_queries().size() == 1);

    // The prepare left the statement on its Worker; close_statement releases it,
    // after which the session has nothing left to execute.
    auto closed = close_scheduler_statement(s, by_plan);
    INFO("close_statement: " << (closed.has_error() ? closed.error().what.c_str() : "ok"));
    REQUIRE_FALSE(closed.has_error());
    auto executed = execute_scheduler_statement(s, by_plan);
    REQUIRE(executed.has_error());
    REQUIRE(executed.error().type == core::error_code_t::invalid_parameter);
    REQUIRE(data_queries().empty());
}

TEST_CASE("Scheduler::execute_plan: a parsed remote SELECT returns the rows execute returns for its SQL") {
    plan_stack_owner owner("/tmp/test_prepare_plan_execute");
    auto s = owner.stack();

    auto by_sql = run_scheduler_sql_payload(s, 9920, std::string{kSql});
    INFO("execute: " << (by_sql.has_error() ? by_sql.error().what.c_str() : "ok"));
    REQUIRE_FALSE(by_sql.has_error());

    GreenplumParser parser(s.resource);
    auto by_plan = execute_scheduler_plan(s, 9921, parse_plan(parser, kSql));
    INFO("execute_plan: " << (by_plan.has_error() ? by_plan.error().what.c_str() : "ok"));
    REQUIRE_FALSE(by_plan.has_error());

    const std::vector<item_row> expected(kRows.begin(), kRows.end());
    REQUIRE(by_sql.value().column_count() == kColumns.size());
    REQUIRE(by_plan.value().column_count() == kColumns.size());
    REQUIRE(rows_of(by_sql.value()) == expected);
    REQUIRE(rows_of(by_plan.value()) == expected);
}

TEST_CASE("spark.sql classification: the parse tells a query from a command and runs nothing") {
    plan_stack_owner owner("/tmp/test_prepare_plan_classify");
    auto s = owner.stack();
    session_hash_t id = 9940;
    std::string err;
    for (const char* setup :
         {"CREATE DATABASE plandb;", "CREATE TABLE plandb.t (id bigint);", "INSERT INTO plandb.t (id) VALUES (1);"}) {
        const bool ran = run_scheduler_sql(s, id++, setup, err);
        INFO(setup << ": " << err);
        REQUIRE(ran);
    }

    // A SELECT over the remote table is a query, and classifying it asked the
    // backend nothing: no data query, not even a describe probe.
    reset_queries();
    auto query = frontend::spark::classify_sql(std::string{kSql}, s.resource);
    INFO("classify_sql(query): " << (query.has_error() ? query.error().what.c_str() : "ok"));
    REQUIRE_FALSE(query.has_error());
    CHECK(query.value() == frontend::spark::sql_statement_t::query);
    CHECK(data_queries().empty());
    CHECK(probe_queries().empty());

    // An INSERT is a command, and classifying it writes nothing: the row is
    // written by the one execute ExecutePlan sends for it.
    const std::string insert = "INSERT INTO plandb.t (id) VALUES (2);";
    auto command = frontend::spark::classify_sql(insert, s.resource);
    INFO("classify_sql(insert): " << (command.has_error() ? command.error().what.c_str() : "ok"));
    REQUIRE_FALSE(command.has_error());
    CHECK(command.value() == frontend::spark::sql_statement_t::command);
    CHECK(engine_row_count(s, "plandb", "t") == 1);
    const bool inserted = run_scheduler_sql(s, id++, insert, err);
    INFO("INSERT: " << err);
    REQUIRE(inserted);
    CHECK(engine_row_count(s, "plandb", "t") == 2);
}

// ── Plan shapes the Spark Connect frontend answers without translating ──────
// (frontend/spark_connect_server/service.hpp)

TEST_CASE("spark plan shapes: the text of a SqlCommand from a 3.5 and a 4.x client") {
    // 4.x: the query of the SQL input relation.
    sc::SqlCommand input_command;
    input_command.mutable_input()->mutable_sql()->set_query("SELECT 1");
    CHECK(frontend::spark::sql_command_text(input_command) == "SELECT 1");

    // 3.5: the flat field.
    const auto flat = flat_sql_command("SELECT 2");
    CHECK(frontend::spark::sql_command_text(flat) == "SELECT 2");

    // spark.sql() over DataFrame arguments: a WithRelations input, and no text.
    sc::SqlCommand with_relations;
    with_relations.mutable_input()->mutable_with_relations()->mutable_root()->mutable_sql()->set_query("SELECT 3");
    CHECK(frontend::spark::sql_command_text(with_relations).empty());
}

TEST_CASE("spark plan shapes: a query's SqlCommand answers its SQL relation") {
    using google::protobuf::util::MessageDifferencer;

    // 4.x: the input relation as sent, its arguments included.
    sc::SqlCommand input_command;
    auto* sql = input_command.mutable_input()->mutable_sql();
    sql->set_query("SELECT * FROM t WHERE id = ?");
    sql->add_pos_arguments()->mutable_literal()->set_integer(7);
    CHECK(MessageDifferencer::Equals(frontend::spark::sql_command_relation(input_command), input_command.input()));

    // 3.5: the SQL relation its flat fields describe, as Spark's server rebuilds it.
    const auto flat = flat_sql_command("SELECT :limit");
    CHECK(MessageDifferencer::Equals(frontend::spark::sql_command_relation(flat), flat_sql_relation("SELECT :limit")));
}

TEST_CASE("spark plan shapes: only a SQL relation without arguments is bare SQL") {
    sc::Relation bare;
    bare.mutable_sql()->set_query("SELECT 1");
    CHECK(frontend::spark::is_bare_sql(bare));

    sc::Relation positional = bare;
    positional.mutable_sql()->add_pos_arguments()->mutable_literal()->set_integer(1);
    CHECK_FALSE(frontend::spark::is_bare_sql(positional));

    sc::Relation named = bare;
    (*named.mutable_sql()->mutable_named_arguments())["x"].mutable_literal()->set_integer(1);
    CHECK_FALSE(frontend::spark::is_bare_sql(named));

    CHECK_FALSE(frontend::spark::is_bare_sql(flat_sql_relation("SELECT :limit")));

    sc::Relation range;
    range.mutable_range()->set_end(3);
    CHECK_FALSE(frontend::spark::is_bare_sql(range));
}

TEST_CASE("spark plan shapes: the empty LocalRelation carries neither data nor a schema") {
    sc::Relation empty;
    empty.mutable_local_relation();
    CHECK(frontend::spark::is_empty_local_relation(empty));

    sc::Relation with_data = empty;
    with_data.mutable_local_relation()->set_data("arrow");
    CHECK_FALSE(frontend::spark::is_empty_local_relation(with_data));

    sc::Relation with_schema = empty;
    with_schema.mutable_local_relation()->set_schema("id INT");
    CHECK_FALSE(frontend::spark::is_empty_local_relation(with_schema));

    sc::Relation sql;
    sql.mutable_sql()->set_query("SELECT 1");
    CHECK_FALSE(frontend::spark::is_empty_local_relation(sql));
}

TEST_CASE("spark plan shapes: an unsupported command is refused by its name") {
    sc::Command write;
    write.mutable_write_operation();
    CHECK(frontend::spark::unsupported_command_message(write) == "Spark command writeOperation is not supported");

    sc::Command view;
    view.mutable_create_dataframe_view()->set_name("v");
    CHECK(frontend::spark::unsupported_command_message(view) == "Spark command createDataframeView is not supported");

    CHECK(frontend::spark::unsupported_command_message(sc::Command{}) == "Empty plan");
}

TEST_CASE("spark plan shapes: classify_sql calls only a SELECT of the core grammar a query") {
    using frontend::spark::sql_statement_t;
    std::pmr::synchronized_pool_resource pool;
    const auto kind_of = [&pool](const std::string& sql) {
        auto kind = frontend::spark::classify_sql(sql, &pool);
        INFO(sql << ": " << (kind.has_error() ? kind.error().what.c_str() : "ok"));
        REQUIRE_FALSE(kind.has_error());
        return kind.value();
    };

    // Queries — one with a derived table included, which the SQL path runs and
    // the DataFrame translator refuses.
    CHECK(kind_of("SELECT 1") == sql_statement_t::query);
    CHECK(kind_of("SELECT d.id FROM (SELECT id FROM planpg.pgdb.public.items) d") == sql_statement_t::query);

    // Commands: DDL, DML, and the statements the file / s3 and Kafka grammar
    // extensions claim.
    CHECK(kind_of("CREATE TABLE plandb.t (id bigint)") == sql_statement_t::command);
    CHECK(kind_of("INSERT INTO plandb.t (id) VALUES (1)") == sql_statement_t::command);
    CHECK(kind_of("CREATE EXTERNAL TABLE file.people WITH (location = '/tmp/people.parquet', format = 'parquet')") ==
          sql_statement_t::command);
    CHECK(kind_of("CREATE SOURCE orders (id BIGINT, amount DOUBLE, note VARCHAR) WITH (KAFKA_TOPIC='orders_topic', "
                  "VALUE_FORMAT='JSON', BOOTSTRAP_SERVERS='localhost:9092')") == sql_statement_t::command);

    // A statement the parser refuses is its error, message included.
    auto broken = frontend::spark::classify_sql("SELEC 1", &pool);
    REQUIRE(broken.has_error());
    CHECK_FALSE(std::string_view{broken.error().what.c_str()}.empty());
    auto explain = frontend::spark::classify_sql("EXPLAIN SELECT 1", &pool);
    REQUIRE(explain.has_error());
    CHECK(std::string_view{explain.error().what.c_str()}.find("EXPLAIN") != std::string_view::npos);
}
