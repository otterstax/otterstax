// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The prepared-statement schema of a SELECT that targets ONE remote backend.
// The transformer wraps such a statement in a node_sequence_t whose last child
// is the external aggregate; the catalog rewrites that child into a
// schema_node_t carrying the projected schema, and prepare_schema must hand
// that schema out the way it does for a cross-backend JOIN — a FlightSQL
// GetFlightInfoStatement refuses a row-producing statement whose prepared
// schema is unresolved, and DoGet streams over the prepared schema alone.
// Driven through the real Scheduler→Worker→catalog stack over a typed
// PostgreSQL mock connector: the schema probe answers the discovered columns
// with their PostgreSQL types, the data path answers rows of the same shape.

#include "catalog/catalog_manager.hpp"
#include "frontend/flight_sql/chunk_to_ipc.hpp"
#include "frontend/flight_sql/ipc/ipc_reader.hpp"
#include "frontend/flight_sql/ipc/ipc_writer.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "scheduler_stack.hpp"
#include "test_helpers.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/otterbrix.hpp"

#include <catch2/catch_all.hpp>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <components/logical_plan/node_data.hpp>
#include <libpq-fe.h>

#include <boost/mysql/column_type.hpp>
#include <boost/mysql/detail/access.hpp>
#include <boost/mysql/detail/coldef_view.hpp>
#include <boost/mysql/detail/ok_view.hpp>
#include <boost/mysql/detail/resultset_encoding.hpp>
#include <boost/mysql/diagnostics.hpp>
#include <boost/mysql/metadata_mode.hpp>
#include <boost/mysql/results.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using otterstax::test::execute_scheduler_statement;
using otterstax::test::init_fresh_test_otterbrix;
using otterstax::test::make_az_scheduler;
using otterstax::test::prepare_scheduler_sql;
using otterstax::test::scheduler_stack;
using otterstax::test::worker_pool_size;

namespace {

    constexpr const char* kUid = "products";
    constexpr std::string_view kTable = "products.pgdb.public.products";

    // The discovered table, in backend column order: (name, PostgreSQL type
    // oid, the logical type pg_to_struct / pg_to_chunk map it to).
    struct discovered_column {
        const char* name;
        Oid typid;
        components::types::logical_type type;
    };

    constexpr Oid kInt4Oid = 23;
    constexpr Oid kFloat8Oid = 701;
    constexpr Oid kVarcharOid = 1043;

    constexpr std::array<discovered_column, 5> kColumns{{
        {"product_id", kInt4Oid, components::types::logical_type::INTEGER},
        {"campaign_id", kInt4Oid, components::types::logical_type::INTEGER},
        {"product_name", kVarcharOid, components::types::logical_type::STRING_LITERAL},
        {"price", kFloat8Oid, components::types::logical_type::DOUBLE},
        {"category", kVarcharOid, components::types::logical_type::STRING_LITERAL},
    }};

    // A PGresult carrying the named columns' attributes and no tuples — what the
    // backend answers a LIMIT-0 probe with, the catalog's over the base table and
    // the prepare probe over a generated statement alike.
    std::unique_ptr<PGresult, decltype(&PQclear)> make_result_for(const std::vector<std::size_t>& columns) {
        std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), &PQclear);
        std::vector<PGresAttDesc> attrs(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i) {
            attrs[i].name = const_cast<char*>(kColumns[columns[i]].name);
            attrs[i].tableid = 0;
            attrs[i].columnid = 0;
            attrs[i].format = 0;
            attrs[i].typid = kColumns[columns[i]].typid;
            attrs[i].typlen = -1;
            attrs[i].atttypmod = -1;
        }
        PQsetResultAttrs(result.get(), static_cast<int>(attrs.size()), attrs.data());
        return result;
    }

    // The probed table's every attribute: the catalog's schema probe.
    std::unique_ptr<PGresult, decltype(&PQclear)> make_probe_result() {
        std::vector<std::size_t> all(kColumns.size());
        for (std::size_t c = 0; c < kColumns.size(); ++c) {
            all[c] = c;
        }
        return make_result_for(all);
    }

    // The wrap Worker::prepare_schema has the backend describe a statement with
    // (db::make_prepare_probe), and the statement inside it: everything between
    // the first `(` and the last `)`. A backend answers the probe with the
    // wrapped statement's own result columns, so the mock has to read it out.
    constexpr std::string_view kProbePrefix = "SELECT * FROM (";

    std::string_view probe_body(std::string_view probe) {
        const auto open = probe.find('(');
        const auto close = probe.rfind(')');
        if (open == std::string_view::npos || close == std::string_view::npos || close <= open) {
            return probe;
        }
        return probe.substr(open + 1, close - open - 1);
    }

    // The columns the generated statement projects, in its SELECT-list order:
    // every discovered column for `*`, otherwise the ones the list names. The
    // backend answers exactly these, which is what the stream must carry.
    std::vector<std::size_t> projected_columns(std::string_view query) {
        const auto select = query.find("SELECT ");
        const auto from = query.find(" FROM ");
        REQUIRE(select != std::string_view::npos);
        REQUIRE(from != std::string_view::npos);
        const auto list = query.substr(select + 7, from - (select + 7));
        std::vector<std::pair<std::size_t, std::size_t>> found; // (position, column index)
        for (std::size_t c = 0; c < kColumns.size(); ++c) {
            if (list.find('*') != std::string_view::npos) {
                found.emplace_back(c, c);
            } else if (const auto at = list.find(kColumns[c].name); at != std::string_view::npos) {
                found.emplace_back(at, c);
            }
        }
        std::sort(found.begin(), found.end());
        std::vector<std::size_t> columns;
        for (const auto& [position, index] : found) {
            columns.push_back(index);
        }
        return columns;
    }

    // Two rows shaped like the projected columns of the discovered table; the
    // executed statement's chunk is what DoGet streams under the prepared
    // schema.
    class typed_pg_connector final : public pg::IConnector {
    public:
        typed_pg_connector(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias)
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

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query, otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)>) override {
            using components::types::complex_logical_type;
            using components::types::logical_value_t;
            const auto projected = projected_columns(query);
            std::pmr::vector<complex_logical_type> fields(resource_);
            for (const auto c : projected) {
                fields.emplace_back(kColumns[c].type, kColumns[c].name);
            }
            auto chunk = std::make_unique<data_chunk_t>(resource_, fields, 2);
            for (std::size_t i = 0; i < projected.size(); ++i) {
                for (std::size_t row = 0; row < 2; ++row) {
                    chunk->set_value(i, row, cell(projected[i], row));
                }
            }
            chunk->set_cardinality(2);
            co_return std::move(chunk);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            co_return int64_t{0};
        }

        // Discovery (the pg_enum query lists no enums, the schema probe answers
        // the table's attributes) and the prepare probe, told apart by the wrap
        // describe puts around the statement: a backend answers that one with
        // the WRAPPED statement's result columns, which for these cases are the
        // ones its SELECT list projects.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                     &PQclear);
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
            }
            if (query.substr(0, kProbePrefix.size()) == kProbePrefix) {
                auto described = make_result_for(projected_columns(probe_body(query)));
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(described.get()));
            }
            auto probe = make_probe_result();
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(probe.get()));
        }

    private:
        // Row `row` of the backend table, column `column` of kColumns.
        components::types::logical_value_t cell(std::size_t column, std::size_t row) const {
            using components::types::logical_value_t;
            switch (column) {
                case 0:
                    return logical_value_t(resource_, static_cast<int32_t>(row + 1));
                case 1:
                    return logical_value_t(resource_, static_cast<int32_t>((row + 1) * 10));
                case 2:
                    return logical_value_t(resource_, std::string_view{row == 0 ? "widget" : "gadget"});
                case 3:
                    return logical_value_t(resource_, row == 0 ? 9.5 : 19.25);
                default:
                    return logical_value_t(resource_, std::string_view{row == 0 ? "tools" : "toys"});
            }
        }

        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<pg::IConnector>
    typed_pg_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<typed_pg_connector>(resource, std::move(params), std::move(alias));
    }

    // Real engine, real parser, real catalog; the PostgreSQL connection is the
    // typed mock registered under kUid with its single table discovered eagerly.
    class single_backend_stack_owner {
    public:
        // Member order is construction order: every actor is spawned after the
        // ones whose addresses it takes, and the pmr deleters carry the engine's
        // resource, so the members are initialised here rather than assigned.
        explicit single_backend_stack_owner(const std::string& data_dir)
            : data_dir_(data_dir)
            , otterbrix_(init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address()))
            , pg_conn_(std::make_unique<pg::ConnectorManager>(resource_,
                                                              catalog_->address(),
                                                              &typed_pg_factory,
                                                              /*pool_size*/ 1))
            , pg_mgr_(actor_zeta::spawn<db::PostgressManager>(resource_, pg_conn_.get()))
            , scheduler_(spawn_scheduler()) {}

        single_backend_stack_owner(const single_backend_stack_owner&) = delete;
        single_backend_stack_owner& operator=(const single_backend_stack_owner&) = delete;

        ~single_backend_stack_owner() {
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
            params.table = "products";
            auto added = pg_conn_->addConnection(params);
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
                                                actor_zeta::address_t::empty_address(), // s3
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

    // The prepared schema is a STRUCT whose children are exactly `expected`
    // (indices into kColumns), in that order, with the discovered types.
    void require_prepared_columns(const core::result_wrapper_t<session_payload>& prepared,
                                  const std::vector<std::size_t>& expected,
                                  std::size_t parameter_count = 0) {
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().tag == T_SelectStmt);
        REQUIRE(prepared.value().parameter_count == parameter_count);
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == expected.size());
        for (std::size_t i = 0; i < expected.size(); ++i) {
            const auto& column = schema.child_types()[i];
            const auto& want = kColumns[expected[i]];
            INFO("column " << i << " expected " << want.name);
            REQUIRE(column.has_alias());
            REQUIRE(column.alias() == want.name);
            REQUIRE(column.type() == want.type);
        }
    }

    std::string from_table(std::string_view select, std::string_view tail = "") {
        std::string sql{select};
        sql += " FROM ";
        sql += kTable;
        sql += tail;
        sql += ";";
        return sql;
    }

    // writer + reader round-trip: the values a client would decode.
    std::vector<std::vector<flight::ipc::Value>>
    decode_batch(const flight::ipc::RecordBatch& batch) {
        const auto message = flight::ipc::serialize_record_batch(batch);
        return flight::ipc::decode_record_batch(*batch.schema,
                                                message.bare_message.data(), message.bare_message.size(),
                                                message.body.data(), message.body.size());
    }

} // namespace

TEST_CASE("single-backend prepare_schema: a column list resolves to the discovered columns in order") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_columns");
    auto s = owner.stack();
    session_hash_t id = 9800;

    require_prepared_columns(prepare_scheduler_sql(s, id++, from_table("SELECT product_id, product_name")), {0, 2});
    // Statement order, not backend order.
    require_prepared_columns(prepare_scheduler_sql(s, id++, from_table("SELECT category, price, product_id")),
                             {4, 3, 0});
}

TEST_CASE("single-backend prepare_schema: SELECT * resolves to every discovered column") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_star");
    auto s = owner.stack();
    session_hash_t id = 9820;

    require_prepared_columns(prepare_scheduler_sql(s, id++, from_table("SELECT *")), {0, 1, 2, 3, 4});
    require_prepared_columns(prepare_scheduler_sql(s, id++, from_table("SELECT *", " WHERE price > 10")),
                             {0, 1, 2, 3, 4});
}

TEST_CASE("single-backend prepare_schema: WHERE, ORDER BY and LIMIT leave the projected schema") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_clauses");
    auto s = owner.stack();
    session_hash_t id = 9840;

    require_prepared_columns(
        prepare_scheduler_sql(s, id++, from_table("SELECT product_name, price", " WHERE price > 10 ORDER BY price")),
        {2, 3});
    require_prepared_columns(prepare_scheduler_sql(s, id++, from_table("SELECT product_id", " LIMIT 1")), {0});
    const std::string clauses = from_table("SELECT *", " WHERE campaign_id = 10 ORDER BY price DESC LIMIT 5");
    require_prepared_columns(prepare_scheduler_sql(s, id++, clauses), {0, 1, 2, 3, 4});
}

TEST_CASE("single-backend prepare_schema: a parameterized SELECT carries its parameter count") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_param");
    auto s = owner.stack();

    // The projection of a remote SELECT is the catalog's and does not depend
    // on the parameter, so the extended-protocol frontends describe its
    // columns at Parse / COM_STMT_PREPARE; GetFlightInfoStatement binds
    // nothing and refuses the statement by this count, whatever the schema.
    auto prepared = prepare_scheduler_sql(s, 9860, from_table("SELECT product_id, price", " WHERE product_id = $1"));
    require_prepared_columns(prepared, {0, 3}, /*parameter_count*/ 1);
}

TEST_CASE("single-backend prepare_schema: DoGet streams the backend rows under the prepared schema") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_stream");
    auto s = owner.stack();
    const session_hash_t stmt = 9880;

    auto prepared = prepare_scheduler_sql(s, stmt, from_table("SELECT product_id, product_name, price"));
    require_prepared_columns(prepared, {0, 2, 3});

    auto flight_schema = flight::conv::schema_to_ipc(prepared.value().schema);
    REQUIRE(flight_schema->fields.size() == 3);
    REQUIRE(flight_schema->fields[0]->type->id == flight::ipc::TypeId::Int32);
    REQUIRE(flight_schema->fields[1]->type->id == flight::ipc::TypeId::Utf8);
    REQUIRE(flight_schema->fields[2]->type->id == flight::ipc::TypeId::Float64);

    auto executed = execute_scheduler_statement(s, stmt);
    INFO("execute error: " << executed.error().what.c_str());
    REQUIRE_FALSE(executed.has_error());
    REQUIRE(executed.value().size() == 2);
    REQUIRE(executed.value().column_count() == 3);

    auto batches = flight::conv::chunks_to_ipc(executed.value(), flight_schema);
    REQUIRE(batches.size() == 1);
    const auto& batch = batches.front();
    REQUIRE(batch.schema.get() == flight_schema.get());
    REQUIRE(batch.num_rows == 2);
    const auto rows = decode_batch(batch);
    REQUIRE(std::get<std::int64_t>(rows[0][0]) == 1);
    REQUIRE(std::get<std::int64_t>(rows[1][0]) == 2);
    REQUIRE(std::get<std::string>(rows[0][1]) == "widget");
    REQUIRE(std::get<std::string>(rows[1][1]) == "gadget");
    REQUIRE(std::get<double>(rows[0][2]) == 9.5);
    REQUIRE(std::get<double>(rows[1][2]) == 19.25);
}

// ── ClickHouse: the prepared schema is the backend's answer ──────────────────
// The catalog's stub of a single-backend SELECT carries a schema read off the
// plan, and for ClickHouse that is not the executed chunk's type for a COUNT
// (UInt64 on the wire, BIGINT on the plan), an arithmetic expression (`score + 1`
// is Int64, the plan says the Int32 operand's type) or a function aliased as a
// base column (`length(name) AS name` is UInt64, the plan says the String
// column's type). ClickhouseManager::describe replaces the stub's schema with
// the header ClickHouse answers for the statement's `SELECT * FROM (...) LIMIT 0`
// wrap, read under the same named types as execute's blocks. Driven over a typed
// ClickHouse mock connector: its discovery answers chdb.events (id Int32, score
// Int32, name String, rec Tuple(a Int32, b Nullable(String))), its describe probe
// the header of the columns the case names, its data query rows of them.

namespace {

    constexpr const char* kChUid = "chx";
    constexpr std::string_view kChEvents = "chx.chdb.schema.events";

    // The wire types the typed connector answers a result column with.
    enum class ch_wire
    {
        Int32,
        Int64,
        UInt64,
        String,
        NamedTuple
    };

    struct ch_result_column {
        std::string name;
        ch_wire wire;
    };

    struct ch_base_column {
        const char* name;
        const char* named_type;
        ch_wire wire;
    };

    constexpr std::array<ch_base_column, 4> kChBase{{
        {"id", "Int32", ch_wire::Int32},
        {"score", "Int32", ch_wire::Int32},
        {"name", "String", ch_wire::String},
        {"rec", "Tuple(a Int32, b Nullable(String))", ch_wire::NamedTuple},
    }};

    // How the connector answers the describe probe.
    enum class ch_probe_mode
    {
        header,      // the header block of the case's result columns, then the end marker
        no_blocks,   // nothing at all
        column_less, // only the column-less end marker
        refused      // the connector's io_error
    };

    // Shared with the connector through file-scope state (connector_factory is
    // a plain function pointer); every access is ordered by the future the
    // io-thread query settles.
    std::mutex g_ch_mutex;
    std::vector<ch_result_column> g_ch_result; // the columns the statement under test answers
    std::vector<std::string> g_ch_probe_queries;
    std::vector<std::string> g_ch_data_queries;
    ch_probe_mode g_ch_probe_mode = ch_probe_mode::header;

    void ch_reset(std::vector<ch_result_column> result, ch_probe_mode mode = ch_probe_mode::header) {
        std::lock_guard guard(g_ch_mutex);
        g_ch_result = std::move(result);
        g_ch_probe_queries.clear();
        g_ch_data_queries.clear();
        g_ch_probe_mode = mode;
    }

    std::vector<std::string> ch_probe_queries() {
        std::lock_guard guard(g_ch_mutex);
        return g_ch_probe_queries;
    }

    std::vector<std::string> ch_data_queries() {
        std::lock_guard guard(g_ch_mutex);
        return g_ch_data_queries;
    }

    // `rows` rows of a column of wire type `wire`: row r holds r + 7 as UInt64,
    // r + 3 000 000 000 as Int64 (past Int32), r as Int32, "v<r>" as String and
    // (r, "b<r>" or NULL on odd rows) as the tuple.
    clickhouse::ColumnRef ch_wire_column(ch_wire wire, size_t rows) {
        switch (wire) {
            case ch_wire::Int32: {
                auto column = std::make_shared<clickhouse::ColumnInt32>();
                for (size_t row = 0; row < rows; ++row) {
                    column->Append(static_cast<int32_t>(row));
                }
                return column;
            }
            case ch_wire::Int64: {
                auto column = std::make_shared<clickhouse::ColumnInt64>();
                for (size_t row = 0; row < rows; ++row) {
                    column->Append(static_cast<int64_t>(row) + 3000000000LL);
                }
                return column;
            }
            case ch_wire::UInt64: {
                auto column = std::make_shared<clickhouse::ColumnUInt64>();
                for (size_t row = 0; row < rows; ++row) {
                    column->Append(static_cast<uint64_t>(row) + 7);
                }
                return column;
            }
            case ch_wire::String: {
                auto column = std::make_shared<clickhouse::ColumnString>();
                for (size_t row = 0; row < rows; ++row) {
                    column->Append("v" + std::to_string(row));
                }
                return column;
            }
            case ch_wire::NamedTuple: {
                auto a = std::make_shared<clickhouse::ColumnInt32>();
                auto b_values = std::make_shared<clickhouse::ColumnString>();
                auto b_nulls = std::make_shared<clickhouse::ColumnUInt8>();
                for (size_t row = 0; row < rows; ++row) {
                    a->Append(static_cast<int32_t>(row));
                    b_values->Append(row % 2 == 0 ? "b" + std::to_string(row) : std::string{});
                    b_nulls->Append(static_cast<uint8_t>(row % 2 == 0 ? 0 : 1));
                }
                return std::make_shared<clickhouse::ColumnTuple>(std::vector<clickhouse::ColumnRef>{
                    a, std::make_shared<clickhouse::ColumnNullable>(b_values, b_nulls)});
            }
        }
        return {};
    }

    clickhouse::Block ch_result_block(const std::vector<ch_result_column>& columns, size_t rows) {
        clickhouse::Block block;
        for (const auto& column : columns) {
            block.AppendColumn(column.name, ch_wire_column(column.wire, rows));
        }
        return block;
    }

    // The header of the base table: what the discovery's `WHERE 1 = 0` probe answers.
    clickhouse::Block ch_base_header() {
        clickhouse::Block block;
        for (const auto& column : kChBase) {
            block.AppendColumn(column.name, ch_wire_column(column.wire, 0));
        }
        return block;
    }

    clickhouse::Block ch_named_types_block() {
        auto names = std::make_shared<clickhouse::ColumnString>();
        auto types = std::make_shared<clickhouse::ColumnString>();
        for (const auto& column : kChBase) {
            names->Append(std::string{column.name});
            types->Append(std::string{column.named_type});
        }
        clickhouse::Block block;
        block.AppendColumn("name", names);
        block.AppendColumn("type", types);
        return block;
    }

    constexpr std::string_view kChProbePrefix = "SELECT * FROM (";

    class typed_ch_connector final : public ch::IConnector {
    public:
        typed_ch_connector(std::pmr::memory_resource* resource, ch::connect_params params, std::string alias)
            : resource_(resource)
            , params_(std::move(params))
            , alias_(std::move(alias)) {}

        ch::Status status() const noexcept override { return ch::Status::Connected; }
        ch::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        // The data statement: the case's result columns, three rows over two blocks.
        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler)
            override {
            std::vector<ch_result_column> result;
            {
                std::lock_guard guard(g_ch_mutex);
                g_ch_data_queries.emplace_back(query);
                result = g_ch_result;
            }
            ch::select_result_t outcome;
            outcome.blocks.push_back(ch_result_block(result, 2));
            outcome.blocks.push_back(ch_result_block(result, 1));
            co_return handler(outcome);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)>) override {
            co_return int64_t{0};
        }

        // Discovery (system.columns, the base table's header) and the describe
        // probe, told apart by the wrap describe puts around the statement.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t answer;
            if (query.find("system.columns") != std::string_view::npos) {
                answer.blocks.push_back(ch_named_types_block());
            } else if (query.substr(0, kChProbePrefix.size()) == kChProbePrefix) {
                std::vector<ch_result_column> result;
                ch_probe_mode mode;
                {
                    std::lock_guard guard(g_ch_mutex);
                    g_ch_probe_queries.emplace_back(query);
                    result = g_ch_result;
                    mode = g_ch_probe_mode;
                }
                switch (mode) {
                    case ch_probe_mode::header:
                        // The server's shape: the header first, the column-less
                        // end marker after it.
                        answer.blocks.push_back(ch_result_block(result, 0));
                        answer.blocks.emplace_back();
                        break;
                    case ch_probe_mode::no_blocks:
                        break;
                    case ch_probe_mode::column_less:
                        answer.blocks.emplace_back();
                        break;
                    case ch_probe_mode::refused:
                        co_return core::error_t(core::error_code_t::io_error,
                                                std::pmr::string{"simulated probe refusal", resource_});
                }
            } else {
                answer.blocks.push_back(ch_base_header());
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(answer));
        }

    private:
        std::pmr::memory_resource* resource_;
        ch::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<ch::IConnector>
    typed_ch_factory(std::pmr::memory_resource* resource, ch::connect_params params, std::string alias) {
        return std::make_unique<typed_ch_connector>(resource, std::move(params), std::move(alias));
    }

    // Real parser, real catalog, the ClickHouse actor over the typed connector
    // and a mock engine (the catalog needs it only to stamp OIDs): the steps
    // Worker::prepare_schema takes for a ClickHouse SELECT — classify, describe —
    // and the execute that follows, each a message to the actor that owns it.
    class ch_actor_stack {
    public:
        explicit ch_actor_stack(std::pmr::memory_resource* res)
            : resource_(res)
            , otterbrix_manager_(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager_->address()))
            , connector_manager_(std::make_unique<ch::ConnectorManager>(res, catalog_->address(), &typed_ch_factory, 1))
            , manager_(actor_zeta::spawn<db::ClickhouseManager>(res, connector_manager_.get()))
            , parser_(res) {
            catalog_->set_backend_managers(actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address(),
                                           manager_->address());
            ch::connect_params params;
            params.host = "localhost";
            params.database = "chdb";
            params.table = "events";
            auto added = connector_manager_->addConnection(std::move(params), kChUid);
            INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());
        }

        ch_actor_stack(const ch_actor_stack&) = delete;
        ch_actor_stack& operator=(const ch_actor_stack&) = delete;

        ParsedQueryDataPtr parse(const std::string& sql) {
            auto parsed = parser_.parse(sql);
            INFO("parse: " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
            REQUIRE_FALSE(parsed.has_error());
            return std::move(parsed.value());
        }

        core::result_wrapper_t<ParsedQueryDataPtr> classify(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(catalog_->address(), &mysql::CatalogManager::get_catalog_schema, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::result_wrapper_t<ParsedQueryDataPtr> describe(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(manager_->address(), &db::ClickhouseManager::describe, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::result_wrapper_t<ParsedQueryDataPtr> execute(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(manager_->address(), &db::ClickhouseManager::execute, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

    private:
        // Members are destroyed in reverse order: the actor before the connector
        // manager it drives, both before the catalog they were built against.
        std::pmr::memory_resource* resource_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<ch::ConnectorManager> connector_manager_;
        std::unique_ptr<db::ClickhouseManager, actor_zeta::pmr::deleter_t> manager_;
        GreenplumParser parser_;
    };

    // The stub the catalog left for a single-backend SELECT: the root, or the
    // last child of the sequence the transformer wrapped it in.
    const schema_utils::schema_node_t& ch_root_stub(const ParsedQueryData& data) {
        const components::logical_plan::node_t* root = data.otterbrix_params->node.get();
        if (root->type() == components::logical_plan::node_type::sequence_t) {
            root = root->children().back().get();
        }
        REQUIRE(root->type() == components::logical_plan::node_type::unused);
        return static_cast<const schema_utils::schema_node_t&>(*root);
    }

    // The raw data execute substituted for the statement's one external slot.
    const components::logical_plan::node_data_t& ch_root_raw(const ParsedQueryData& data) {
        const auto& slot = data.otterbrix_params->external_nodes.front().front();
        REQUIRE((*slot.node)->type() == components::logical_plan::node_type::data_t);
        return static_cast<const components::logical_plan::node_data_t&>(**slot.node);
    }

    // `statement` wrapped the way describe probes it.
    std::string ch_probe_of(std::string statement) {
        while (!statement.empty() && (statement.back() == ';' || statement.back() == ' ')) {
            statement.pop_back();
        }
        return std::string{kChProbePrefix} + statement + ") LIMIT 0";
    }

    std::string ch_from(std::string_view select, std::string_view tail = "") {
        std::string sql{select};
        sql += " FROM ";
        sql += kChEvents;
        sql += tail;
        sql += ";";
        return sql;
    }

    // Parse → classify → describe → execute `sql` on `stack`, checking that the
    // one described column is `alias` of type `described` while the plan had
    // read it as `plan_type`, and that the executed chunk's column is the
    // described one. Returns the executed data for value checks.
    ParsedQueryDataPtr ch_describe_round_trip(ch_actor_stack& stack,
                                              session_hash_t id,
                                              const std::string& sql,
                                              std::string_view alias,
                                              components::types::logical_type plan_type,
                                              components::types::logical_type described) {
        auto classified = stack.classify(id, stack.parse(sql));
        INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
        REQUIRE_FALSE(classified.has_error());
        auto data = std::move(classified.value());
        {
            const auto& plan_schema = ch_root_stub(*data).schema();
            REQUIRE(plan_schema.child_types().size() == 1);
            REQUIRE(plan_schema.child_types()[0].type() == plan_type);
        }

        auto described_data = stack.describe(id, std::move(data));
        INFO("describe: " << (described_data.has_error() ? described_data.error().what.c_str() : "ok"));
        REQUIRE_FALSE(described_data.has_error());
        data = std::move(described_data.value());
        const auto schema = ch_root_stub(*data).schema();
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        REQUIRE(schema.child_types()[0].has_alias());
        REQUIRE(schema.child_types()[0].alias() == alias);
        REQUIRE(schema.child_types()[0].type() == described);

        auto executed = stack.execute(id, std::move(data));
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        data = std::move(executed.value());
        const auto& raw = ch_root_raw(*data);
        REQUIRE(raw.size() == 3);
        const auto executed_types = raw.data_chunk().types();
        REQUIRE(executed_types.size() == 1);
        REQUIRE(executed_types[0] == schema.child_types()[0]);
        return data;
    }

    // Real engine, real parser, real catalog, a Scheduler: the ClickHouse
    // connection is the typed mock registered under kChUid with chdb.events
    // discovered eagerly — the PostgreSQL stack owner's ClickHouse twin.
    class ch_single_backend_stack_owner {
    public:
        explicit ch_single_backend_stack_owner(const std::string& data_dir)
            : data_dir_(data_dir)
            , otterbrix_(init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address()))
            , ch_conn_(std::make_unique<ch::ConnectorManager>(resource_, catalog_->address(), &typed_ch_factory, 1))
            , ch_mgr_(actor_zeta::spawn<db::ClickhouseManager>(resource_, ch_conn_.get()))
            , scheduler_(spawn_scheduler()) {}

        ch_single_backend_stack_owner(const ch_single_backend_stack_owner&) = delete;
        ch_single_backend_stack_owner& operator=(const ch_single_backend_stack_owner&) = delete;

        ~ch_single_backend_stack_owner() {
            scheduler_.reset();
            ch_mgr_.reset();
            ch_conn_.reset();
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
                                           actor_zeta::address_t::empty_address(),
                                           ch_mgr_->address());
            ch::connect_params params;
            params.host = "localhost";
            params.database = "chdb";
            params.table = "events";
            auto added = ch_conn_->addConnection(std::move(params), kChUid);
            INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());

            return actor_zeta::spawn<Scheduler>(resource_,
                                                az_scheduler_.get(),
                                                worker_pool_size(),
                                                &make_parser,
                                                actor_zeta::address_t::empty_address(), // sql
                                                actor_zeta::address_t::empty_address(), // pg
                                                ch_mgr_->address(),
                                                otb_mgr_->address(),
                                                catalog_->address(),
                                                actor_zeta::address_t::empty_address(), // s3
                                                actor_zeta::address_t::empty_address()); // file
        }

        std::string data_dir_;
        db::otterbrix_engine_ptr otterbrix_;
        std::pmr::memory_resource* resource_{nullptr};
        std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otb_mgr_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<ch::ConnectorManager> ch_conn_;
        std::unique_ptr<db::ClickhouseManager, actor_zeta::pmr::deleter_t> ch_mgr_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
    };

    // Prepare `sql` through the Scheduler, execute it, and check that the one
    // prepared column is `alias` of `expected`, that the executed chunk carries
    // the same column type, and that the chunks stream through ChunkBatchReader
    // under the prepared schema's Arrow form — the FlightSQL contract.
    void ch_require_prepared_and_streamed(const scheduler_stack& s,
                                          session_hash_t stmt,
                                          const std::string& sql,
                                          std::string_view alias,
                                          components::types::logical_type expected,
                                          flight::ipc::TypeId arrow_type) {
        auto prepared = prepare_scheduler_sql(s, stmt, sql);
        INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().tag == T_SelectStmt);
        const auto schema = prepared.value().schema;
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        REQUIRE(schema.child_types()[0].alias() == alias);
        REQUIRE(schema.child_types()[0].type() == expected);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(executed.value().size() == 3);
        const auto column_types = executed.value().chunks.front().types();
        REQUIRE(column_types.size() == 1);
        REQUIRE(column_types[0] == schema.child_types()[0]);

        auto flight_schema = flight::conv::schema_to_ipc(schema);
        REQUIRE(flight_schema->fields[0]->type->id == arrow_type);
        auto batches = flight::conv::chunks_to_ipc(executed.value(), flight_schema);
        REQUIRE_FALSE(batches.empty());
        for (const auto& batch : batches) {
            REQUIRE(batch.schema.get() == flight_schema.get());
        }
    }

} // namespace

TEST_CASE("ClickhouseManager::describe: COUNT(*) is prepared as the UInt64 ClickHouse answers, not BIGINT") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"n", ch_wire::UInt64}});

    auto data = ch_describe_round_trip(stack,
                                       1,
                                       ch_from("SELECT COUNT(*) AS n"),
                                       "n",
                                       components::types::logical_type::UBIGINT,
                                       components::types::logical_type::UBIGINT);
    REQUIRE(ch_root_raw(*data).data_chunk().value(0, 0).value<uint64_t>() == 7);
}

TEST_CASE("ClickhouseManager::describe: score + 1 AS score is prepared as the Int64 ClickHouse widens to") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"score", ch_wire::Int64}});

    // The plan reads the alias as the Int32 base column; the backend answers Int64.
    auto data = ch_describe_round_trip(stack,
                                       2,
                                       ch_from("SELECT score + 1 AS score"),
                                       "score",
                                       components::types::logical_type::INTEGER,
                                       components::types::logical_type::BIGINT);
    REQUIRE(ch_root_raw(*data).data_chunk().value(0, 0).value<int64_t>() == 3000000000LL);
}

TEST_CASE("ClickhouseManager::describe: length(name) AS name is prepared as the function's UInt64") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"name", ch_wire::UInt64}});

    // A non-aggregate function aliased as a base column: the plan has no type
    // for the call at all (NA, an unresolved column), the backend answers the
    // function's type.
    auto data = ch_describe_round_trip(stack,
                                       3,
                                       ch_from("SELECT length(name) AS name"),
                                       "name",
                                       components::types::logical_type::NA,
                                       components::types::logical_type::UBIGINT);
    REQUIRE(ch_root_raw(*data).data_chunk().value(0, 1).value<uint64_t>() == 8);
}

TEST_CASE("ClickhouseManager::describe: the probe's header is read under the discovered named types") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"rec", ch_wire::NamedTuple}});

    // The control: a named Tuple base column, which discovery and the probe
    // shape the same way — the STRUCT keeps the field names the wire drops.
    auto data = ch_describe_round_trip(stack,
                                       4,
                                       ch_from("SELECT rec"),
                                       "rec",
                                       components::types::logical_type::STRUCT,
                                       components::types::logical_type::STRUCT);
    // types() answers a copy: keep it alive for the field checks.
    const auto executed_types = ch_root_raw(*data).data_chunk().types();
    const auto& rec = executed_types[0];
    REQUIRE(rec.child_types().size() == 2);
    REQUIRE(rec.child_types()[0].alias() == "a");
    REQUIRE(rec.child_types()[1].alias() == "b");
    REQUIRE(ch_root_raw(*data).data_chunk().value(0, 1).children()[1].is_null());
}

TEST_CASE("ClickhouseManager::describe: the probe is the executed statement wrapped in SELECT * FROM (...) LIMIT 0") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"n", ch_wire::UInt64}});

    ch_describe_round_trip(stack,
                           5,
                           ch_from("SELECT COUNT(*) AS n", " WHERE score > 10"),
                           "n",
                           components::types::logical_type::UBIGINT,
                           components::types::logical_type::UBIGINT);

    const auto probes = ch_probe_queries();
    const auto statements = ch_data_queries();
    REQUIRE(probes.size() == 1);
    REQUIRE(statements.size() == 1);
    REQUIRE(statements.front().back() == ';');
    REQUIRE(probes.front() == ch_probe_of(statements.front()));
    REQUIRE(probes.front().find("WHERE") != std::string::npos);
}

TEST_CASE("ClickhouseManager::describe: a probe the backend refuses is that error, never a plan-derived schema") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"n", ch_wire::UInt64}}, ch_probe_mode::refused);

    auto classified = stack.classify(6, stack.parse(ch_from("SELECT COUNT(*) AS n")));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe(6, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{described.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
    REQUIRE(ch_probe_queries().size() == 1);
}

TEST_CASE("ClickhouseManager::describe: a probe answered without a header block is schema_error") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    const auto mode = GENERATE(ch_probe_mode::no_blocks, ch_probe_mode::column_less);
    ch_reset({{"n", ch_wire::UInt64}}, mode);

    auto classified = stack.classify(7, stack.parse(ch_from("SELECT COUNT(*) AS n")));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe(7, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::schema_error);
    REQUIRE(std::string{described.error().what.c_str()}.find("without a header block") != std::string::npos);
}

TEST_CASE("ClickhouseManager::describe: a parameterized statement is unimplemented_yet before the backend is asked") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"id", ch_wire::Int32}});

    // The binder holds the statement's parameters until Bind, so there is no
    // statement to probe with; the plan's reading is not answered in its place.
    auto classified = stack.classify(8, stack.parse(ch_from("SELECT id", " WHERE id = $1")));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(classified.value()->otterbrix_params->parameters_count == 1);
    auto described = stack.describe(8, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::unimplemented_yet);
    REQUIRE(std::string{described.error().what.c_str()}.find("1 unbound parameter") != std::string::npos);
    REQUIRE(ch_probe_queries().empty());
}

TEST_CASE("ClickhouseManager::describe: a statement without a stub is handed back unchanged and nothing is probed") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({});

    auto classified = stack.classify(9, stack.parse("DELETE FROM chx.chdb.schema.events WHERE id = 1;"));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(classified.value()->backend_type == backend_type_t::ClickHouse);
    auto described = stack.describe(9, std::move(classified.value()));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());
    const auto& slot = described.value()->otterbrix_params->external_nodes.front().front();
    REQUIRE((*slot.node)->type() == components::logical_plan::node_type::delete_t);
    REQUIRE(ch_probe_queries().empty());
}

// The end-to-end acceptance of the same three columns through the Scheduler:
// Worker::prepare_schema sends the classified statement to
// ClickhouseManager::describe before it reads the stub, so what a frontend is
// told at prepare is what the backend answers. Without that send each case is
// red with the plan's type for exactly the reason above — a red case here is
// that routing gone, not a mock detail.
TEST_CASE("ClickHouse prepare_schema: COUNT(*) AS n is prepared and streamed as the same UBIGINT column",
          "[ch-describe-routing]") {
    ch_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_ch_count");
    ch_reset({{"n", ch_wire::UInt64}});
    ch_require_prepared_and_streamed(owner.stack(),
                                     9900,
                                     ch_from("SELECT COUNT(*) AS n"),
                                     "n",
                                     components::types::logical_type::UBIGINT,
                                     flight::ipc::TypeId::UInt64);
    REQUIRE(ch_probe_queries().size() == 1);
    REQUIRE(ch_probe_queries().front() == ch_probe_of(ch_data_queries().front()));
}

TEST_CASE("ClickHouse prepare_schema: score + 1 AS score is prepared and streamed as the same BIGINT column",
          "[ch-describe-routing]") {
    ch_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_ch_arith");
    ch_reset({{"score", ch_wire::Int64}});
    ch_require_prepared_and_streamed(owner.stack(),
                                     9920,
                                     ch_from("SELECT score + 1 AS score"),
                                     "score",
                                     components::types::logical_type::BIGINT,
                                     flight::ipc::TypeId::Int64);
}

TEST_CASE("ClickHouse prepare_schema: length(name) AS name is prepared and streamed as the same UBIGINT column",
          "[ch-describe-routing]") {
    ch_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_ch_func");
    ch_reset({{"name", ch_wire::UInt64}});
    ch_require_prepared_and_streamed(owner.stack(),
                                     9940,
                                     ch_from("SELECT length(name) AS name"),
                                     "name",
                                     components::types::logical_type::UBIGINT,
                                     flight::ipc::TypeId::UInt64);
}

TEST_CASE("ClickHouse prepare_schema: a probe the backend refuses fails the prepare with the backend's error",
          "[ch-describe-routing]") {
    ch_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_ch_refused");
    ch_reset({{"n", ch_wire::UInt64}}, ch_probe_mode::refused);
    auto prepared = prepare_scheduler_sql(owner.stack(), 9960, ch_from("SELECT COUNT(*) AS n"));
    REQUIRE(prepared.has_error());
    REQUIRE(prepared.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{prepared.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
}

// ── ClickHouse: a raw-SQL sub-query stub is described too ────────────────────
// A sub-query the SQL generator cannot write — `toString` is no function of its
// white list — is lifted out of the statement by the parser and handed to the
// backend as the user's own text with the table qualifiers rewritten. Nothing on
// the plan describes it: the stub carries the text, not a schema, so before this
// routing the column's type reached the client as NA (a derived table) or failed
// the prepare outright (a JOIN, where the NA side makes the merged schema NA).
// describe probes such a stub with the same statement execute generates for it
// and fills the stub's schema in place, keeping the text execute still needs.

namespace {

    // The sub-query is `toString(id)`: String on the backend, while the base
    // column `id` the plan reads is Int32.
    constexpr std::string_view kChSubquerySql =
        "SELECT s.id FROM (SELECT toString(id) AS id FROM chx.chdb.schema.events) s;";
    constexpr std::string_view kChSubqueryBody = "SELECT toString(id) AS id FROM chx.chdb.schema.events";
    constexpr std::string_view kChSubqueryRendered = "SELECT toString(id) AS id FROM `chdb`.`events`";
    // The same sub-query joined against the base table: two slots, so two stubs
    // of different kinds — the catalog's of the table, the parser's of the text.
    constexpr std::string_view kChSubqueryJoinSql =
        "SELECT e.name, s.id FROM chx.chdb.schema.events e "
        "JOIN (SELECT toString(id) AS id FROM chx.chdb.schema.events) s ON s.id = e.name;";

    // The statement's stubs, in slot order: the raw-SQL ones and the catalog's
    // alike, since describe must fill both.
    std::vector<const schema_utils::schema_node_t*> ch_stubs(const ParsedQueryData& data) {
        std::vector<const schema_utils::schema_node_t*> stubs;
        for (const auto& batch : data.otterbrix_params->external_nodes) {
            for (const auto& slot : batch) {
                if ((*slot.node)->type() == components::logical_plan::node_type::unused) {
                    stubs.push_back(static_cast<const schema_utils::schema_node_t*>(slot.node->get()));
                }
            }
        }
        return stubs;
    }

    const schema_utils::schema_node_t* ch_raw_stub(const ParsedQueryData& data) {
        for (const auto* stub : ch_stubs(data)) {
            if (stub->has_raw_sql()) {
                return stub;
            }
        }
        return nullptr;
    }

    // The one column `described` names, under `alias`.
    void require_single_column(const components::types::complex_logical_type& schema,
                               std::string_view alias,
                               components::types::logical_type described) {
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        REQUIRE(schema.child_types()[0].has_alias());
        REQUIRE(schema.child_types()[0].alias() == alias);
        REQUIRE(schema.child_types()[0].type() == described);
    }

} // namespace

TEST_CASE("ClickhouseManager::describe: a raw-SQL sub-query stub is filled from the statement it generates") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"id", ch_wire::String}});

    auto classified = stack.classify(10, stack.parse(std::string{kChSubquerySql}));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    auto data = std::move(classified.value());
    {
        // What the parser leaves: the user's text, and no schema at all.
        const auto* stub = ch_raw_stub(*data);
        REQUIRE(stub != nullptr);
        REQUIRE(stub->raw_sql() == kChSubqueryBody);
        REQUIRE(stub->schema().type() == components::types::logical_type::NA);
    }

    auto described = stack.describe(10, std::move(data));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());
    data = std::move(described.value());

    const auto* stub = ch_raw_stub(*data);
    REQUIRE(stub != nullptr);
    // Filled, not replaced: execute still generates its statement from this text.
    REQUIRE(stub->has_raw_sql());
    REQUIRE(stub->raw_sql() == kChSubqueryBody);
    REQUIRE_FALSE(stub->qualifiers().empty());
    require_single_column(stub->schema(), "id", components::types::logical_type::STRING_LITERAL);

    // The probe is the sub-query as execute would send it — the federated
    // 4-part qualifier rewritten to the ClickHouse table reference — wrapped.
    const auto probes = ch_probe_queries();
    REQUIRE(probes.size() == 1);
    REQUIRE(probes.front() == ch_probe_of(std::string{kChSubqueryRendered}));
}

TEST_CASE("ClickhouseManager::describe: every stub of a statement is probed — raw-SQL and catalog alike") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"id", ch_wire::String}});

    auto classified = stack.classify(11, stack.parse(std::string{kChSubqueryJoinSql}));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(ch_stubs(*classified.value()).size() == 2);

    auto described = stack.describe(11, std::move(classified.value()));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());

    // Both sides carry the backend's answer; neither is left NA, which is what
    // the schema merge of a JOIN needs from every side.
    const auto stubs = ch_stubs(*described.value());
    REQUIRE(stubs.size() == 2);
    for (const auto* stub : stubs) {
        require_single_column(stub->schema(), "id", components::types::logical_type::STRING_LITERAL);
    }

    // One probe per slot, each the statement that slot sends.
    const auto probes = ch_probe_queries();
    REQUIRE(probes.size() == 2);
    const auto has_probe = [&probes](const std::string& statement) {
        return std::find(probes.begin(), probes.end(), ch_probe_of(statement)) != probes.end();
    };
    REQUIRE(has_probe(std::string{kChSubqueryRendered}));
    REQUIRE(has_probe("SELECT * FROM `chdb`.`events`"));
}

TEST_CASE("ClickhouseManager::describe: a refused raw-sub-query probe is the backend's error") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    ch_actor_stack stack(&arena);
    ch_reset({{"id", ch_wire::String}}, ch_probe_mode::refused);

    auto classified = stack.classify(12, stack.parse(std::string{kChSubquerySql}));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe(12, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{described.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
    REQUIRE(ch_probe_queries().size() == 1);
}

TEST_CASE("ClickHouse prepare_schema: a raw sub-query column is prepared as the backend's type",
          "[ch-describe-routing]") {
    ch_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_ch_subquery");
    ch_reset({{"id", ch_wire::String}});

    // Without the described stub this column reaches the client as NA: the
    // aggregate over the sub-query names no table, so nothing types `id`.
    auto prepared = prepare_scheduler_sql(owner.stack(), 9980, std::string{kChSubquerySql});
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    REQUIRE(prepared.value().tag == T_SelectStmt);
    require_single_column(prepared.value().schema, "id", components::types::logical_type::STRING_LITERAL);
    REQUIRE(ch_probe_queries().size() == 1);
    REQUIRE(ch_probe_queries().front() == ch_probe_of(std::string{kChSubqueryRendered}));
}

TEST_CASE("ClickHouse prepare_schema: a JOIN with a raw sub-query is prepared from both described stubs",
          "[ch-describe-routing]") {
    ch_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_ch_subquery_join");
    ch_reset({{"id", ch_wire::String}});

    // An undescribed side makes the merged schema of a JOIN NA, which the
    // schema computation answers with `OtterBrix collection is missing in
    // catalog` — the prepare failed outright before both sides were described.
    auto prepared = prepare_scheduler_sql(owner.stack(), 9990, std::string{kChSubqueryJoinSql});
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    // Both sides answer the one column the mock is set to, and the merge of a
    // JOIN keeps one entry per column name.
    require_single_column(prepared.value().schema, "id", components::types::logical_type::STRING_LITERAL);
    REQUIRE(ch_probe_queries().size() == 2);
}

// ── MySQL: the prepared schema is the backend's answer as well ───────────────
// The catalog's stub of a single-backend SELECT carries a schema read off the
// plan, and for MySQL that is not the executed chunk's type for an arithmetic
// expression (`score + 1` over an INT column is BIGINT, the plan says the INT
// operand's type) nor for a function aliased as a base column
// (`length(name) AS name` is BIGINT, the plan has no type for the call at all);
// a raw-SQL sub-query stub carries no schema whatsoever, and a JOIN with one
// failed the prepare outright. MySQLManager::describe fills every stub with the
// columns MySQL answers for the statement's LIMIT-0 wrap, read by the very
// translator execute reads its rows with. Driven over a typed MySQL mock
// connector: its discovery answers mydb.events (id INT, score INT, name
// VARCHAR), its describe probe the column definitions the case names, its data
// query rows of them.

namespace {

    constexpr const char* kMyUid = "myx";
    constexpr std::string_view kMyEvents = "myx.mydb.schema.events";

    // A result column as the backend answers it: the name and wire type its
    // column definition carries, and the logical type the translators map that
    // wire type to — the type the prepared schema and the executed chunk must
    // both end up with.
    struct my_column {
        std::string name;
        boost::mysql::column_type wire;
        components::types::logical_type type;
    };

    // The base table, as discovery sees it.
    std::vector<my_column> my_base_columns() {
        return {{"id", boost::mysql::column_type::int_, components::types::logical_type::INTEGER},
                {"score", boost::mysql::column_type::int_, components::types::logical_type::INTEGER},
                {"name", boost::mysql::column_type::varchar, components::types::logical_type::STRING_LITERAL}};
    }

    // How the connector answers the describe probe.
    enum class my_probe_mode
    {
        header,     // the column definitions of the case's result columns
        no_columns, // a bare OK packet, which describes nothing
        refused     // the connector's io_error
    };

    // Shared with the connector through file-scope state (connector_factory is a
    // plain function pointer); every access is ordered by the future the
    // io-thread query settles.
    std::mutex g_my_mutex;
    std::vector<my_column> g_my_result;
    std::vector<std::string> g_my_probe_queries;
    std::vector<std::string> g_my_data_queries;
    my_probe_mode g_my_probe_mode = my_probe_mode::header;

    void my_reset(std::vector<my_column> result, my_probe_mode mode = my_probe_mode::header) {
        std::lock_guard guard(g_my_mutex);
        g_my_result = std::move(result);
        g_my_probe_queries.clear();
        g_my_data_queries.clear();
        g_my_probe_mode = mode;
    }

    std::vector<std::string> my_probe_queries() {
        std::lock_guard guard(g_my_mutex);
        return g_my_probe_queries;
    }

    std::vector<std::string> my_data_queries() {
        std::lock_guard guard(g_my_mutex);
        return g_my_data_queries;
    }

    // A completed text result set carrying `columns` and no rows: what a LIMIT-0
    // SELECT answers, its column definitions preceding the rows it has none of.
    // metadata_mode::full is the mode the real connection reads them in
    // (connectors/mysql/connector.cpp); under `minimal` the names would be gone.
    boost::mysql::results my_results(const std::vector<my_column>& columns) {
        boost::mysql::results result;
        auto& impl = boost::mysql::detail::access::get_impl(result);
        impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::full);
        impl.on_num_meta(columns.size());
        for (const auto& column : columns) {
            boost::mysql::detail::coldef_view coldef{};
            coldef.name = column.name;
            coldef.type = column.wire;
            boost::mysql::diagnostics diag;
            [[maybe_unused]] auto meta_ec = impl.on_meta(coldef, diag);
            assert(!meta_ec && "typed MySQL result set: on_meta must not fail");
        }
        [[maybe_unused]] auto ok_ec = impl.on_row_ok_packet(boost::mysql::detail::ok_view{0, 0, 0, 0, {}});
        assert(!ok_ec && "typed MySQL result set: on_row_ok_packet must not fail");
        return result;
    }

    // A bare OK packet: no columns, no rows — a DML's answer, and what a probe
    // that describes nothing looks like on the wire.
    boost::mysql::results my_ok_results() {
        boost::mysql::results result;
        auto& impl = boost::mysql::detail::access::get_impl(result);
        impl.reset(boost::mysql::detail::resultset_encoding::text, boost::mysql::metadata_mode::full);
        boost::mysql::diagnostics diag;
        [[maybe_unused]] auto ec = impl.on_head_ok_packet(boost::mysql::detail::ok_view{0, 0, 0, 0, {}}, diag);
        assert(!ec && "OK-packet result set: on_head_ok_packet must not fail");
        return result;
    }

    // Three rows of `columns`, typed as the case declares them: the executed side
    // of the comparison with the described schema.
    data_chunk_t my_chunk(std::pmr::memory_resource* resource, const std::vector<my_column>& columns) {
        using components::types::complex_logical_type;
        using components::types::logical_value_t;
        std::pmr::vector<complex_logical_type> fields(resource);
        for (const auto& column : columns) {
            fields.emplace_back(column.type, column.name);
        }
        data_chunk_t chunk(resource, fields, 3);
        for (std::size_t c = 0; c < columns.size(); ++c) {
            for (std::size_t row = 0; row < 3; ++row) {
                switch (columns[c].type) {
                    case components::types::logical_type::BIGINT:
                        chunk.set_value(c, row, logical_value_t(resource, static_cast<int64_t>(row) + 3000000000LL));
                        break;
                    case components::types::logical_type::STRING_LITERAL:
                        chunk.set_value(c, row, logical_value_t(resource, "v" + std::to_string(row)));
                        break;
                    default:
                        chunk.set_value(c, row, logical_value_t(resource, static_cast<int32_t>(row)));
                        break;
                }
            }
        }
        chunk.set_cardinality(3);
        return chunk;
    }

    class typed_my_connector final : public mysql::IConnector {
    public:
        typed_my_connector(std::pmr::memory_resource* resource,
                           boost::mysql::connect_params params,
                           std::string alias)
            : resource_(resource)
            , params_(std::move(params))
            , alias_(std::move(alias)) {}

        mysql::Status status() const noexcept override { return mysql::Status::Connected; }
        boost::mysql::connect_params params() const noexcept override { return params_; }
        void close() override {}
        core::error_t connect() override { return core::error_t::no_error(); }
        bool isConnected() override { return true; }
        core::error_t tryReconnect() override { return core::error_t::no_error(); }
        bool isClosed() const noexcept override { return false; }
        std::string alias() const noexcept override { return alias_; }

        // The data statement: the case's result columns, three rows.
        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)>) override {
            std::vector<my_column> result;
            {
                std::lock_guard guard(g_my_mutex);
                g_my_data_queries.emplace_back(query);
                result = g_my_result;
            }
            co_return std::make_unique<data_chunk_t>(my_chunk(resource_, result));
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const boost::mysql::results&)>) override {
            co_return int64_t{0};
        }

        // Discovery — the information_schema listing, which lists no table, so
        // the statement's table is registered lazily on its first
        // classification, and that table's own probe — and the describe probe,
        // told apart by the wrap describe puts around the statement.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            if (query.find("information_schema") != std::string_view::npos) {
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(my_ok_results()));
            }
            if (query.substr(0, kProbePrefix.size()) != kProbePrefix) {
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(my_results(my_base_columns())));
            }
            std::vector<my_column> result;
            my_probe_mode mode;
            {
                std::lock_guard guard(g_my_mutex);
                g_my_probe_queries.emplace_back(query);
                result = g_my_result;
                mode = g_my_probe_mode;
            }
            switch (mode) {
                case my_probe_mode::header:
                    co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(my_results(result)));
                case my_probe_mode::no_columns:
                    co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(my_ok_results()));
                case my_probe_mode::refused:
                    break;
            }
            co_return core::error_t(core::error_code_t::io_error,
                                    std::pmr::string{"simulated probe refusal", resource_});
        }

    private:
        std::pmr::memory_resource* resource_;
        boost::mysql::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<mysql::IConnector> typed_my_factory(std::pmr::memory_resource* resource,
                                                        boost::asio::io_context&,
                                                        boost::mysql::connect_params params,
                                                        std::string alias) {
        return std::make_unique<typed_my_connector>(resource, std::move(params), std::move(alias));
    }

    // Real parser, real catalog, the MySQL actor over the typed connector and a
    // mock engine (the catalog needs it only to stamp OIDs): the steps
    // Worker::prepare_schema takes for a MySQL SELECT — classify, describe — and
    // the execute that follows, each a message to the actor that owns it.
    class my_actor_stack {
    public:
        explicit my_actor_stack(std::pmr::memory_resource* res)
            : resource_(res)
            , otterbrix_manager_(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager_->address()))
            , connector_manager_(
                  std::make_unique<mysql::ConnectorManager>(res, catalog_->address(), &typed_my_factory, 1))
            , manager_(actor_zeta::spawn<db::MySQLManager>(res, connector_manager_.get()))
            , parser_(res) {
            catalog_->set_backend_managers(manager_->address(),
                                           actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address());
            boost::mysql::connect_params params;
            params.database = "mydb";
            auto added = connector_manager_->addConnection(params, kMyUid);
            INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());
        }

        my_actor_stack(const my_actor_stack&) = delete;
        my_actor_stack& operator=(const my_actor_stack&) = delete;

        ParsedQueryDataPtr parse(const std::string& sql) {
            auto parsed = parser_.parse(sql);
            INFO("parse: " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
            REQUIRE_FALSE(parsed.has_error());
            return std::move(parsed.value());
        }

        core::result_wrapper_t<ParsedQueryDataPtr> classify(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(catalog_->address(), &mysql::CatalogManager::get_catalog_schema, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::result_wrapper_t<ParsedQueryDataPtr> describe(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(manager_->address(), &db::MySQLManager::describe, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::result_wrapper_t<ParsedQueryDataPtr> execute(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(manager_->address(), &db::MySQLManager::execute, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

    private:
        // Members are destroyed in reverse order: the actor before the connector
        // manager it drives, both before the catalog they were built against.
        std::pmr::memory_resource* resource_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<mysql::ConnectorManager> connector_manager_;
        std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> manager_;
        GreenplumParser parser_;
    };

    // The stub the catalog left for a single-backend SELECT: the root, or the
    // last child of the sequence the transformer wrapped it in.
    const schema_utils::schema_node_t& my_root_stub(const ParsedQueryData& data) {
        const components::logical_plan::node_t* root = data.otterbrix_params->node.get();
        if (root->type() == components::logical_plan::node_type::sequence_t) {
            root = root->children().back().get();
        }
        REQUIRE(root->type() == components::logical_plan::node_type::unused);
        return static_cast<const schema_utils::schema_node_t&>(*root);
    }

    // The raw data execute substituted for the statement's one external slot.
    const components::logical_plan::node_data_t& my_root_raw(const ParsedQueryData& data) {
        const auto& slot = data.otterbrix_params->external_nodes.front().front();
        REQUIRE((*slot.node)->type() == components::logical_plan::node_type::data_t);
        return static_cast<const components::logical_plan::node_data_t&>(**slot.node);
    }

    // The statement's stubs, in slot order: the raw-SQL ones and the catalog's
    // alike, since describe must fill both.
    std::vector<const schema_utils::schema_node_t*> my_stubs(const ParsedQueryData& data) {
        std::vector<const schema_utils::schema_node_t*> stubs;
        for (const auto& batch : data.otterbrix_params->external_nodes) {
            for (const auto& slot : batch) {
                if ((*slot.node)->type() == components::logical_plan::node_type::unused) {
                    stubs.push_back(static_cast<const schema_utils::schema_node_t*>(slot.node->get()));
                }
            }
        }
        return stubs;
    }

    const schema_utils::schema_node_t* my_raw_stub(const ParsedQueryData& data) {
        for (const auto* stub : my_stubs(data)) {
            if (stub->has_raw_sql()) {
                return stub;
            }
        }
        return nullptr;
    }

    // `statement` wrapped the way describe probes it — MySQL requires the alias
    // on the derived table, which ClickHouse's wrap does without.
    std::string aliased_probe_of(std::string statement) {
        while (!statement.empty() && (statement.back() == ';' || statement.back() == ' ')) {
            statement.pop_back();
        }
        return std::string{kProbePrefix} + statement + ") AS otx_prepare_probe LIMIT 0";
    }

    std::string my_from(std::string_view select, std::string_view tail = "") {
        std::string sql{select};
        sql += " FROM ";
        sql += kMyEvents;
        sql += tail;
        sql += ";";
        return sql;
    }

    // The sub-query is `toString(id)`: no function of the generator's white list,
    // so the parser lifts the whole derived table out as the user's own text,
    // which no plan describes. The backend answers it as the case sets it.
    constexpr std::string_view kMySubquerySql =
        "SELECT s.id FROM (SELECT toString(id) AS id FROM myx.mydb.schema.events) s;";
    constexpr std::string_view kMySubqueryBody = "SELECT toString(id) AS id FROM myx.mydb.schema.events";
    constexpr std::string_view kMySubqueryRendered = "SELECT toString(id) AS id FROM `mydb`.`events`";
    constexpr std::string_view kMySubqueryJoinSql =
        "SELECT e.name, s.id FROM myx.mydb.schema.events e "
        "JOIN (SELECT toString(id) AS id FROM myx.mydb.schema.events) s ON s.id = e.name;";

    // Parse → classify → describe → execute `sql`, checking that the one
    // described column is `alias` of type `described` while the plan had read it
    // as `plan_type`, and that the executed chunk's column is the described one.
    ParsedQueryDataPtr my_describe_round_trip(my_actor_stack& stack,
                                              session_hash_t id,
                                              const std::string& sql,
                                              std::string_view alias,
                                              components::types::logical_type plan_type,
                                              components::types::logical_type described) {
        auto classified = stack.classify(id, stack.parse(sql));
        INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
        REQUIRE_FALSE(classified.has_error());
        auto data = std::move(classified.value());
        REQUIRE(data->backend_type == backend_type_t::MySQL);
        {
            const auto& plan_schema = my_root_stub(*data).schema();
            REQUIRE(plan_schema.child_types().size() == 1);
            REQUIRE(plan_schema.child_types()[0].type() == plan_type);
        }

        auto described_data = stack.describe(id, std::move(data));
        INFO("describe: " << (described_data.has_error() ? described_data.error().what.c_str() : "ok"));
        REQUIRE_FALSE(described_data.has_error());
        data = std::move(described_data.value());
        const auto schema = my_root_stub(*data).schema();
        require_single_column(schema, alias, described);

        auto executed = stack.execute(id, std::move(data));
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        data = std::move(executed.value());
        const auto& raw = my_root_raw(*data);
        REQUIRE(raw.size() == 3);
        const auto executed_types = raw.data_chunk().types();
        REQUIRE(executed_types.size() == 1);
        REQUIRE(executed_types[0] == schema.child_types()[0]);
        return data;
    }

    // Real engine, real parser, real catalog, a Scheduler: the MySQL connection
    // is the typed mock registered under kMyUid — the ClickHouse stack owner's
    // MySQL twin.
    class my_single_backend_stack_owner {
    public:
        explicit my_single_backend_stack_owner(const std::string& data_dir)
            : data_dir_(data_dir)
            , otterbrix_(init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address()))
            , my_conn_(std::make_unique<mysql::ConnectorManager>(resource_,
                                                                  catalog_->address(),
                                                                  &typed_my_factory,
                                                                  /*pool_size*/ 1))
            , my_mgr_(actor_zeta::spawn<db::MySQLManager>(resource_, my_conn_.get()))
            , scheduler_(spawn_scheduler()) {}

        my_single_backend_stack_owner(const my_single_backend_stack_owner&) = delete;
        my_single_backend_stack_owner& operator=(const my_single_backend_stack_owner&) = delete;

        ~my_single_backend_stack_owner() {
            scheduler_.reset();
            my_mgr_.reset();
            my_conn_.reset();
            catalog_.reset();
            otb_mgr_.reset();
            az_scheduler_->stop();
            otterbrix_.reset();
            std::filesystem::remove_all(data_dir_);
        }

        scheduler_stack stack() const { return scheduler_stack{scheduler_->address(), otterbrix_, resource_}; }

    private:
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> spawn_scheduler() {
            catalog_->set_backend_managers(my_mgr_->address(),
                                           actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address());
            boost::mysql::connect_params params;
            params.database = "mydb";
            auto added = my_conn_->addConnection(params, kMyUid);
            INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());

            return actor_zeta::spawn<Scheduler>(resource_,
                                                az_scheduler_.get(),
                                                worker_pool_size(),
                                                &make_parser,
                                                my_mgr_->address(),
                                                actor_zeta::address_t::empty_address(), // pg
                                                actor_zeta::address_t::empty_address(), // ch
                                                otb_mgr_->address(),
                                                catalog_->address(),
                                                actor_zeta::address_t::empty_address(), // s3
                                                actor_zeta::address_t::empty_address()); // file
        }

        std::string data_dir_;
        db::otterbrix_engine_ptr otterbrix_;
        std::pmr::memory_resource* resource_{nullptr};
        std::unique_ptr<actor_zeta::scheduler::sharing_scheduler> az_scheduler_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otb_mgr_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<mysql::ConnectorManager> my_conn_;
        std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> my_mgr_;
        std::unique_ptr<Scheduler, actor_zeta::pmr::deleter_t> scheduler_;
    };

    // Prepare `sql` through the Scheduler, execute it, and check that the one
    // prepared column is `alias` of `expected`, that the executed chunk carries
    // the same column type, and that the chunks stream through ChunkBatchReader
    // under the prepared schema's Arrow form.
    void my_require_prepared_and_streamed(const scheduler_stack& s,
                                          session_hash_t stmt,
                                          const std::string& sql,
                                          std::string_view alias,
                                          components::types::logical_type expected,
                                          flight::ipc::TypeId arrow_type) {
        auto prepared = prepare_scheduler_sql(s, stmt, sql);
        INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().tag == T_SelectStmt);
        const auto schema = prepared.value().schema;
        require_single_column(schema, alias, expected);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(executed.value().size() == 3);
        const auto column_types = executed.value().chunks.front().types();
        REQUIRE(column_types.size() == 1);
        REQUIRE(column_types[0] == schema.child_types()[0]);

        auto flight_schema = flight::conv::schema_to_ipc(schema);
        REQUIRE(flight_schema->fields[0]->type->id == arrow_type);
        auto batches = flight::conv::chunks_to_ipc(executed.value(), flight_schema);
        REQUIRE_FALSE(batches.empty());
        for (const auto& batch : batches) {
            REQUIRE(batch.schema.get() == flight_schema.get());
        }
    }

} // namespace

TEST_CASE("MySQLManager::describe: score + 1 AS score is prepared as the BIGINT MySQL widens to") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    my_actor_stack stack(&arena);
    my_reset({{"score", boost::mysql::column_type::bigint, components::types::logical_type::BIGINT}});

    // The plan reads the alias as the INT base column; MySQL answers BIGINT.
    auto data = my_describe_round_trip(stack,
                                       21,
                                       my_from("SELECT score + 1 AS score"),
                                       "score",
                                       components::types::logical_type::INTEGER,
                                       components::types::logical_type::BIGINT);
    REQUIRE(my_root_raw(*data).data_chunk().value(0, 0).value<int64_t>() == 3000000000LL);
}

TEST_CASE("MySQLManager::describe: length(name) AS name is prepared as the function's BIGINT") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    my_actor_stack stack(&arena);
    my_reset({{"name", boost::mysql::column_type::bigint, components::types::logical_type::BIGINT}});

    // A non-aggregate function aliased as a base column: the plan has no type for
    // the call at all (NA, an unresolved column), the backend answers the
    // function's own. The probe and the executed rows are both read by
    // mysql_to_chunk — schema and data come off one table (`to_local_translator`,
    // each arm naming the engine type AND the reader of its values), so the
    // prepared type is the executed one by construction.
    my_describe_round_trip(stack,
                           22,
                           my_from("SELECT length(name) AS name"),
                           "name",
                           components::types::logical_type::NA,
                           components::types::logical_type::BIGINT);
}

TEST_CASE("MySQLManager::describe: the probe is the executed statement wrapped and aliased") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    my_actor_stack stack(&arena);
    my_reset({{"score", boost::mysql::column_type::bigint, components::types::logical_type::BIGINT}});

    my_describe_round_trip(stack,
                           23,
                           my_from("SELECT score + 1 AS score", " WHERE score > 10"),
                           "score",
                           components::types::logical_type::INTEGER,
                           components::types::logical_type::BIGINT);

    const auto probes = my_probe_queries();
    const auto statements = my_data_queries();
    REQUIRE(probes.size() == 1);
    REQUIRE(statements.size() == 1);
    REQUIRE(statements.front().back() == ';');
    REQUIRE(probes.front() == aliased_probe_of(statements.front()));
    REQUIRE(probes.front().find("WHERE") != std::string::npos);
}

TEST_CASE("MySQLManager::describe: a probe the backend refuses is that error and never a plan-derived schema") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    my_actor_stack stack(&arena);
    my_reset({{"score", boost::mysql::column_type::bigint, components::types::logical_type::BIGINT}},
             my_probe_mode::refused);

    auto classified = stack.classify(24, stack.parse(my_from("SELECT score + 1 AS score")));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe(24, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{described.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
    REQUIRE(my_probe_queries().size() == 1);
}

TEST_CASE("MySQLManager::describe: a probe answered without column metadata is schema_error") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    my_actor_stack stack(&arena);
    my_reset({{"score", boost::mysql::column_type::bigint, components::types::logical_type::BIGINT}},
             my_probe_mode::no_columns);

    auto classified = stack.classify(25, stack.parse(my_from("SELECT score + 1 AS score")));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe(25, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::schema_error);
    REQUIRE(std::string{described.error().what.c_str()}.find("without column metadata") != std::string::npos);
}

TEST_CASE("MySQLManager::describe: a parameterized statement is unimplemented_yet before the backend is asked") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    my_actor_stack stack(&arena);
    my_reset({{"id", boost::mysql::column_type::int_, components::types::logical_type::INTEGER}});

    // The binder holds the statement's parameters until Bind, so there is no
    // statement to probe with; the plan's reading is not answered in its place.
    auto classified = stack.classify(26, stack.parse(my_from("SELECT id", " WHERE id = $1")));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(classified.value()->otterbrix_params->parameters_count == 1);
    auto described = stack.describe(26, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::unimplemented_yet);
    REQUIRE(std::string{described.error().what.c_str()}.find("1 unbound parameter") != std::string::npos);
    REQUIRE(my_probe_queries().empty());
}

TEST_CASE("MySQLManager::describe: a raw-SQL sub-query stub is filled from the statement it generates") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    my_actor_stack stack(&arena);
    my_reset({{"id", boost::mysql::column_type::varchar, components::types::logical_type::STRING_LITERAL}});

    auto classified = stack.classify(27, stack.parse(std::string{kMySubquerySql}));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    auto data = std::move(classified.value());
    {
        // What the parser leaves: the user's text, and no schema at all.
        const auto* stub = my_raw_stub(*data);
        REQUIRE(stub != nullptr);
        REQUIRE(stub->raw_sql() == kMySubqueryBody);
        REQUIRE(stub->schema().type() == components::types::logical_type::NA);
    }

    auto described = stack.describe(27, std::move(data));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());
    data = std::move(described.value());

    const auto* stub = my_raw_stub(*data);
    REQUIRE(stub != nullptr);
    // Filled, not replaced: execute still generates its statement from this text.
    REQUIRE(stub->has_raw_sql());
    REQUIRE(stub->raw_sql() == kMySubqueryBody);
    REQUIRE_FALSE(stub->qualifiers().empty());
    require_single_column(stub->schema(), "id", components::types::logical_type::STRING_LITERAL);

    const auto probes = my_probe_queries();
    REQUIRE(probes.size() == 1);
    REQUIRE(probes.front() == aliased_probe_of(std::string{kMySubqueryRendered}));
}

TEST_CASE("MySQLManager::describe: every stub of a statement is probed — raw-SQL and catalog alike") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    my_actor_stack stack(&arena);
    my_reset({{"id", boost::mysql::column_type::varchar, components::types::logical_type::STRING_LITERAL}});

    auto classified = stack.classify(28, stack.parse(std::string{kMySubqueryJoinSql}));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(my_stubs(*classified.value()).size() == 2);

    auto described = stack.describe(28, std::move(classified.value()));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());

    // Both sides carry the backend's answer; neither is left NA, which is what
    // the schema merge of a JOIN needs from every side.
    const auto stubs = my_stubs(*described.value());
    REQUIRE(stubs.size() == 2);
    for (const auto* stub : stubs) {
        require_single_column(stub->schema(), "id", components::types::logical_type::STRING_LITERAL);
    }

    // One probe per slot, each the statement that slot sends.
    const auto probes = my_probe_queries();
    REQUIRE(probes.size() == 2);
    REQUIRE(std::find(probes.begin(), probes.end(), aliased_probe_of(std::string{kMySubqueryRendered})) != probes.end());
}

// The end-to-end acceptance through the Scheduler: Worker::prepare_schema sends
// the classified statement to MySQLManager::describe before it reads the stub,
// so what a frontend is told at prepare is what the backend answers. Without
// that send each case is red with the plan's type — a red case here is that
// routing gone, not a mock detail.
TEST_CASE("MySQL prepare_schema: score + 1 AS score is prepared and streamed as the same BIGINT column",
          "[mysql-describe-routing]") {
    my_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_my_arith");
    my_reset({{"score", boost::mysql::column_type::bigint, components::types::logical_type::BIGINT}});
    my_require_prepared_and_streamed(owner.stack(),
                                     9500,
                                     my_from("SELECT score + 1 AS score"),
                                     "score",
                                     components::types::logical_type::BIGINT,
                                     flight::ipc::TypeId::Int64);
    REQUIRE(my_probe_queries().size() == 1);
    REQUIRE(my_probe_queries().front() == aliased_probe_of(my_data_queries().front()));
}

TEST_CASE("MySQL prepare_schema: a raw sub-query column is prepared as the backend's type",
          "[mysql-describe-routing]") {
    my_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_my_subquery");
    my_reset({{"id", boost::mysql::column_type::varchar, components::types::logical_type::STRING_LITERAL}});

    // Without the described stub this column reaches the client as NA: the
    // aggregate over the sub-query names no table, so nothing types `id`.
    auto prepared = prepare_scheduler_sql(owner.stack(), 9520, std::string{kMySubquerySql});
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    REQUIRE(prepared.value().tag == T_SelectStmt);
    require_single_column(prepared.value().schema, "id", components::types::logical_type::STRING_LITERAL);
    REQUIRE(my_probe_queries().size() == 1);
    REQUIRE(my_probe_queries().front() == aliased_probe_of(std::string{kMySubqueryRendered}));
}

TEST_CASE("MySQL prepare_schema: a JOIN with a raw sub-query is prepared from both described stubs",
          "[mysql-describe-routing]") {
    my_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_my_subquery_join");
    my_reset({{"id", boost::mysql::column_type::varchar, components::types::logical_type::STRING_LITERAL}});

    // An undescribed side makes the merged schema of a JOIN NA, which the schema
    // computation answers with `OtterBrix collection is missing in catalog` — the
    // prepare failed outright before both sides were described.
    auto prepared = prepare_scheduler_sql(owner.stack(), 9540, std::string{kMySubqueryJoinSql});
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    require_single_column(prepared.value().schema, "id", components::types::logical_type::STRING_LITERAL);
    REQUIRE(my_probe_queries().size() == 2);
}

TEST_CASE("MySQL prepare_schema: a probe the backend refuses fails the prepare with the backend's error",
          "[mysql-describe-routing]") {
    my_single_backend_stack_owner owner("/tmp/test_single_backend_prepare_my_refused");
    my_reset({{"score", boost::mysql::column_type::bigint, components::types::logical_type::BIGINT}},
             my_probe_mode::refused);
    auto prepared = prepare_scheduler_sql(owner.stack(), 9560, my_from("SELECT score + 1 AS score"));
    REQUIRE(prepared.has_error());
    REQUIRE(prepared.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{prepared.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
}

// ── PostgreSQL: the prepared schema is the backend's answer as well ──────────
// PostgreSQL agrees with the plan on more expressions than the other two — `x + 1`
// over an int4 column is int4 — but not on a function aliased as a base column
// (`length(name) AS name` is int4 where the plan has no type for the call at all)
// and not on a raw-SQL sub-query stub, which carries the user's text and no
// schema whatsoever: its column reached the client as NA, and a JOIN with it
// failed the prepare outright. PostgressManager::describe fills every stub with
// the columns the backend answers for the statement's LIMIT-0 wrap, read through
// tsl::pg_to_struct under the connection's ENUM oids — the translator execute
// reads its tuples with. Driven over a second typed PostgreSQL mock connector,
// whose discovery answers pgdb.public.events (id int4, score int4, name varchar)
// and whose probe answers the columns the case names.

namespace {

    constexpr const char* kPgUid = "pgx";
    constexpr std::string_view kPgEvents = "pgx.pgdb.public.events";

    // A result column as the backend answers it: its name, its PostgreSQL type
    // oid, and the logical type pg_to_struct / pg_to_chunk map that oid to.
    struct pg_column {
        std::string name;
        Oid typid;
        components::types::logical_type type;
    };

    std::vector<pg_column> pg_base_columns() {
        return {{"id", kInt4Oid, components::types::logical_type::INTEGER},
                {"score", kInt4Oid, components::types::logical_type::INTEGER},
                {"name", kVarcharOid, components::types::logical_type::STRING_LITERAL}};
    }

    // How the connector answers the describe probe.
    enum class pg_probe_mode
    {
        header,     // a RowDescription of the case's result columns
        no_columns, // a result with no fields, which describes nothing
        refused     // the connector's io_error
    };

    // Shared with the connector through file-scope state (connector_factory is a
    // plain function pointer); every access is ordered by the future the
    // io-thread query settles.
    std::mutex g_pg_mutex;
    std::vector<pg_column> g_pg_result;
    std::vector<std::string> g_pg_probe_queries;
    std::vector<std::string> g_pg_data_queries;
    pg_probe_mode g_pg_probe_mode = pg_probe_mode::header;

    void pg_reset(std::vector<pg_column> result, pg_probe_mode mode = pg_probe_mode::header) {
        std::lock_guard guard(g_pg_mutex);
        g_pg_result = std::move(result);
        g_pg_probe_queries.clear();
        g_pg_data_queries.clear();
        g_pg_probe_mode = mode;
    }

    std::vector<std::string> pg_probe_queries() {
        std::lock_guard guard(g_pg_mutex);
        return g_pg_probe_queries;
    }

    std::vector<std::string> pg_data_queries() {
        std::lock_guard guard(g_pg_mutex);
        return g_pg_data_queries;
    }

    // A PGresult carrying `columns` and no tuples: the RowDescription a LIMIT-0
    // SELECT is answered with.
    std::unique_ptr<PGresult, decltype(&PQclear)> pg_result_of(const std::vector<pg_column>& columns) {
        std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), &PQclear);
        if (columns.empty()) {
            return result;
        }
        std::vector<PGresAttDesc> attrs(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i) {
            attrs[i].name = const_cast<char*>(columns[i].name.c_str());
            attrs[i].tableid = 0;
            attrs[i].columnid = 0;
            attrs[i].format = 0;
            attrs[i].typid = columns[i].typid;
            attrs[i].typlen = -1;
            attrs[i].atttypmod = -1;
        }
        PQsetResultAttrs(result.get(), static_cast<int>(attrs.size()), attrs.data());
        return result;
    }

    // Three rows of `columns`, typed as the case declares them: the executed side
    // of the comparison with the described schema.
    data_chunk_t pg_chunk(std::pmr::memory_resource* resource, const std::vector<pg_column>& columns) {
        using components::types::complex_logical_type;
        using components::types::logical_value_t;
        std::pmr::vector<complex_logical_type> fields(resource);
        for (const auto& column : columns) {
            fields.emplace_back(column.type, column.name);
        }
        data_chunk_t chunk(resource, fields, 3);
        for (std::size_t c = 0; c < columns.size(); ++c) {
            for (std::size_t row = 0; row < 3; ++row) {
                switch (columns[c].type) {
                    case components::types::logical_type::STRING_LITERAL:
                        chunk.set_value(c, row, logical_value_t(resource, "v" + std::to_string(row)));
                        break;
                    default:
                        chunk.set_value(c, row, logical_value_t(resource, static_cast<int32_t>(row)));
                        break;
                }
            }
        }
        chunk.set_cardinality(3);
        return chunk;
    }

    class typed_pgd_connector final : public pg::IConnector {
    public:
        typed_pgd_connector(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias)
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

        // The data statement: the case's result columns, three rows.
        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query, otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)>) override {
            std::vector<pg_column> result;
            {
                std::lock_guard guard(g_pg_mutex);
                g_pg_data_queries.emplace_back(query);
                result = g_pg_result;
            }
            co_return std::make_unique<data_chunk_t>(pg_chunk(resource_, result));
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            co_return int64_t{0};
        }

        // Discovery (the pg_enum query lists no enums, the schema probe answers
        // the base table) and the describe probe, told apart by the wrap describe
        // puts around the statement.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                std::unique_ptr<PGresult, decltype(&PQclear)> empty(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                    &PQclear);
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(empty.get()));
            }
            if (query.substr(0, kProbePrefix.size()) != kProbePrefix) {
                auto base = pg_result_of(pg_base_columns());
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(base.get()));
            }
            std::vector<pg_column> result;
            pg_probe_mode mode;
            {
                std::lock_guard guard(g_pg_mutex);
                g_pg_probe_queries.emplace_back(query);
                result = g_pg_result;
                mode = g_pg_probe_mode;
            }
            switch (mode) {
                case pg_probe_mode::header: {
                    auto described = pg_result_of(result);
                    co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(described.get()));
                }
                case pg_probe_mode::no_columns: {
                    auto empty = pg_result_of({});
                    co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(empty.get()));
                }
                case pg_probe_mode::refused:
                    break;
            }
            co_return core::error_t(core::error_code_t::io_error,
                                    std::pmr::string{"simulated probe refusal", resource_});
        }

    private:
        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<pg::IConnector>
    typed_pgd_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<typed_pgd_connector>(resource, std::move(params), std::move(alias));
    }

    conn::api_server::PgConnectionParams pgd_params() {
        conn::api_server::PgConnectionParams params;
        params.alias = kPgUid;
        params.host = "localhost";
        params.port = "5432";
        params.username = "user";
        params.password = "pass";
        params.database = "pgdb";
        params.schema = "public";
        params.table = "events";
        return params;
    }

    // Real parser, real catalog, the PostgreSQL actor over the typed connector
    // and a mock engine: the steps Worker::prepare_schema takes for a PostgreSQL
    // SELECT — classify, describe — and the execute that follows.
    class pg_actor_stack {
    public:
        explicit pg_actor_stack(std::pmr::memory_resource* res)
            : resource_(res)
            , otterbrix_manager_(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager_->address()))
            , connector_manager_(
                  std::make_unique<pg::ConnectorManager>(res, catalog_->address(), &typed_pgd_factory, 1))
            , manager_(actor_zeta::spawn<db::PostgressManager>(res, connector_manager_.get()))
            , parser_(res) {
            catalog_->set_backend_managers(actor_zeta::address_t::empty_address(),
                                           manager_->address(),
                                           actor_zeta::address_t::empty_address());
            auto added = connector_manager_->addConnection(pgd_params());
            INFO("addConnection: " << (added.has_error() ? added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(added.has_error());
        }

        pg_actor_stack(const pg_actor_stack&) = delete;
        pg_actor_stack& operator=(const pg_actor_stack&) = delete;

        ParsedQueryDataPtr parse(const std::string& sql) {
            auto parsed = parser_.parse(sql);
            INFO("parse: " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
            REQUIRE_FALSE(parsed.has_error());
            return std::move(parsed.value());
        }

        core::result_wrapper_t<ParsedQueryDataPtr> classify(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(catalog_->address(), &mysql::CatalogManager::get_catalog_schema, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::result_wrapper_t<ParsedQueryDataPtr> describe(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(manager_->address(), &db::PostgressManager::describe, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::result_wrapper_t<ParsedQueryDataPtr> execute(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(manager_->address(), &db::PostgressManager::execute, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

    private:
        // Members are destroyed in reverse order: the actor before the connector
        // manager it drives, both before the catalog they were built against.
        std::pmr::memory_resource* resource_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<pg::ConnectorManager> connector_manager_;
        std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> manager_;
        GreenplumParser parser_;
    };

    const schema_utils::schema_node_t& pg_root_stub(const ParsedQueryData& data) {
        const components::logical_plan::node_t* root = data.otterbrix_params->node.get();
        if (root->type() == components::logical_plan::node_type::sequence_t) {
            root = root->children().back().get();
        }
        REQUIRE(root->type() == components::logical_plan::node_type::unused);
        return static_cast<const schema_utils::schema_node_t&>(*root);
    }

    const components::logical_plan::node_data_t& pg_root_raw(const ParsedQueryData& data) {
        const auto& slot = data.otterbrix_params->external_nodes.front().front();
        REQUIRE((*slot.node)->type() == components::logical_plan::node_type::data_t);
        return static_cast<const components::logical_plan::node_data_t&>(**slot.node);
    }

    std::vector<const schema_utils::schema_node_t*> pg_stubs(const ParsedQueryData& data) {
        std::vector<const schema_utils::schema_node_t*> stubs;
        for (const auto& batch : data.otterbrix_params->external_nodes) {
            for (const auto& slot : batch) {
                if ((*slot.node)->type() == components::logical_plan::node_type::unused) {
                    stubs.push_back(static_cast<const schema_utils::schema_node_t*>(slot.node->get()));
                }
            }
        }
        return stubs;
    }

    const schema_utils::schema_node_t* pg_raw_stub(const ParsedQueryData& data) {
        for (const auto* stub : pg_stubs(data)) {
            if (stub->has_raw_sql()) {
                return stub;
            }
        }
        return nullptr;
    }

    std::string pg_from(std::string_view select, std::string_view tail = "") {
        std::string sql{select};
        sql += " FROM ";
        sql += kPgEvents;
        sql += tail;
        sql += ";";
        return sql;
    }

    // The sub-query is a function the generator's white list does not spell, so
    // the parser lifts the whole derived table out as the user's own text, which
    // no plan describes; PostgreSQL's table reference is schema-qualified.
    constexpr std::string_view kPgSubquerySql =
        "SELECT s.id FROM (SELECT toString(id) AS id FROM pgx.pgdb.public.events) s;";
    constexpr std::string_view kPgSubqueryBody = "SELECT toString(id) AS id FROM pgx.pgdb.public.events";
    constexpr std::string_view kPgSubqueryRendered = "SELECT toString(id) AS id FROM \"public\".\"events\"";
    constexpr std::string_view kPgSubqueryJoinSql =
        "SELECT e.name, s.id FROM pgx.pgdb.public.events e "
        "JOIN (SELECT toString(id) AS id FROM pgx.pgdb.public.events) s ON s.id = e.name;";

    ParsedQueryDataPtr pg_describe_round_trip(pg_actor_stack& stack,
                                              session_hash_t id,
                                              const std::string& sql,
                                              std::string_view alias,
                                              components::types::logical_type plan_type,
                                              components::types::logical_type described) {
        auto classified = stack.classify(id, stack.parse(sql));
        INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
        REQUIRE_FALSE(classified.has_error());
        auto data = std::move(classified.value());
        REQUIRE(data->backend_type == backend_type_t::PostgreSQL);
        {
            const auto& plan_schema = pg_root_stub(*data).schema();
            REQUIRE(plan_schema.child_types().size() == 1);
            REQUIRE(plan_schema.child_types()[0].type() == plan_type);
        }

        auto described_data = stack.describe(id, std::move(data));
        INFO("describe: " << (described_data.has_error() ? described_data.error().what.c_str() : "ok"));
        REQUIRE_FALSE(described_data.has_error());
        data = std::move(described_data.value());
        const auto schema = pg_root_stub(*data).schema();
        require_single_column(schema, alias, described);

        auto executed = stack.execute(id, std::move(data));
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        data = std::move(executed.value());
        const auto& raw = pg_root_raw(*data);
        REQUIRE(raw.size() == 3);
        const auto executed_types = raw.data_chunk().types();
        REQUIRE(executed_types.size() == 1);
        REQUIRE(executed_types[0] == schema.child_types()[0]);
        return data;
    }

    // Real engine, real parser, real catalog, a Scheduler over the typed
    // PostgreSQL mock — the ClickHouse and MySQL stack owners' twin.
    class pg_describe_stack_owner {
    public:
        explicit pg_describe_stack_owner(const std::string& data_dir)
            : data_dir_(data_dir)
            , otterbrix_(init_fresh_test_otterbrix(data_dir_))
            , resource_(otterbrix_->dispatcher()->resource())
            , az_scheduler_(make_az_scheduler())
            , otb_mgr_(actor_zeta::spawn<db::OtterbrixManager>(resource_, make_otterbrix_manager(otterbrix_)))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(resource_, otb_mgr_->address()))
            , pg_conn_(std::make_unique<pg::ConnectorManager>(resource_,
                                                               catalog_->address(),
                                                               &typed_pgd_factory,
                                                               /*pool_size*/ 1))
            , pg_mgr_(actor_zeta::spawn<db::PostgressManager>(resource_, pg_conn_.get()))
            , scheduler_(spawn_scheduler()) {}

        pg_describe_stack_owner(const pg_describe_stack_owner&) = delete;
        pg_describe_stack_owner& operator=(const pg_describe_stack_owner&) = delete;

        ~pg_describe_stack_owner() {
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
            auto added = pg_conn_->addConnection(pgd_params());
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
                                                actor_zeta::address_t::empty_address(), // s3
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

    void pg_require_prepared_and_streamed(const scheduler_stack& s,
                                          session_hash_t stmt,
                                          const std::string& sql,
                                          std::string_view alias,
                                          components::types::logical_type expected,
                                          flight::ipc::TypeId arrow_type) {
        auto prepared = prepare_scheduler_sql(s, stmt, sql);
        INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().tag == T_SelectStmt);
        const auto schema = prepared.value().schema;
        require_single_column(schema, alias, expected);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(executed.value().size() == 3);
        const auto column_types = executed.value().chunks.front().types();
        REQUIRE(column_types.size() == 1);
        REQUIRE(column_types[0] == schema.child_types()[0]);

        auto flight_schema = flight::conv::schema_to_ipc(schema);
        REQUIRE(flight_schema->fields[0]->type->id == arrow_type);
        auto batches = flight::conv::chunks_to_ipc(executed.value(), flight_schema);
        REQUIRE_FALSE(batches.empty());
        for (const auto& batch : batches) {
            REQUIRE(batch.schema.get() == flight_schema.get());
        }
    }

} // namespace

TEST_CASE("PostgressManager::describe: length(name) AS name is prepared as the function's INTEGER") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    pg_actor_stack stack(&arena);
    pg_reset({{"name", kInt4Oid, components::types::logical_type::INTEGER}});

    // A non-aggregate function aliased as a base column: the plan has no type for
    // the call at all (NA, an unresolved column), the backend answers int4.
    pg_describe_round_trip(stack,
                           31,
                           pg_from("SELECT length(name) AS name"),
                           "name",
                           components::types::logical_type::NA,
                           components::types::logical_type::INTEGER);
}

TEST_CASE("PostgressManager::describe: the probe is the executed statement wrapped and aliased") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    pg_actor_stack stack(&arena);
    pg_reset({{"name", kInt4Oid, components::types::logical_type::INTEGER}});

    pg_describe_round_trip(stack,
                           32,
                           pg_from("SELECT length(name) AS name", " WHERE score > 10"),
                           "name",
                           components::types::logical_type::NA,
                           components::types::logical_type::INTEGER);

    const auto probes = pg_probe_queries();
    const auto statements = pg_data_queries();
    REQUIRE(probes.size() == 1);
    REQUIRE(statements.size() == 1);
    REQUIRE(statements.front().back() == ';');
    REQUIRE(probes.front() == aliased_probe_of(statements.front()));
    REQUIRE(probes.front().find("WHERE") != std::string::npos);
}

TEST_CASE("PostgressManager::describe: a probe the backend refuses is that error and never a plan-derived schema") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    pg_actor_stack stack(&arena);
    pg_reset({{"name", kInt4Oid, components::types::logical_type::INTEGER}}, pg_probe_mode::refused);

    auto classified = stack.classify(33, stack.parse(pg_from("SELECT length(name) AS name")));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe(33, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{described.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
    REQUIRE(pg_probe_queries().size() == 1);
}

TEST_CASE("PostgressManager::describe: a probe answered without columns is schema_error") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    pg_actor_stack stack(&arena);
    pg_reset({{"name", kInt4Oid, components::types::logical_type::INTEGER}}, pg_probe_mode::no_columns);

    auto classified = stack.classify(34, stack.parse(pg_from("SELECT length(name) AS name")));
    REQUIRE_FALSE(classified.has_error());
    auto described = stack.describe(34, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::schema_error);
    REQUIRE(std::string{described.error().what.c_str()}.find("without columns") != std::string::npos);
}

TEST_CASE("PostgressManager::describe: a parameterized statement is unimplemented_yet before the backend is asked") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    pg_actor_stack stack(&arena);
    pg_reset({{"id", kInt4Oid, components::types::logical_type::INTEGER}});

    auto classified = stack.classify(35, stack.parse(pg_from("SELECT id", " WHERE id = $1")));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(classified.value()->otterbrix_params->parameters_count == 1);
    auto described = stack.describe(35, std::move(classified.value()));
    REQUIRE(described.has_error());
    REQUIRE(described.error().type == core::error_code_t::unimplemented_yet);
    REQUIRE(std::string{described.error().what.c_str()}.find("1 unbound parameter") != std::string::npos);
    REQUIRE(pg_probe_queries().empty());
}

TEST_CASE("PostgressManager::describe: a raw-SQL sub-query stub is filled from the statement it generates") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    pg_actor_stack stack(&arena);
    pg_reset({{"id", kVarcharOid, components::types::logical_type::STRING_LITERAL}});

    auto classified = stack.classify(36, stack.parse(std::string{kPgSubquerySql}));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    auto data = std::move(classified.value());
    {
        const auto* stub = pg_raw_stub(*data);
        REQUIRE(stub != nullptr);
        REQUIRE(stub->raw_sql() == kPgSubqueryBody);
        REQUIRE(stub->schema().type() == components::types::logical_type::NA);
    }

    auto described = stack.describe(36, std::move(data));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());
    data = std::move(described.value());

    const auto* stub = pg_raw_stub(*data);
    REQUIRE(stub != nullptr);
    // Filled, not replaced: execute still generates its statement from this text.
    REQUIRE(stub->has_raw_sql());
    REQUIRE(stub->raw_sql() == kPgSubqueryBody);
    REQUIRE_FALSE(stub->qualifiers().empty());
    require_single_column(stub->schema(), "id", components::types::logical_type::STRING_LITERAL);

    const auto probes = pg_probe_queries();
    REQUIRE(probes.size() == 1);
    REQUIRE(probes.front() == aliased_probe_of(std::string{kPgSubqueryRendered}));
}

TEST_CASE("PostgressManager::describe: every stub of a statement is probed — raw-SQL and catalog alike") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    pg_actor_stack stack(&arena);
    pg_reset({{"id", kVarcharOid, components::types::logical_type::STRING_LITERAL}});

    auto classified = stack.classify(37, stack.parse(std::string{kPgSubqueryJoinSql}));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    REQUIRE(pg_stubs(*classified.value()).size() == 2);

    auto described = stack.describe(37, std::move(classified.value()));
    INFO("describe: " << (described.has_error() ? described.error().what.c_str() : "ok"));
    REQUIRE_FALSE(described.has_error());

    const auto stubs = pg_stubs(*described.value());
    REQUIRE(stubs.size() == 2);
    for (const auto* stub : stubs) {
        require_single_column(stub->schema(), "id", components::types::logical_type::STRING_LITERAL);
    }

    const auto probes = pg_probe_queries();
    REQUIRE(probes.size() == 2);
    REQUIRE(std::find(probes.begin(), probes.end(), aliased_probe_of(std::string{kPgSubqueryRendered})) !=
            probes.end());
}

// The end-to-end acceptance through the Scheduler: without the describe send in
// Worker::prepare_schema each case is red with the plan's own type — a red case
// here is that routing gone, not a mock detail.
TEST_CASE("PostgreSQL prepare_schema: length(name) AS name is prepared and streamed as the same INTEGER column",
          "[pg-describe-routing]") {
    pg_describe_stack_owner owner("/tmp/test_single_backend_prepare_pg_func");
    pg_reset({{"name", kInt4Oid, components::types::logical_type::INTEGER}});
    pg_require_prepared_and_streamed(owner.stack(),
                                     9600,
                                     pg_from("SELECT length(name) AS name"),
                                     "name",
                                     components::types::logical_type::INTEGER,
                                     flight::ipc::TypeId::Int32);
    REQUIRE(pg_probe_queries().size() == 1);
    REQUIRE(pg_probe_queries().front() == aliased_probe_of(pg_data_queries().front()));
}

TEST_CASE("PostgreSQL prepare_schema: a raw sub-query column is prepared as the backend's type",
          "[pg-describe-routing]") {
    pg_describe_stack_owner owner("/tmp/test_single_backend_prepare_pg_subquery");
    pg_reset({{"id", kVarcharOid, components::types::logical_type::STRING_LITERAL}});

    // Without the described stub this column reaches the client as NA: the
    // aggregate over the sub-query names no table, so nothing types `id`.
    auto prepared = prepare_scheduler_sql(owner.stack(), 9620, std::string{kPgSubquerySql});
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    REQUIRE(prepared.value().tag == T_SelectStmt);
    require_single_column(prepared.value().schema, "id", components::types::logical_type::STRING_LITERAL);
    REQUIRE(pg_probe_queries().size() == 1);
    REQUIRE(pg_probe_queries().front() == aliased_probe_of(std::string{kPgSubqueryRendered}));
}

TEST_CASE("PostgreSQL prepare_schema: a JOIN with a raw sub-query is prepared from both described stubs",
          "[pg-describe-routing]") {
    pg_describe_stack_owner owner("/tmp/test_single_backend_prepare_pg_subquery_join");
    pg_reset({{"id", kVarcharOid, components::types::logical_type::STRING_LITERAL}});

    // An undescribed side makes the merged schema of a JOIN NA, which the schema
    // computation answers with `OtterBrix collection is missing in catalog` — the
    // prepare failed outright before both sides were described.
    auto prepared = prepare_scheduler_sql(owner.stack(), 9640, std::string{kPgSubqueryJoinSql});
    INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
    REQUIRE_FALSE(prepared.has_error());
    require_single_column(prepared.value().schema, "id", components::types::logical_type::STRING_LITERAL);
    REQUIRE(pg_probe_queries().size() == 2);
}

TEST_CASE("PostgreSQL prepare_schema: a probe the backend refuses fails the prepare with the backend's error",
          "[pg-describe-routing]") {
    pg_describe_stack_owner owner("/tmp/test_single_backend_prepare_pg_refused");
    pg_reset({{"name", kInt4Oid, components::types::logical_type::INTEGER}}, pg_probe_mode::refused);
    auto prepared = prepare_scheduler_sql(owner.stack(), 9660, pg_from("SELECT length(name) AS name"));
    REQUIRE(prepared.has_error());
    REQUIRE(prepared.error().type == core::error_code_t::io_error);
    REQUIRE(std::string{prepared.error().what.c_str()}.find("simulated probe refusal") != std::string::npos);
}

// ── Mixed: every participant describes its own slots ─────────────────────────
// A cross-backend statement has one stub per slot, and the Worker sends it to
// each backend that owns one, in the order run_pipeline executes them. A manager
// fills only the stubs its node_backend_types entry names — the same skip
// `execute` makes — so the two backends' answers land in their own slots and
// neither probes the other's.

namespace {

    constexpr std::string_view kMixedJoinSql = "SELECT e.name FROM myx.mydb.schema.events e "
                                               "JOIN pgx.pgdb.public.events p ON e.name = p.name;";

    // Both typed connectors under one catalog: what a two-backend deployment
    // gives a Mixed statement.
    class mixed_actor_stack {
    public:
        explicit mixed_actor_stack(std::pmr::memory_resource* res)
            : resource_(res)
            , otterbrix_manager_(actor_zeta::spawn<db::OtterbrixManager>(
                  res,
                  std::make_unique<SimpleMockOtterbrixManager>(mock_config{.resource = res})))
            , catalog_(actor_zeta::spawn<mysql::CatalogManager>(res, otterbrix_manager_->address()))
            , my_conn_(std::make_unique<mysql::ConnectorManager>(res, catalog_->address(), &typed_my_factory, 1))
            , my_mgr_(actor_zeta::spawn<db::MySQLManager>(res, my_conn_.get()))
            , pg_conn_(std::make_unique<pg::ConnectorManager>(res, catalog_->address(), &typed_pgd_factory, 1))
            , pg_mgr_(actor_zeta::spawn<db::PostgressManager>(res, pg_conn_.get()))
            , parser_(res) {
            catalog_->set_backend_managers(my_mgr_->address(),
                                           pg_mgr_->address(),
                                           actor_zeta::address_t::empty_address());
            boost::mysql::connect_params my_params;
            my_params.database = "mydb";
            auto my_added = my_conn_->addConnection(my_params, kMyUid);
            INFO("addConnection mysql: " << (my_added.has_error() ? my_added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(my_added.has_error());
            auto pg_added = pg_conn_->addConnection(pgd_params());
            INFO("addConnection pg: " << (pg_added.has_error() ? pg_added.error().what.c_str() : "ok"));
            REQUIRE_FALSE(pg_added.has_error());
        }

        mixed_actor_stack(const mixed_actor_stack&) = delete;
        mixed_actor_stack& operator=(const mixed_actor_stack&) = delete;

        ParsedQueryDataPtr parse(const std::string& sql) {
            auto parsed = parser_.parse(sql);
            INFO("parse: " << (parsed.has_error() ? parsed.error().what.c_str() : "ok"));
            REQUIRE_FALSE(parsed.has_error());
            return std::move(parsed.value());
        }

        core::result_wrapper_t<ParsedQueryDataPtr> classify(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(catalog_->address(), &mysql::CatalogManager::get_catalog_schema, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::result_wrapper_t<ParsedQueryDataPtr> describe_mysql(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(my_mgr_->address(), &db::MySQLManager::describe, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

        core::result_wrapper_t<ParsedQueryDataPtr> describe_pg(session_hash_t id, ParsedQueryDataPtr data) {
            auto [needs_sched, future] =
                actor_zeta::send(pg_mgr_->address(), &db::PostgressManager::describe, id, std::move(data));
            otterstax::test::wait_until_ready(future);
            return std::move(future).take_ready();
        }

    private:
        std::pmr::memory_resource* resource_;
        std::unique_ptr<db::OtterbrixManager, actor_zeta::pmr::deleter_t> otterbrix_manager_;
        std::unique_ptr<mysql::CatalogManager, actor_zeta::pmr::deleter_t> catalog_;
        std::unique_ptr<mysql::ConnectorManager> my_conn_;
        std::unique_ptr<db::MySQLManager, actor_zeta::pmr::deleter_t> my_mgr_;
        std::unique_ptr<pg::ConnectorManager> pg_conn_;
        std::unique_ptr<db::PostgressManager, actor_zeta::pmr::deleter_t> pg_mgr_;
        GreenplumParser parser_;
    };

    // The stub standing in for `uid`'s slot, or nullptr when that slot is not a
    // stub at all.
    const schema_utils::schema_node_t* stub_of_uid(const ParsedQueryData& data, std::string_view uid) {
        for (const auto& batch : data.otterbrix_params->external_nodes) {
            for (const auto& slot : batch) {
                if (slot.target.name.unique_identifier != uid) {
                    continue;
                }
                if ((*slot.node)->type() != components::logical_plan::node_type::unused) {
                    return nullptr;
                }
                return static_cast<const schema_utils::schema_node_t*>(slot.node->get());
            }
        }
        return nullptr;
    }

} // namespace

TEST_CASE("Mixed describe: each backend fills its own stubs and probes nothing else") {
    std::pmr::synchronized_pool_resource arena(std::pmr::new_delete_resource());
    mixed_actor_stack stack(&arena);
    // Each backend answers a column of its own type under the same name.
    my_reset({{"name", boost::mysql::column_type::bigint, components::types::logical_type::BIGINT}});
    pg_reset({{"name", kVarcharOid, components::types::logical_type::STRING_LITERAL}});

    auto classified = stack.classify(41, stack.parse(std::string{kMixedJoinSql}));
    INFO("classify: " << (classified.has_error() ? classified.error().what.c_str() : "ok"));
    REQUIRE_FALSE(classified.has_error());
    auto data = std::move(classified.value());
    REQUIRE(data->backend_type == backend_type_t::Mixed);
    REQUIRE(stub_of_uid(*data, kMyUid) != nullptr);
    REQUIRE(stub_of_uid(*data, kPgUid) != nullptr);

    // The Worker's order: MySQL first, then PostgreSQL.
    auto after_mysql = stack.describe_mysql(41, std::move(data));
    INFO("describe mysql: " << (after_mysql.has_error() ? after_mysql.error().what.c_str() : "ok"));
    REQUIRE_FALSE(after_mysql.has_error());
    data = std::move(after_mysql.value());
    // The PostgreSQL slot is untouched by the MySQL pass, and unprobed.
    require_single_column(stub_of_uid(*data, kMyUid)->schema(), "name", components::types::logical_type::BIGINT);
    REQUIRE(stub_of_uid(*data, kPgUid)->schema().type() != components::types::logical_type::STRING_LITERAL);
    REQUIRE(my_probe_queries().size() == 1);
    REQUIRE(pg_probe_queries().empty());

    auto after_pg = stack.describe_pg(41, std::move(data));
    INFO("describe pg: " << (after_pg.has_error() ? after_pg.error().what.c_str() : "ok"));
    REQUIRE_FALSE(after_pg.has_error());
    data = std::move(after_pg.value());

    // Both slots now carry their own backend's answer, and neither backend was
    // asked about the other's slot.
    require_single_column(stub_of_uid(*data, kMyUid)->schema(), "name", components::types::logical_type::BIGINT);
    require_single_column(stub_of_uid(*data, kPgUid)->schema(),
                          "name",
                          components::types::logical_type::STRING_LITERAL);
    REQUIRE(my_probe_queries().size() == 1);
    REQUIRE(pg_probe_queries().size() == 1);
    REQUIRE(my_probe_queries().front().find("`mydb`.`events`") != std::string::npos);
    REQUIRE(pg_probe_queries().front().find("\"public\".\"events\"") != std::string::npos);
}
