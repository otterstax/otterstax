// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The prepared-statement schema of a SELECT over remote tables, and the rows
// the execute streams under it. The engine describes the statement (plan-only
// EXPLAIN) with the remote tables' declared columns from
// federation.remote_columns — no backend round-trip — under its own type rules
// (count(*) UBIGINT, an integer sum BIGINT, avg DOUBLE, the engine's arithmetic
// and function types); the execute holds the backend's answer to exactly those
// types (otterbrix/federation, conform_to_result), so what a frontend is told at
// prepare is what it streams. A FlightSQL GetFlightInfoStatement refuses a
// row-producing statement whose prepared schema is unresolved, and DoGet
// streams over the prepared schema alone.
//
// First over a typed PostgreSQL mock (the discovered columns with their
// PostgreSQL types; the data path answers rows of the projected columns), then
// the same rules over a typed mock of each backend — ClickHouse, MySQL,
// PostgreSQL — whose pushed statements answer the backend's own result types
// (UInt64 counts, MySQL's DECIMAL text for SUM / AVG, PostgreSQL's numeric).

#include "otterbrix/translators/output/chunk_to_arrow.hpp"
#include "integration/clickhouse/connection_manager.hpp"
#include "integration/postgresql/connection_manager.hpp"
#include "integration/sql/connection_manager.hpp"
#include "session_stack.hpp"
#include "test_helpers.hpp"

#include "../mock/mock_config.hpp"
#include "../mock/sql_db_connector.hpp"

#include <catch2/catch_all.hpp>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <libpq-fe.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using otterstax::test::execute_session_statement;
using otterstax::test::prepare_session_sql;
using otterstax::test::session_stack;

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
    // backend answers the discovery's probe over the base table with.
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

    // The probed table's every attribute: the discovery's schema probe.
    std::unique_ptr<PGresult, decltype(&PQclear)> make_probe_result() {
        std::vector<std::size_t> all(kColumns.size());
        for (std::size_t c = 0; c < kColumns.size(); ++c) {
            all[c] = c;
        }
        return make_result_for(all);
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

        // Discovery: the pg_enum query lists no enums, the schema probe answers
        // the table's attributes. Nothing else reaches this overload: a prepare
        // asks the backend nothing.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                     &PQclear);
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
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

    // Real engine, real parser; the PostgreSQL connection is the typed mock
    // configured under kUid with its single table discovered at startup.
    class single_backend_stack_owner {
    public:
        explicit single_backend_stack_owner(const std::string& data_dir)
            : owner_(data_dir, options()) {}

        single_backend_stack_owner(const single_backend_stack_owner&) = delete;
        single_backend_stack_owner& operator=(const single_backend_stack_owner&) = delete;

        session_stack stack() const { return owner_.stack(); }

    private:
        static otterstax::test::stack_options_t options() {
            otterstax::test::stack_options_t options;
            options.connectors.postgres = &typed_pg_factory;
            options.connections.postgresql.push_back(config::PgConnectionDesc{.alias = kUid,
                                                                              .host = "localhost",
                                                                              .port = "5432",
                                                                              .username = "user",
                                                                              .password = "pass",
                                                                              .database = "pgdb",
                                                                              .schema = "public",
                                                                              .table = "products"});
            return options;
        }

        otterstax::test::session_stack_owner owner_;
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


} // namespace

TEST_CASE("single-backend prepare_schema: a column list resolves to the discovered columns in order") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_columns");
    auto s = owner.stack();
    session_hash_t id = 9800;

    require_prepared_columns(prepare_session_sql(s, id++, from_table("SELECT product_id, product_name")), {0, 2});
    // Statement order, not backend order.
    require_prepared_columns(prepare_session_sql(s, id++, from_table("SELECT category, price, product_id")),
                             {4, 3, 0});
}

TEST_CASE("single-backend prepare_schema: SELECT * resolves to every discovered column") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_star");
    auto s = owner.stack();
    session_hash_t id = 9820;

    require_prepared_columns(prepare_session_sql(s, id++, from_table("SELECT *")), {0, 1, 2, 3, 4});
    require_prepared_columns(prepare_session_sql(s, id++, from_table("SELECT *", " WHERE price > 10")),
                             {0, 1, 2, 3, 4});
}

TEST_CASE("single-backend prepare_schema: WHERE, ORDER BY and LIMIT leave the projected schema") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_clauses");
    auto s = owner.stack();
    session_hash_t id = 9840;

    require_prepared_columns(
        prepare_session_sql(s, id++, from_table("SELECT product_name, price", " WHERE price > 10 ORDER BY price")),
        {2, 3});
    require_prepared_columns(prepare_session_sql(s, id++, from_table("SELECT product_id", " LIMIT 1")), {0});
    const std::string clauses = from_table("SELECT *", " WHERE campaign_id = 10 ORDER BY price DESC LIMIT 5");
    require_prepared_columns(prepare_session_sql(s, id++, clauses), {0, 1, 2, 3, 4});
}

TEST_CASE("single-backend prepare_schema: a parameterized SELECT carries its parameter count") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_param");
    auto s = owner.stack();

    // The projection of a remote SELECT is the declared columns' and does not
    // depend on the parameter, so the extended-protocol frontends describe its
    // columns at Parse / COM_STMT_PREPARE; GetFlightInfoStatement binds
    // nothing and refuses the statement by this count, whatever the schema.
    auto prepared = prepare_session_sql(s, 9860, from_table("SELECT product_id, price", " WHERE product_id = $1"));
    require_prepared_columns(prepared, {0, 3}, /*parameter_count*/ 1);
}

TEST_CASE("single-backend prepare_schema: DoGet streams the backend rows under the prepared schema") {
    single_backend_stack_owner owner("/tmp/test_single_backend_prepare_stream");
    auto s = owner.stack();
    const session_hash_t stmt = 9880;

    auto prepared = prepare_session_sql(s, stmt, from_table("SELECT product_id, product_name, price"));
    require_prepared_columns(prepared, {0, 2, 3});

    auto flight_schema = to_arrow_schema(s.resource, prepared.value().schema);
    INFO("arrow schema error: " << (flight_schema.has_error() ? flight_schema.error().what.c_str() : "ok"));
    REQUIRE_FALSE(flight_schema.has_error());
    REQUIRE(flight_schema.value()->num_fields() == 3);
    REQUIRE(flight_schema.value()->field(0)->type()->id() == arrow::Type::INT32);
    REQUIRE(flight_schema.value()->field(1)->type()->id() == arrow::Type::STRING);
    REQUIRE(flight_schema.value()->field(2)->type()->id() == arrow::Type::DOUBLE);

    auto executed = execute_session_statement(s, stmt);
    INFO("execute error: " << executed.error().what.c_str());
    REQUIRE_FALSE(executed.has_error());
    REQUIRE(executed.value().size() == 2);
    REQUIRE(executed.value().column_count() == 3);

    auto batch = chunk_to_record_batch(s.resource, executed.value().chunks.front());
    REQUIRE_FALSE(batch.has_error());
    REQUIRE(batch.value()->schema()->Equals(*flight_schema.value()));
    REQUIRE(batch.value()->num_rows() == 2);
    const auto ids = std::static_pointer_cast<arrow::Int32Array>(batch.value()->column(0));
    const auto names = std::static_pointer_cast<arrow::StringArray>(batch.value()->column(1));
    const auto prices = std::static_pointer_cast<arrow::DoubleArray>(batch.value()->column(2));
    REQUIRE(ids->Value(0) == 1);
    REQUIRE(ids->Value(1) == 2);
    REQUIRE(names->GetString(0) == "widget");
    REQUIRE(names->GetString(1) == "gadget");
    REQUIRE(prices->Value(0) == 9.5);
    REQUIRE(prices->Value(1) == 19.25);
}

// ── The engine's type rules, per backend ─────────────────────────────────────
// One base table events(id INT32, score INT32, name STRING) on each backend.
// A pushed statement (it ends with `;`) answers the result columns the case
// configures, typed as that backend types them; a scan of the table (no `;`)
// answers the base columns. Three rows either way, row r holding r + 1 in an
// integer, r + 0.5 in a float, the decimal text "<r>.5" and "v<r>".

namespace {

    using components::types::logical_type;

    enum class wire_t
    {
        int32,
        int64,
        uint64,
        float64,
        decimal_text,
        string
    };

    struct result_column_t {
        std::string name;
        wire_t wire;
    };

    std::mutex g_rules_mutex;
    std::vector<result_column_t> g_rules_result;
    bool g_rules_refuse = false;

    void rules_reset(std::vector<result_column_t> result, bool refuse = false) {
        std::lock_guard guard(g_rules_mutex);
        g_rules_result = std::move(result);
        g_rules_refuse = refuse;
    }

    // What the backend answers `query` with: nullopt for a refusal.
    std::optional<std::vector<result_column_t>> rules_answer(std::string_view query) {
        std::lock_guard guard(g_rules_mutex);
        if (g_rules_refuse) {
            return std::nullopt;
        }
        if (!query.empty() && query.back() == ';') {
            return g_rules_result;
        }
        return std::vector<result_column_t>{{"id", wire_t::int32}, {"score", wire_t::int32}, {"name", wire_t::string}};
    }

    constexpr std::size_t kRulesRows = 3;

    std::string text_of(wire_t wire, std::size_t row) {
        switch (wire) {
            case wire_t::int32:
            case wire_t::int64:
            case wire_t::uint64:
                return std::to_string(row + 1);
            case wire_t::float64:
            case wire_t::decimal_text:
                return std::to_string(row) + ".5";
            case wire_t::string:
                return "v" + std::to_string(row);
        }
        return {};
    }

    // ── ClickHouse ──

    clickhouse::ColumnRef ch_column(wire_t wire, std::size_t rows) {
        switch (wire) {
            case wire_t::int32: {
                auto column = std::make_shared<clickhouse::ColumnInt32>();
                for (std::size_t row = 0; row < rows; ++row) {
                    column->Append(static_cast<int32_t>(row + 1));
                }
                return column;
            }
            case wire_t::int64: {
                auto column = std::make_shared<clickhouse::ColumnInt64>();
                for (std::size_t row = 0; row < rows; ++row) {
                    column->Append(static_cast<int64_t>(row + 1));
                }
                return column;
            }
            case wire_t::uint64: {
                auto column = std::make_shared<clickhouse::ColumnUInt64>();
                for (std::size_t row = 0; row < rows; ++row) {
                    column->Append(static_cast<uint64_t>(row + 1));
                }
                return column;
            }
            case wire_t::float64: {
                auto column = std::make_shared<clickhouse::ColumnFloat64>();
                for (std::size_t row = 0; row < rows; ++row) {
                    column->Append(static_cast<double>(row) + 0.5);
                }
                return column;
            }
            case wire_t::decimal_text:
            case wire_t::string: {
                auto column = std::make_shared<clickhouse::ColumnString>();
                for (std::size_t row = 0; row < rows; ++row) {
                    column->Append(text_of(wire, row));
                }
                return column;
            }
        }
        return {};
    }

    clickhouse::Block ch_block(const std::vector<result_column_t>& columns, std::size_t rows) {
        clickhouse::Block block;
        for (const auto& column : columns) {
            block.AppendColumn(column.name, ch_column(column.wire, rows));
        }
        return block;
    }

    class rules_ch_connector final : public ch::IConnector {
    public:
        rules_ch_connector(std::pmr::memory_resource* resource, ch::connect_params params, std::string alias)
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

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const ch::select_result_t&)> handler)
            override {
            auto answer = rules_answer(query);
            if (!answer) {
                co_return core::error_t(core::error_code_t::io_error,
                                        std::pmr::string{"simulated ClickHouse refusal", resource_});
            }
            ch::select_result_t outcome;
            outcome.blocks.push_back(ch_block(*answer, kRulesRows));
            co_return handler(outcome);
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(const ch::select_result_t&)>) override {
            co_return int64_t{0};
        }

        // Discovery: system.columns, then the base table's header.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t answer;
            if (query.find("system.columns") != std::string_view::npos) {
                auto names = std::make_shared<clickhouse::ColumnString>();
                auto types = std::make_shared<clickhouse::ColumnString>();
                for (const char* name : {"id", "score"}) {
                    names->Append(name);
                    types->Append("Int32");
                }
                names->Append("name");
                types->Append("String");
                clickhouse::Block block;
                block.AppendColumn("name", names);
                block.AppendColumn("type", types);
                answer.blocks.push_back(std::move(block));
            } else {
                answer.blocks.push_back(ch_block(*rules_answer(""), 0));
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(answer));
        }

    private:
        std::pmr::memory_resource* resource_;
        ch::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<ch::IConnector>
    rules_ch_factory(std::pmr::memory_resource* resource, ch::connect_params params, std::string alias) {
        return std::make_unique<rules_ch_connector>(resource, std::move(params), std::move(alias));
    }

    // ── MySQL ──

    mysql::mock_column_t my_column(const result_column_t& column) {
        switch (column.wire) {
            case wire_t::int32:
                return {column.name, boost::mysql::column_type::int_};
            case wire_t::int64:
                return {column.name, boost::mysql::column_type::bigint};
            case wire_t::uint64:
                return {column.name, boost::mysql::column_type::bigint, true};
            case wire_t::float64:
                return {column.name, boost::mysql::column_type::double_};
            case wire_t::decimal_text:
                return {column.name, boost::mysql::column_type::decimal};
            case wire_t::string:
                return {column.name, boost::mysql::column_type::varchar};
        }
        return {column.name, boost::mysql::column_type::varchar};
    }

    boost::mysql::results my_results(const std::vector<result_column_t>& columns, std::size_t rows) {
        std::vector<mysql::mock_column_t> definitions;
        for (const auto& column : columns) {
            definitions.push_back(my_column(column));
        }
        std::vector<mysql::mock_row_t> values(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            for (const auto& column : columns) {
                values[row].emplace_back(text_of(column.wire, row));
            }
        }
        return mysql::mock_results(definitions, values);
    }

    class rules_my_connector final : public mysql::MockConnector {
    public:
        rules_my_connector(mock_config config, std::string alias)
            : mysql::MockConnector(config, std::move(alias))
            , resource_(config.resource) {}

        // The data statement through the real translator: the handler reads the
        // manufactured result set.
        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(const boost::mysql::results&)> handler)
            override {
            auto answer = rules_answer(query);
            if (!answer) {
                co_return core::error_t(core::error_code_t::io_error,
                                        std::pmr::string{"simulated MySQL refusal", resource_});
            }
            const auto results = my_results(*answer, kRulesRows);
            co_return handler(results);
        }

        // Discovery: the information_schema listing (MySQL discovers the whole
        // configured database), then the probe of the base table.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            if (query.find("information_schema") != std::string_view::npos) {
                const auto listing =
                    mysql::mock_results({{"TABLE_NAME", boost::mysql::column_type::varchar}}, {{std::string{"events"}}});
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(listing));
            }
            const auto results = my_results(*rules_answer(""), 0);
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(results));
        }

    private:
        std::pmr::memory_resource* resource_;
    };

    std::unique_ptr<mysql::IConnector> rules_my_factory(std::pmr::memory_resource* resource,
                                                        boost::asio::io_context&,
                                                        boost::mysql::connect_params,
                                                        std::string alias) {
        return std::make_unique<rules_my_connector>(mock_config{.resource = resource}, std::move(alias));
    }

    // ── PostgreSQL ──

    Oid pg_oid(wire_t wire) {
        switch (wire) {
            case wire_t::int32:
                return 23;
            case wire_t::int64:
            case wire_t::uint64:
                return 20;
            case wire_t::float64:
                return 701;
            case wire_t::decimal_text:
                return 1700;
            case wire_t::string:
                return 25;
        }
        return 25;
    }

    std::unique_ptr<PGresult, decltype(&PQclear)> pg_result(const std::vector<result_column_t>& columns,
                                                            std::size_t rows) {
        std::unique_ptr<PGresult, decltype(&PQclear)> result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), &PQclear);
        std::vector<PGresAttDesc> attrs(columns.size());
        for (std::size_t c = 0; c < columns.size(); ++c) {
            attrs[c] = PGresAttDesc{const_cast<char*>(columns[c].name.c_str()), 0, 0, 0, pg_oid(columns[c].wire), -1, -1};
        }
        PQsetResultAttrs(result.get(), static_cast<int>(attrs.size()), attrs.data());
        for (std::size_t row = 0; row < rows; ++row) {
            for (std::size_t c = 0; c < columns.size(); ++c) {
                auto text = text_of(columns[c].wire, row);
                PQsetvalue(result.get(),
                           static_cast<int>(row),
                           static_cast<int>(c),
                           text.data(),
                           static_cast<int>(text.size()));
            }
        }
        return result;
    }

    class rules_pg_connector final : public pg::IConnector {
    public:
        rules_pg_connector(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias)
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
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<data_chunk_t>(PGresult*)> handler) override {
            auto answer = rules_answer(query);
            if (!answer) {
                co_return core::error_t(core::error_code_t::io_error,
                                        std::pmr::string{"simulated PostgreSQL refusal", resource_});
            }
            auto result = pg_result(*answer, kRulesRows);
            co_return handler(result.get());
        }

        boost::asio::awaitable<core::result_wrapper_t<int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<int64_t(PGresult*)>) override {
            co_return int64_t{0};
        }

        // Discovery: no enums, the base table's attributes.
        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            if (query.find("pg_enum") != std::string_view::npos) {
                std::unique_ptr<PGresult, decltype(&PQclear)> empty(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK),
                                                                    &PQclear);
                co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(empty.get()));
            }
            auto result = pg_result(*rules_answer(""), 0);
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(result.get()));
        }

    private:
        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    std::unique_ptr<pg::IConnector>
    rules_pg_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<rules_pg_connector>(resource, std::move(params), std::move(alias));
    }

    // ── the stack: one connection of each backend, `events` discovered ──

    class rules_stack_owner {
    public:
        explicit rules_stack_owner(const std::string& leaf)
            : owner_(otterstax::test::unique_test_dir(leaf), options()) {}

        session_stack stack() const { return owner_.stack(); }

    private:
        static otterstax::test::stack_options_t options() {
            otterstax::test::stack_options_t options;
            options.connectors.clickhouse = &rules_ch_factory;
            options.connectors.mysql = &rules_my_factory;
            options.connectors.postgres = &rules_pg_factory;
            options.connections.clickhouse.push_back(
                config::ChConnectionDesc{.alias = "chx", .host = "localhost", .database = "chdb", .table = "events"});
            options.connections.mysql.push_back(
                config::MysqlConnectionDesc{.alias = "myx", .host = "localhost", .database = "mydb", .table = "events"});
            options.connections.postgresql.push_back(config::PgConnectionDesc{.alias = "pgx",
                                                                              .host = "localhost",
                                                                              .database = "pgdb",
                                                                              .schema = "public",
                                                                              .table = "events"});
            return options;
        }

        otterstax::test::session_stack_owner owner_;
    };

    struct backend_case_t {
        const char* label;
        const char* table; // the remote table as SQL names it
    };

    constexpr backend_case_t kBackends[] = {
        {"ClickHouse", "chx.chdb.events"},
        {"MySQL", "myx.mydb.events"},
        {"PostgreSQL", "pgx.pgdb.public.events"},
    };

    std::string over(std::string_view select, std::string_view table, std::string_view tail = "") {
        std::string sql{select};
        sql += " FROM ";
        sql += table;
        sql += tail;
        sql += ";";
        return sql;
    }

    // Prepare `sql`, execute the prepared statement and check that the
    // prepared columns are `expected` (name, type), that every executed chunk
    // carries exactly those types, and that it streams through the Arrow form
    // of the prepared schema — the FlightSQL contract. Returns the executed
    // payload.
    session_payload require_prepared_and_streamed(const session_stack& s,
                                                  session_hash_t stmt,
                                                  const std::string& sql,
                                                  const std::vector<std::pair<std::string, logical_type>>& expected) {
        INFO(sql);
        auto prepared = prepare_session_sql(s, stmt, sql);
        INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        const auto schema = prepared.value().schema;
        REQUIRE(schema.type() == logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == expected.size());
        for (std::size_t i = 0; i < expected.size(); ++i) {
            INFO("prepared column " << i);
            CHECK(schema.child_types()[i].alias() == expected[i].first);
            CHECK(schema.child_types()[i].type() == expected[i].second);
        }

        auto executed = execute_session_statement(s, stmt);
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        auto flight_schema = to_arrow_schema(s.resource, schema);
        INFO("arrow schema: " << (flight_schema.has_error() ? flight_schema.error().what.c_str() : "ok"));
        REQUIRE_FALSE(flight_schema.has_error());
        for (const auto& chunk : executed.value().chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            const auto types = chunk.types();
            REQUIRE(types.size() == expected.size());
            for (std::size_t i = 0; i < expected.size(); ++i) {
                INFO("executed column " << i);
                CHECK(types[i].type() == schema.child_types()[i].type());
            }
            auto batch = chunk_to_record_batch(s.resource, chunk);
            INFO("chunk_to_record_batch: " << (batch.has_error() ? batch.error().what.c_str() : "ok"));
            REQUIRE_FALSE(batch.has_error());
            CHECK(batch.value()->schema()->Equals(*flight_schema.value()));
        }
        return std::move(executed.value());
    }

} // namespace

TEST_CASE("remote prepare_schema: COUNT(*) is the engine's UBIGINT whatever the backend answers") {
    rules_stack_owner owner("test_prepare_rules_count");
    const auto s = owner.stack();
    session_hash_t id = 20000;
    const std::pair<const char*, wire_t> wires[] = {{"ClickHouse", wire_t::uint64},
                                                    {"MySQL", wire_t::int64},
                                                    {"PostgreSQL", wire_t::int64}};
    for (std::size_t b = 0; b < std::size(kBackends); ++b) {
        INFO(kBackends[b].label);
        rules_reset({{"n", wires[b].second}});
        auto executed =
            require_prepared_and_streamed(s, id++, over("SELECT count(*) AS n", kBackends[b].table), {{"n", logical_type::UBIGINT}});
        CHECK(executed.size() == kRulesRows);
    }
}

TEST_CASE("remote prepare_schema: SUM and AVG of an integer column are BIGINT and DOUBLE") {
    rules_stack_owner owner("test_prepare_rules_sum_avg");
    const auto s = owner.stack();
    session_hash_t id = 20100;
    // ClickHouse answers Int64 and Float64, MySQL DECIMAL (its text), PostgreSQL
    // int8 for the sum and numeric for the average.
    const std::pair<wire_t, wire_t> wires[] = {{wire_t::int64, wire_t::float64},
                                               {wire_t::decimal_text, wire_t::decimal_text},
                                               {wire_t::int64, wire_t::decimal_text}};
    for (std::size_t b = 0; b < std::size(kBackends); ++b) {
        INFO(kBackends[b].label);
        rules_reset({{"s", wires[b].first}, {"a", wires[b].second}});
        auto executed = require_prepared_and_streamed(s,
                                                      id++,
                                                      over("SELECT sum(score) AS s, avg(score) AS a", kBackends[b].table),
                                                      {{"s", logical_type::BIGINT}, {"a", logical_type::DOUBLE}});
        REQUIRE(executed.size() == kRulesRows);
        const auto& chunk = executed.chunks.front();
        CHECK(chunk.value(0, 0).value<int64_t>() == 1);
        CHECK(chunk.value(1, 0).value<double>() == 0.5);
    }
}

TEST_CASE("remote prepare_schema: arithmetic and a function aliased as a column take the engine's types") {
    rules_stack_owner owner("test_prepare_rules_expressions");
    const auto s = owner.stack();
    session_hash_t id = 20200;
    // What each backend widens to: ClickHouse Int64 / UInt64, MySQL BIGINT,
    // PostgreSQL int4.
    const std::pair<wire_t, wire_t> wires[] = {{wire_t::int64, wire_t::uint64},
                                               {wire_t::int64, wire_t::int64},
                                               {wire_t::int32, wire_t::int32}};
    for (std::size_t b = 0; b < std::size(kBackends); ++b) {
        INFO(kBackends[b].label);
        rules_reset({{"score", wires[b].first}});
        require_prepared_and_streamed(s, id++, over("SELECT score + 1 AS score", kBackends[b].table), {{"score", logical_type::BIGINT}});
        rules_reset({{"name", wires[b].second}});
        require_prepared_and_streamed(s, id++, over("SELECT length(name) AS name", kBackends[b].table), {{"name", logical_type::BIGINT}});
    }
}

TEST_CASE("remote prepare_schema: a derived table's column is prepared as the engine types it") {
    rules_stack_owner owner("test_prepare_rules_derived");
    const auto s = owner.stack();
    session_hash_t id = 20300;
    const wire_t wires[] = {wire_t::int64, wire_t::int64, wire_t::int32};
    for (std::size_t b = 0; b < std::size(kBackends); ++b) {
        INFO(kBackends[b].label);
        rules_reset({{"s", wires[b]}});
        const std::string sql =
            std::string{"SELECT d.s FROM (SELECT score + 1 AS s FROM "} + kBackends[b].table + ") AS d;";
        auto executed = require_prepared_and_streamed(s, id++, sql, {{"s", logical_type::BIGINT}});
        CHECK(executed.size() == kRulesRows);
    }
}

TEST_CASE("remote prepare_schema: a JOIN of a remote table with a derived table over it is prepared whole") {
    rules_stack_owner owner("test_prepare_rules_join");
    const auto s = owner.stack();
    session_hash_t id = 20400;
    for (std::size_t b = 0; b < std::size(kBackends); ++b) {
        INFO(kBackends[b].label);
        // The table and the derived table over it are of one server: the JOIN is one statement of the
        // backend, which answers (name, s).
        rules_reset({{"name", wire_t::string}, {"s", b == 2 ? wire_t::int32 : wire_t::int64}});
        const std::string sql = std::string{"SELECT e.name, d.s FROM "} + kBackends[b].table +
                                " AS e JOIN (SELECT id, score + 1 AS s FROM " + kBackends[b].table +
                                ") AS d ON e.id = d.id;";
        auto executed = require_prepared_and_streamed(s,
                                                      id++,
                                                      sql,
                                                      {{"name", logical_type::STRING_LITERAL}, {"s", logical_type::BIGINT}});
        CHECK(executed.size() == kRulesRows);
    }
}

TEST_CASE("remote prepare_schema: the backend is not asked at prepare; its refusal fails the execute") {
    rules_stack_owner owner("test_prepare_rules_refused");
    const auto s = owner.stack();
    session_hash_t id = 20500;
    const char* refusals[] = {"simulated ClickHouse refusal", "simulated MySQL refusal", "simulated PostgreSQL refusal"};
    for (std::size_t b = 0; b < std::size(kBackends); ++b) {
        INFO(kBackends[b].label);
        rules_reset({{"n", wire_t::int64}}, /*refuse=*/true);
        const auto stmt = id++;
        auto prepared = prepare_session_sql(s, stmt, over("SELECT count(*) AS n", kBackends[b].table));
        INFO("prepare: " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        auto executed = execute_session_statement(s, stmt);
        REQUIRE(executed.has_error());
        CHECK(executed.error().type == core::error_code_t::io_error);
        CHECK(std::string{executed.error().what.c_str()}.find(refusals[b]) != std::string::npos);
    }
    rules_reset({});
}

