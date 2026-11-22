// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Prepared-statement types over a backend, the system half of the [types-29]
// grid: the type a prepare announces is the type its execute delivers, when
// the rows come from a remote table of each dialect.
//
// - A table whose discovery declares price DECIMAL(10, 2): the prepare of
//   `SELECT price ... WHERE id = $1` announces DECIMAL(10, 2) and its execute
//   delivers it with $1 bound; `SELECT SUM(price)` announces the engine's
//   DECIMAL(38, 2) and delivers it — PostgreSQL's constrained NUMERIC answer
//   read as declared, MySQL's own DECIMAL(32, 2) rescaled, ClickHouse's
//   Decimal(38, 2) wire column read as declared.
// - The dialect's own aggregate typing held to the announced type by
//   conform_to_result: PostgreSQL answers SUM(bigint) as an unconstrained
//   NUMERIC, which pg_to_chunk carries as its exact text and conform parses
//   into the announced BIGINT; MySQL answers it as a DECIMAL(32, 0) form,
//   conformed by the engine's decimal-to-integer cast. Announced and
//   delivered are BIGINT either way.
// - An unconstrained NUMERIC column: registered as its exact text, so a
//   prepared SELECT of it announces TEXT and delivers the text.
// - A backend answer the registration does not describe is refused and
//   streams nothing: two columns where the plan declares one is
//   datatype_mismatch (the remote table changed since it was registered), a
//   text answer under a column registered BIGINT whose value is no number is
//   the value's parse error naming the column and the statement.
//
// fake_servers.hpp's discovery probes can declare only BIGINT / DOUBLE / text
// columns, so each dialect gets a connector of its own here
// (test_backend_managers.cpp's kind): typed probes, and reads answered
// through the real translators (pg_to_chunk / mysql_to_chunk / ch_to_chunk).
// A read is answered by shape: the table's aggregate arm when the list sums,
// the table's whole answer when it carries one, else the columns the list
// names, in its order.

#include "session_stack.hpp"
#include "../mock/cell.hpp"

#include "frontend/common/utils.hpp"

#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace otterstax::test;
using components::types::logical_type;
using code = core::error_code_t;

namespace {

    std::string label_of(const components::types::complex_logical_type& type) {
        return std::string{frontend::logical_type_name(type.type())};
    }

    void check_decimal(const components::types::complex_logical_type& type, int width, int scale) {
        const auto* spec = type.extension_as<components::types::decimal_logical_type_extension>();
        REQUIRE(spec != nullptr);
        REQUIRE(spec->width() == width);
        REQUIRE(spec->scale() == scale);
    }

    // The unscaled value of a DECIMAL cell, whatever storage its width uses.
    components::types::int128_t unscaled_of(const cell_t& value) {
        switch (value.type().to_physical_type()) {
            case components::types::physical_type::INT16:
                return value.value<std::int16_t>();
            case components::types::physical_type::INT32:
                return value.value<std::int32_t>();
            case components::types::physical_type::INT64:
                return value.value<std::int64_t>();
            default:
                return value.value<components::types::int128_t>();
        }
    }

    // A prepare of `sql`, an execute with `parameters` (empty: a plain
    // execute), and what its one column announced and delivered. The bind of
    // every parameter carries the type the describe announced for it.
    struct column_run_t {
        components::types::complex_logical_type announced;
        components::vector::data_chunk_t chunk;
    };

    column_run_t run_column(const session_stack& s,
                            session_hash_t id,
                            const std::string& sql,
                            std::pmr::vector<components::vector::vector_t> parameters = {}) {
        auto prepared = prepare_session_sql(s, id, sql);
        INFO("prepare " << sql << ": " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().parameter_count == parameters.size());
        REQUIRE(prepared.value().engine_types.size() == parameters.size());
        for (std::size_t i = 0; i < parameters.size(); ++i) {
            INFO("parameter $" << (i + 1));
            REQUIRE(prepared.value().engine_types[i] == parameters[i].type());
        }
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        auto executed = parameters.empty() ? execute_session_statement(s, id)
                                           : execute_session_prepared(s, id, std::move(parameters));
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE_FALSE(executed.has_error());
        REQUIRE_FALSE(executed.value().chunks.empty());
        auto& chunk = executed.value().chunks.front();
        REQUIRE(chunk.column_count() == 1);
        return {schema.child_types()[0], std::move(chunk)};
    }

    // A prepare of `sql` that succeeds (a describe reaches no backend) and an
    // execute that is refused.
    core::error_t refused_column(const session_stack& s,
                                 session_hash_t id,
                                 const std::string& sql,
                                 std::pmr::vector<components::vector::vector_t> parameters = {}) {
        auto prepared = prepare_session_sql(s, id, sql);
        INFO("prepare " << sql << ": " << (prepared.has_error() ? prepared.error().what.c_str() : "ok"));
        REQUIRE_FALSE(prepared.has_error());
        auto executed = parameters.empty() ? execute_session_statement(s, id)
                                           : execute_session_prepared(s, id, std::move(parameters));
        INFO("execute: " << (executed.has_error() ? executed.error().what.c_str() : "ok"));
        REQUIRE(executed.has_error());
        return executed.error();
    }

    std::pmr::vector<components::vector::vector_t> bound_id(const session_stack& s, std::int64_t id) {
        std::pmr::vector<components::vector::vector_t> values{s.resource};
        values.push_back(components::vector::make_scalar(s.resource, id));
        return values;
    }

    // The select list of a generated read: between its SELECT and its FROM.
    std::string select_list_of(std::string_view sql) {
        const auto select = sql.find("SELECT ");
        const auto from = sql.find(" FROM ");
        if (select == std::string_view::npos || from == std::string_view::npos || from < select) {
            return std::string{sql};
        }
        return std::string{sql.substr(select + 7, from - (select + 7))};
    }

    // ── PostgreSQL ──────────────────────────────────────────────────────────

    constexpr unsigned kInt8Oid = 20;
    constexpr unsigned kTextOid = 25;
    constexpr unsigned kNumericOid = 1700;
    // NUMERIC(p, s)'s type modifier, as PostgreSQL sends it in a row description.
    constexpr int numeric_typmod(int precision, int scale) {
        return ((precision << 16) | (scale & 0x7ff)) + 4;
    }

    struct pg_column_t {
        std::string name;
        unsigned oid;
        int typmod;
    };

    struct pg_read_t {
        std::vector<pg_column_t> columns;
        std::vector<std::vector<std::string>> rows;
    };

    struct pg_table_t {
        std::string schema;
        std::string name;
        // The `WHERE 1 = 0` probe's answer: the types the catalog registers.
        std::vector<pg_column_t> columns;
        // The rows a read of those columns answers.
        std::vector<std::vector<std::string>> rows;
        // What a read answers instead of the list's columns (the negative
        // tables, whose data changed since the probe).
        std::optional<pg_read_t> whole_read;
        // The read a SUM answers: the type and value the dialect gives.
        std::optional<pg_column_t> sum_answer;
        std::string sum_value;
    };

    std::vector<pg_table_t>& pg_tables() {
        static std::vector<pg_table_t> tables;
        return tables;
    }

    const pg_table_t* pg_table_of(const std::string& sql) {
        for (const auto& table : pg_tables()) {
            if (sql.find("\"" + table.schema + "\".\"" + table.name + "\"") != std::string::npos) {
                return &table;
            }
        }
        return nullptr;
    }

    using pg_result_ptr = std::unique_ptr<PGresult, void (*)(PGresult*)>;

    pg_result_ptr pg_answer(const std::vector<pg_column_t>& columns,
                            const std::vector<std::vector<std::string>>& rows) {
        pg_result_ptr result(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), &PQclear);
        std::vector<PGresAttDesc> attrs;
        attrs.reserve(columns.size());
        for (const auto& column : columns) {
            attrs.push_back({const_cast<char*>(column.name.c_str()),
                             0,
                             0,
                             0,
                             static_cast<Oid>(column.oid),
                             column.oid == kInt8Oid ? 8 : -1,
                             column.typmod});
        }
        PQsetResultAttrs(result.get(), static_cast<int>(attrs.size()), attrs.data());
        for (std::size_t r = 0; r < rows.size(); ++r) {
            for (std::size_t c = 0; c < rows[r].size(); ++c) {
                PQsetvalue(result.get(),
                           static_cast<int>(r),
                           static_cast<int>(c),
                           const_cast<char*>(rows[r][c].c_str()),
                           static_cast<int>(rows[r][c].size()));
            }
        }
        return result;
    }

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

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<components::vector::data_chunk_t>(PGresult*)> handler)
            override {
            const auto sql = std::string{query};
            const auto* table = pg_table_of(sql);
            if (table == nullptr) {
                co_return core::error_t{core::error_code_t::undefined_object,
                                        std::pmr::string{("no fake PostgreSQL table answers " + sql).c_str(),
                                                         resource_}};
            }
            const auto list = select_list_of(sql);
            if (list.find("SUM(") != std::string::npos) {
                if (!table->sum_answer) {
                    co_return refused(sql, "has no aggregate answer");
                }
                auto answer = pg_answer({*table->sum_answer}, {{table->sum_value}});
                co_return handler(answer.get());
            }
            if (table->whole_read) {
                auto answer = pg_answer(table->whole_read->columns, table->whole_read->rows);
                co_return handler(answer.get());
            }
            std::vector<std::pair<std::size_t, std::size_t>> picked; // (position in the list, column)
            for (std::size_t c = 0; c < table->columns.size(); ++c) {
                const auto at = list.find("\"" + table->columns[c].name + "\"");
                if (at != std::string::npos) {
                    picked.emplace_back(at, c);
                }
            }
            std::sort(picked.begin(), picked.end());
            std::vector<pg_column_t> columns;
            std::vector<std::vector<std::string>> rows(table->rows.size());
            for (auto [at, c] : picked) {
                columns.push_back(table->columns[c]);
                for (std::size_t r = 0; r < table->rows.size(); ++r) {
                    rows[r].push_back(table->rows[r][c]);
                }
            }
            auto answer = pg_answer(columns, rows);
            co_return handler(answer.get());
        }

        boost::asio::awaitable<core::result_wrapper_t<std::int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<std::int64_t(PGresult*)>) override {
            co_return std::int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(PGresult*)> handler) override {
            pg_result_ptr answer(PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK), &PQclear);
            if (query.find("pg_enum") == std::string_view::npos) {
                if (query.find("information_schema") != std::string_view::npos) {
                    const std::vector<pg_column_t> attrs{{"table_schema", kTextOid, -1},
                                                         {"table_name", kTextOid, -1}};
                    std::vector<std::vector<std::string>> rows;
                    for (const auto& table : pg_tables()) {
                        rows.push_back({table.schema, table.name});
                    }
                    answer = pg_answer(attrs, rows);
                } else if (query.find("1 = 0") != std::string_view::npos) {
                    if (const auto* table = pg_table_of(std::string{query})) {
                        answer = pg_answer(table->columns, {});
                    }
                }
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(answer.get()));
        }

    private:
        core::error_t refused(const std::string& sql, const std::string& why) const {
            return core::error_t{core::error_code_t::undefined_object,
                                 std::pmr::string{("the fake PostgreSQL table behind " + sql + " " + why).c_str(),
                                                  resource_}};
        }

        std::pmr::memory_resource* resource_;
        pg::connect_params params_;
        std::string alias_;
    };

    // ── MySQL ───────────────────────────────────────────────────────────────

    struct my_table_t {
        std::string database;
        std::string name;
        std::vector<mysql::mock_column_t> columns;
        std::vector<std::vector<std::string>> rows;
        std::optional<mysql::mock_column_t> sum_answer;
        std::string sum_rows;
    };

    std::vector<my_table_t>& my_tables() {
        static std::vector<my_table_t> tables;
        return tables;
    }

    const my_table_t* my_table_of(const std::string& sql) {
        for (const auto& table : my_tables()) {
            if (sql.find("`" + table.database + "`.`" + table.name + "`") != std::string::npos) {
                return &table;
            }
        }
        return nullptr;
    }

    class typed_mysql_connector final : public mysql::IConnector {
    public:
        typed_mysql_connector(std::pmr::memory_resource* resource, boost::mysql::connect_params params, std::string alias)
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

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<components::vector::data_chunk_t>(const boost::mysql::results&)>
                     handler) override {
            const auto sql = std::string{query};
            const auto* table = my_table_of(sql);
            if (table == nullptr) {
                co_return core::error_t{core::error_code_t::undefined_object,
                                        std::pmr::string{("no fake MySQL table answers " + sql).c_str(), resource_}};
            }
            const auto list = select_list_of(sql);
            if (list.find("SUM(") != std::string::npos) {
                if (!table->sum_answer) {
                    co_return refused(sql, "has no aggregate answer");
                }
                co_return handler(mysql::mock_results({*table->sum_answer}, {{table->sum_rows}}));
            }
            std::vector<std::pair<std::size_t, std::size_t>> picked; // (position in the list, column)
            for (std::size_t c = 0; c < table->columns.size(); ++c) {
                const auto at = list.find("`" + table->columns[c].name + "`");
                if (at != std::string::npos) {
                    picked.emplace_back(at, c);
                }
            }
            std::sort(picked.begin(), picked.end());
            std::vector<mysql::mock_column_t> columns;
            std::vector<std::vector<std::optional<std::string>>> rows(table->rows.size());
            for (auto [at, c] : picked) {
                columns.push_back(table->columns[c]);
                for (std::size_t r = 0; r < table->rows.size(); ++r) {
                    rows[r].push_back(table->rows[r][c]);
                }
            }
            co_return handler(mysql::mock_results(columns, rows));
        }

        boost::asio::awaitable<core::result_wrapper_t<std::int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<std::int64_t(const boost::mysql::results&)>) override {
            co_return std::int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const boost::mysql::results&)> handler) override {
            if (query.find("sql_mode") != std::string_view::npos) {
                co_return otterstax::as_query_result<otterstax::asio_error_t>(
                    handler(mysql::mock_results({{"@@SESSION.sql_mode", boost::mysql::column_type::varchar, false, 0, 0}},
                                                {{std::string{""}}})));
            }
            if (query.find("information_schema.schemata") != std::string_view::npos) {
                std::vector<std::string> databases;
                for (const auto& table : my_tables()) {
                    if (std::find(databases.begin(), databases.end(), table.database) == databases.end()) {
                        databases.push_back(table.database);
                    }
                }
                std::vector<std::vector<std::optional<std::string>>> rows;
                for (const auto& database : databases) {
                    rows.push_back({database});
                }
                co_return otterstax::as_query_result<otterstax::asio_error_t>(
                    handler(mysql::mock_results({{"SCHEMA_NAME", boost::mysql::column_type::varchar, false, 0, 0}}, rows)));
            }
            if (query.find("information_schema") != std::string_view::npos) {
                std::vector<std::vector<std::optional<std::string>>> rows;
                for (const auto& table : my_tables()) {
                    if (query.find("table_schema = '" + table.database + "'") != std::string_view::npos) {
                        rows.push_back({table.name});
                    }
                }
                co_return otterstax::as_query_result<otterstax::asio_error_t>(
                    handler(mysql::mock_results({{"TABLE_NAME", boost::mysql::column_type::varchar, false, 0, 0}}, rows)));
            }
            if (query.find("1 = 0") != std::string_view::npos) {
                if (const auto* table = my_table_of(std::string{query})) {
                    co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(mysql::mock_results(table->columns)));
                }
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(mysql::mock_results({})));
        }

    private:
        core::error_t refused(const std::string& sql, const std::string& why) const {
            return core::error_t{core::error_code_t::undefined_object,
                                 std::pmr::string{("the fake MySQL table behind " + sql + " " + why).c_str(),
                                                  resource_}};
        }

        std::pmr::memory_resource* resource_;
        boost::mysql::connect_params params_;
        std::string alias_;
    };

    // ── ClickHouse ──────────────────────────────────────────────────────────

    struct ch_column_t {
        std::string name;
        std::string declared; // the system.columns type string
        bool decimal;
        int precision;
        int scale;
    };

    struct ch_table_t {
        std::string database;
        std::string name;
        std::vector<ch_column_t> columns;
        std::vector<std::vector<std::int64_t>> rows;
        std::optional<ch_column_t> sum_answer;
        std::int64_t sum_value;
    };

    std::vector<ch_table_t>& ch_tables() {
        static std::vector<ch_table_t> tables;
        return tables;
    }

    const ch_table_t* ch_table_of(const std::string& sql) {
        for (const auto& table : ch_tables()) {
            if (sql.find("`" + table.database + "`.`" + table.name + "`") != std::string::npos ||
                sql.find("'" + table.name + "'") != std::string::npos) {
                return &table;
            }
        }
        return nullptr;
    }

    clickhouse::Block ch_block(const std::vector<ch_column_t>& columns,
                               const std::vector<std::vector<std::int64_t>>& rows) {
        clickhouse::Block block;
        for (std::size_t c = 0; c < columns.size(); ++c) {
            if (columns[c].decimal) {
                auto values = std::make_shared<clickhouse::ColumnDecimal>(
                    static_cast<size_t>(columns[c].precision), static_cast<size_t>(columns[c].scale));
                for (const auto& row : rows) {
                    values->Append(static_cast<clickhouse::Int128>(row[c]));
                }
                block.AppendColumn(columns[c].name, values);
            } else {
                auto values = std::make_shared<clickhouse::ColumnInt64>();
                for (const auto& row : rows) {
                    values->Append(row[c]);
                }
                block.AppendColumn(columns[c].name, values);
            }
        }
        return block;
    }

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

        boost::asio::awaitable<core::result_wrapper_t<std::unique_ptr<components::vector::data_chunk_t>>>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<std::unique_ptr<components::vector::data_chunk_t>(const ch::select_result_t&)>
                     handler) override {
            const auto sql = std::string{query};
            const auto* table = ch_table_of(sql);
            if (table == nullptr) {
                co_return core::error_t{core::error_code_t::undefined_object,
                                        std::pmr::string{("no fake ClickHouse table answers " + sql).c_str(),
                                                         resource_}};
            }
            const auto list = select_list_of(sql);
            if (list.find("SUM(") != std::string::npos) {
                if (!table->sum_answer) {
                    co_return refused(sql, "has no aggregate answer");
                }
                ch::select_result_t answer;
                answer.blocks.push_back(ch_block({*table->sum_answer}, {{table->sum_value}}));
                co_return handler(answer);
            }
            std::vector<std::pair<std::size_t, std::size_t>> picked; // (position in the list, column)
            for (std::size_t c = 0; c < table->columns.size(); ++c) {
                const auto at = list.find("`" + table->columns[c].name + "`");
                if (at != std::string::npos) {
                    picked.emplace_back(at, c);
                }
            }
            std::sort(picked.begin(), picked.end());
            std::vector<ch_column_t> columns;
            std::vector<std::vector<std::int64_t>> rows(table->rows.size());
            for (auto [at, c] : picked) {
                columns.push_back(table->columns[c]);
                for (std::size_t r = 0; r < table->rows.size(); ++r) {
                    rows[r].push_back(table->rows[r][c]);
                }
            }
            ch::select_result_t answer;
            answer.blocks.push_back(ch_block(columns, rows));
            co_return handler(answer);
        }

        boost::asio::awaitable<core::result_wrapper_t<std::int64_t>>
        runQuery(std::string_view, otterstax::function_ref_t<std::int64_t(const ch::select_result_t&)>) override {
            co_return std::int64_t{0};
        }

        boost::asio::awaitable<core::error_t>
        runQuery(std::string_view query,
                 otterstax::function_ref_t<otterstax::asio_error_t(const ch::select_result_t&)> handler) override {
            ch::select_result_t answer;
            if (query.find("system.tables") != std::string_view::npos) {
                auto names = std::make_shared<clickhouse::ColumnString>();
                for (const auto& table : ch_tables()) {
                    names->Append(table.name);
                }
                clickhouse::Block block;
                block.AppendColumn("name", names);
                answer.blocks.push_back(std::move(block));
            } else if (query.find("system.columns") != std::string_view::npos) {
                if (const auto* table = ch_table_of(std::string{query})) {
                    auto names = std::make_shared<clickhouse::ColumnString>();
                    auto types = std::make_shared<clickhouse::ColumnString>();
                    for (const auto& column : table->columns) {
                        names->Append(column.name);
                        types->Append(column.declared);
                    }
                    clickhouse::Block block;
                    block.AppendColumn("name", names);
                    block.AppendColumn("type", types);
                    answer.blocks.push_back(std::move(block));
                }
            } else if (const auto* table = ch_table_of(std::string{query})) {
                // The `WHERE 1 = 0` probe: the header block of the columns.
                answer.blocks.push_back(ch_block(table->columns, {}));
            }
            co_return otterstax::as_query_result<otterstax::asio_error_t>(handler(answer));
        }

    private:
        core::error_t refused(const std::string& sql, const std::string& why) const {
            return core::error_t{core::error_code_t::undefined_object,
                                 std::pmr::string{("the fake ClickHouse table behind " + sql + " " + why).c_str(),
                                                  resource_}};
        }

        std::pmr::memory_resource* resource_;
        ch::connect_params params_;
        std::string alias_;
    };

    // ── the stack over the three typed connections ──────────────────────────

    std::unique_ptr<pg::IConnector>
    typed_pg_factory(std::pmr::memory_resource* resource, pg::connect_params params, std::string alias) {
        return std::make_unique<typed_pg_connector>(resource, params, std::move(alias));
    }

    std::unique_ptr<mysql::IConnector> typed_mysql_factory(std::pmr::memory_resource* resource,
                                                           boost::asio::io_context&,
                                                           boost::mysql::connect_params params,
                                                           std::string alias) {
        return std::make_unique<typed_mysql_connector>(resource, params, std::move(alias));
    }

    std::unique_ptr<ch::IConnector>
    typed_ch_factory(std::pmr::memory_resource* resource, ch::connect_params params, std::string alias) {
        return std::make_unique<typed_ch_connector>(resource, params, std::move(alias));
    }

    stack_options_t typed_options() {
        stack_options_t options;
        options.connectors.mysql = &typed_mysql_factory;
        options.connectors.postgres = &typed_pg_factory;
        options.connectors.clickhouse = &typed_ch_factory;
        options.connections.postgresql.push_back(
            config::PgConnectionDesc{.alias = "pgdec", .host = "fake", .database = "shop", .schema = "public"});
        options.connections.mysql.push_back(config::MysqlConnectionDesc{.alias = "mydec", .host = "fake", .database = "shop"});
        options.connections.clickhouse.push_back(config::ChConnectionDesc{.alias = "chdec", .host = "fake", .database = "shop"});
        return options;
    }

} // namespace

TEST_CASE("a prepared decimal column over a backend announces what its execute delivers", "[types-29]") {
    pg_tables() = {pg_table_t{.schema = "public",
                              .name = "prices",
                              .columns = {{"id", kInt8Oid, -1}, {"price", kNumericOid, numeric_typmod(10, 2)}},
                              .rows = {{"1", "1.25"}, {"2", "2.50"}},
                              .sum_answer = pg_column_t{"sum", kNumericOid, numeric_typmod(38, 2)},
                              .sum_value = "3.75"}};
    my_tables() = {my_table_t{.database = "shop",
                              .name = "prices",
                              .columns = {{"id", boost::mysql::column_type::bigint, false, 21, 0},
                                          {"price", boost::mysql::column_type::decimal, false, 12, 2}},
                              .rows = {{"1", "1.25"}, {"2", "2.50"}},
                              .sum_answer = mysql::mock_column_t{"sum", boost::mysql::column_type::decimal, false, 34, 2},
                              .sum_rows = {"3.75"}}};
    ch_tables() = {ch_table_t{.database = "shop",
                              .name = "prices",
                              .columns = {{"id", "Int64", false, 0, 0}, {"price", "Decimal(10, 2)", true, 10, 2}},
                              .rows = {{1, 125}, {2, 250}},
                              .sum_answer = ch_column_t{"sum", "Decimal(38, 2)", true, 38, 2},
                              .sum_value = 375}};
    session_stack_owner owner(unique_test_dir("otterstax_prepared_types_decimal"), typed_options());
    const auto s = owner.stack();
    session_hash_t id = 1;
    for (const char* prices : {"pgdec.public.prices", "mydec.shop.prices", "chdec.shop.prices"}) {
        INFO("connection " << prices);

        // The point read, $1 bound.
        const std::string read = std::string{"SELECT price FROM "} + prices + " WHERE id = $1";
        auto column = run_column(s, id++, read, bound_id(s, 1));
        CHECK(label_of(column.announced) == "DECIMAL");
        check_decimal(column.announced, 10, 2);
        const auto delivered = column.chunk.data[0].type();
        CHECK(label_of(delivered) == "DECIMAL");
        check_decimal(delivered, 10, 2);
        CHECK(unscaled_of(cell(column.chunk, 0, 0)) == 125);
        CHECK(unscaled_of(cell(column.chunk, 0, 1)) == 250);

        // The aggregate: the engine's DECIMAL(38, 2), MySQL's own DECIMAL(32, 2)
        // answer rescaled to it.
        const std::string sum = std::string{"SELECT SUM(price) AS total FROM "} + prices;
        auto total = run_column(s, id++, sum);
        CHECK(label_of(total.announced) == "DECIMAL");
        check_decimal(total.announced, 38, 2);
        const auto sum_delivered = total.chunk.data[0].type();
        CHECK(label_of(sum_delivered) == "DECIMAL");
        check_decimal(sum_delivered, 38, 2);
        CHECK(unscaled_of(cell(total.chunk, 0, 0)) == 375);
    }
}

TEST_CASE("a dialect's own aggregate typing is conformed to the announced type", "[types-29]") {
    pg_tables() = {pg_table_t{.schema = "public",
                              .name = "sums",
                              .columns = {{"id", kInt8Oid, -1}, {"amount", kInt8Oid, -1}},
                              .rows = {{"1", "10"}, {"2", "20"}},
                              // What PostgreSQL answers for SUM(bigint): an unconstrained
                              // NUMERIC, its exact text.
                              .sum_answer = pg_column_t{"sum", kNumericOid, -1},
                              .sum_value = "30"}};
    my_tables() = {my_table_t{.database = "shop",
                              .name = "sums",
                              .columns = {{"id", boost::mysql::column_type::bigint, false, 21, 0},
                                          {"amount", boost::mysql::column_type::bigint, false, 21, 0}},
                              .rows = {{"1", "10"}, {"2", "20"}},
                              // What MySQL answers for SUM(int): a DECIMAL(32, 0) form.
                              .sum_answer = mysql::mock_column_t{"sum", boost::mysql::column_type::decimal, false, 33, 0},
                              .sum_rows = {"30"}}};
    ch_tables().clear();
    session_stack_owner owner(unique_test_dir("otterstax_prepared_types_conform"), typed_options());
    const auto s = owner.stack();
    session_hash_t id = 1;
    for (const char* sums : {"pgdec.public.sums", "mydec.shop.sums"}) {
        INFO("connection " << sums);
        const std::string sum = std::string{"SELECT SUM(amount) AS total FROM "} + sums;
        auto total = run_column(s, id++, sum);
        CHECK(label_of(total.announced) == "BIGINT");
        CHECK(label_of(total.chunk.data[0].type()) == "BIGINT");
        CHECK(cell(total.chunk, 0, 0).value<std::int64_t>() == 30);
    }
}

TEST_CASE("an unconstrained NUMERIC column announces text and delivers it through a prepared statement", "[types-29]") {
    pg_tables() = {pg_table_t{.schema = "public",
                              .name = "loose",
                              .columns = {{"id", kInt8Oid, -1}, {"wide", kNumericOid, -1}},
                              .rows = {{"1", "12345.6789"}}}};
    my_tables().clear();
    ch_tables().clear();
    session_stack_owner owner(unique_test_dir("otterstax_prepared_types_numeric_text"), typed_options());
    const auto s = owner.stack();
    auto column = run_column(s, 1, "SELECT wide FROM pgdec.public.loose WHERE id = $1", bound_id(s, 1));
    CHECK(label_of(column.announced) == "TEXT");
    CHECK(label_of(column.chunk.data[0].type()) == "TEXT");
    CHECK(cell(column.chunk, 0, 0).value<std::string_view>() == "12345.6789");
}

TEST_CASE("a backend answer the registration does not describe is refused and streams nothing", "[types-29]") {
    pg_tables() = {
        pg_table_t{.schema = "public",
                   .name = "changed",
                   .columns = {{"id", kInt8Oid, -1}},
                   .rows = {},
                   // The backend table grew a column after the probe registered it.
                   .whole_read =
                       pg_read_t{{{"id", kInt8Oid, -1}, {"price", kNumericOid, numeric_typmod(10, 2)}}, {{"7", "9.5"}}}},
        pg_table_t{.schema = "public",
                   .name = "idonly",
                   .columns = {{"id", kInt8Oid, -1}},
                   .rows = {},
                   // The backend answers the column as text now, and the value is no number.
                   .whole_read = pg_read_t{{{"id", kTextOid, -1}}, {{"not-a-number"}}}},
    };
    my_tables().clear();
    ch_tables().clear();
    session_stack_owner owner(unique_test_dir("otterstax_prepared_types_foreign_answer"), typed_options());
    const auto s = owner.stack();

    // Two columns where the plan declares one.
    auto mismatched = refused_column(s, 1, "SELECT id FROM pgdec.public.changed WHERE id = $1", bound_id(s, 7));
    CHECK(mismatched.type == code::datatype_mismatch);
    const std::string mismatch_text{mismatched.what.c_str()};
    CHECK(mismatch_text.find("answered 2 columns where 1 are expected") != std::string::npos);
    CHECK(mismatch_text.find("changed since it was registered") != std::string::npos);

    // A text answer under a column registered BIGINT: the value is refused,
    // the column and the statement named.
    auto retyped = refused_column(s, 2, "SELECT id FROM pgdec.public.idonly WHERE id = $1", bound_id(s, 1));
    CHECK(retyped.type == code::invalid_text_representation);
    CHECK(std::string{retyped.what.c_str()}.find("column 1 (id) of the remote statement") != std::string::npos);
}
