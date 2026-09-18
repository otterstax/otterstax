// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The FlightSQL contract between GetFlightInfo and DoGet, driven through the
// real Scheduler→Worker→engine stack the way the frontend drives it:
// GetFlightInfoStatement hands out the schema prepare_schema resolved from the
// plan, and DoGetStatement builds the record batch stream over that same
// schema. Nothing is re-derived from the result chunks, so the two RPCs can
// never disagree — a JOIN keeps both key columns under the same name, a column
// Arrow cannot carry is refused where the schema is produced (before any
// ticket exists), and a statement whose schema is not resolved at prepare is
// recognisable as such.

#include "scheduler_stack.hpp"

#include "frontend/flight_sql_server/batch_reader.hpp"
#include "otterbrix/translators/output/chunk_to_arrow.hpp"

#include <arrow/api.h>
#include <catch2/catch_all.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

using otterstax::test::execute_scheduler_statement;
using otterstax::test::prepare_scheduler_sql;
using otterstax::test::run_scheduler_sql;
using otterstax::test::scheduler_stack;
using otterstax::test::with_scheduler_stack;

namespace {

    void run_or_fail(const scheduler_stack& s, session_hash_t id, const std::string& sql) {
        std::string err;
        const bool ok = run_scheduler_sql(s, id, sql, err);
        INFO(sql << ": " << err);
        REQUIRE(ok);
    }

    // Two INT tables sharing the key name, so a `SELECT *` JOIN result carries
    // `id` twice.
    void seed_join_tables(const scheduler_stack& s, session_hash_t& id) {
        run_or_fail(s, id++, "CREATE DATABASE fdb;");
        run_or_fail(s, id++, "CREATE TABLE fdb.a (id INT, x INT);");
        run_or_fail(s, id++, "CREATE TABLE fdb.b (id INT, y INT);");
        run_or_fail(s, id++, "INSERT INTO fdb.a (id, x) VALUES (1, 10), (2, 20);");
        run_or_fail(s, id++, "INSERT INTO fdb.b (id, y) VALUES (1, 100), (2, 200);");
    }

    size_t count_named(const components::types::complex_logical_type& schema, std::string_view name) {
        size_t n = 0;
        for (const auto& child : schema.child_types()) {
            if (child.has_alias() && std::string_view{child.alias()} == name) {
                ++n;
            }
        }
        return n;
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> drain(ChunkBatchReader& reader) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        while (true) {
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = reader.ReadNext(&batch);
            INFO("ReadNext: " << status.ToString());
            REQUIRE(status.ok());
            if (!batch) {
                break;
            }
            batches.push_back(std::move(batch));
        }
        return batches;
    }

    int32_t int_at(const arrow::RecordBatch& batch, int column, int64_t row) {
        return std::static_pointer_cast<arrow::Int32Array>(batch.column(column))->Value(row);
    }

    bool mentions(const core::error_t& error, std::string_view text) {
        return std::string_view{error.what.c_str()}.find(text) != std::string_view::npos;
    }

} // namespace

TEST_CASE("FlightSQL contract: a local JOIN streams both key columns under the prepared schema") {
    with_scheduler_stack("/tmp/test_flightsql_contract_join", [](scheduler_stack s) {
        session_hash_t id = 9500;
        seed_join_tables(s, id);
        const session_hash_t stmt = id++;

        // GetFlightInfo: the schema is the one the engine's plan validation
        // stamps at prepare — duplicate names included.
        auto prepared = prepare_scheduler_sql(s, stmt, "SELECT * FROM fdb.a JOIN fdb.b ON a.id = b.id;");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().tag == T_SelectStmt);
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 4);
        REQUIRE(count_named(schema, "id") == 2);
        REQUIRE(count_named(schema, "x") == 1);
        REQUIRE(count_named(schema, "y") == 1);

        auto converted = to_arrow_schema(s.resource, schema);
        INFO("arrow schema error: " << converted.error().what.c_str());
        REQUIRE_FALSE(converted.has_error());
        auto flight_schema = converted.value();
        REQUIRE(flight_schema->num_fields() == 4);

        // DoGet: the rows come back with the prepared schema, and the stream is
        // built over that schema — never over one re-derived from the chunks.
        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(executed.value().size() == 2);
        REQUIRE(executed.value().column_count() == 4);
        REQUIRE(executed.value().schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(executed.value().schema.child_types().size() == 4);

        auto reader = ChunkBatchReader::Make(flight_schema, std::move(executed.value().chunks));
        REQUIRE(reader.ok());
        auto batches = drain(**reader);
        REQUIRE(batches.size() == 1);
        const auto& batch = *batches.front();
        REQUIRE(batch.schema()->Equals(*flight_schema));
        REQUIRE(batch.num_columns() == 4);
        REQUIRE(batch.num_rows() == 2);
        REQUIRE(batch.ValidateFull().ok());

        // Every field is fed from its own chunk column: the second `id` is b's
        // key, not a copy of a's and not an empty array.
        const int x_col = flight_schema->GetFieldIndex("x");
        const int y_col = flight_schema->GetFieldIndex("y");
        REQUIRE(x_col >= 0);
        REQUIRE(y_col >= 0);
        std::vector<int> id_cols;
        for (int i = 0; i < flight_schema->num_fields(); ++i) {
            if (flight_schema->field(i)->name() == "id") {
                id_cols.push_back(i);
            }
        }
        REQUIRE(id_cols.size() == 2);

        std::vector<std::array<int32_t, 4>> rows;
        for (int64_t r = 0; r < batch.num_rows(); ++r) {
            rows.push_back({int_at(batch, id_cols[0], r),
                            int_at(batch, x_col, r),
                            int_at(batch, id_cols[1], r),
                            int_at(batch, y_col, r)});
        }
        std::sort(rows.begin(), rows.end());
        REQUIRE(rows[0] == std::array<int32_t, 4>{1, 10, 1, 100});
        REQUIRE(rows[1] == std::array<int32_t, 4>{2, 20, 2, 200});
    });
}

TEST_CASE("FlightSQL contract: a column without an Arrow mapping is refused where the schema is produced") {
    with_scheduler_stack("/tmp/test_flightsql_contract_unmappable", [](scheduler_stack s) {
        session_hash_t id = 9600;
        run_or_fail(s, id++, "CREATE DATABASE fdb;");

        SECTION("a UHUGEINT column names the column in the error") {
            run_or_fail(s, id++, "CREATE TABLE fdb.uhuge (id INT, big uhugeint);");

            auto prepared = prepare_scheduler_sql(s, id++, "SELECT big FROM fdb.uhuge;");
            INFO("prepare error: " << prepared.error().what.c_str());
            REQUIRE_FALSE(prepared.has_error());
            const auto& schema = prepared.value().schema;
            REQUIRE(schema.type() == components::types::logical_type::STRUCT);
            REQUIRE(schema.child_types().size() == 1);
            // The engine resolves the column to its declared 128-bit unsigned type;
            // the refusal is Arrow's side, where decimal128 — signed — cannot carry
            // the upper half of that range and no other 128-bit carrier exists.
            const auto& big = schema.child_types()[0];
            REQUIRE(big.alias() == "big");
            REQUIRE(big.to_physical_type() == components::types::physical_type::UINT128);

            auto converted = to_arrow_schema(s.resource, schema);
            REQUIRE(converted.has_error());
            REQUIRE(converted.error().type == core::error_code_t::conversion_failure);
            REQUIRE(mentions(converted.error(), "big"));
        }

        SECTION("the mappable columns of the same table still stream") {
            run_or_fail(s, id++, "CREATE TABLE fdb.uhuge (id INT, big uhugeint);");
            const session_hash_t stmt = id++;

            auto prepared = prepare_scheduler_sql(s, stmt, "SELECT id FROM fdb.uhuge;");
            INFO("prepare error: " << prepared.error().what.c_str());
            REQUIRE_FALSE(prepared.has_error());
            auto converted = to_arrow_schema(s.resource, prepared.value().schema);
            INFO("arrow schema error: " << converted.error().what.c_str());
            REQUIRE_FALSE(converted.has_error());
            REQUIRE(converted.value()->num_fields() == 1);
            REQUIRE(converted.value()->field(0)->type()->id() == arrow::Type::INT32);

            auto executed = execute_scheduler_statement(s, stmt);
            INFO("execute error: " << executed.error().what.c_str());
            REQUIRE_FALSE(executed.has_error());
            REQUIRE(executed.value().size() == 0);

            auto reader = ChunkBatchReader::Make(converted.value(), std::move(executed.value().chunks));
            REQUIRE(reader.ok());
            REQUIRE(drain(**reader).empty());
        }
    });
}

TEST_CASE("FlightSQL contract: a HUGEINT column is carried, not refused") {
    with_scheduler_stack("/tmp/test_flightsql_contract_hugeint", [](scheduler_stack s) {
        session_hash_t id = 9650;
        run_or_fail(s, id++, "CREATE DATABASE hdb;");
        run_or_fail(s, id++, "CREATE TABLE hdb.huge (id INT, big hugeint);");

        const session_hash_t stmt = id++;
        auto prepared = prepare_scheduler_sql(s, stmt, "SELECT big FROM hdb.huge;");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        // rc-3 gives a 128-bit integer its own pg_type row, so the column resolves
        // to its declared type instead of coming back as an unknown user type.
        const auto& big = schema.child_types()[0];
        REQUIRE(big.alias() == "big");
        REQUIRE(big.type() == components::types::logical_type::HUGEINT);
        REQUIRE(big.to_physical_type() == components::types::physical_type::INT128);

        // Arrow has no 128-bit integer type; decimal128(38, 0) is the carrier, and
        // GetFlightInfo hands out exactly this schema for DoGet to stream under.
        auto converted = to_arrow_schema(s.resource, schema);
        INFO("arrow schema error: " << converted.error().what.c_str());
        REQUIRE_FALSE(converted.has_error());
        REQUIRE(converted.value()->num_fields() == 1);
        REQUIRE(converted.value()->field(0)->name() == "big");
        REQUIRE(converted.value()->field(0)->type()->id() == arrow::Type::DECIMAL128);
        const auto& decimal = static_cast<const arrow::Decimal128Type&>(*converted.value()->field(0)->type());
        REQUIRE(decimal.precision() == 38);
        REQUIRE(decimal.scale() == 0);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        auto reader = ChunkBatchReader::Make(converted.value(), std::move(executed.value().chunks));
        REQUIRE(reader.ok());
        REQUIRE(drain(**reader).empty());
    });
}

TEST_CASE("FlightSQL contract: a DECIMAL column is carried with its scale") {
    with_scheduler_stack("/tmp/test_flightsql_contract_decimal", [](scheduler_stack s) {
        session_hash_t id = 9660;
        run_or_fail(s, id++, "CREATE DATABASE ddb;");
        run_or_fail(s, id++, "CREATE TABLE ddb.money (id INT, amount decimal(38, 10));");

        const session_hash_t stmt = id++;
        auto prepared = prepare_scheduler_sql(s, stmt, "SELECT amount FROM ddb.money;");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 1);
        const auto& amount = schema.child_types()[0];
        REQUIRE(amount.alias() == "amount");
        REQUIRE(amount.type() == components::types::logical_type::DECIMAL);

        // The scale is the half of the type the client cannot reconstruct: mapped by its
        // physical type this column left as a bare integer of the stored unscaled value, so
        // 1.2345 arrived as 12345 under a schema with nowhere to put the point back. 38 is
        // both the engine's widest precision and decimal128's, so even the widest DECIMAL
        // the engine can declare is carried rather than refused.
        auto converted = to_arrow_schema(s.resource, schema);
        INFO("arrow schema error: " << converted.error().what.c_str());
        REQUIRE_FALSE(converted.has_error());
        REQUIRE(converted.value()->num_fields() == 1);
        REQUIRE(converted.value()->field(0)->name() == "amount");
        REQUIRE(converted.value()->field(0)->type()->id() == arrow::Type::DECIMAL128);
        const auto& declared = static_cast<const arrow::Decimal128Type&>(*converted.value()->field(0)->type());
        REQUIRE(declared.precision() == 38);
        REQUIRE(declared.scale() == 10);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        auto reader = ChunkBatchReader::Make(converted.value(), std::move(executed.value().chunks));
        REQUIRE(reader.ok());
        REQUIRE(drain(**reader).empty());
    });
}

TEST_CASE("FlightSQL contract: the DoGet stream carries a DECIMAL value with its point") {
    // The case above settles the schema GetFlightInfo hands out; DoGet builds its arrays cell
    // by cell against that same schema, through a path that had no decimal128 case at all —
    // so a column advertised as one could not be streamed even when the schema was right.
    // The chunk is built here rather than selected, so what is asserted is the encoder and
    // the values are exact on both signs.
    auto* res = std::pmr::new_delete_resource();
    auto amount = components::types::complex_logical_type::create_decimal(res, 18, 4, "amount");
    REQUIRE_FALSE(amount.has_error());
    std::pmr::vector<components::types::complex_logical_type> types{res};
    types.push_back(amount.value());

    components::vector::data_chunk_t chunk(res, types, 3);
    chunk.set_cardinality(3);
    chunk.set_value(0, 0, components::types::logical_value_t::create_decimal(res, types[0], int64_t{12345}));
    chunk.set_value(0, 1, components::types::logical_value_t::create_decimal(res, types[0], int64_t{-12345}));
    chunk.set_value(0, 2, components::types::logical_value_t{res, nullptr});

    auto converted = to_arrow_schema(res, types);
    INFO("arrow schema error: " << converted.error().what.c_str());
    REQUIRE_FALSE(converted.has_error());

    std::pmr::vector<components::vector::data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));
    auto reader = ChunkBatchReader::Make(converted.value(), std::move(chunks));
    REQUIRE(reader.ok());
    auto batches = drain(**reader);
    REQUIRE(batches.size() == 1);
    const auto& batch = *batches.front();
    REQUIRE(batch.schema()->Equals(*converted.value()));
    REQUIRE(batch.num_rows() == 3);
    REQUIRE(batch.ValidateFull().ok());
    const auto& values = static_cast<const arrow::Decimal128Array&>(*batch.column(0));
    REQUIRE(values.FormatValue(0) == "1.2345");
    REQUIRE(values.FormatValue(1) == "-1.2345");
    REQUIRE(values.IsNull(2));
}

TEST_CASE("FlightSQL contract: a parameterized SELECT is prepared from the engine's own columns") {
    with_scheduler_stack("/tmp/test_flightsql_contract_params", [](scheduler_stack s) {
        session_hash_t id = 9700;
        run_or_fail(s, id++, "CREATE DATABASE fdb;");
        run_or_fail(s, id++, "CREATE TABLE fdb.a (id INT, x INT);");

        // The engine validates a plan only with every parameter bound — its
        // plan-only pass refuses one that still carries `$n` — so the prepare
        // answers the projection resolved against the columns the engine
        // reports for the statement's own relations (a `LIMIT 0` probe each):
        // the projected columns, under their own names and with the types they
        // will arrive under. GetFlightInfoStatement binds nothing and refuses
        // this payload by its parameter_count, whatever the schema holds; the
        // schema is for the extended-protocol frontends, which describe it
        // before Bind and hold the executed result to it.
        auto prepared = prepare_scheduler_sql(s, id++, "SELECT id, x FROM fdb.a WHERE id = $1;");
        INFO("prepare error: " << prepared.error().what.c_str());
        REQUIRE_FALSE(prepared.has_error());
        REQUIRE(prepared.value().tag == T_SelectStmt);
        REQUIRE(prepared.value().parameter_count == 1);
        const auto& schema = prepared.value().schema;
        REQUIRE(schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(schema.child_types().size() == 2);
        REQUIRE(schema.child_types()[0].alias() == "id");
        REQUIRE(schema.child_types()[1].alias() == "x");
        INFO("described id: " << static_cast<int>(schema.child_types()[0].type())
                              << ", x: " << static_cast<int>(schema.child_types()[1].type()));
        REQUIRE(schema.child_types()[0].type() == components::types::logical_type::INTEGER);
        REQUIRE(schema.child_types()[1].type() == components::types::logical_type::INTEGER);

        // The schema converts like any other, into the fields the rows will
        // carry; the ticket is still refused by the parameter count, which this
        // RPC has no way to fill.
        auto converted = to_arrow_schema(s.resource, schema);
        REQUIRE_FALSE(converted.has_error());
        REQUIRE(converted.value()->num_fields() == 2);
        REQUIRE(converted.value()->field(0)->type()->id() == arrow::Type::INT32);
        REQUIRE(converted.value()->field(1)->type()->id() == arrow::Type::INT32);
    });
}
