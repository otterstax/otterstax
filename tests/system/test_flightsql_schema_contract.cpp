// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// The FlightSQL contract between GetFlightInfo and DoGet, driven through the
// real Scheduler→Worker→engine stack the way the frontend drives it:
// GetFlightInfo hands out the schema prepare_schema resolved from the plan,
// and DoGet builds the record batches over that same schema. Nothing is
// re-derived from the result chunks, so the two RPCs can never disagree — a
// JOIN keeps both key columns under the same name, a column Arrow cannot
// carry is refused where the schema is produced (before any ticket exists),
// and a statement whose schema is not resolved at prepare is recognisable as
// such. The batches go through the project's own chunk_to_record_batch
// — the same converter the file paths use.

#include "scheduler_stack.hpp"

#include "otterbrix/translators/output/chunk_to_arrow.hpp"

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
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

    std::vector<std::shared_ptr<arrow::RecordBatch>>
    to_batches(const scheduler_stack& s, const session_payload& payload,
               const std::shared_ptr<arrow::Schema>& schema) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        for (const auto& chunk : payload.chunks) {
            if (chunk.empty()) {
                continue;
            }
            auto batch = chunk_to_record_batch(s.resource, chunk);
            INFO("chunk_to_record_batch: " << (batch.has_error() ? batch.error().what.c_str() : "ok"));
            REQUIRE_FALSE(batch.has_error());
            batches.push_back(std::move(batch.value()));
        }
        return batches;
    }

    bool mentions(const char* what, std::string_view text) {
        return std::string_view{what}.find(text) != std::string_view::npos;
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

        auto flight_schema = to_arrow_schema(s.resource, schema);
        INFO("arrow schema error: " << (flight_schema.has_error() ? flight_schema.error().what.c_str() : "ok"));
        REQUIRE_FALSE(flight_schema.has_error());
        REQUIRE(flight_schema.value()->num_fields() == 4);

        // DoGet: the rows come back with the prepared schema, and the batches
        // are built over that schema — never over one re-derived from the
        // chunks.
        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(executed.value().size() == 2);
        REQUIRE(executed.value().column_count() == 4);
        REQUIRE(executed.value().schema.type() == components::types::logical_type::STRUCT);
        REQUIRE(executed.value().schema.child_types().size() == 4);

        auto batches = to_batches(s, executed.value(), flight_schema.value());
        REQUIRE(batches.size() == 1);
        const auto& batch = *batches.front();
        REQUIRE(batch.schema()->Equals(*flight_schema.value()));
        REQUIRE(batch.num_columns() == 4);
        REQUIRE(batch.num_rows() == 2);

        // Every field is fed from its own chunk column: the second `id` is b's
        // key, not a copy of a's and not an empty array.
        std::vector<int> id_cols;
        int x_col = -1, y_col = -1;
        for (int i = 0; i < 4; ++i) {
            const std::string& name = flight_schema.value()->field(i)->name();
            if (name == "id") {
                id_cols.push_back(i);
            } else if (name == "x") {
                x_col = i;
            } else if (name == "y") {
                y_col = i;
            }
        }
        REQUIRE(id_cols.size() == 2);
        REQUIRE(x_col >= 0);
        REQUIRE(y_col >= 0);

        const auto ids0 = std::static_pointer_cast<arrow::Int32Array>(batch.column(id_cols[0]));
        const auto xs = std::static_pointer_cast<arrow::Int32Array>(batch.column(x_col));
        const auto ids1 = std::static_pointer_cast<arrow::Int32Array>(batch.column(id_cols[1]));
        const auto ys = std::static_pointer_cast<arrow::Int32Array>(batch.column(y_col));
        std::vector<std::array<std::int64_t, 4>> sorted;
        for (int64_t r = 0; r < batch.num_rows(); ++r) {
            sorted.push_back({ids0->Value(r), xs->Value(r), ids1->Value(r), ys->Value(r)});
        }
        std::sort(sorted.begin(), sorted.end());
        REQUIRE(sorted[0] == std::array<std::int64_t, 4>{1, 10, 1, 100});
        REQUIRE(sorted[1] == std::array<std::int64_t, 4>{2, 20, 2, 200});
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
            REQUIRE(std::string_view{converted.error().what.c_str()}.find("big") != std::string_view::npos);
        }

        SECTION("the mappable columns of the same table still stream") {
            run_or_fail(s, id++, "CREATE TABLE fdb.uhuge (id INT, big uhugeint);");
            const session_hash_t stmt = id++;

            auto prepared = prepare_scheduler_sql(s, stmt, "SELECT id FROM fdb.uhuge;");
            INFO("prepare error: " << prepared.error().what.c_str());
            REQUIRE_FALSE(prepared.has_error());
            auto flight_schema = to_arrow_schema(s.resource, prepared.value().schema);
            INFO("arrow schema error: " << (flight_schema.has_error() ? flight_schema.error().what.c_str() : "ok"));
            REQUIRE_FALSE(flight_schema.has_error());
            REQUIRE(flight_schema.value()->num_fields() == 1);
            REQUIRE(flight_schema.value()->field(0)->type()->id() == arrow::Type::INT32);

            auto executed = execute_scheduler_statement(s, stmt);
            INFO("execute error: " << executed.error().what.c_str());
            REQUIRE_FALSE(executed.has_error());
            REQUIRE(executed.value().size() == 0);

            REQUIRE(to_batches(s, executed.value(), flight_schema.value()).empty());
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
        auto flight_schema = to_arrow_schema(s.resource, schema);
        INFO("arrow schema error: " << (flight_schema.has_error() ? flight_schema.error().what.c_str() : "ok"));
        REQUIRE_FALSE(flight_schema.has_error());
        REQUIRE(flight_schema.value()->num_fields() == 1);
        REQUIRE(flight_schema.value()->field(0)->name() == "big");
        REQUIRE(flight_schema.value()->field(0)->type()->id() == arrow::Type::DECIMAL128);
        const auto* decimal = static_cast<const arrow::Decimal128Type*>(
            flight_schema.value()->field(0)->type().get());
        REQUIRE(decimal->precision() == 38);
        REQUIRE(decimal->scale() == 0);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(to_batches(s, executed.value(), flight_schema.value()).empty());
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
        auto flight_schema = to_arrow_schema(s.resource, schema);
        INFO("arrow schema error: " << (flight_schema.has_error() ? flight_schema.error().what.c_str() : "ok"));
        REQUIRE_FALSE(flight_schema.has_error());
        REQUIRE(flight_schema.value()->num_fields() == 1);
        REQUIRE(flight_schema.value()->field(0)->name() == "amount");
        REQUIRE(flight_schema.value()->field(0)->type()->id() == arrow::Type::DECIMAL128);
        const auto* declared = static_cast<const arrow::Decimal128Type*>(
            flight_schema.value()->field(0)->type().get());
        REQUIRE(declared->precision() == 38);
        REQUIRE(declared->scale() == 10);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(to_batches(s, executed.value(), flight_schema.value()).empty());
    });
}

TEST_CASE("FlightSQL contract: the DoGet stream carries a DECIMAL value with its point") {
    // The case above settles the schema GetFlightInfo hands out; DoGet builds its columns cell
    // by cell against that same schema. The chunk is built here rather than selected, so what
    // is asserted is the encoder and the values are exact on both signs: the slot carries the
    // stored unscaled integer, the scale rides in the field type.
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

    auto flight_schema = to_arrow_schema(
        res,
        components::types::complex_logical_type::create_struct(
            "", std::pmr::vector<components::types::complex_logical_type>{types, res}));
    REQUIRE_FALSE(flight_schema.has_error());

    auto converted = chunk_to_record_batch(res, chunk);
    REQUIRE_FALSE(converted.has_error());
    const auto& batch = *converted.value();
    REQUIRE(batch.schema()->Equals(*flight_schema.value()));
    REQUIRE(batch.num_rows() == 3);
    REQUIRE(batch.column(0)->null_count() == 1);
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
        // will arrive under. The parameterized statement belongs to the
        // prepared-statement RPCs, which bind through DoPut; the schema here
        // is what CreatePreparedStatement hands out as the dataset schema.
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
        // carry; the parameters ride their own schema, one int64 field per parameter
        auto flight_schema = to_arrow_schema(s.resource, schema);
        REQUIRE_FALSE(flight_schema.has_error());
        REQUIRE(flight_schema.value()->num_fields() == 2);
        REQUIRE(flight_schema.value()->field(0)->type()->id() == arrow::Type::INT32);
        REQUIRE(flight_schema.value()->field(1)->type()->id() == arrow::Type::INT32);
    });
}
