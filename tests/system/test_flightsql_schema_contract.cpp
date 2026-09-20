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
// such. The batches are verified through the IPC reader — the same bytes a
// client decodes.

#include "scheduler_stack.hpp"

#include "frontend/flight_sql/chunk_to_ipc.hpp"
#include "frontend/flight_sql/ipc/ipc_reader.hpp"
#include "frontend/flight_sql/ipc/ipc_writer.hpp"

#include <absl/numeric/int128.h>

#include <catch2/catch_all.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

using flight::core::EngineError;
using flight::ipc::RecordBatch;
using flight::ipc::TypeId;
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

    // writer + reader round-trip: the values a client would decode.
    std::vector<std::vector<flight::ipc::Value>> decode(const RecordBatch& batch) {
        const auto message = flight::ipc::serialize_record_batch(batch);
        return flight::ipc::decode_record_batch(*batch.schema,
                                                message.bare_message.data(), message.bare_message.size(),
                                                message.body.data(), message.body.size());
    }

    // The unscaled integer a decimal128 slot carries, as int128.
    components::types::int128_t decimal_slot(const RecordBatch& batch, int column, int64_t row) {
        const auto& buffer = batch.columns[column].buffers[1];
        std::uint64_t low = 0, high = 0;
        std::memcpy(&low, buffer.data() + static_cast<std::size_t>(row) * 16, sizeof(low));
        std::memcpy(&high, buffer.data() + static_cast<std::size_t>(row) * 16 + 8, sizeof(high));
        return absl::MakeInt128(static_cast<std::int64_t>(high), low);
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

        auto flight_schema = flight::conv::schema_to_ipc(schema);
        REQUIRE(flight_schema->fields.size() == 4);

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

        auto batches = flight::conv::chunks_to_ipc(executed.value(), flight_schema);
        REQUIRE(batches.size() == 1);
        const auto& batch = batches.front();
        REQUIRE(batch.schema.get() == flight_schema.get());
        REQUIRE(batch.columns.size() == 4);
        REQUIRE(batch.num_rows == 2);

        // Every field is fed from its own chunk column: the second `id` is b's
        // key, not a copy of a's and not an empty array.
        std::vector<int> id_cols;
        int x_col = -1, y_col = -1;
        for (int i = 0; i < 4; ++i) {
            const std::string name = flight_schema->fields[i]->name;
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

        const auto rows = decode(batch);
        std::vector<std::array<std::int64_t, 4>> sorted;
        for (const auto& row : rows) {
            sorted.push_back({std::get<std::int64_t>(row[id_cols[0]]),
                              std::get<std::int64_t>(row[x_col]),
                              std::get<std::int64_t>(row[id_cols[1]]),
                              std::get<std::int64_t>(row[y_col])});
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

            try {
                flight::conv::schema_to_ipc(schema);
                FAIL("expected the UHUGEINT column to be refused");
            } catch (const EngineError& e) {
                REQUIRE(mentions(e.what(), "big"));
            }
        }

        SECTION("the mappable columns of the same table still stream") {
            run_or_fail(s, id++, "CREATE TABLE fdb.uhuge (id INT, big uhugeint);");
            const session_hash_t stmt = id++;

            auto prepared = prepare_scheduler_sql(s, stmt, "SELECT id FROM fdb.uhuge;");
            INFO("prepare error: " << prepared.error().what.c_str());
            REQUIRE_FALSE(prepared.has_error());
            auto flight_schema = flight::conv::schema_to_ipc(prepared.value().schema);
            REQUIRE(flight_schema->fields.size() == 1);
            REQUIRE(flight_schema->fields[0]->type->id == TypeId::Int32);

            auto executed = execute_scheduler_statement(s, stmt);
            INFO("execute error: " << executed.error().what.c_str());
            REQUIRE_FALSE(executed.has_error());
            REQUIRE(executed.value().size() == 0);

            REQUIRE(flight::conv::chunks_to_ipc(executed.value(), flight_schema).empty());
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
        auto flight_schema = flight::conv::schema_to_ipc(schema);
        REQUIRE(flight_schema->fields.size() == 1);
        REQUIRE(flight_schema->fields[0]->name == "big");
        REQUIRE(flight_schema->fields[0]->type->id == TypeId::Decimal128);
        REQUIRE(flight_schema->fields[0]->type->precision == 38);
        REQUIRE(flight_schema->fields[0]->type->scale == 0);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(flight::conv::chunks_to_ipc(executed.value(), flight_schema).empty());
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
        auto flight_schema = flight::conv::schema_to_ipc(schema);
        REQUIRE(flight_schema->fields.size() == 1);
        REQUIRE(flight_schema->fields[0]->name == "amount");
        REQUIRE(flight_schema->fields[0]->type->id == TypeId::Decimal128);
        REQUIRE(flight_schema->fields[0]->type->precision == 38);
        REQUIRE(flight_schema->fields[0]->type->scale == 10);

        auto executed = execute_scheduler_statement(s, stmt);
        INFO("execute error: " << executed.error().what.c_str());
        REQUIRE_FALSE(executed.has_error());
        REQUIRE(flight::conv::chunks_to_ipc(executed.value(), flight_schema).empty());
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

    auto flight_schema = flight::conv::schema_to_ipc(
        components::types::complex_logical_type::create_struct("", std::pmr::vector<components::types::complex_logical_type>{types, res}));

    std::pmr::vector<components::vector::data_chunk_t> chunks(res);
    chunks.push_back(std::move(chunk));
    session_payload payload{components::types::complex_logical_type::create_struct(
                                "", std::pmr::vector<components::types::complex_logical_type>{types, res}),
                            std::move(chunks), 0, NodeTag::T_SelectStmt};
    auto batches = flight::conv::chunks_to_ipc(payload, flight_schema);
    REQUIRE(batches.size() == 1);
    const auto& batch = batches.front();
    REQUIRE(batch.schema.get() == flight_schema.get());
    REQUIRE(batch.num_rows == 3);
    REQUIRE(batch.columns[0].null_count == 1);
    REQUIRE(decimal_slot(batch, 0, 0) == 12345);
    REQUIRE(decimal_slot(batch, 0, 1) == -12345);
    // a null slot is the validity bitmap's business; its bytes stay zeroed
    REQUIRE(decimal_slot(batch, 0, 2) == 0);
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
        auto flight_schema = flight::conv::schema_to_ipc(schema);
        REQUIRE(flight_schema->fields.size() == 2);
        REQUIRE(flight_schema->fields[0]->type->id == TypeId::Int32);
        REQUIRE(flight_schema->fields[1]->type->id == TypeId::Int32);
        auto parameter_schema = flight::conv::parameter_ipc_schema(prepared.value().parameter_count);
        REQUIRE(parameter_schema->fields.size() == 1);
        REQUIRE(parameter_schema->fields[0]->type->id == TypeId::Int64);
    });
}
