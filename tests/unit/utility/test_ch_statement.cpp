// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// ch::make_statement wires a driver Query to a select_result_t. The driver
// invokes the QueryEvents callbacks itself; here they are driven by hand
// through the QueryEvents base, the only seam the driver offers without a
// server.

#include "connectors/clickhouse/connector.hpp"

#include <catch2/catch_all.hpp>

#include <clickhouse/columns/numeric.h>

#include <memory>

namespace {

    clickhouse::Block one_row_block(int32_t value) {
        auto column = std::make_shared<clickhouse::ColumnInt32>();
        column->Append(value);
        clickhouse::Block block;
        block.AppendColumn("id", column);
        return block;
    }

    clickhouse::Progress progress_with_written(uint64_t written_rows) {
        clickhouse::Progress progress;
        progress.written_rows = written_rows;
        return progress;
    }

} // namespace

TEST_CASE("ch::make_statement: keeps the query text and starts from zero written rows") {
    ch::select_result_t out;
    auto statement = ch::make_statement("INSERT INTO t VALUES (1)", out);

    REQUIRE(statement.GetText() == "INSERT INTO t VALUES (1)");
    REQUIRE(out.blocks.empty());
    REQUIRE(out.written_rows == 0);
}

TEST_CASE("ch::make_statement: written rows are the sum of every Progress packet") {
    ch::select_result_t out;
    auto statement = ch::make_statement("INSERT INTO t VALUES (1), (2), (3)", out);
    clickhouse::QueryEvents& events = statement;

    // The server reports progress piecewise: each packet is a delta, never a
    // running total, and an INSERT streams no data block at all.
    events.OnProgress(progress_with_written(2));
    events.OnProgress(progress_with_written(0));
    events.OnProgress(progress_with_written(1));

    REQUIRE(out.written_rows == 3);
    REQUIRE(out.blocks.empty());
}

TEST_CASE("ch::make_statement: every data block is collected in order, header block included") {
    ch::select_result_t out;
    auto statement = ch::make_statement("SELECT id FROM t", out);
    clickhouse::QueryEvents& events = statement;

    clickhouse::Block header;
    header.AppendColumn("id", std::make_shared<clickhouse::ColumnInt32>());
    events.OnData(header);
    events.OnData(one_row_block(7));
    events.OnData(one_row_block(9));
    // A SELECT reports read progress only; nothing was written.
    clickhouse::Progress read_only;
    read_only.rows = 2;
    events.OnProgress(read_only);

    REQUIRE(out.blocks.size() == 3);
    REQUIRE(out.blocks[0].GetRowCount() == 0);
    REQUIRE(out.blocks[0].GetColumnCount() == 1);
    REQUIRE(out.blocks[1].GetRowCount() == 1);
    REQUIRE(out.blocks[2].GetRowCount() == 1);
    REQUIRE(out.written_rows == 0);
}
