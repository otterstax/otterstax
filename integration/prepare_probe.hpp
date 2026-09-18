// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/parser/parser.hpp"

#include <string>
#include <string_view>

namespace db {

    // The alias the probe's derived table carries. MySQL and PostgreSQL both
    // require one on a derived table ("Every derived table must have its own
    // alias" / "subquery in FROM must have an alias"); ClickHouse does not and
    // gets none.
    inline constexpr std::string_view kPrepareProbeAlias = "otx_prepare_probe";

    // The prepare probe of one slot's statement:
    // `SELECT * FROM (<statement>) [AS otx_prepare_probe] LIMIT 0`.
    //
    // Each backend answers the wrapped statement's result columns and their wire
    // types before any row — MySQL and ClickHouse send the column definitions
    // ahead of the data, PostgreSQL the RowDescription — and the outer LIMIT 0
    // keeps the answer at that: PostgreSQL's ExecLimit reports the empty result
    // without ever pulling from the subplan, ClickHouse's LimitTransform closes
    // its input once `rows_read >= offset + limit` (true before the first
    // block), and MySQL/MariaDB short-circuit the query block while optimizing
    // it ("Zero limit"), materializing a derived table only when the outer plan
    // reads from it. So an aggregate under the wrap costs its analysis, not its
    // data.
    //
    // A `WHERE 1 != 1` wrap would not do: a constant-false filter over a derived
    // table is a filter on THAT table's rows, so the aggregate under it has to
    // produce them first. The generator ends every statement with `;`, which
    // cannot stand inside the parentheses.
    //
    // What the wrap does not survive is a statement projecting two columns of
    // one name: PostgreSQL and ClickHouse accept such a derived table, MySQL
    // refuses it (ER_DUP_FIELDNAME). A slot statement is one table's projection
    // or a sub-query the user already wrote as a derived table, so neither
    // carries duplicates.
    inline std::string make_prepare_probe(std::string_view statement, backend_type_t backend) {
        while (!statement.empty() &&
               (statement.back() == ';' || statement.back() == ' ' || statement.back() == '\n')) {
            statement.remove_suffix(1);
        }
        std::string probe;
        probe.reserve(statement.size() + kPrepareProbeAlias.size() + 32);
        probe.append("SELECT * FROM (");
        probe.append(statement);
        probe.append(")");
        if (backend != backend_type_t::ClickHouse) {
            probe.append(" AS ");
            probe.append(kPrepareProbeAlias);
        }
        probe.append(" LIMIT 0");
        return probe;
    }

} // namespace db
