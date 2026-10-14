// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <libpq-fe.h>

#include <vector>

namespace otterstax::test {

    struct pg_column {
        const char* name;
        Oid typid;
        // The column's type modifier (PQfmod): NUMERIC(p, s) is ((p << 16) | s) + 4; -1 none.
        int typmod = -1;
    };

    // A PGresult with `columns` and one tuple per entry of `rows`; a nullptr cell is SQL
    // NULL. Built through libpq's public PQsetResultAttrs / PQsetvalue. The caller owns
    // the result (PQclear).
    PGresult* make_tuples_result(const std::vector<pg_column>& columns,
                                 const std::vector<std::vector<const char*>>& rows);

    // RAII owner for a PGresult built by the factory above.
    class pg_result_guard {
    public:
        explicit pg_result_guard(PGresult* result)
            : result_(result) {}
        ~pg_result_guard() {
            if (result_) {
                PQclear(result_);
            }
        }
        pg_result_guard(const pg_result_guard&) = delete;
        pg_result_guard& operator=(const pg_result_guard&) = delete;

        PGresult* get() const noexcept { return result_; }

    private:
        PGresult* result_;
    };

} // namespace otterstax::test
