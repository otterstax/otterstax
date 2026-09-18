// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <libpq-fe.h>

#include <string_view>
#include <vector>

namespace otterstax::test {

    // A PGresult shaped like libpq's CommandComplete outcome: no columns, no tuples, and
    // `command_tag` ("UPDATE 5000", "CREATE TABLE") as its command status. The caller
    // owns the result (PQclear).
    PGresult* make_command_result(std::string_view command_tag);

    struct pg_column {
        const char* name;
        Oid typid;
    };

    // A PGresult with `columns` and one tuple per entry of `rows`; a nullptr cell is SQL
    // NULL. Built through libpq's public PQsetResultAttrs / PQsetvalue. The caller owns
    // the result (PQclear).
    PGresult* make_tuples_result(const std::vector<pg_column>& columns,
                                 const std::vector<std::vector<const char*>>& rows);

    // RAII owner for a PGresult built by the factories above.
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
