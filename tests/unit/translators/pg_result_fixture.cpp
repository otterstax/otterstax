// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "pg_result_fixture.hpp"

#include <cstring>

namespace otterstax::test {

    PGresult* make_tuples_result(const std::vector<pg_column>& columns,
                                 const std::vector<std::vector<const char*>>& rows) {
        PGresult* result = PQmakeEmptyPGresult(nullptr, PGRES_TUPLES_OK);
        std::vector<PGresAttDesc> attrs(columns.size());
        for (std::size_t c = 0; c < columns.size(); ++c) {
            attrs[c].name = const_cast<char*>(columns[c].name);
            attrs[c].tableid = 0;
            attrs[c].columnid = 0;
            attrs[c].format = 0;
            attrs[c].typid = columns[c].typid;
            attrs[c].typlen = -1;
            attrs[c].atttypmod = columns[c].typmod;
        }
        PQsetResultAttrs(result, static_cast<int>(attrs.size()), attrs.data());
        for (std::size_t r = 0; r < rows.size(); ++r) {
            for (std::size_t c = 0; c < columns.size(); ++c) {
                const char* cell = rows[r][c];
                PQsetvalue(result,
                           static_cast<int>(r),
                           static_cast<int>(c),
                           const_cast<char*>(cell),
                           cell == nullptr ? -1 : static_cast<int>(std::strlen(cell)));
            }
        }
        return result;
    }

} // namespace otterstax::test
