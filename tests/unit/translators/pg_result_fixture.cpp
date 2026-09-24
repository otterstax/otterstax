// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "pg_result_fixture.hpp"

#include <algorithm>
#include <cstring>

// libpq has no public setter for a result's command status: it is written only while
// parsing a server's CommandComplete message. The layout header libpq installs
// (postgresql/internal/libpq-int.h) is the one way to manufacture such a result without a
// live connection. It is kept in this translation unit alone because c.h/port.h define
// macros (Assert, Min, Max, the printf family) that must not leak into test code.
#include "c.h"
#include "libpq-int.h"

namespace otterstax::test {

    PGresult* make_command_result(std::string_view command_tag) {
        PGresult* result = PQmakeEmptyPGresult(nullptr, PGRES_COMMAND_OK);
        const std::size_t length = std::min<std::size_t>(command_tag.size(), CMDSTATUS_LEN - 1);
        std::memcpy(result->cmdStatus, command_tag.data(), length);
        result->cmdStatus[length] = '\0';
        return result;
    }

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
            attrs[c].atttypmod = -1;
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
