// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <cstdint>

namespace mysql_front {
    // The status flags are a bit-field
    using server_status_flags_t = uint16_t;

    enum class server_status : uint16_t
    {
        SERVER_STATUS_IN_TRANS = 1,
        SERVER_STATUS_AUTOCOMMIT = 2,   // Server in auto_commit mode
        SERVER_MORE_RESULTS_EXISTS = 8, // Multi query - next query exists
        SERVER_QUERY_NO_GOOD_INDEX_USED = 16,
        SERVER_QUERY_NO_INDEX_USED = 32,
        /** The server was able to fulfill the clients request and opened a
        read-only non-scrollable cursor for a query. This flag comes
        in reply to COM_STMT_EXECUTE and COM_STMT_FETCH commands.
        Used by Binary Protocol Resultset to signal that COM_STMT_FETCH
        must be used to fetch the row-data. */
        SERVER_STATUS_CURSOR_EXISTS = 64,
        /** This flag is sent when a read-only cursor is exhausted, in reply to
        COM_STMT_FETCH command. */
        SERVER_STATUS_LAST_ROW_SENT = 128,
        SERVER_STATUS_DB_DROPPED = 256,
        SERVER_STATUS_NO_BACKSLASH_ESCAPES = 512,
        /** Sent to the client if after a prepared statement reprepare we
        discovered that the new statement returns a different number of result set columns. */
        SERVER_STATUS_METADATA_CHANGED = 1024,
        SERVER_QUERY_WAS_SLOW = 2048,
        SERVER_PS_OUT_PARAMS = 4096, // To mark ResultSet containing output parameter values.
        SERVER_STATUS_IN_TRANS_READONLY = 8192,
        /** This status flag, when on, implies that one of the state information has
        changed on the server because of the execution of the last statement. */
        SERVER_SESSION_STATE_CHANGED = (1UL << 14)
    };
} // namespace mysql_front