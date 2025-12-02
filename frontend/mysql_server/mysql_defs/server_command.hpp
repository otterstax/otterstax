// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once

#include <cstdint>

namespace mysql_front {
    enum server_command : uint8_t
    {
        COM_SLEEP, // Currently refused by the server. Also used internally to mark the start of a session
        COM_QUIT,
        COM_INIT_DB,
        COM_QUERY,
        COM_FIELD_LIST, // Deprecated
        COM_CREATE_DB,  // Currently refused by the server
        COM_DROP_DB,    // Currently refused by the server
        COM_UNUSED_2,   // Removed, used to be COM_REFRESH
        COM_UNUSED_1,   // Removed, used to be COM_SHUTDOWN
        COM_STATISTICS,
        COM_UNUSED_4, // Removed, used to be COM_PROCESS_INFO
        COM_CONNECT,  // Currently refused by the server
        COM_UNUSED_5, // Removed, used to be COM_PROCESS_KILL
        COM_DEBUG,
        COM_PING,
        COM_TIME,           // Currently refused by the server
        COM_DELAYED_INSERT, // Functionality removed
        COM_CHANGE_USER,
        COM_BINLOG_DUMP,
        COM_TABLE_DUMP,
        COM_REGISTER_SLAVE,
        COM_CONNECT_OUT,
        COM_STMT_PREPARE,
        COM_STMT_EXECUTE,
        COM_STMT_SEND_LONG_DATA,
        COM_STMT_CLOSE,
        COM_STMT_RESET,
        COM_SET_OPTION,
        COM_STMT_FETCH,
        COM_DAEMON, // Currently refused by the server. Also used internally to mark the session as a "daemon",
        COM_BINLOG_DUMP_GTID,
        COM_RESET_CONNECTION,
        COM_CLONE,
        COM_SUBSCRIBE_GROUP_REPLICATION_STREAM,
        /* Must be last */
        COM_END // Not a real command. Refused.
    };
} // namespace mysql_front