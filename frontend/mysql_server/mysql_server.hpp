// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../common/frontend_server.hpp"
#include "connection/mysql_connection.hpp"

namespace frontend::mysql {
    class mysql_server : public frontend_server<mysql_connection> {
        using frontend_server<mysql_connection>::frontend_server;
    };
} // namespace frontend::mysql
