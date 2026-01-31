// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "../common/frontend_server.hpp"
#include "connection/postgres_connection.hpp"

namespace frontend::postgres {
    class postgres_server : public frontend_server<postgres_connection> {
        using frontend_server<postgres_connection>::frontend_server;
    };
} // namespace frontend::postgres
