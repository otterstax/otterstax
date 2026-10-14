// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstddef>
#include <iostream>
#include <string>

namespace conn::api_server {

    struct ConnectionParams {
        std::string alias;
        std::string host;
        std::string port;
        std::string username;
        std::string password;
        std::string database;
        std::string table;
        // Connections the manager keeps open to this backend (the config's
        // `pool_size`); each statement checks one out, a transaction pins one (>= 1).
        std::size_t pool_size{1};
    };

    inline std::ostream& operator<<(std::ostream& os, const ConnectionParams& params) {
        os << "Alias: " << params.alias << std::endl;
        os << "Host: " << params.host << std::endl;
        os << "Port: " << params.port << std::endl;
        os << "Username: " << params.username << std::endl;
        os << "Password: " << params.password << std::endl;
        os << "Database: " << params.database << std::endl;
        os << "Table: " << params.table << std::endl;
        os << "Pool size: " << params.pool_size << std::endl;
        return os;
    }

} // namespace conn::api_server
