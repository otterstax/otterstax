// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <iostream>
#include <string>

namespace http_server {

    struct ConnectionParams {
        std::string alias;
        std::string host;
        std::string port;
        std::string username;
        std::string password;
        std::string database;
        std::string table;
    };

    inline std::ostream& operator<<(std::ostream& os, const ConnectionParams& params) {
        os << "Alias: " << params.alias << std::endl;
        os << "Host: " << params.host << std::endl;
        os << "Port: " << params.port << std::endl;
        os << "Username: " << params.username << std::endl;
        os << "Password: " << params.password << std::endl;
        os << "Database: " << params.database << std::endl;
        os << "Table: " << params.table << std::endl;
        return os;
    }

} // namespace http_server
