// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "service.hpp"

namespace frontend::spark {

class spark_connect_server {
public:
    explicit spark_connect_server(const SparkConnectServerConfig& config);
    ~spark_connect_server();

    void start();
    void stop();

private:
    SparkConnectServiceImpl impl_;
};

}  // namespace frontend::spark
