// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "spark_connect_server.hpp"

namespace frontend::spark {

spark_connect_server::spark_connect_server(const SparkConnectServerConfig& config)
    : impl_(config) {}

spark_connect_server::~spark_connect_server() { stop(); }

void spark_connect_server::start() { impl_.start(); }

void spark_connect_server::stop() { impl_.stop(); }

}  // namespace frontend::spark
