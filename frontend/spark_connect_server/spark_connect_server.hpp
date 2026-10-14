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
        // The bound port (tests bind port 0); 0 when not listening.
        [[nodiscard]] int local_port() const noexcept { return impl_.local_port(); }

    private:
        SparkConnectServiceImpl impl_;
    };

} // namespace frontend::spark
