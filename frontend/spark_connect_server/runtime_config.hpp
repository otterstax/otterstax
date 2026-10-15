// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#pragma once

#include <grpcpp/support/status.h>
#include <spark/connect/base.pb.h>

namespace frontend::spark {
    // Answers the fixed settings the engine and result encoder implement.
    // Unsupported mutations must never be acknowledged as applied.
    grpc::Status runtime_config(const ::spark::connect::ConfigRequest& request,
                                ::spark::connect::ConfigResponse& response);
}
