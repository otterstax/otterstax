// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "pipeline_state.hpp"
namespace frontend::postgres {
    void pipeline_state::begin_pipeline() { in_pipeline_ = true; }

    void pipeline_state::set_error() {
        if (in_pipeline_) {
            has_error_ = true;
        }
    }

    bool pipeline_state::has_error() { return has_error_; }

    void pipeline_state::end_pipeline() {
        in_pipeline_ = false;
        has_error_ = false;
    }
} // namespace frontend::postgres