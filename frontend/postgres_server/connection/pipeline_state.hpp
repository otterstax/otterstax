// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <cstdint>

namespace frontend::postgres {
    class pipeline_state {
    public:
        void begin_pipeline();

        void set_error();
        bool has_error();

        void end_pipeline();

    private:
        bool in_pipeline_ = false;
        bool has_error_ = false;
    };
} // namespace frontend::postgres