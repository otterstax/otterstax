// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix_engine.hpp"

#include <services/dispatcher/dispatcher.hpp>

namespace db {

    actor_zeta::address_t otterbrix_engine_t::engine_dispatcher_address() {
        return manager_dispatcher_->address();
    }

} // namespace db
