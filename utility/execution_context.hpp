// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#pragma once

#include "utility/session.hpp"
#include <actor-zeta.hpp>

// The request ID owns a prepared plan; this value identifies the database
// session that executes it. Passed by value across actor boundaries.
struct execution_context {
    session_id engine_session;
    bool persistent{false};
    bool in_transaction{false};
    actor_zeta::address_t transaction_backend{actor_zeta::address_t::empty_address()};
};
