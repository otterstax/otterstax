// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#pragma once
#include <components/session/session.hpp>

using session_id = components::session::session_id_t;
using session_hash_t = std::size_t;

enum class session_type : uint8_t
{
    GET_FLIGHT_INFO,
    DO_GET,
};