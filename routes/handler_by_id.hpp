// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <actor-zeta.hpp>

#include <cstdint>
#include <cstdlib>

enum class group_id_t : uint8_t
{
    scheduler = 0,
    catalog_manager,
    sql_connection_manager,
    nosql_connection_manager,
    otterbrix_manager,
};

template<class T>
constexpr auto handler_id(group_id_t group, T type) {
    return actor_zeta::make_message_id(100 * static_cast<uint64_t>(group) + static_cast<uint64_t>(type));
}
