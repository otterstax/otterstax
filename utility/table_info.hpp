// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/types/types.hpp>

struct table_info {
    collection_full_name_t name;
    components::types::complex_logical_type schema;
};
