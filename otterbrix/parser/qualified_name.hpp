// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>

#include <tuple>

// The engine's qualified_name_t lost its defaulted ordering; OtterStax keys
// std::map's by it (the schema dependency maps), so the ordering it had —
// lexicographic over (unique_identifier, database, schema, collection) — is
// restored here. Declared in the global namespace, qualified_name_t's own, so
// std::less finds it by ADL.
inline bool operator<(const qualified_name_t& lhs, const qualified_name_t& rhs) {
    return std::tie(lhs.unique_identifier.t, lhs.database.t, lhs.schema.t, lhs.collection.t) <
           std::tie(rhs.unique_identifier.t, rhs.database.t, rhs.schema.t, rhs.collection.t);
}
