// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>

#include <string>

namespace otterstax {

    // Encodes the engine-side collection name for an external table registered
    // in the Otterbrix catalog. The per-connection uid becomes the engine
    // database name; the collection name folds the remaining qualifiers as
    //   <db> ':' <schema> ':' <collection>
    // The encoding is one-way by design — the OID-keyed schema_store_t holds
    // the original qualified name, so no decode function exists.
    inline std::string encode_external_collection(const qualified_name_t& name) {
        std::string encoded;
        encoded.reserve(name.database.size() + name.schema.size() + name.collection.size() + 2);
        encoded += name.database;
        encoded += ':';
        encoded += name.schema;
        encoded += ':';
        encoded += name.collection;
        return encoded;
    }

} // namespace otterstax
