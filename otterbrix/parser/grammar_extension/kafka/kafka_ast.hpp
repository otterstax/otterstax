// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <memory_resource>
#include <new>
#include <string>

namespace kafka_grammar {

    enum class stmt_kind
    {
        create_source,
        create_stream,
        drop_object
    };

    enum class object_kind
    {
        source,
        stream
    };

    // Singly-linked, order-preserving list node for a column declaration.
    struct column_def {
        std::pmr::string name;
        std::pmr::string type; // raw type keyword, lowered in the transform stage
        column_def* next;
    };

    // Singly-linked, order-preserving list node for a WITH (...) option.
    struct with_option {
        std::pmr::string key;
        std::pmr::string value;
        with_option* next;
    };

    struct kafka_stmt {
        stmt_kind kind;
        object_kind obj;            // CREATE: which primitive; DROP: which object
        std::pmr::string name;      // object name
        column_def* columns;        // CREATE SOURCE column list, else nullptr
        with_option* options;       // WITH (...) options, else nullptr
        std::pmr::string as_select; // CREATE STREAM ... AS <raw sql>, else empty
        bool if_exists;             // DROP ... IF EXISTS
    };

    inline kafka_stmt*
    make_stmt(std::pmr::memory_resource* resource, stmt_kind kind, object_kind obj, const char* name) {
        auto* stmt = static_cast<kafka_stmt*>(resource->allocate(sizeof(kafka_stmt)));
        return new (stmt) kafka_stmt{kind,
                                     obj,
                                     std::pmr::string(name, resource),
                                     nullptr,
                                     nullptr,
                                     std::pmr::string(resource),
                                     false};
    }

    inline column_def* make_column(std::pmr::memory_resource* resource, const char* name, const char* type) {
        auto* col = static_cast<column_def*>(resource->allocate(sizeof(column_def)));
        return new (col) column_def{std::pmr::string(name, resource), std::pmr::string(type, resource), nullptr};
    }

    inline with_option* make_option(std::pmr::memory_resource* resource, const char* key, const char* value) {
        auto* opt = static_cast<with_option*>(resource->allocate(sizeof(with_option)));
        return new (opt) with_option{std::pmr::string(key, resource), std::pmr::string(value, resource), nullptr};
    }

} // namespace kafka_grammar
