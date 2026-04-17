// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
#pragma once

#include <cstring>
#include <memory_resource>

// AST for the s3 grammar extension. Plain arena-allocated structs (no PG
// nodes), mirroring the demo_extension template. Every node is allocated from
// the parser arena passed to s3_ext::parse(), so the tree lives exactly as long
// as the parse.
namespace s3_ext {

    enum class s3_stmt_kind {
        create_external_table, // CREATE EXTERNAL TABLE <db>.<table> WITH (...)
        copy_to                // COPY (<inner_sql>) TO '<location>' WITH (...)
    };

    // A single WITH ( key = 'value' ) option, kept as a singly-linked list so
    // the bison grammar can build it without a container.
    struct s3_option {
        const char* key;
        const char* value;
        s3_option*  next;
    };

    struct s3_stmt {
        s3_stmt_kind kind;
        const char*  db;        // create only: e.g. "s3"
        const char*  table;     // create only: e.g. "trades"
        const char*  inner_sql; // copy only: raw SELECT text (filled by the driver)
        const char*  location;  // create: WITH location=...   copy: the TO target
        const char*  s3_alias;  // resolved from options, or nullptr
        const char*  format;    // resolved from options, or nullptr
        s3_option*   options;   // full raw option list
    };

    inline char* arena_strdup(std::pmr::memory_resource* resource, const char* s, std::size_t n) {
        char* p = static_cast<char*>(resource->allocate(n + 1));
        std::memcpy(p, s, n);
        p[n] = '\0';
        return p;
    }

    // Copy a single-quoted scanner lexeme (including the surrounding quotes)
    // into the arena, stripping the quotes and collapsing the SQL '' escape.
    inline const char* arena_sconst(std::pmr::memory_resource* resource, const char* text, std::size_t len) {
        // text is "'...'"; inner content is text[1 .. len-2]
        const char* in = text + 1;
        std::size_t inner = (len >= 2) ? len - 2 : 0;
        char* out = static_cast<char*>(resource->allocate(inner + 1));
        std::size_t j = 0;
        for (std::size_t i = 0; i < inner; ++i) {
            if (in[i] == '\'' && i + 1 < inner && in[i + 1] == '\'') {
                out[j++] = '\'';
                ++i; // skip the second quote of the '' pair
            } else {
                out[j++] = in[i];
            }
        }
        out[j] = '\0';
        return out;
    }

    inline s3_option* make_option(std::pmr::memory_resource* resource, const char* key, const char* value) {
        auto* o = static_cast<s3_option*>(resource->allocate(sizeof(s3_option)));
        o->key = key;
        o->value = value;
        o->next = nullptr;
        return o;
    }

    inline s3_option* append_option(s3_option* head, s3_option* tail_node) {
        if (head == nullptr) {
            return tail_node;
        }
        s3_option* cur = head;
        while (cur->next != nullptr) {
            cur = cur->next;
        }
        cur->next = tail_node;
        return head;
    }

    inline const char* option_value(const s3_option* head, const char* key) {
        for (const s3_option* o = head; o != nullptr; o = o->next) {
            if (o->key != nullptr && std::strcmp(o->key, key) == 0) {
                return o->value;
            }
        }
        return nullptr;
    }

    inline s3_stmt* make_stmt(std::pmr::memory_resource* resource, s3_stmt_kind kind) {
        auto* s = static_cast<s3_stmt*>(resource->allocate(sizeof(s3_stmt)));
        s->kind = kind;
        s->db = nullptr;
        s->table = nullptr;
        s->inner_sql = nullptr;
        s->location = nullptr;
        s->s3_alias = nullptr;
        s->format = nullptr;
        s->options = nullptr;
        return s;
    }

    inline s3_stmt*
    make_create(std::pmr::memory_resource* resource, const char* db, const char* table, s3_option* options) {
        s3_stmt* s = make_stmt(resource, s3_stmt_kind::create_external_table);
        s->db = db;
        s->table = table;
        s->options = options;
        return s;
    }

    inline s3_stmt* make_copy(std::pmr::memory_resource* resource, const char* location, s3_option* options) {
        s3_stmt* s = make_stmt(resource, s3_stmt_kind::copy_to);
        s->location = location;
        s->options = options;
        return s;
    }

} // namespace s3_ext
