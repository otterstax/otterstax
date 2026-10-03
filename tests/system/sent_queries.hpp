// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace otterstax::test {
    struct sent_query_t {
        std::string alias;
        std::string query;
    };

    inline std::mutex g_sent_mutex;
    inline std::vector<sent_query_t> g_sent;

    inline void record_sent(std::string_view alias, std::string_view query) {
        std::lock_guard guard(g_sent_mutex);
        g_sent.push_back({std::string{alias}, std::string{query}});
    }

    inline void forget_sent() {
        std::lock_guard guard(g_sent_mutex);
        g_sent.clear();
    }

    inline std::vector<sent_query_t> all_sent() {
        std::lock_guard guard(g_sent_mutex);
        return g_sent;
    }

    inline std::vector<std::string> sent_to(std::string_view alias) {
        std::lock_guard guard(g_sent_mutex);
        std::vector<std::string> queries;
        for (const auto& sent : g_sent) {
            if (sent.alias == alias) {
                queries.push_back(sent.query);
            }
        }
        return queries;
    }
} // namespace otterstax::test
