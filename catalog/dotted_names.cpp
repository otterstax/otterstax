// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "dotted_names.hpp"

#include "catalog/host_functions.hpp"
#include "catalog/sql_tokens.hpp"
#include "otterbrix/federation/metadata.hpp"
#include "utility/tracy_profiler.hpp"

#include <algorithm>
#include <array>
#include <vector>

namespace otterstax::catalog {
    namespace {
        using kind_t = sql_token_t::kind_t;

        bool punct_is(std::string_view sql, const std::vector<sql_token_t>& tokens, std::size_t at, char c) {
            return at < tokens.size() && tokens[at].kind == kind_t::punct && sql[tokens[at].begin] == c;
        }

        // The segments of `text` split at its dots: none when it holds no dot or an empty segment.
        std::vector<std::string_view> segments_of(std::string_view text) {
            std::vector<std::string_view> segments;
            if (text.find('.') == std::string_view::npos) {
                return segments;
            }
            std::size_t from = 0;
            while (true) {
                const auto dot = text.find('.', from);
                const auto segment =
                    text.substr(from, dot == std::string_view::npos ? std::string_view::npos : dot - from);
                if (segment.empty()) {
                    return {};
                }
                segments.push_back(segment);
                if (dot == std::string_view::npos) {
                    return segments;
                }
                from = dot + 1;
            }
        }
    } // namespace

    std::string split_dotted_names(std::string_view sql, std::span<const configured_connection_t> connections) {
        OTX_ZONE_N("catalog::split_dotted_names");
        if (sql.find('"') == std::string_view::npos) {
            return std::string{sql};
        }
        const auto names_a_place = [&](std::string_view first) {
            return first == federation::local_database ||
                   std::any_of(connections.begin(), connections.end(), [&](const auto& c) { return c.name == first; });
        };
        const auto tokens = engine_tokens(sql);
        std::string out;
        std::size_t copied = 0; // sql[copied, …) is still to be written as it is
        for (std::size_t i = 0; i < tokens.size(); ++i) {
            const auto& token = tokens[i];
            if (token.kind != kind_t::quoted_identifier || token.escaped || !punct_is(sql, tokens, i + 1, '.')) {
                continue;
            }
            const auto segments = segments_of(sql.substr(token.begin + 1, token.end - token.begin - 2));
            if (segments.empty() || !names_a_place(segments.front())) {
                continue;
            }
            out.append(sql.substr(copied, token.begin - copied));
            for (std::size_t s = 0; s < segments.size(); ++s) {
                if (s > 0) {
                    out.push_back('.');
                }
                out.push_back('"');
                out.append(segments[s]);
                out.push_back('"');
            }
            copied = token.end;
        }
        out.append(sql.substr(copied));
        return out;
    }

    // Why this rewrite exists: otterbrix scopes a bare call to its built-ins and the user functions of the
    // session's place schema, and a `pg_catalog.`-qualified call to its built-ins alone — and a function a host
    // registers cannot be put in pg_catalog: otterbrix's catalog serialization keeps no user-defined function or
    // cast there. So the host's functions live in local.public (host_function_registrations), which a call reaches
    // from any place only when it names that schema. Drivers call them bare or as pg_catalog.f() (SQLAlchemy's
    // PostgreSQL dialects run `select pg_catalog.version()` on connect), so every such call of a host function is
    // written `local.public.f(...)` before the parse. Drop this pass once pg_catalog can hold a host's functions
    // ([.engine-defect-pgcatalog-udf] then turns visible).
    // pg_table_is_visible(oid), which drivers' table lists filter by (Django's get_table_list): a relation is visible
    // when its schema is on the session's search path — the engine's is the one place the client chose, and
    // pg_catalog, always searched — so OtterStax's own tables, another connection's and another schema's are not.
    // A host function cannot answer it (a kernel sees no session's place), so the call is written as the
    // uncorrelated sub-query over the catalog that says it.
    constexpr std::string_view table_is_visible = "pg_table_is_visible";
    constexpr std::string_view visible_relations =
        "SELECT otx_vc.oid FROM pg_catalog.pg_class otx_vc JOIN pg_catalog.pg_namespace otx_vn ON otx_vc.relnamespace "
        "= otx_vn.oid JOIN pg_catalog.pg_database otx_vd ON otx_vn.nspdatabase = otx_vd.oid WHERE otx_vn.nspname = "
        "'pg_catalog' OR (otx_vn.nspname = current_schema() AND otx_vd.datname = current_database() AND "
        "otx_vd.datconnection = current_connection())";

    std::string qualify_host_calls(std::string_view sql) {
        OTX_ZONE_N("catalog::qualify_host_calls");
        const auto names = host_function_names();
        const auto lower = [](std::string_view text) {
            std::string out{text};
            std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return std::tolower(c); });
            return out;
        };
        // A cheap test first: most statements name no host function at all.
        const auto folded = lower(sql);
        if (folded.find(table_is_visible) == std::string::npos &&
            std::none_of(names.begin(), names.end(), [&](std::string_view name) {
                return folded.find(name) != std::string::npos;
            })) {
            return std::string{sql};
        }
        // A table (or type, view, index target) may be named like a host function: `INSERT INTO version (a)`,
        // `CREATE TABLE version (...)` are no calls.
        constexpr std::array<std::string_view, 7> names_a_relation{"into", "table", "exists", "on", "references",
                                                                  "view", "type"};
        const auto tokens = engine_tokens(sql);
        std::string out;
        std::size_t copied = 0;
        for (std::size_t i = 0; i < tokens.size(); ++i) {
            const auto& token = tokens[i];
            if (token.kind != kind_t::word || !punct_is(sql, tokens, i + 1, '(')) {
                continue;
            }
            const bool visible_call = word_is(sql, token, table_is_visible);
            const bool host = visible_call || std::any_of(names.begin(), names.end(), [&](std::string_view name) {
                                  return word_is(sql, token, name);
                              });
            if (!host) {
                continue;
            }
            // The call's written qualifier: none, or pg_catalog (unquoted, or quoted as written) with nothing
            // before it. Any other qualifier names a schema of its own.
            std::size_t first = i;
            if (punct_is(sql, tokens, i - 1, '.')) {
                if (i < 2) {
                    continue;
                }
                const auto& schema = tokens[i - 2];
                const bool pg_catalog = word_is(sql, schema, "pg_catalog") ||
                                        (schema.kind == kind_t::quoted_identifier &&
                                         sql.substr(schema.begin, schema.end - schema.begin) == "\"pg_catalog\"");
                if (!pg_catalog || punct_is(sql, tokens, i - 3, '.')) {
                    continue;
                }
                first = i - 2;
            } else if (i > 0 && std::any_of(names_a_relation.begin(), names_a_relation.end(), [&](std::string_view w) {
                           return word_is(sql, tokens[i - 1], w);
                       })) {
                continue;
            }
            if (visible_call) {
                // The call's argument: up to its balanced closing parenthesis; an unbalanced text is left to the
                // engine's parser.
                std::size_t depth = 0;
                std::size_t close = i + 1;
                for (; close < tokens.size(); ++close) {
                    if (punct_is(sql, tokens, close, '(')) {
                        ++depth;
                    } else if (punct_is(sql, tokens, close, ')') && --depth == 0) {
                        break;
                    }
                }
                if (close == tokens.size()) {
                    continue;
                }
                const auto argument = sql.substr(tokens[i + 1].end, tokens[close].begin - tokens[i + 1].end);
                out.append(sql.substr(copied, tokens[first].begin - copied));
                out.append("((").append(argument).append(") IN (").append(visible_relations).append("))");
                copied = tokens[close].end;
                i = close;
                continue;
            }
            out.append(sql.substr(copied, tokens[first].begin - copied));
            out.append(std::string{federation::local_database} + "." + std::string{host_function_schema} + ".");
            out.append(sql.substr(token.begin, token.end - token.begin));
            copied = token.end;
        }
        out.append(sql.substr(copied));
        return out;
    }
} // namespace otterstax::catalog
