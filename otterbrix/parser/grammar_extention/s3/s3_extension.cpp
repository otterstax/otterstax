// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
#include "s3_extension.hpp"

#include "../external_node.hpp"
#include "s3_ast.hpp"
#include "s3_gram.hpp"
#include "s3_scan.h"

#include <cassert>
#include <cctype>
#include <cstring>
#include <string>
#include <string_view>

#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/flex_scanner_guard.hpp>
#include <components/sql/parser/nodes/parsenodes.h>
#include <components/sql/parser/pg_std_list.h>
#include <components/types/logical_value.hpp>

namespace {
    // s3's scanner: the shared macro binds the s3yy entry points into the RAII guard type
    using s3_scanner = EXTENSION_FLEX_SCANNER(s3yy);

    std::size_t skip_ws(const std::string& q, std::size_t i) {
        while (i < q.size() && std::isspace(static_cast<unsigned char>(q[i]))) {
            ++i;
        }
        return i;
    }

    // Case-insensitive: does q (from `i`, skipping leading ws) begin with the
    // whole word `kw`? On success advances `i` past the keyword.
    bool match_word_ci(const std::string& q, std::size_t& i, const char* kw) {
        std::size_t j = skip_ws(q, i);
        std::size_t k = 0;
        while (kw[k] != '\0') {
            if (j >= q.size() || std::tolower(static_cast<unsigned char>(q[j])) != kw[k]) {
                return false;
            }
            ++j;
            ++k;
        }
        if (j < q.size() && (std::isalnum(static_cast<unsigned char>(q[j])) || q[j] == '_')) {
            return false; // keyword only prefixes a longer identifier
        }
        i = j;
        return true;
    }

    // From `from`, skipping ws, expect '(' and find its matching ')', honouring
    // single-quoted strings (and the '' escape) so a ')' inside a literal does
    // not close the group. Sets open/close to the outer paren indices.
    bool find_balanced_parens(const std::string& q, std::size_t from, std::size_t& open, std::size_t& close) {
        std::size_t i = skip_ws(q, from);
        if (i >= q.size() || q[i] != '(') {
            return false;
        }
        open = i;
        int depth = 0;
        bool in_str = false;
        for (std::size_t j = i; j < q.size(); ++j) {
            char c = q[j];
            if (in_str) {
                if (c == '\'') {
                    if (j + 1 < q.size() && q[j + 1] == '\'') {
                        ++j; // skip escaped ''
                    } else {
                        in_str = false;
                    }
                }
            } else if (c == '\'') {
                in_str = true;
            } else if (c == '(') {
                ++depth;
            } else if (c == ')') {
                if (--depth == 0) {
                    close = j;
                    return true;
                }
            }
        }
        return false;
    }

    std::string trim(const std::string& s) {
        std::size_t b = 0;
        std::size_t e = s.size();
        while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) {
            ++b;
        }
        while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) {
            --e;
        }
        return s.substr(b, e - b);
    }

    bool is_s3_uri(const char* s) { return s != nullptr && std::strncmp(s, "s3://", 5) == 0; }
} // namespace

namespace s3_ext {
    using namespace components::sql::parser;

    parse_extension_result_t parse(std::pmr::memory_resource* resource, const std::string& query) {
        // Cheap claim check: only CREATE EXTERNAL TABLE / COPY are ours. The
        // core parser already ran and rejected this query, so we never see a
        // statement the core grammar accepts.
        std::string inner_sql;            // captured embedded SELECT for COPY
        std::string scrubbed = query;     // what the grammar actually parses

        std::size_t pos = 0;
        bool is_create = match_word_ci(query, pos, "create") && match_word_ci(query, pos, "external") &&
                         match_word_ci(query, pos, "table");
        bool is_copy = false;
        if (!is_create) {
            pos = 0;
            is_copy = match_word_ci(query, pos, "copy");
        }
        if (!is_create && !is_copy) {
            return NIL; // not ours
        }

        if (is_copy) {
            // Strip the embedded "( <select> )" so the grammar sees a flat
            // "COPY TO '<loc>' WITH (...)". Re-attached onto the AST below.
            std::size_t open = 0;
            std::size_t close = 0;
            if (!find_balanced_parens(query, pos, open, close)) {
                return NIL; // COPY but not our parenthesised-query form
            }
            inner_sql = trim(query.substr(open + 1, close - open - 1));
            scrubbed = query.substr(0, open) + " " + query.substr(close + 1);
        }

        s3_scanner scanner(scrubbed.c_str());
        if (!scanner.valid()) {
            return NIL; // scanner failed — not ours
        }
        s3yyset_extra(resource, scanner.handle());

        Node* root = nullptr;
        if (s3yyparse(scanner.handle(), resource, &root) != 0 || root == nullptr) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"s3: malformed CREATE EXTERNAL TABLE or COPY statement", resource});
        }

        auto* ext = reinterpret_cast<ExtensionNode*>(root);
        auto* stmt = static_cast<s3_stmt*>(ext->data);

        // Resolve the well-known options. For COPY the location is the TO target
        // (already on the stmt); for CREATE it lives in the WITH list.
        if (stmt->kind == s3_stmt_kind::create_external_table) {
            stmt->location = option_value(stmt->options, "location");
        }
        stmt->s3_alias = option_value(stmt->options, "s3_alias");
        stmt->format = option_value(stmt->options, "format");

        // Claim only s3:// locations — local paths belong to the `file` extension.
        if (!is_s3_uri(stmt->location)) {
            return NIL;
        }

        if (stmt->kind == s3_stmt_kind::copy_to && !inner_sql.empty()) {
            stmt->inner_sql = arena_strdup(resource, inner_sql.c_str(), inner_sql.size());
        }

        return list_make1(resource, root);
    }

    components::logical_plan::node_ptr transform(std::pmr::memory_resource* resource,
                                                 ExtensionNode* node,
                                                 components::logical_plan::parameter_node_t* /*params*/) {
        // The transformer routes by extension_id, so this only ever runs for our nodes.
        assert(node->extension_id != nullptr && std::string_view(node->extension_id) == "s3");

        // Lower the parsed statement into an external_node_t the Scheduler routes
        // to the S3 manager. nullptr option fields collapse to empty strings.
        const auto* stmt = static_cast<const s3_stmt*>(node->data);
        const auto str = [](const char* s) { return s ? std::string{s} : std::string{}; };
        const auto op = stmt->kind == s3_stmt_kind::create_external_table
                            ? otterstax::external::external_op_t::create_external_table
                            : otterstax::external::external_op_t::copy_to;
        return otterstax::external::make_external_node(resource,
                                                       op,
                                                       str(stmt->db),
                                                       str(stmt->table),
                                                       str(stmt->location),
                                                       str(stmt->s3_alias),
                                                       str(stmt->format),
                                                       str(stmt->inner_sql));
    }
} // namespace s3_ext
