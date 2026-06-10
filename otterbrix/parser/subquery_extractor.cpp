// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "subquery_extractor.hpp"

#include <components/sql/parser/nodes/parsenodes.h>
#include <components/sql/parser/nodes/primnodes.h>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_std_list.h>

#include <algorithm>
#include <cctype>
#include <regex>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace otterstax::parser {
    namespace {
        bool is_ident_char(char c) {
            unsigned char uc = static_cast<unsigned char>(c);
            return std::isalnum(uc) || c == '_';
        }

        // given an interior position, walk the source forward while tracking
        // quotes and paren nesting, return the (open, close) positions of
        // the OUTERMOST `(...)` pair that contains `interior`. The AST guarantees
        // we're looking at a real RangeSubselect
        std::pair<int, int> find_outermost_containing_paren(std::string_view sql, int interior) {
            std::pair<int, int> best{-1, -1};
            std::vector<int> stack;
            bool in_single = false;
            bool in_double = false;
            size_t i = 0;
            while (i < sql.size()) {
                char c = sql[i];
                if (in_single) {
                    if (c == '\'') {
                        if (i + 1 < sql.size() && sql[i + 1] == '\'') {
                            i += 2;
                            continue;
                        }
                        in_single = false;
                    }
                    ++i;
                    continue;
                }
                if (in_double) {
                    if (c == '"') {
                        in_double = false;
                    }
                    ++i;
                    continue;
                }
                if (c == '\'') {
                    in_single = true;
                    ++i;
                    continue;
                }
                if (c == '"') {
                    in_double = true;
                    ++i;
                    continue;
                }
                if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
                    auto nl = sql.find('\n', i + 2);
                    i = (nl == std::string_view::npos) ? sql.size() : nl + 1;
                    continue;
                }
                if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
                    auto end = sql.find("*/", i + 2);
                    i = (end == std::string_view::npos) ? sql.size() : end + 2;
                    continue;
                }
                if (c == '(') {
                    stack.push_back(static_cast<int>(i));
                } else if (c == ')') {
                    if (!stack.empty()) {
                        int open_pos = stack.back();
                        stack.pop_back();
                        int close_pos = static_cast<int>(i);
                        if (open_pos <= interior && interior <= close_pos) {
                            // Keep the outermost: smallest open position.
                            if (best.first < 0 || open_pos < best.first) {
                                best = {open_pos, close_pos};
                            }
                        }
                    }
                }
                ++i;
            }
            return best;
        }

        std::string trim(std::string s) {
            auto first = s.find_first_not_of(" \t\n\r");
            auto last = s.find_last_not_of(" \t\n\r");
            if (first == std::string::npos) {
                return {};
            }
            return s.substr(first, last - first + 1);
        }

        std::string first_segment(RangeVar* rv) {
            return (rv->uid && rv->uid[0] != '\0') ? std::string{rv->uid} : std::string{};
        }

        // Generic RangeVar walk shared by collect_qualifiers_in_subtree and
        // collect_qualified_names. DropStmt names are NOT RangeVars (string
        // lists) — they are handled by register_drop_stmt_names.
        //
        // NOTE: promote_three_part_qualifiers keeps its own select-only walk
        // on purpose — promoting 3-part DML relations (`db.schema.tbl`) to
        // `uid=db` would misclassify plain DML targets as federated.
        template<typename F>
        void for_each_range_var(Node* node, F&& fn) {
            if (!node) {
                return;
            }
            switch (nodeTag(node)) {
                case T_SelectStmt: {
                    auto* stmt = reinterpret_cast<SelectStmt*>(node);
                    if (stmt->fromClause) {
                        for (auto& cell : stmt->fromClause->lst) {
                            for_each_range_var(reinterpret_cast<Node*>(cell.data), fn);
                        }
                    }
                    for_each_range_var(reinterpret_cast<Node*>(stmt->larg), fn);
                    for_each_range_var(reinterpret_cast<Node*>(stmt->rarg), fn);
                    break;
                }
                case T_JoinExpr: {
                    auto* j = reinterpret_cast<JoinExpr*>(node);
                    for_each_range_var(reinterpret_cast<Node*>(j->larg), fn);
                    for_each_range_var(reinterpret_cast<Node*>(j->rarg), fn);
                    break;
                }
                case T_RangeSubselect: {
                    auto* rs = reinterpret_cast<RangeSubselect*>(node);
                    for_each_range_var(reinterpret_cast<Node*>(rs->subquery), fn);
                    break;
                }
                case T_InsertStmt: {
                    auto* stmt = reinterpret_cast<InsertStmt*>(node);
                    for_each_range_var(reinterpret_cast<Node*>(stmt->relation), fn);
                    for_each_range_var(stmt->selectStmt, fn);
                    break;
                }
                case T_UpdateStmt: {
                    auto* stmt = reinterpret_cast<UpdateStmt*>(node);
                    for_each_range_var(reinterpret_cast<Node*>(stmt->relation), fn);
                    if (stmt->fromClause) {
                        for (auto& cell : stmt->fromClause->lst) {
                            for_each_range_var(reinterpret_cast<Node*>(cell.data), fn);
                        }
                    }
                    break;
                }
                case T_DeleteStmt: {
                    auto* stmt = reinterpret_cast<DeleteStmt*>(node);
                    for_each_range_var(reinterpret_cast<Node*>(stmt->relation), fn);
                    if (stmt->usingClause) {
                        for (auto& cell : stmt->usingClause->lst) {
                            for_each_range_var(reinterpret_cast<Node*>(cell.data), fn);
                        }
                    }
                    break;
                }
                case T_CreateStmt: {
                    auto* stmt = reinterpret_cast<CreateStmt*>(node);
                    for_each_range_var(reinterpret_cast<Node*>(stmt->relation), fn);
                    break;
                }
                case T_IndexStmt: {
                    auto* stmt = reinterpret_cast<IndexStmt*>(node);
                    for_each_range_var(reinterpret_cast<Node*>(stmt->relation), fn);
                    break;
                }
                case T_RangeVar:
                    fn(reinterpret_cast<RangeVar*>(node));
                    break;
                default:
                    break;
            }
        }

        RangeVar* find_first_qualified_range_var(Node* node) {
            if (!node) {
                return nullptr;
            }
            switch (nodeTag(node)) {
                case T_RangeVar: {
                    auto* rv = reinterpret_cast<RangeVar*>(node);
                    return first_segment(rv).empty() ? nullptr : rv;
                }
                case T_SelectStmt: {
                    auto* stmt = reinterpret_cast<SelectStmt*>(node);
                    if (stmt->fromClause) {
                        for (auto& cell : stmt->fromClause->lst) {
                            if (auto* rv = find_first_qualified_range_var(reinterpret_cast<Node*>(cell.data))) {
                                return rv;
                            }
                        }
                    }
                    if (stmt->larg) {
                        if (auto* rv = find_first_qualified_range_var(reinterpret_cast<Node*>(stmt->larg))) {
                            return rv;
                        }
                    }
                    if (stmt->rarg) {
                        return find_first_qualified_range_var(reinterpret_cast<Node*>(stmt->rarg));
                    }
                    return nullptr;
                }
                case T_JoinExpr: {
                    auto* j = reinterpret_cast<JoinExpr*>(node);
                    if (auto* rv = find_first_qualified_range_var(reinterpret_cast<Node*>(j->larg))) {
                        return rv;
                    }
                    return find_first_qualified_range_var(reinterpret_cast<Node*>(j->rarg));
                }
                case T_RangeSubselect: {
                    auto* rs = reinterpret_cast<RangeSubselect*>(node);
                    return find_first_qualified_range_var(reinterpret_cast<Node*>(rs->subquery));
                }
                default:
                    return nullptr;
            }
        }

        struct subquery_location_t {
            int paren_open;
            int paren_close;
            std::string source_uid;
            bool inside_join = false;
            Node* select_stmt = nullptr; // RangeSubselect's inner SelectStmt — for qualifier walk
        };

        std::string cstr_or_empty(const char* s) { return (s && s[0] != '\0') ? s : ""; }

        void collect_qualifiers_in_subtree(Node* node,
                                           std::string_view sql,
                                           int offset_base,
                                           std::vector<qualifier_rewrite_t>& out) {
            for_each_range_var(node, [sql, offset_base, &out](RangeVar* rv) {
                if (rv->location < 0 || !rv->relname || rv->relname[0] == '\0') {
                    return;
                }
                // local names don't need rewriting.
                if (!rv->uid || rv->uid[0] == '\0') {
                    return;
                }

                qualifier_rewrite_t q;
                q.start = rv->location - offset_base;
                int abs_end = rv->location;
                while (abs_end < static_cast<int>(sql.size()) &&
                       (std::isalnum(static_cast<unsigned char>(sql[abs_end])) || sql[abs_end] == '.' ||
                        sql[abs_end] == '_')) {
                    ++abs_end;
                }
                q.length = abs_end - rv->location;

                q.name = qualified_name_t(cstr_or_empty(rv->uid),
                                          cstr_or_empty(rv->catalogname),
                                          cstr_or_empty(rv->schemaname),
                                          cstr_or_empty(rv->relname));
                if (q.start >= 0 && q.length > 0) {
                    out.push_back(std::move(q));
                }
            });
        }

        void collect_subqueries(Node* node,
                                std::string_view sql,
                                std::vector<subquery_location_t>& out,
                                bool inside_join = false) {
            if (!node) {
                return;
            }
            switch (nodeTag(node)) {
                case T_SelectStmt: {
                    auto* stmt = reinterpret_cast<SelectStmt*>(node);
                    if (stmt->fromClause) {
                        for (auto& cell : stmt->fromClause->lst) {
                            collect_subqueries(reinterpret_cast<Node*>(cell.data), sql, out, inside_join);
                        }
                    }
                    if (stmt->larg) {
                        collect_subqueries(reinterpret_cast<Node*>(stmt->larg), sql, out, inside_join);
                    }
                    if (stmt->rarg) {
                        collect_subqueries(reinterpret_cast<Node*>(stmt->rarg), sql, out, inside_join);
                    }
                    break;
                }
                case T_JoinExpr: {
                    auto* j = reinterpret_cast<JoinExpr*>(node);
                    collect_subqueries(reinterpret_cast<Node*>(j->larg), sql, out, /*inside_join=*/true);
                    collect_subqueries(reinterpret_cast<Node*>(j->rarg), sql, out, /*inside_join=*/true);
                    break;
                }
                case T_RangeSubselect: {
                    auto* rs = reinterpret_cast<RangeSubselect*>(node);
                    RangeVar* inside = find_first_qualified_range_var(reinterpret_cast<Node*>(rs->subquery));
                    if (!inside || inside->location < 0) {
                        break;
                    }
                    auto [open_pos, close_pos] = find_outermost_containing_paren(sql, inside->location);
                    if (open_pos < 0) {
                        break;
                    }
                    subquery_location_t info;
                    info.paren_open = open_pos;
                    info.paren_close = close_pos;
                    info.source_uid = first_segment(inside);
                    info.inside_join = inside_join;
                    info.select_stmt = reinterpret_cast<Node*>(rs->subquery);
                    out.push_back(std::move(info));
                    break;
                }
                default:
                    break;
            }
        }

        extraction_result_t rewrite_with_stubs(std::string_view sql, std::vector<subquery_location_t> subs) {
            extraction_result_t result;
            // left-to-right so stub_ids match the SQL reading order.
            std::sort(subs.begin(), subs.end(), [](const auto& a, const auto& b) {
                return a.paren_open < b.paren_open;
            });

            std::string out;
            out.reserve(sql.size() + subs.size() * 64);
            int last = 0;
            int counter = 0;
            for (const auto& sub : subs) {
                out.append(sql.substr(last, sub.paren_open - last));

                std::string stub_id{k_stub_prefix};
                stub_id += std::to_string(counter++);

                std::string body = std::string(sql.substr(sub.paren_open + 1, sub.paren_close - sub.paren_open - 1));

                subquery_stub_t stub;
                stub.stub_id = stub_id;
                stub.source_uid = sub.source_uid;
                stub.raw_sql = std::move(body);
                collect_qualifiers_in_subtree(sub.select_stmt, sql, sub.paren_open + 1, stub.qualifiers);

                // if in JOIN - plain stub
                // not in JOIN - wrap in (SELECT * FROM ...) to keep WHERE / LIMIT clause to outer
                if (sub.inside_join) {
                    out.append(sub.source_uid);
                    out.append(".subq.subq.");
                    out.append(stub_id);
                } else {
                    out.append("(SELECT * FROM ");
                    out.append(sub.source_uid);
                    out.append(".subq.subq.");
                    out.append(stub_id);
                    out.append(")");
                }

                result.stubs.push_back(std::move(stub));
                last = sub.paren_close + 1;
            }
            out.append(sql.substr(last));
            result.modified_sql = std::move(out);
            return result;
        }
    } // namespace

    void promote_three_part_qualifiers(::Node* node) {
        if (!node) {
            return;
        }
        switch (nodeTag(node)) {
            case T_SelectStmt: {
                auto* stmt = reinterpret_cast<SelectStmt*>(node);
                if (stmt->fromClause) {
                    for (auto& cell : stmt->fromClause->lst) {
                        promote_three_part_qualifiers(reinterpret_cast<::Node*>(cell.data));
                    }
                }
                if (stmt->larg) {
                    promote_three_part_qualifiers(reinterpret_cast<::Node*>(stmt->larg));
                }
                if (stmt->rarg) {
                    promote_three_part_qualifiers(reinterpret_cast<::Node*>(stmt->rarg));
                }
                break;
            }
            case T_JoinExpr: {
                auto* j = reinterpret_cast<JoinExpr*>(node);
                promote_three_part_qualifiers(reinterpret_cast<::Node*>(j->larg));
                promote_three_part_qualifiers(reinterpret_cast<::Node*>(j->rarg));
                break;
            }
            case T_RangeSubselect: {
                auto* rs = reinterpret_cast<RangeSubselect*>(node);
                promote_three_part_qualifiers(reinterpret_cast<::Node*>(rs->subquery));
                break;
            }
            case T_RangeVar: {
                auto* rv = reinterpret_cast<RangeVar*>(node);
                const bool has_uid = rv->uid && rv->uid[0] != '\0';
                const bool has_catalog = rv->catalogname && rv->catalogname[0] != '\0';
                const bool has_schema = rv->schemaname && rv->schemaname[0] != '\0';
                const bool has_rel = rv->relname && rv->relname[0] != '\0';
                if (!has_uid && has_catalog && has_schema && has_rel) {
                    // 3-part `<a>.<b>.<c>` → uid=a, catalog=b, schema=b (dup), rel=c.
                    rv->uid = rv->catalogname;
                    rv->catalogname = rv->schemaname;
                }
                break;
            }
            default:
                break;
        }
    }

    namespace {
        // DROP statements carry name lists (List of String values), not
        // RangeVars, so the generic walker cannot see them. The part-splitting
        // below must mirror the engine's transform_drop so the registry key
        // (db, rel) matches the (dbname, relname) the transformer stamps on
        // the catalog_resolve_table_t sibling. Only the first object is
        // registered — transform_drop consumes only objects.front() too.
        // removeTypes other than TABLE/INDEX transform into node types never
        // classified external, so they need no registry entries.
        void register_drop_stmt_names(DropStmt* stmt, otterstax::names::name_registry_t& out) {
            if (!stmt || !stmt->objects || stmt->objects->lst.empty()) {
                return;
            }
            auto* name_list = reinterpret_cast<List*>(stmt->objects->lst.front().data);
            if (!name_list) {
                return;
            }
            std::vector<std::string> parts;
            for (auto& cell : name_list->lst) {
                const char* s = strVal(cell.data);
                parts.emplace_back(s ? s : "");
            }
            switch (stmt->removeType) {
                case OBJECT_TABLE:
                    // rel | db.rel | db.schema.rel | uid.db.schema.rel
                    switch (parts.size()) {
                        case 1:
                            out.add(qualified_name_t("", "", "", parts[0]));
                            break;
                        case 2:
                            out.add(qualified_name_t("", parts[0], "", parts[1]));
                            break;
                        case 3:
                            out.add(qualified_name_t("", parts[0], parts[1], parts[2]));
                            break;
                        case 4:
                            out.add(qualified_name_t(parts[0], parts[1], parts[2], parts[3]));
                            break;
                        default:
                            // transform_drop rejects other arities with a
                            // parse error before resolution runs.
                            break;
                    }
                    break;
                case OBJECT_INDEX:
                    // Trailing part is the index name; the leading parts are
                    // the parent table: db.rel.idx | db.schema.rel.idx |
                    // uid.db.schema.rel.idx (transform_drop has no 1-part
                    // table form for indexes). Both the table AND the index
                    // get entries: the wrapping sequence resolves the table
                    // first and the index second, and each lookup must recover
                    // the full name (the uid in particular).
                    switch (parts.size()) {
                        case 3:
                            out.add(qualified_name_t("", parts[0], "", parts[1]));
                            out.add(qualified_name_t("", parts[0], "", parts[2]));
                            break;
                        case 4:
                            out.add(qualified_name_t("", parts[0], parts[1], parts[2]));
                            out.add(qualified_name_t("", parts[0], parts[1], parts[3]));
                            break;
                        case 5:
                            out.add(qualified_name_t(parts[0], parts[1], parts[2], parts[3]));
                            out.add(qualified_name_t(parts[0], parts[1], parts[2], parts[4]));
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        }
    } // namespace

    void collect_qualified_names(::Node* root, otterstax::names::name_registry_t& out) {
        if (root && nodeTag(root) == T_DropStmt) {
            register_drop_stmt_names(reinterpret_cast<DropStmt*>(root), out);
            return;
        }
        for_each_range_var(root, [&out](RangeVar* rv) {
            if (!rv->relname || rv->relname[0] == '\0') {
                return;
            }
            // Register EVERY RangeVar — including local (uid-less) ones — so
            // a registry hit with an empty unique_identifier marks a LOCAL
            // (otterbrix) table, while a miss is a real resolution error.
            out.add(qualified_name_t(cstr_or_empty(rv->uid),
                                     cstr_or_empty(rv->catalogname),
                                     cstr_or_empty(rv->schemaname),
                                     cstr_or_empty(rv->relname)));
        });
    }

    extraction_result_t
    prepare_sql(std::string_view sql, std::pmr::memory_resource* arena, ::Node** out_root_if_unmodified) {
        if (out_root_if_unmodified) {
            *out_root_if_unmodified = nullptr;
        }

        ::List* raw = nullptr;
        try {
            raw = raw_parser(arena, std::string(sql).c_str());
        } catch (...) {
            return {std::string{sql}, {}};
        }

        if (!raw) {
            return {std::string{sql}, {}};
        }

        auto* root = reinterpret_cast<Node*>(linitial(raw));
        if (!root) {
            return {std::string{sql}, {}};
        }

        promote_three_part_qualifiers(root);
        std::vector<subquery_location_t> subs;
        collect_subqueries(root, sql, subs);
        subs.erase(std::remove_if(subs.begin(), subs.end(), [](const auto& s) { return s.source_uid.empty(); }),
                   subs.end());

        if (!subs.empty()) {
            return rewrite_with_stubs(sql, std::move(subs));
        }

        if (out_root_if_unmodified) {
            *out_root_if_unmodified = root;
        }
        return {std::string{sql}, {}};
    }

} // namespace otterstax::parser
