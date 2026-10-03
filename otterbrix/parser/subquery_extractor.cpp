// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "subquery_extractor.hpp"

#include "utility/tracy_profiler.hpp"

#include <components/sql/parser/nodes/parsenodes.h>
#include <components/sql/parser/nodes/primnodes.h>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_std_list.h>

#include <algorithm>
#include <cctype>
#include <cstring>
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
        std::pair<int, int>
        find_outermost_containing_paren(std::pmr::memory_resource* resource, std::string_view sql, int interior) {
            std::pair<int, int> best{-1, -1};
            std::pmr::vector<int> stack{resource};
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

        // A view into the raw parse tree: valid as long as the arena it was
        // parsed into.
        std::string_view first_segment(RangeVar* rv) {
            return (rv->uid && rv->uid[0] != '\0') ? std::string_view{rv->uid} : std::string_view{};
        }

        // Generic RangeVar walk shared by canonicalize_names,
        // collect_qualifiers_in_subtree and collect_qualified_names. DropStmt
        // names are NOT RangeVars (string lists) — they are handled by
        // register_drop_stmt_names.
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
            // Points into the raw parse tree (first_segment); rewrite_with_stubs
            // copies it into the stub while the arena is still alive.
            std::string_view source_uid;
            bool inside_join = false;
            Node* select_stmt = nullptr; // RangeSubselect's inner SelectStmt — for qualifier walk
        };

        const char* cstr_or_empty(const char* s) { return (s && s[0] != '\0') ? s : ""; }

        char* arena_c_str(std::pmr::memory_resource* arena, std::string_view text) {
            if (text.empty()) {
                return nullptr;
            }
            auto* copy = static_cast<char*>(arena->allocate(text.size() + 1, alignof(char)));
            std::memcpy(copy, text.data(), text.size());
            copy[text.size()] = '\0';
            return copy;
        }

        void collect_qualifiers_in_subtree(Node* node,
                                           std::string_view sql,
                                           int offset_base,
                                           std::pmr::vector<qualifier_rewrite_t>& out) {
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
                                std::pmr::vector<subquery_location_t>& out,
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
                    auto [open_pos, close_pos] =
                        find_outermost_containing_paren(out.get_allocator().resource(), sql, inside->location);
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

        extraction_result_t rewrite_with_stubs(std::pmr::memory_resource* resource,
                                               std::string_view sql,
                                               std::pmr::vector<subquery_location_t> subs) {
            extraction_result_t result{std::pmr::string{resource}, std::pmr::vector<subquery_stub_t>{resource}};
            // left-to-right so stub_ids match the SQL reading order.
            std::sort(subs.begin(), subs.end(), [](const auto& a, const auto& b) {
                return a.paren_open < b.paren_open;
            });

            std::pmr::string out{resource};
            out.reserve(sql.size() + subs.size() * 64);
            // No reallocation: every stub stays where it was built, on `resource`.
            result.stubs.reserve(subs.size());
            int last = 0;
            int counter = 0;
            for (const auto& sub : subs) {
                out.append(sql.substr(last, sub.paren_open - last));

                std::pmr::string stub_id{k_stub_prefix, resource};
                stub_id += std::to_string(counter++);

                const std::string_view body = sql.substr(sub.paren_open + 1, sub.paren_close - sub.paren_open - 1);

                subquery_stub_t stub{std::pmr::string{stub_id, resource},
                                     std::pmr::string{sub.source_uid, resource},
                                     std::pmr::string{body, resource},
                                     std::pmr::vector<qualifier_rewrite_t>{resource}};
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

    core::error_t canonicalize_names(std::pmr::memory_resource* arena,
                                     std::pmr::memory_resource* resource,
                                     const otterstax::names::alias_registry_t& aliases,
                                     ::Node* root) {
        OTX_ZONE_N("parser::canonicalize_names");
        core::error_t refused = core::error_t::no_error();
        for_each_range_var(root, [&](RangeVar* rv) {
            if (refused.contains_error() || !rv->relname || rv->relname[0] == '\0') {
                return;
            }
            auto canonical = otterstax::names::canonical_table_name(resource,
                                                                    aliases,
                                                                    qualified_name_t(cstr_or_empty(rv->uid),
                                                                                     cstr_or_empty(rv->catalogname),
                                                                                     cstr_or_empty(rv->schemaname),
                                                                                     cstr_or_empty(rv->relname)));
            if (canonical.has_error()) {
                refused = core::error_t{canonical.error().type, std::pmr::string{canonical.error().what, resource}};
                return;
            }
            const auto& name = canonical.value();
            if (name.unique_identifier.empty()) {
                return;
            }
            rv->uid = arena_c_str(arena, name.unique_identifier);
            rv->catalogname = arena_c_str(arena, name.database);
            rv->schemaname = arena_c_str(arena, name.schema);
        });
        return refused;
    }

    namespace {
        // DROP statements carry name lists (List of String values), not
        // RangeVars, so the generic walker cannot see them. The part-splitting
        // below must mirror the engine's transform_drop so the registry key
        // (db, rel) matches the (dbname, relname) the transformer stamps on
        // the catalog_resolve (kind==table) sibling. A DROP with several
        // objects is rejected by parse() before resolution, so only the
        // single object is registered. removeTypes other than TABLE/INDEX
        // transform into drop kinds never classified external, so they need
        // no registry entries.
        core::error_t register_drop_stmt_names(std::pmr::memory_resource* resource,
                                               const otterstax::names::alias_registry_t& aliases,
                                               DropStmt* stmt,
                                               otterstax::names::name_registry_t& out) {
            if (!stmt || !stmt->objects || stmt->objects->lst.empty()) {
                return core::error_t::no_error();
            }
            auto* name_list = reinterpret_cast<List*>(stmt->objects->lst.front().data);
            if (!name_list) {
                return core::error_t::no_error();
            }
            // Pointers into the raw parse tree; qualified_name_t copies them.
            std::pmr::vector<const char*> parts{resource};
            for (auto& cell : name_list->lst) {
                const char* s = strVal(cell.data);
                parts.emplace_back(s ? s : "");
            }
            // The table the DROP names, read against the aliases like any
            // RangeVar, under the (dbname, relname) transform_drop stamps for
            // this arity — a three-segment federated name keeps the alias as
            // its dbname there. A DROP INDEX registers its index beside it:
            // the wrapping sequence resolves the table first and the index
            // second, and each lookup must recover the full name (the uid in
            // particular).
            auto register_target = [&](const qualified_name_t& written,
                                       const char* key_db,
                                       const char* key_rel,
                                       const char* index) -> core::error_t {
                auto canonical = otterstax::names::canonical_table_name(resource, aliases, written);
                if (canonical.has_error()) {
                    return core::error_t{canonical.error().type, std::pmr::string{canonical.error().what, resource}};
                }
                if (index != nullptr) {
                    auto index_name = canonical.value();
                    index_name.collection = index;
                    out.add(key_db, index, std::move(index_name));
                }
                out.add(key_db, key_rel, std::move(canonical.value()));
                return core::error_t::no_error();
            };
            switch (stmt->removeType) {
                case OBJECT_TABLE:
                    // rel | db.rel | db.schema.rel | uid.db.schema.rel
                    switch (parts.size()) {
                        case 1:
                            return register_target(qualified_name_t("", "", "", parts[0]), "", parts[0], nullptr);
                        case 2:
                            return register_target(qualified_name_t("", parts[0], "", parts[1]),
                                                   parts[0],
                                                   parts[1],
                                                   nullptr);
                        case 3:
                            return register_target(qualified_name_t("", parts[0], parts[1], parts[2]),
                                                   parts[0],
                                                   parts[2],
                                                   nullptr);
                        case 4:
                            return register_target(qualified_name_t(parts[0], parts[1], parts[2], parts[3]),
                                                   parts[1],
                                                   parts[3],
                                                   nullptr);
                        default:
                            // transform_drop rejects other arities with a
                            // parse error before resolution runs.
                            return core::error_t::no_error();
                    }
                case OBJECT_INDEX:
                    // Trailing part is the index name; the leading parts are
                    // the parent table: db.rel.idx | db.schema.rel.idx |
                    // uid.db.schema.rel.idx (transform_drop has no 1-part
                    // table form for indexes).
                    switch (parts.size()) {
                        case 3:
                            return register_target(qualified_name_t("", parts[0], "", parts[1]),
                                                   parts[0],
                                                   parts[1],
                                                   parts[2]);
                        case 4:
                            return register_target(qualified_name_t("", parts[0], parts[1], parts[2]),
                                                   parts[0],
                                                   parts[2],
                                                   parts[3]);
                        case 5:
                            return register_target(qualified_name_t(parts[0], parts[1], parts[2], parts[3]),
                                                   parts[1],
                                                   parts[3],
                                                   parts[4]);
                        default:
                            return core::error_t::no_error();
                    }
                default:
                    return core::error_t::no_error();
            }
        }
    } // namespace

    core::error_t collect_qualified_names(std::pmr::memory_resource* resource,
                                          const otterstax::names::alias_registry_t& aliases,
                                          ::Node* root,
                                          otterstax::names::name_registry_t& out) {
        OTX_ZONE_N("parser::collect_qualified_names");
        if (root && nodeTag(root) == T_DropStmt) {
            return register_drop_stmt_names(resource, aliases, reinterpret_cast<DropStmt*>(root), out);
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
        return core::error_t::no_error();
    }

    core::result_wrapper_t<extraction_result_t> prepare_sql(std::string_view sql,
                                                            const otterstax::names::alias_registry_t& aliases,
                                                            std::pmr::memory_resource* arena,
                                                            std::pmr::memory_resource* resource,
                                                            ::Node** out_root_if_unmodified) {
        OTX_ZONE_N("parser::prepare_sql");
        if (out_root_if_unmodified) {
            *out_root_if_unmodified = nullptr;
        }
        auto unmodified = [&] {
            return extraction_result_t{std::pmr::string{sql, resource}, std::pmr::vector<subquery_stub_t>{resource}};
        };

        ::List* raw = nullptr;
        try {
            raw = raw_parser(arena, std::pmr::string{sql, resource}.c_str());
        } catch (...) {
            return unmodified();
        }

        // Extraction only makes sense for exactly one statement: linitial on
        // an empty list is undefined behaviour, and a multi-statement input is
        // left untouched for parse() to reject.
        if (!raw || list_length(raw) != 1) {
            return unmodified();
        }

        auto* root = reinterpret_cast<Node*>(linitial(raw));
        if (!root) {
            return unmodified();
        }

        if (auto refused = canonicalize_names(arena, resource, aliases, root); refused.contains_error()) {
            return refused;
        }
        std::pmr::vector<subquery_location_t> subs{resource};
        collect_subqueries(root, sql, subs);
        subs.erase(std::remove_if(subs.begin(), subs.end(), [](const auto& s) { return s.source_uid.empty(); }),
                   subs.end());

        if (!subs.empty()) {
            return rewrite_with_stubs(resource, sql, std::move(subs));
        }

        if (out_root_if_unmodified) {
            *out_root_if_unmodified = root;
        }
        return unmodified();
    }

} // namespace otterstax::parser
