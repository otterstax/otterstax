// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// A real engine (runtime::engine_t) opened with the federation's primitives in a temporary
// directory, and fake backends that answer the storage's scans with canned rows. No live backend is
// involved.

#include "catalog/remote_metadata.hpp"
#include "otterbrix/federation/name_resolution.hpp"
#include "otterbrix/federation/provider.hpp"
#include "runtime/engine.hpp"
#include "runtime/runtime.hpp"
#include "tests/runtime/runtime_fixture.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/casts/kernels/string_cast.hpp>
#include <components/cursor/cursor.hpp>
#include <components/session/session.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace fedtest {

    namespace fed = otterstax::federation;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    // A NULL cell of a canned row.
    struct null_t {
        bool operator==(const null_t&) const = default;
    };

    using value_t = std::variant<std::int64_t, std::int32_t, double, std::string, null_t>;
    using row_t = std::vector<value_t>;

    // One remote table of a fake backend: its columns (named in the backend's spelling) and rows.
    struct canned_table_t {
        std::vector<complex_logical_type> columns;
        std::vector<row_t> rows;
        // A backend that refuses the scan with this error.
        std::optional<core::error_code_t> fail_code;
        std::string fail_text;
    };

    class fake_backend_t final : public fed::backend_t {
    public:
        explicit fake_backend_t(backend_type_t kind)
            : kind_(kind) {}

        backend_type_t kind() const noexcept { return kind_; }

        void set_table(const std::string& remote_table, canned_table_t table) {
            std::lock_guard guard(mutex_);
            tables_[remote_table] = std::move(table);
        }

        // Answer from a thread of the backend's own, once `scans_before_answer` scans were asked for
        // (bounded by a 5 s deadline) and `delay` passed: overlapping fetches become observable.
        void answer_asynchronously(int scans_before_answer, std::chrono::milliseconds delay) {
            async_ = true;
            scans_before_answer_ = scans_before_answer;
            delay_ = delay;
        }

        std::vector<std::string> sql_log() const {
            std::lock_guard guard(mutex_);
            return sql_log_;
        }

        // The writes the backend received, in order.
        std::vector<std::string> write_log() const {
            std::lock_guard guard(mutex_);
            return write_log_;
        }

        void clear_logs() {
            std::lock_guard guard(mutex_);
            sql_log_.clear();
            scan_requests_.clear();
            write_log_.clear();
        }

        // A read the hook answers — rows typed as the hook types them, or an error — before the canned
        // tables and the scripts are looked at; nullopt: not the hook's read. Called under the
        // backend's lock with the backend's own resource, which outlives the statement's cursor.
        using scan_hook_t = std::function<std::optional<core::result_wrapper_t<components::vector::data_chunk_t>>(
            const fed::scan_request_t&,
            std::pmr::memory_resource*)>;
        void on_scan(scan_hook_t hook) {
            std::lock_guard guard(mutex_);
            scan_hook_ = std::move(hook);
        }

        // Every read the backend was asked for, in order.
        std::vector<fed::scan_request_t> scan_requests() const {
            std::lock_guard guard(mutex_);
            return scan_requests_;
        }

        // A read whose SQL text is exactly `sql` answers `table` instead of a table's rows (a statement
        // the pushdown rule made).
        void script(const std::string& sql, canned_table_t table) {
            std::lock_guard guard(mutex_);
            scripted_[sql] = std::move(table);
        }

        // Every write fails with `code` from now on (nullopt: writes are applied again).
        void fail_writes(std::optional<core::error_code_t> code) {
            std::lock_guard guard(mutex_);
            write_fail_ = code;
        }

        // Runs `change` on the canned tables once, right before the next write is applied: a row
        // changed on the backend between the engine's scan and its write.
        void before_next_write(std::function<void(std::map<std::string, canned_table_t>&)> change) {
            std::lock_guard guard(mutex_);
            before_next_write_ = std::move(change);
        }

        // The rows of a canned table as the writes left them.
        std::vector<row_t> rows(const std::string& remote_table) const {
            std::lock_guard guard(mutex_);
            auto it = tables_.find(remote_table);
            return it == tables_.end() ? std::vector<row_t>{} : it->second.rows;
        }

        // Applies the statement to the canned table it names (write_evaluator_t) and answers the rows
        // it matched — a ClickHouse mutation answers 0, as the real backend reports nothing for it.
        actor_zeta::unique_future<fed::write_result_t> write(std::pmr::memory_resource* resource,
                                                             fed::write_request_t request) override {
            actor_zeta::promise<fed::write_result_t> promise(resource);
            auto future = promise.get_future();
            std::lock_guard guard(mutex_);
            write_log_.push_back(request.sql);
            if (write_fail_) {
                promise.set_value(fed::write_result_t{
                    core::error_t{*write_fail_, std::pmr::string{"fake backend refuses the write", &resource_}}});
                return future;
            }
            if (before_next_write_) {
                auto change = std::move(before_next_write_);
                before_next_write_ = nullptr;
                change(tables_);
            }
            auto applied = apply_write(tables_, request.sql);
            if (!applied) {
                promise.set_value(fed::write_result_t{core::error_t{
                    core::error_code_t::other_error,
                    std::pmr::string{("fake backend cannot apply: " + request.sql).c_str(), &resource_}}});
                return future;
            }
            const bool mutation = kind_ == backend_type_t::ClickHouse && request.kind != fed::write_kind_t::insert;
            promise.set_value(fed::write_result_t{mutation ? 0 : *applied});
            return future;
        }

        // The federation tests discover nothing through a backend: they write the replies themselves.
        core::result_wrapper_t<fed::discovered_tables_t> discover(const qualified_name_t&) override {
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"fake backend has no discovery", &resource_}};
        }

        core::error_t execute_ddl(const std::string&, const std::string&) override {
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"fake backend runs no DDL", &resource_}};
        }

        int scans() const noexcept { return scans_.load(); }
        int peak_in_flight() const noexcept { return peak_in_flight_.load(); }
        bool answer_deadline_missed() const noexcept { return deadline_missed_.load(); }

        actor_zeta::unique_future<core::error_t> scan(std::pmr::memory_resource* resource,
                                                      fed::scan_request_t request,
                                                      std::shared_ptr<fed::scan_rows_t> rows) override {
            actor_zeta::promise<core::error_t> promise(resource);
            auto future = promise.get_future();
            canned_table_t table;
            {
                std::lock_guard guard(mutex_);
                sql_log_.push_back(request.sql);
                scan_requests_.push_back(request);
                if (scan_hook_) {
                    if (auto hooked = scan_hook_(request, &resource_)) {
                        scans_.fetch_add(1);
                        if (hooked->has_error()) {
                            promise.set_value(core::error_t{hooked->error().type,
                                                            std::pmr::string{hooked->error().what.c_str(), &resource_}});
                        } else {
                            rows->chunks.push_back(std::move(hooked->value()));
                            promise.set_value(core::error_t::no_error());
                        }
                        return future;
                    }
                }
                if (auto scripted = scripted_.find(request.sql); scripted != scripted_.end()) {
                    table = scripted->second;
                } else {
                    const auto remote_table = table_of(request.sql);
                    auto it = tables_.find(remote_table);
                    if (it != tables_.end()) {
                        table = it->second;
                    } else {
                        table.fail_code = core::error_code_t::table_not_exists;
                        table.fail_text = "fake backend has no table " + remote_table;
                    }
                }
            }
            scans_.fetch_add(1);
            const int in_flight = in_flight_.fetch_add(1) + 1;
            int peak = peak_in_flight_.load();
            while (in_flight > peak && !peak_in_flight_.compare_exchange_weak(peak, in_flight)) {
            }

            auto answer = [this, table = std::move(table), rows](actor_zeta::promise<core::error_t> p) mutable {
                core::error_t outcome = core::error_t::no_error();
                if (table.fail_code) {
                    outcome = core::error_t{*table.fail_code, std::pmr::string{table.fail_text.c_str(), &resource_}};
                } else {
                    rows->chunks.push_back(build_chunk(table));
                }
                in_flight_.fetch_sub(1);
                p.set_value(std::move(outcome));
            };
            if (!async_) {
                answer(std::move(promise));
                return future;
            }
            std::thread([this, answer = std::move(answer), p = std::move(promise)]() mutable {
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                while (scans_.load() < scans_before_answer_ && std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                if (scans_.load() < scans_before_answer_) {
                    deadline_missed_.store(true);
                }
                std::this_thread::sleep_for(delay_);
                answer(std::move(p));
            }).detach();
            return future;
        }

    private:
        // The remote table a read names: the last identifier of the reference after FROM, unquoted
        // (the fake tables' names hold no quote, dot or blank).
        static std::string table_of(const std::string& sql) {
            const auto from = sql.find(" FROM ");
            auto reference = from == std::string::npos ? sql : sql.substr(from + 6);
            reference = reference.substr(0, reference.find_first_of(" ;"));
            auto name = reference.substr(reference.rfind('.') + 1);
            if (name.size() >= 2 && (name.front() == '`' || name.front() == '"')) {
                name = name.substr(1, name.size() - 2);
            }
            return name;
        }

        components::vector::data_chunk_t build_chunk(const canned_table_t& table) {
            std::pmr::vector<complex_logical_type> types{&resource_};
            for (const auto& column : table.columns) {
                types.push_back(column);
            }
            const auto count = table.rows.size();
            components::vector::data_chunk_t chunk(&resource_, types, std::max<std::size_t>(count, 1));
            chunk.set_cardinality(count);
            for (std::size_t row = 0; row < count; ++row) {
                for (std::size_t col = 0; col < types.size(); ++col) {
                    std::visit(
                        [&](const auto& value) {
                            using cell_t = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<cell_t, null_t>) {
                                chunk.data[col].set_null(row, true);
                            } else if constexpr (std::is_same_v<cell_t, std::string>) {
                                // A DECIMAL cell is canned as its text (`12.30`).
                                if (types[col].type() == logical_type::DECIMAL) {
                                    const auto* spec =
                                        types[col].template extension_as<components::types::decimal_logical_type_extension>();
                                    components::types::int128_t unscaled = 0;
                                    REQUIRE(components::casts::kernels::detail::parse_decimal(value,
                                                                                              spec->width(),
                                                                                              spec->scale(),
                                                                                              &unscaled));
                                    chunk.set_value(
                                        col,
                                        row,
                                        components::types::logical_value_t::create_decimal(&resource_, types[col], unscaled));
                                } else {
                                    chunk.set_value(col, row, components::types::logical_value_t(&resource_, value));
                                }
                            } else {
                                chunk.set_value(col, row, components::types::logical_value_t(&resource_, value));
                            }
                        },
                        table.rows[row][col]);
                }
            }
            return chunk;
        }

        // ── the writes: a tokenizer and an evaluator for the statements the federation sends ──
        //
        //   INSERT INTO <t> (<c>, ...) VALUES (<v>, ...), ...
        //   UPDATE <t> SET <c> = <v>, ... [WHERE <cond>]
        //   ALTER TABLE <t> UPDATE <c> = <v>, ... WHERE <cond> [SETTINGS ...]
        //   DELETE FROM <t> [WHERE <cond>]
        //
        // <t> is a dotted reference whose last part names the canned table; <cond> is `1` or
        // `<c> = <v>` joined by AND; <v> an integer, a decimal number, a quoted string or NULL, or
        // `CAST(<v> AS <type word>)` of one (the value as written). Anything else is not applied
        // (nullopt).
        struct token_t {
            enum kind_t
            {
                word,
                identifier,
                text,
                number,
                symbol
            } kind;
            std::string value;
        };

        static std::optional<std::vector<token_t>> tokenize(const std::string& sql) {
            std::vector<token_t> out;
            std::size_t i = 0;
            while (i < sql.size()) {
                const char c = sql[i];
                if (c == ' ') {
                    ++i;
                } else if (c == '`' || c == '"' || c == '\'') {
                    std::string value;
                    ++i;
                    while (true) {
                        if (i >= sql.size()) {
                            return std::nullopt;
                        }
                        if (sql[i] == c && i + 1 < sql.size() && sql[i + 1] == c) {
                            value.push_back(c);
                            i += 2;
                        } else if (sql[i] == c) {
                            ++i;
                            break;
                        } else {
                            value.push_back(sql[i++]);
                        }
                    }
                    out.push_back(token_t{c == '\'' ? token_t::text : token_t::identifier, std::move(value)});
                } else if (std::isdigit(static_cast<unsigned char>(c)) || (c == '-' && i + 1 < sql.size() &&
                                                                         std::isdigit(static_cast<unsigned char>(sql[i + 1])))) {
                    std::string value{c};
                    ++i;
                    while (i < sql.size() && (std::isdigit(static_cast<unsigned char>(sql[i])) || sql[i] == '.' ||
                                              sql[i] == 'e' || sql[i] == 'E' || sql[i] == '+' || sql[i] == '-')) {
                        value.push_back(sql[i++]);
                    }
                    out.push_back(token_t{token_t::number, std::move(value)});
                } else if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
                    std::string value;
                    while (i < sql.size() && (std::isalnum(static_cast<unsigned char>(sql[i])) || sql[i] == '_')) {
                        value.push_back(sql[i++]);
                    }
                    out.push_back(token_t{token_t::word, std::move(value)});
                } else {
                    out.push_back(token_t{token_t::symbol, std::string{c}});
                    ++i;
                }
            }
            return out;
        }

        static bool is_word(const token_t& token, std::string_view word) {
            if (token.kind != token_t::word || token.value.size() != word.size()) {
                return false;
            }
            for (std::size_t i = 0; i < word.size(); ++i) {
                if (std::toupper(static_cast<unsigned char>(token.value[i])) != word[i]) {
                    return false;
                }
            }
            return true;
        }

        static std::optional<value_t> literal(const token_t& token) {
            if (token.kind == token_t::text) {
                return value_t{token.value};
            }
            if (token.kind == token_t::number) {
                if (token.value.find_first_of(".eE") == std::string::npos) {
                    return value_t{std::int64_t{std::stoll(token.value)}};
                }
                return value_t{std::stod(token.value)};
            }
            if (is_word(token, "NULL")) {
                return value_t{null_t{}};
            }
            return std::nullopt;
        }

        static bool same_value(const value_t& stored, const value_t& written) {
            auto as_double = [](const value_t& v) -> std::optional<double> {
                if (auto* i = std::get_if<std::int64_t>(&v)) {
                    return static_cast<double>(*i);
                }
                if (auto* i = std::get_if<std::int32_t>(&v)) {
                    return static_cast<double>(*i);
                }
                if (auto* d = std::get_if<double>(&v)) {
                    return *d;
                }
                return std::nullopt;
            };
            if (std::holds_alternative<null_t>(stored) || std::holds_alternative<null_t>(written)) {
                return false; // `=` never matches NULL
            }
            if (auto* text = std::get_if<std::string>(&stored)) {
                auto* other = std::get_if<std::string>(&written);
                return other != nullptr && *text == *other;
            }
            auto a = as_double(stored);
            auto b = as_double(written);
            return a && b && *a == *b;
        }

        // `written` stored under the column's type.
        static value_t stored_as(const value_t& written, const complex_logical_type& column) {
            if (std::holds_alternative<null_t>(written)) {
                return written;
            }
            switch (column.type()) {
                case logical_type::BIGINT:
                    if (auto* d = std::get_if<double>(&written)) {
                        return value_t{static_cast<std::int64_t>(*d)};
                    }
                    return written;
                case logical_type::INTEGER:
                    if (auto* i = std::get_if<std::int64_t>(&written)) {
                        return value_t{static_cast<std::int32_t>(*i)};
                    }
                    return written;
                case logical_type::DOUBLE:
                    if (auto* i = std::get_if<std::int64_t>(&written)) {
                        return value_t{static_cast<double>(*i)};
                    }
                    return written;
                default:
                    return written;
            }
        }

        static std::optional<std::uint64_t> apply_write(std::map<std::string, canned_table_t>& tables,
                                                        const std::string& sql) {
            auto tokens = tokenize(sql);
            if (!tokens) {
                return std::nullopt;
            }
            auto& t = *tokens;
            std::size_t at = 0;
            auto symbol = [&](char c) { return at < t.size() && t[at].kind == token_t::symbol && t[at].value[0] == c; };
            auto word = [&](std::string_view w) { return at < t.size() && is_word(t[at], w); };
            // A dotted reference; the canned table is its last part.
            auto reference = [&]() -> canned_table_t* {
                std::string name;
                while (at < t.size() && (t[at].kind == token_t::identifier || t[at].kind == token_t::word)) {
                    name = t[at++].value;
                    if (!symbol('.')) {
                        break;
                    }
                    ++at;
                }
                auto it = tables.find(name);
                return it == tables.end() ? nullptr : &it->second;
            };
            auto column_index = [](const canned_table_t& table, const std::string& name) -> std::optional<std::size_t> {
                for (std::size_t i = 0; i < table.columns.size(); ++i) {
                    if (table.columns[i].alias() == name) {
                        return i;
                    }
                }
                return std::nullopt;
            };
            using assignments_t = std::vector<std::pair<std::size_t, value_t>>;
            // <c> = <v> {sep <c> = <v>}
            auto pairs = [&](const canned_table_t& table, auto separator) -> std::optional<assignments_t> {
                assignments_t out;
                while (true) {
                    if (at + 2 >= t.size()) {
                        return std::nullopt;
                    }
                    auto column = column_index(table, t[at].value);
                    if (!column || !(t[at + 1].kind == token_t::symbol && t[at + 1].value == "=")) {
                        return std::nullopt;
                    }
                    at += 2;
                    std::optional<value_t> value;
                    if (word("CAST") && at + 5 < t.size() && t[at + 1].kind == token_t::symbol &&
                        t[at + 1].value == "(" && is_word(t[at + 3], "AS") && t[at + 4].kind == token_t::word &&
                        t[at + 5].kind == token_t::symbol && t[at + 5].value == ")") {
                        value = literal(t[at + 2]);
                        at += 6;
                    } else {
                        value = literal(t[at]);
                        ++at;
                    }
                    if (!value) {
                        return std::nullopt;
                    }
                    out.emplace_back(*column, *value);
                    if (!separator()) {
                        return out;
                    }
                    ++at;
                }
            };
            auto end_of_statement = [&]() {
                if (word("SETTINGS")) {
                    at = t.size();
                }
                if (symbol(';')) {
                    ++at;
                }
                return at == t.size();
            };
            // [WHERE 1 | WHERE <c> = <v> AND ...]: the conditions, empty for every row.
            auto where = [&](const canned_table_t& table) -> std::optional<assignments_t> {
                if (!word("WHERE")) {
                    return assignments_t{};
                }
                ++at;
                if (at < t.size() && t[at].kind == token_t::number && t[at].value == "1") {
                    ++at;
                    return assignments_t{};
                }
                return pairs(table, [&] { return word("AND"); });
            };
            // A DECIMAL cell (canned as its text) matches a number of the same value.
            auto matches = [](const canned_table_t& table, const row_t& row, const assignments_t& conditions) {
                return std::all_of(conditions.begin(), conditions.end(), [&](const auto& condition) {
                    const auto& stored = row[condition.first];
                    if (table.columns[condition.first].type() == logical_type::DECIMAL &&
                        std::holds_alternative<std::string>(stored) && !std::holds_alternative<null_t>(condition.second)) {
                        return same_value(value_t{std::stod(std::get<std::string>(stored))}, condition.second);
                    }
                    return same_value(stored, condition.second);
                });
            };

            if (word("INSERT")) {
                ++at;
                if (!word("INTO")) {
                    return std::nullopt;
                }
                ++at;
                auto* table = reference();
                if (table == nullptr || !symbol('(')) {
                    return std::nullopt;
                }
                ++at;
                std::vector<std::size_t> columns;
                while (at < t.size() && !symbol(')')) {
                    auto column = column_index(*table, t[at].value);
                    if (!column) {
                        return std::nullopt;
                    }
                    columns.push_back(*column);
                    ++at;
                    if (symbol(',')) {
                        ++at;
                    }
                }
                ++at;
                if (!word("VALUES")) {
                    return std::nullopt;
                }
                ++at;
                std::uint64_t inserted = 0;
                while (symbol('(')) {
                    ++at;
                    row_t row(table->columns.size(), value_t{null_t{}});
                    for (std::size_t i = 0; i < columns.size(); ++i) {
                        auto value = at < t.size() ? literal(t[at]) : std::nullopt;
                        if (!value) {
                            return std::nullopt;
                        }
                        row[columns[i]] = stored_as(*value, table->columns[columns[i]]);
                        ++at;
                        if (symbol(',')) {
                            ++at;
                        }
                    }
                    if (!symbol(')')) {
                        return std::nullopt;
                    }
                    ++at;
                    table->rows.push_back(std::move(row));
                    ++inserted;
                    if (symbol(',')) {
                        ++at;
                    }
                }
                return end_of_statement() ? std::optional<std::uint64_t>{inserted} : std::nullopt;
            }
            const bool alter = word("ALTER");
            if (alter || word("UPDATE")) {
                ++at;
                if (alter) {
                    if (!word("TABLE")) {
                        return std::nullopt;
                    }
                    ++at;
                }
                auto* table = reference();
                if (table == nullptr || !word(alter ? "UPDATE" : "SET")) {
                    return std::nullopt;
                }
                ++at;
                auto assignments = pairs(*table, [&] { return symbol(','); });
                if (!assignments) {
                    return std::nullopt;
                }
                auto conditions = where(*table);
                if (!conditions || !end_of_statement()) {
                    return std::nullopt;
                }
                std::uint64_t matched = 0;
                for (auto& row : table->rows) {
                    if (matches(*table, row, *conditions)) {
                        for (const auto& [column, value] : *assignments) {
                            row[column] = stored_as(value, table->columns[column]);
                        }
                        ++matched;
                    }
                }
                return matched;
            }
            if (word("DELETE")) {
                ++at;
                if (!word("FROM")) {
                    return std::nullopt;
                }
                ++at;
                auto* table = reference();
                if (table == nullptr) {
                    return std::nullopt;
                }
                auto conditions = where(*table);
                if (!conditions || !end_of_statement()) {
                    return std::nullopt;
                }
                const auto before = table->rows.size();
                std::erase_if(table->rows, [&](const row_t& row) { return matches(*table, row, *conditions); });
                return before - table->rows.size();
            }
            return std::nullopt;
        }

        backend_type_t kind_;
        // The rows outlive the statement's cursor: this resource lives as long as the backend,
        // which the registry keeps until exit.
        std::pmr::synchronized_pool_resource resource_;
        mutable std::mutex mutex_;
        std::map<std::string, canned_table_t> tables_;
        std::vector<std::string> sql_log_;
        std::vector<fed::scan_request_t> scan_requests_;
        scan_hook_t scan_hook_;
        std::vector<std::string> write_log_;
        std::map<std::string, canned_table_t> scripted_;
        std::optional<core::error_code_t> write_fail_;
        std::function<void(std::map<std::string, canned_table_t>&)> before_next_write_;
        std::atomic<bool> async_{false};
        int scans_before_answer_{0};
        std::chrono::milliseconds delay_{0};
        std::atomic<int> scans_{0};
        std::atomic<int> in_flight_{0};
        std::atomic<int> peak_in_flight_{0};
        std::atomic<bool> deadline_missed_{false};
    };

    inline complex_logical_type column(logical_type type, const std::string& name) {
        return complex_logical_type{type, name};
    }

    // Installs the servers as the process-wide registry the hooks read.
    inline void install(std::vector<fed::server_t> servers) {
        fed::install_registry(std::make_shared<const fed::registry_t>(std::move(servers)));
    }

    inline fed::server_t server(const std::string& uid, const std::shared_ptr<fake_backend_t>& backend) {
        return fed::server_t{uid, backend->kind(), backend};
    }

    // The engine, opened through runtime::engine_t with the federation's primitives, on a runtime of
    // its own (declared first: it outlives the engine).
    class engine_t final {
    public:
        engine_t(std::unique_ptr<otterstax::runtime::runtime_t> runtime, otterstax::runtime::engine_t::engine_ptr engine)
            : runtime_(std::move(runtime))
            , engine_(std::move(engine)) {}

        std::pmr::memory_resource* resource() const noexcept { return engine_->resource(); }

        components::cursor::cursor_t_ptr sql(const std::string& text) {
            return sql(components::session::session_id_t(), text);
        }
        components::cursor::cursor_t_ptr sql(const components::session::session_id_t& session, const std::string& text) {
            return otterstax::runtime::test::execute_sql(*engine_, session, text);
        }

        // The plans on one session, inside BEGIN ... COMMIT; every statement must succeed.
        void apply(std::pmr::vector<components::logical_plan::execution_plan_t> plans) {
            const components::session::session_id_t session;
            REQUIRE(sql(session, "BEGIN;")->is_success());
            for (auto& plan : plans) {
                auto cursor = engine_->execute_plan_blocking(session, std::move(plan));
                INFO((cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
                REQUIRE(cursor->is_success());
            }
            REQUIRE(sql(session, "COMMIT;")->is_success());
        }

        void bootstrap() { apply(otterstax::catalog::make_metadata_bootstrap(resource())); }

        // What a discovery reply writes; `scope` as the backend's discover took it.
        void upsert(backend_type_t type,
                    const qualified_name_t& scope,
                    const fed::discovered_tables_t& tables) {
            auto upsert = otterstax::catalog::make_discovery_upsert(resource(), type, scope, tables);
            INFO((upsert.has_error() ? std::string{upsert.error().what} : std::string{"ok"}));
            REQUIRE_FALSE(upsert.has_error());
            apply(std::move(upsert.value()));
        }

    private:
        std::unique_ptr<otterstax::runtime::runtime_t> runtime_;
        otterstax::runtime::engine_t::engine_ptr engine_;
    };

    inline std::filesystem::path test_directory(const std::string& leaf) {
        return std::filesystem::temp_directory_path() / "otterstax-federation-tests" /
               (std::to_string(::getpid()) + "-" + leaf);
    }

    inline std::unique_ptr<engine_t> open_engine(const std::string& leaf) {
        const auto dir = test_directory(leaf);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        REQUIRE_FALSE(ec);
        auto runtime = std::make_unique<otterstax::runtime::runtime_t>(otterstax::runtime::test::small_runtime());
        auto engine_config = otterstax::runtime::test::engine_config(dir);
        auto opened = otterstax::runtime::engine_t::open(*runtime, engine_config, fed::make_primitives());
        INFO((opened.has_error() ? std::string{opened.error().what} : std::string{"ok"}));
        REQUIRE_FALSE(opened.has_error());
        return std::make_unique<engine_t>(std::move(runtime), std::move(opened.value()));
    }

    // A discovered table as the backends answer it: the remote spelling of the name and of
    // each column, the engine type of each column, the primary key's columns (remote spelling).
    inline fed::discovered_table_t discovered(const std::string& uid,
                                                      const std::string& database,
                                                      const std::string& schema,
                                                      const std::string& table,
                                                      const std::vector<complex_logical_type>& columns,
                                                      std::vector<std::string> primary_key = {}) {
        std::pmr::vector<complex_logical_type> fields{std::pmr::get_default_resource()};
        for (const auto& c : columns) {
            fields.push_back(c);
        }
        return fed::discovered_table_t{
            qualified_name_t{core::uid_t{uid}, core::dbname_t{database}, core::schema_t{schema}, core::relname_t{table}},
            complex_logical_type::create_struct("", fields),
            std::move(primary_key)};
    }

    inline fed::discovered_tables_t tables(std::vector<fed::discovered_table_t> list) {
        fed::discovered_tables_t out{std::pmr::get_default_resource()};
        for (auto& t : list) {
            out.push_back(std::move(t));
        }
        return out;
    }

    inline qualified_name_t scope(const std::string& uid,
                                  const std::string& database,
                                  const std::string& schema,
                                  const std::string& table = {}) {
        return qualified_name_t{core::uid_t{uid}, core::dbname_t{database}, core::schema_t{schema}, core::relname_t{table}};
    }

    inline std::string error_of(const components::cursor::cursor_t_ptr& cursor) {
        return cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"};
    }

    // A cell as text, whatever its numeric type; "NULL" for a NULL.
    inline std::string cell(const components::vector::data_chunk_t& chunk, std::size_t col, std::uint64_t row) {
        const auto value = chunk.value(col, row);
        if (value.is_null()) {
            return "NULL";
        }
        switch (value.type().type()) {
            case logical_type::BOOLEAN:
                return value.value<bool>() ? "true" : "false";
            case logical_type::TINYINT:
                return std::to_string(value.value<std::int8_t>());
            case logical_type::SMALLINT:
                return std::to_string(value.value<std::int16_t>());
            case logical_type::INTEGER:
                return std::to_string(value.value<std::int32_t>());
            case logical_type::BIGINT:
                return std::to_string(value.value<std::int64_t>());
            case logical_type::UBIGINT:
                return std::to_string(value.value<std::uint64_t>());
            case logical_type::UINTEGER:
                return std::to_string(value.value<std::uint32_t>());
            case logical_type::DOUBLE: {
                auto text = std::to_string(value.value<double>());
                return text;
            }
            case logical_type::STRING_LITERAL:
                return std::string{value.value<std::string_view>()};
            case logical_type::DECIMAL: {
                // The exact digits, the point where the scale puts it (DECIMAL(10, 2) 1230 is 12.30).
                const auto* spec = value.type().extension_as<components::types::decimal_logical_type_extension>();
                components::types::int128_t unscaled = 0;
                switch (value.type().to_physical_type()) {
                    case components::types::physical_type::INT16:
                        unscaled = value.value<std::int16_t>();
                        break;
                    case components::types::physical_type::INT32:
                        unscaled = value.value<std::int32_t>();
                        break;
                    case components::types::physical_type::INT64:
                        unscaled = value.value<std::int64_t>();
                        break;
                    default:
                        unscaled = value.value<components::types::int128_t>();
                        break;
                }
                const bool negative = unscaled < 0;
                auto magnitude = negative ? -static_cast<components::types::uint128_t>(unscaled)
                                          : static_cast<components::types::uint128_t>(unscaled);
                std::string digits;
                do {
                    digits.insert(digits.begin(), static_cast<char>('0' + static_cast<int>(magnitude % 10)));
                    magnitude /= 10;
                } while (magnitude != 0);
                const std::size_t scale = spec->scale();
                if (digits.size() <= scale) {
                    digits.insert(digits.begin(), scale + 1 - digits.size(), '0');
                }
                if (scale > 0) {
                    digits.insert(digits.end() - static_cast<std::ptrdiff_t>(scale), '.');
                }
                return (negative ? "-" : "") + digits;
            }
            default:
                return "<type " + std::to_string(static_cast<int>(value.type().type())) + ">";
        }
    }

    using text_rows_t = std::vector<std::vector<std::string>>;

    inline text_rows_t rows_of(const components::cursor::cursor_t_ptr& cursor) {
        text_rows_t out;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                std::vector<std::string> values;
                for (std::size_t col = 0; col < chunk.column_count(); ++col) {
                    values.push_back(cell(chunk, col, row));
                }
                out.push_back(std::move(values));
            }
        }
        return out;
    }

    inline text_rows_t sorted_rows_of(const components::cursor::cursor_t_ptr& cursor) {
        auto out = rows_of(cursor);
        std::sort(out.begin(), out.end());
        return out;
    }

    // The column names of a result, in order.
    inline std::vector<std::string> column_names(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<std::string> names;
        if (cursor->chunks().empty()) {
            return names;
        }
        for (const auto& type : cursor->chunks().front().types()) {
            names.push_back(type.has_alias() ? type.alias() : std::string{});
        }
        return names;
    }

} // namespace fedtest
