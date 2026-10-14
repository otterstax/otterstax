// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "remote_storage.hpp"

#include "otterbrix/translators/error.hpp"
#include "utility/tracy_profiler.hpp"

#include <components/casts/kernels/decimal_cast.hpp>
#include <components/casts/kernels/string_cast.hpp>
#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

#include <algorithm>
#include <cassert>
#include <limits>
#include <optional>
#include <utility>

using namespace components;

namespace otterstax::federation {

    namespace {

        // The address of this static is the owner tag of every federation storage
        // (table_storage_t::owner()).
        const char federation_owner_tag = 0;

        // The full scan of a table: SELECT <columns> FROM <table>. Every identifier is quoted in the
        // backend's dialect (sql_gen::quote_identifier: "..." for PostgreSQL, `...` for MySQL and
        // ClickHouse, the quote character doubled inside, a backslash doubled for ClickHouse), so the
        // backend's own spelling survives whatever its case or characters; the table is
        // database.table for MySQL and ClickHouse, schema.table for PostgreSQL. A table read without
        // columns (no column survived discovery) is not a statement the federation sends: discovery
        // never records such a table. The statements the pushdown rule and the write operators send
        // are written by sql_gen from the validated tree.
        std::string make_scan_sql(backend_type_t kind,
                                  const remote_table_name_t& table,
                                  std::span<const std::string> remote_columns) {
            OTX_ZONE_N("federation::make_scan_sql");
            std::string sql{"SELECT "};
            for (std::size_t i = 0; i < remote_columns.size(); ++i) {
                if (i != 0) {
                    sql += ", ";
                }
                sql += sql_gen::quote_identifier(remote_columns[i], kind);
            }
            sql += " FROM ";
            const auto& qualifier = kind == backend_type_t::PostgreSQL ? table.schema : table.database;
            if (!qualifier.empty()) {
                sql += sql_gen::quote_identifier(qualifier, kind);
                sql.push_back('.');
            }
            sql += sql_gen::quote_identifier(table.table, kind);
            return sql;
        }

        using tsl::make_error;

        using next_t = core::result_wrapper_t<std::optional<vector::data_chunk_t>>;

        // A primary key column whose value, as the scan read it, names the stored value exactly when
        // written back as a literal: integers, BOOLEAN, DECIMAL (a MySQL DECIMAL / PostgreSQL NUMERIC /
        // ClickHouse Decimal of at most 38 digits; its literal is its exact text, which every backend
        // compares as an exact number: `k = 12.30` names the row holding 12.30 and no other), text (the
        // date / time family of MySQL and ClickHouse and PostgreSQL's arrive as text too, which each
        // backend converts back to the column's type, and so does PostgreSQL's wider / unconstrained
        // NUMERIC, carried as its exact digits: the untyped literal takes the column's numeric type),
        // DATE and TIMESTAMP. Not a floating-point column: a FLOAT's literal is the float's digits while
        // the backend compares the double it widens to. Nor the nested types. Two kinds of column arrive
        // as STRING_LITERAL with no literal that names them and never reach here as a key column: the
        // discovery records no key over one (each backend's discover drops such a key, integration/) —
        // a binary column (bytea, BINARY / VARBINARY / BLOB: its raw bytes, which no character literal
        // names) and a MySQL DECIMAL wider than 38 digits (its exact text, which MySQL compares with the
        // column as DOUBLE).
        bool names_a_row(const types::complex_logical_type& type) noexcept {
            const auto kind = type.type();
            return types::is_integer(kind) || kind == types::logical_type::BOOLEAN ||
                   kind == types::logical_type::DECIMAL || kind == types::logical_type::STRING_LITERAL ||
                   kind == types::logical_type::DATE || kind == types::logical_type::TIMESTAMP;
        }

        // The source over one remote table. open() starts the backend fetch (the engine opens every
        // source of a plan before pumping any, so the fetches of several remote tables overlap);
        // source_next() hands the fetched rows out in chunks of at most DEFAULT_VECTOR_CAPACITY rows,
        // each row numbered in row_ids by the storage.
        class remote_scan_t final : public operators::read_only_operator_t {
        public:
            remote_scan_t(std::pmr::memory_resource* resource, log_t log, remote_storage_t* storage)
                : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
                , storage_(storage) {}

            [[nodiscard]] operators::pipeline_role role() const noexcept override {
                return operators::pipeline_role::source;
            }

            [[nodiscard]] actor_zeta::unique_future<next_t> source_next(pipeline::context_t*) override {
                OTX_ZONE_N("federation::remote_scan::source_next");
                // The engine opens every source before it pumps any.
                assert(rows_ && "a remote scan is asked for rows only once it was opened");
                if (!conformed_) {
                    // Held to the declared columns: the same count, each column of the declared type
                    // compared whole (conform_to_result's discovered check). Either mismatch is the
                    // remote table changed since it was discovered, which the session's rediscovery reads.
                    const auto& declared = storage_->table().columns;
                    const std::vector<std::optional<types::complex_logical_type>> discovered(declared.begin(),
                                                                                             declared.end());
                    for (auto& chunk : rows_->chunks) {
                        if (auto err = conform_to_result(resource(),
                                                         storage_->name(),
                                                         storage_->name(),
                                                         declared,
                                                         discovered,
                                                         core::date::timezone_offset_t{},
                                                         chunk);
                            err.contains_error()) {
                            return actor_zeta::make_ready_future<next_t>(resource(), std::move(err));
                        }
                    }
                    conformed_ = true;
                }
                auto& chunks = rows_->chunks;
                while (chunk_index_ < chunks.size() && offset_ >= chunks[chunk_index_].size()) {
                    ++chunk_index_;
                    offset_ = 0;
                }
                if (chunk_index_ == chunks.size()) {
                    if (!handed_out_) {
                        // An empty remote table is one typed chunk without rows, as an empty local
                        // table scans: the statement's result keeps its columns.
                        handed_out_ = true;
                        std::pmr::vector<types::complex_logical_type> types(storage_->table().columns.begin(),
                                                                            storage_->table().columns.end(),
                                                                            resource());
                        vector::data_chunk_t empty(resource(), types, 1);
                        empty.set_cardinality(0);
                        return actor_zeta::make_ready_future<next_t>(resource(), next_t{std::move(empty)});
                    }
                    return actor_zeta::make_ready_future<next_t>(resource(), next_t{std::nullopt});
                }
                handed_out_ = true;
                const auto& source = chunks[chunk_index_];
                const auto count = std::min<std::uint64_t>(source.size() - offset_, vector::DEFAULT_VECTOR_CAPACITY);
                auto window = source.partial_copy(resource(), offset_, count);
                const auto first = storage_->number_rows(rows_, chunk_index_, offset_, count);
                auto* numbers = window.row_ids.data<std::int64_t>();
                for (std::uint64_t row = 0; row < count; ++row) {
                    numbers[row] = first + static_cast<std::int64_t>(row);
                }
                offset_ += count;
                return actor_zeta::make_ready_future<next_t>(resource(), next_t{std::move(window)});
            }

            // A re-drive (LATERAL, recursive CTE) reads the same rows again: nothing is fetched twice.
            void reset_pipeline_state() noexcept override {
                chunk_index_ = 0;
                offset_ = 0;
                handed_out_ = false;
            }

        private:
            actor_zeta::unique_future<core::error_t> open_impl(pipeline::context_t*) override {
                OTX_ZONE_N("federation::remote_scan::open");
                if (rows_) {
                    // Opened before (a re-drive): the rows are here, or the first open reported why not.
                    return actor_zeta::make_ready_future<core::error_t>(resource(), core::error_t::no_error());
                }
                rows_ = std::make_shared<scan_rows_t>();
                const auto& backend = storage_->table().server.backend;
                // The registry holds a backend for every configured connection.
                assert(backend);
                return backend->scan(resource(), storage_->make_scan_request(), rows_);
            }

            std::pmr::string explain_label_impl() const override {
                return std::pmr::string{("Foreign Scan on " + storage_->name()).c_str(), resource()};
            }

            std::pmr::vector<std::pmr::string> explain_details_impl() const override {
                std::pmr::vector<std::pmr::string> details{resource()};
                details.emplace_back(("Remote SQL: " + storage_->scan_sql()).c_str());
                return details;
            }

            remote_storage_t* storage_;
            std::shared_ptr<scan_rows_t> rows_;
            bool conformed_{false};
            bool handed_out_{false};
            std::size_t chunk_index_{0};
            std::uint64_t offset_{0};
        };

        // A sink of the storage: buffers in push, talks to the backend in await_async_and_resume (the
        // executor may drive several rounds of pushes + await in one statement).
        class remote_sink_t : public operators::read_write_operator_t {
        public:
            remote_sink_t(std::pmr::memory_resource* resource, log_t log, remote_storage_t* storage)
                : operators::read_write_operator_t(resource, std::move(log), operators::operator_type::extension)
                , storage_(storage) {}

            [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        protected:
            // Sends one statement and waits for its answer: the rows the backend reports, or nothing
            // with the error set on the operator. Defined in the class: an actor-zeta coroutine member
            // takes its resource from `this`.
            actor_zeta::unique_future<std::optional<std::uint64_t>> send(std::string sql, write_kind_t kind) {
                const auto& backend = storage_->table().server.backend;
                // The registry holds a backend for every configured connection.
                assert(backend);
                auto future =
                    backend->write(resource(), write_request_t{storage_->table().server.uid, std::move(sql), kind});
                auto answer = co_await std::move(future);
                if (answer.has_error()) {
                    set_error(answer.error());
                    co_return std::nullopt;
                }
                co_return answer.value();
            }

            remote_storage_t* storage_;
        };

        // INSERT: full rows in declared order; only the columns the statement wrote are sent.
        class remote_insert_t final : public remote_sink_t {
        public:
            using remote_sink_t::remote_sink_t;

            [[nodiscard]] core::error_t
            push(pipeline::context_t*, vector::data_chunk_t&& input, operators::chunks_vector_t&) override {
                if (input.size() != 0) {
                    buffered_.push_back(std::move(input));
                }
                return core::error_t::no_error();
            }

            actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t*) override {
                std::vector<std::string> names;
                std::vector<std::size_t> indexes;
                {
                    OTX_ZONE_N("federation::remote_insert::columns");
                    const auto& table = storage_->table();
                    if (storage_->insert_columns().empty()) {
                        for (std::size_t i = 0; i < table.remote_columns.size(); ++i) {
                            names.push_back(table.remote_columns[i]);
                            indexes.push_back(i);
                        }
                    } else {
                        for (auto i : storage_->insert_columns()) {
                            names.push_back(table.remote_columns.at(i));
                            indexes.push_back(i);
                        }
                    }
                }
                auto chunks = std::move(buffered_);
                buffered_.clear();
                for (const auto& chunk : chunks) {
                    std::string sql;
                    {
                        OTX_ZONE_N("federation::remote_insert::generate");
                        auto generated = sql_gen::generate_insert_rows(storage_->dialect(),
                                                                       storage_->generator_table(),
                                                                       names,
                                                                       indexes,
                                                                       chunk,
                                                                       resource());
                        if (generated.has_error()) {
                            set_error(generated.error());
                            co_return;
                        }
                        sql = std::move(generated.value());
                    }
                    if (!co_await send(std::move(sql), write_kind_t::insert)) {
                        co_return;
                    }
                }
                mark_executed();
                co_return;
            }

        private:
            std::pmr::string explain_label_impl() const override {
                return std::pmr::string{("Foreign Insert on " + storage_->name()).c_str(), resource()};
            }

            std::vector<vector::data_chunk_t> buffered_;
        };

        // UPDATE / DELETE by row number: each row is named on the backend by its primary key as the
        // scan read it, one statement per row, and each statement must report exactly one row — the
        // row the engine picked. Fewer: no row holds that key any more (deleted on the backend since
        // the scan read it, or its key changed); more: the discovered key does not name one row any
        // more. Either fails the statement (the rows changed before it stay changed: writes run in
        // the backend's autocommit). Only the key is compared: a row whose other columns changed on
        // the backend after the read still matches and is overwritten — a lost update, not a
        // write_conflict. So the count the engine reports — the rows it handed this sink — is the
        // sum the backend reported.
        class remote_row_change_t final : public remote_sink_t {
        public:
            remote_row_change_t(std::pmr::memory_resource* resource,
                                log_t log,
                                remote_storage_t* storage,
                                write_kind_t kind)
                : remote_sink_t(resource, std::move(log), storage)
                , kind_(kind) {}

            [[nodiscard]] core::error_t
            push(pipeline::context_t*, vector::data_chunk_t&& input, operators::chunks_vector_t&) override {
                OTX_ZONE_N("federation::remote_row_change::push");
                const auto* numbers = input.row_ids.data<std::int64_t>();
                for (std::uint64_t row = 0; row < input.size(); ++row) {
                    change_t change{numbers[row], {}};
                    if (kind_ == write_kind_t::update) {
                        for (std::uint64_t col = 0; col < input.column_count(); ++col) {
                            change.after.push_back(input.value(col, row));
                        }
                    }
                    changes_.push_back(std::move(change));
                }
                return core::error_t::no_error();
            }

            actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t*) override {
                auto changes = std::move(changes_);
                changes_.clear();
                for (auto& change : changes) {
                    std::string sql;
                    {
                        OTX_ZONE_N("federation::remote_row_change::generate");
                        auto before = storage_->row_values(resource(), change.number);
                        if (before.has_error()) {
                            set_error(before.error());
                            co_return;
                        }
                        const auto& table = storage_->table();
                        auto generated = kind_ == write_kind_t::update
                                             ? sql_gen::generate_update_row(storage_->dialect(),
                                                                            storage_->generator_table(),
                                                                            table.remote_columns,
                                                                            table.key_columns,
                                                                            before.value(),
                                                                            change.after,
                                                                            resource())
                                             : sql_gen::generate_delete_row(storage_->dialect(),
                                                                            storage_->generator_table(),
                                                                            table.remote_columns,
                                                                            table.key_columns,
                                                                            before.value(),
                                                                            resource());
                        if (generated.has_error()) {
                            set_error(generated.error());
                            co_return;
                        }
                        sql = std::move(generated.value());
                    }
                    auto reported = co_await send(sql, kind_);
                    if (!reported) {
                        co_return;
                    }
                    if (*reported != 1) {
                        OTX_ZONE_N("federation::remote_row_change::mismatch");
                        set_error(make_error(resource(),
                                             *reported == 0 ? core::error_code_t::write_conflict
                                                            : core::error_code_t::invalid_constraint,
                                             "the remote statement " + sql + " on \"" + storage_->name() +
                                                 "\" changed " + std::to_string(*reported) +
                                                 (*reported == 0
                                                      ? " rows: no row holds the key it was read with any more "
                                                        "(deleted on the backend, or its key changed, after it "
                                                        "was read)"
                                                      : " rows where its primary key names one")));
                        co_return;
                    }
                }
                mark_executed();
                co_return;
            }

        private:
            struct change_t {
                std::int64_t number;
                std::vector<types::logical_value_t> after;
            };

            std::pmr::string explain_label_impl() const override {
                return std::pmr::string{((kind_ == write_kind_t::update ? "Foreign Update on " : "Foreign Delete on ") +
                                         storage_->name())
                                            .c_str(),
                                        resource()};
            }

            write_kind_t kind_;
            std::vector<change_t> changes_;
        };

        // The whole type, as discovery recorded it: what complex_logical_type::operator== compares
        // (the logical type, DECIMAL width and scale, LIST / ARRAY / MAP element, ENUM labels) and the
        // names of STRUCT fields at every depth, which operator== does not compare (a STRUCT's fields
        // are compared by type alone) — a ClickHouse Tuple whose fields were renamed is a changed
        // table, and the engine binds `column.field` by the declared names.
        bool same_type(const types::complex_logical_type& a, const types::complex_logical_type& b) {
            if (!(a == b)) {
                return false;
            }
            switch (a.type()) {
                case types::logical_type::STRUCT: {
                    const auto& left = a.child_types();
                    const auto& right = b.child_types();
                    if (left.size() != right.size()) {
                        return false;
                    }
                    for (std::size_t i = 0; i < left.size(); ++i) {
                        const auto left_name = left[i].has_alias() ? left[i].alias() : std::string{};
                        const auto right_name = right[i].has_alias() ? right[i].alias() : std::string{};
                        if (left_name != right_name || !same_type(left[i], right[i])) {
                            return false;
                        }
                    }
                    return true;
                }
                case types::logical_type::LIST:
                case types::logical_type::ARRAY:
                case types::logical_type::MAP:
                    return same_type(a.child_type(), b.child_type());
                default:
                    return true;
            }
        }

        types::int128_t unscaled_of(const types::logical_value_t& value) {
            switch (value.type().to_physical_type()) {
                case types::physical_type::INT16:
                    return value.value<std::int16_t>();
                case types::physical_type::INT32:
                    return value.value<std::int32_t>();
                case types::physical_type::INT64:
                    return value.value<std::int64_t>();
                default:
                    return value.value<types::int128_t>();
            }
        }

        // One value of a pushed-down answer as `target`.
        core::result_wrapper_t<types::logical_value_t> convert_value(std::pmr::memory_resource* resource,
                                                                     const types::logical_value_t& value,
                                                                     const types::complex_logical_type& target,
                                                                     core::date::timezone_offset_t timezone) {
            const auto source = value.type().type();
            if (source == types::logical_type::DECIMAL && target.type() == types::logical_type::DECIMAL) {
                // A DECIMAL as a DECIMAL of `target`'s width and scale (MySQL answers SUM over DECIMAL(10, 2)
                // as DECIMAL(32, 2), ClickHouse Decimal(38, 2); the engine's type is DECIMAL(38, 2)): the
                // engine's own rescale, a dropped fraction rounded half away from zero as its DECIMAL cast
                // rounds, NaN kept NaN. conversion_failure for a value past the target's width (never cut down
                // to fit) and for an infinity. logical_value_t::cast_as has no DECIMAL-to-DECIMAL conversion.
                const auto* from = value.type().extension_as<types::decimal_logical_type_extension>();
                const auto* to = target.extension_as<types::decimal_logical_type_extension>();
                if (from == nullptr || to == nullptr) {
                    return make_error(resource,
                                      core::error_code_t::conversion_failure,
                                      "a DECIMAL without its width and scale");
                }
                const auto raw = unscaled_of(value);
                if (types::decimal_special::is_special(from->stored_as(), raw)) {
                    if (raw == types::decimal_special::not_a_number(from->stored_as())) {
                        return types::logical_value_t::create_decimal(
                            resource,
                            target,
                            types::decimal_special::not_a_number(to->stored_as()));
                    }
                    return make_error(resource, core::error_code_t::conversion_failure, "an infinite DECIMAL value");
                }
                // The engine's rescale adds half the divisor (up to 5 * 10^37) before dividing, which leaves
                // the int128 (1.7 * 10^38) for |raw| past 1.2 * 10^38: a raw of more than 38 digits (a
                // ClickHouse Decimal read under decimal_check_overflow = 0) is refused before it.
                const auto max_raw = types::POWERS_OF_TEN[types::DECIMAL_MAX_WIDTH];
                types::int128_t result = 0;
                if (raw >= max_raw || raw <= -max_raw ||
                    !components::casts::kernels::detail::rescale_decimal(raw,
                                                                         from->scale(),
                                                                         to->scale(),
                                                                         to->width(),
                                                                         &result)) {
                    return make_error(resource,
                                      core::error_code_t::conversion_failure,
                                      "the DECIMAL value does not fit DECIMAL(" + std::to_string(to->width()) +
                                          ", " + std::to_string(to->scale()) + ")");
                }
                return types::logical_value_t::create_decimal(resource, target, result);
            }
            if (source == types::logical_type::STRING_LITERAL && target.type() != types::logical_type::STRING_LITERAL) {
                const std::string text{value.value<std::string_view>()};
                if (target.type() == types::logical_type::DECIMAL) {
                    // Every digit of the text (MySQL answers a DECIMAL wider than 38 digits, PostgreSQL an
                    // unconstrained NUMERIC, as text), rounded half away from zero to the declared scale
                    // as the engine's DECIMAL cast rounds; a value past the declared width is refused.
                    // PostgreSQL's NaN is the engine's DECIMAL NaN, as pg_to_chunk reads it off a
                    // NUMERIC(p, s); an infinity has no DECIMAL the engine computes with (as above).
                    const auto* extension = target.extension_as<types::decimal_logical_type_extension>();
                    if (extension != nullptr && text == "NaN") {
                        return types::logical_value_t::create_decimal(
                            resource,
                            target,
                            types::decimal_special::not_a_number(extension->stored_as()));
                    }
                    types::int128_t unscaled = 0;
                    if (extension != nullptr &&
                        components::casts::kernels::detail::parse_decimal(text,
                                                                          extension->width(),
                                                                          extension->scale(),
                                                                          &unscaled)) {
                        return types::logical_value_t::create_decimal(resource, target, unscaled);
                    }
                } else if (target.type() == types::logical_type::FLOAT || target.type() == types::logical_type::DOUBLE) {
                    // The engine's own reading of a number's text (its string-to-DOUBLE cast).
                    double parsed = 0;
                    if (components::casts::kernels::detail::parse_number<double>(text, parsed)) {
                        return types::logical_value_t(resource, parsed).cast_as(target, timezone);
                    }
                } else if (types::is_integer(target.type())) {
                    types::int128_t unscaled = 0;
                    if (components::casts::kernels::detail::parse_decimal(text,
                                                                          types::DECIMAL_MAX_WIDTH,
                                                                          0,
                                                                          &unscaled) &&
                        unscaled >= std::numeric_limits<std::int64_t>::min() &&
                        unscaled <= std::numeric_limits<std::int64_t>::max()) {
                        return types::logical_value_t(resource, static_cast<std::int64_t>(unscaled))
                            .cast_as(target, timezone);
                    }
                } else if (target.type() == types::logical_type::BOOLEAN) {
                    // The engine's own reading of a boolean's text: PostgreSQL's spellings.
                    if (const auto parsed = components::casts::kernels::detail::parse_bool(text)) {
                        return types::logical_value_t(resource, *parsed);
                    }
                }
                return make_error(resource,
                                  core::error_code_t::conversion_failure,
                                  "the text '" + text + "' has no value of type " +
                                      std::to_string(static_cast<int>(target.type())));
            }
            return value.cast_as(target, timezone);
        }

    } // namespace

    std::string display_name(const table_key_t& key) {
        std::string out = key.uid;
        out += '.';
        out += key.database.empty() ? key.schema : key.database;
        out += '.';
        out += key.table;
        return out;
    }

    const void* storage_owner() noexcept { return &federation_owner_tag; }

    remote_storage_t::remote_storage_t(std::pmr::memory_resource*, remote_table_t table)
        : table_storage_t(&federation_owner_tag)
        , table_(std::move(table))
        , name_(display_name(table_.key))
        , scan_sql_(make_scan_sql(table_.server.kind, table_.remote, table_.remote_columns))
        , generator_table_(generator_name(table_.server.kind, table_.remote)) {
        for (std::size_t i = 0; i < table_.columns.size() && i < table_.remote_columns.size(); ++i) {
            const auto& column = table_.columns[i];
            column_map_.emplace(column.has_alias() ? column.alias() : std::string{},
                                sql_gen::remote_column_t{table_.remote_columns[i], column});
        }
    }

    backend_type_t remote_storage_t::dialect() const noexcept { return table_.server.kind; }

    scan_request_t remote_storage_t::make_scan_request() const {
        std::vector<std::optional<column_origin_t>> columns;
        columns.reserve(table_.remote_columns.size());
        for (const auto& column : table_.remote_columns) {
            columns.emplace_back(column_origin_t{table_.remote, column});
        }
        return scan_request_t{table_.server.uid, scan_sql_, std::move(columns)};
    }

    std::int64_t remote_storage_t::number_rows(const std::shared_ptr<scan_rows_t>& rows,
                                               std::size_t chunk,
                                               std::uint64_t offset,
                                               std::uint64_t count) {
        std::lock_guard guard(numbers_mutex_);
        const auto first = next_number_;
        numbered_.push_back(numbered_range_t{first, rows, chunk, offset, count});
        next_number_ += static_cast<std::int64_t>(count);
        return first;
    }

    core::result_wrapper_t<std::vector<types::logical_value_t>>
    remote_storage_t::row_values(std::pmr::memory_resource* resource, std::int64_t number) const {
        std::lock_guard guard(numbers_mutex_);
        auto it = std::upper_bound(numbered_.begin(), numbered_.end(), number, [](std::int64_t n, const numbered_range_t& r) {
            return n < r.first;
        });
        if (it == numbered_.begin()) {
            return make_error(resource,
                              core::error_code_t::invalid_parameter,
                              "remote table \"" + name_ + "\" never gave out row " + std::to_string(number));
        }
        --it;
        if (number - it->first >= static_cast<std::int64_t>(it->count)) {
            return make_error(resource,
                              core::error_code_t::invalid_parameter,
                              "remote table \"" + name_ + "\" never gave out row " + std::to_string(number));
        }
        const auto& chunk = it->rows->chunks.at(it->chunk);
        const auto row = it->offset + static_cast<std::uint64_t>(number - it->first);
        std::vector<types::logical_value_t> values;
        values.reserve(chunk.column_count());
        for (std::uint64_t col = 0; col < chunk.column_count(); ++col) {
            values.push_back(chunk.value(col, row));
        }
        return values;
    }

    logical_plan::storage_operator_t remote_storage_t::make_scan_impl(const services::context_storage_t& context) {
        OTX_ZONE_N("federation::remote_storage::make_scan");
        return operators::operator_ptr{new remote_scan_t(context.resource, context.log.clone(), this)};
    }

    logical_plan::storage_operator_t remote_storage_t::make_insert_impl(const services::context_storage_t& context) {
        OTX_ZONE_N("federation::remote_storage::make_insert");
        return operators::operator_ptr{new remote_insert_t(context.resource, context.log.clone(), this)};
    }

    core::error_t remote_storage_t::check_row_changes(std::pmr::memory_resource* resource) const {
        if (table_.server.kind == backend_type_t::ClickHouse) {
            return make_error(resource,
                              core::error_code_t::unimplemented_yet,
                              "ClickHouse table \"" + name_ + "\" cannot change a row by its number");
        }
        if (table_.key_columns.empty()) {
            return make_error(resource,
                              core::error_code_t::unimplemented_yet,
                              "remote table \"" + name_ +
                                  "\" has no primary key: a row the engine picked cannot be named on the backend");
        }
        for (auto position : table_.key_columns) {
            if (position >= table_.columns.size() || !names_a_row(table_.columns[position])) {
                const auto column = position < table_.columns.size() && table_.columns[position].has_alias()
                                        ? table_.columns[position].alias()
                                        : std::to_string(position + 1);
                return make_error(resource,
                                  core::error_code_t::unimplemented_yet,
                                  "primary key column \"" + column + "\" of remote table \"" + name_ +
                                      "\" has a type whose value does not name a row exactly");
            }
        }
        return core::error_t::no_error();
    }

    logical_plan::storage_operator_t remote_storage_t::make_update_impl(const services::context_storage_t& context) {
        OTX_ZONE_N("federation::remote_storage::make_update");
        if (auto refused = check_row_changes(context.resource); refused.contains_error()) {
            return refused;
        }
        return operators::operator_ptr{
            new remote_row_change_t(context.resource, context.log.clone(), this, write_kind_t::update)};
    }

    logical_plan::storage_operator_t remote_storage_t::make_delete_impl(const services::context_storage_t& context) {
        OTX_ZONE_N("federation::remote_storage::make_delete");
        if (auto refused = check_row_changes(context.resource); refused.contains_error()) {
            return refused;
        }
        return operators::operator_ptr{
            new remote_row_change_t(context.resource, context.log.clone(), this, write_kind_t::delete_)};
    }

    core::error_t conform_to_result(std::pmr::memory_resource* resource,
                                    std::string_view statement,
                                    std::string_view table_name,
                                    std::span<const types::complex_logical_type> declared,
                                    std::span<const std::optional<types::complex_logical_type>> discovered,
                                    core::date::timezone_offset_t timezone,
                                    vector::data_chunk_t& chunk) {
        OTX_ZONE_N("federation::conform_to_result");
        if (chunk.column_count() != declared.size()) {
            return make_error(resource,
                              core::error_code_t::schema_error,
                              "the remote statement " + std::string{statement} + " answered " +
                                  std::to_string(chunk.column_count()) + " columns where " +
                                  std::to_string(declared.size()) +
                                  " are expected: its schema changed since it was discovered");
        }
        const auto count = chunk.size();
        for (std::size_t i = 0; i < declared.size(); ++i) {
            auto& column = chunk.data[i];
            const auto alias = declared[i].has_alias() ? declared[i].alias() : std::string{};
            if (i < discovered.size() && discovered[i] && !same_type(column.type(), *discovered[i])) {
                return make_error(resource,
                                  core::error_code_t::schema_error,
                                  "column \"" + alias + "\" of remote table \"" + std::string{table_name} +
                                      "\" arrived as type " + std::to_string(static_cast<int>(column.type().type())) +
                                      ", declared " + std::to_string(static_cast<int>(discovered[i]->type())) +
                                      ": its schema changed since it was discovered");
            }
            if (column.type() == declared[i]) {
                if (!alias.empty()) {
                    column.set_type_alias(alias);
                }
                continue;
            }
            vector::vector_t converted(chunk.resource(), declared[i], std::max<std::uint64_t>(count, 1));
            for (std::uint64_t row = 0; row < count; ++row) {
                auto value = chunk.value(i, row);
                if (value.is_null()) {
                    converted.set_null(row, true);
                    continue;
                }
                auto as_declared = convert_value(resource, value, declared[i], timezone);
                if (as_declared.has_error()) {
                    return make_error(resource,
                                      core::error_code_t::schema_error,
                                      "column " + std::to_string(i + 1) + " (" + alias + ") of the remote statement " +
                                          std::string{statement} + ": " + as_declared.error().what.c_str());
                }
                converted.set_value(row, as_declared.value());
            }
            if (!alias.empty()) {
                converted.set_type_alias(alias);
            }
            column = std::move(converted);
        }
        return core::error_t::no_error();
    }

} // namespace otterstax::federation
