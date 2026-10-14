// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/federation/provider.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"

#include <components/logical_plan/table_storage.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/date/date_types.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <vector>

// The engine-side face of one remote table for one statement (components/logical_plan/
// table_storage.hpp): decide makes it, the statement's resolves own it, it dies with the statement.
//
// - make_scan reads the whole table — `SELECT <declared columns> FROM <table>` on the backend through
//   the provider registry, the rows typed by the input translators — and numbers every row it gives
//   out (the row id the update and delete sinks get back). What the pushdown rule
//   (rules/pushdown.hpp) does not turn into one remote statement runs in the engine over this scan.
// - make_insert buffers the rows the engine hands it (full rows, declared order, already cast) and
//   sends them as INSERT ... VALUES, one statement per chunk, naming only the columns the statement
//   wrote (set by the rule; every column otherwise), so the backend's defaults fill the rest.
// - make_update / make_delete change the rows the scan numbered, one remote statement per row, the
//   row named by its primary key as the scan read it (sql_gen::generate_update_row /
//   generate_delete_row); each statement must change exactly one row, or the statement fails
//   (write_conflict when no row holds the key any more). Only the key is compared: a change to the
//   row's other columns on the backend between the read and the write is overwritten (lost
//   update). A table without a discovered primary key (or with a key column no literal names
//   exactly), and every ClickHouse table, refuse both: only the rule takes an UPDATE / DELETE there.
//
// Writes run at once, in the backend's autocommit, inside an explicit transaction as well (a
// ROLLBACK does not undo them: the engine has no storage transaction hooks yet).
namespace otterstax::federation {

    // Everything decide knows about a remote table: the server, the engine-facing key, the backend's
    // spelling, the declared columns (alias = the engine column name, lower case) and the positions
    // of its primary key's columns among them, in key order (empty: no primary key discovered).
    struct remote_table_t {
        server_t server;
        table_key_t key;
        remote_table_name_t remote;
        std::vector<std::string> remote_columns;
        std::vector<components::types::complex_logical_type> columns;
        std::vector<std::size_t> key_columns;
    };

    // The name the federation's messages use: uid.database.table (MySQL, ClickHouse) or
    // uid.schema.table (PostgreSQL), lower case.
    std::string display_name(const table_key_t& key);

    // The owner tag every federation storage carries (table_storage_t::owner()): the rule takes a
    // table by it.
    const void* storage_owner() noexcept;

    class remote_storage_t final : public components::logical_plan::table_storage_t {
    public:
        // `resource` is the one core::pmr::make_polymorphic_unique allocates the storage on.
        remote_storage_t(std::pmr::memory_resource* resource, remote_table_t table);

        const remote_table_t& table() const noexcept { return table_; }
        const std::string& name() const noexcept { return name_; }
        const std::string& scan_sql() const noexcept { return scan_sql_; }
        backend_type_t dialect() const noexcept;
        // The table as the generator names it, backend spelling.
        const qualified_name_t& generator_table() const noexcept { return generator_table_; }
        // engine column name -> backend spelling and declared type.
        const sql_gen::column_map_t& column_map() const noexcept { return column_map_; }

        scan_request_t make_scan_request() const;

        // The columns an INSERT names (positions in the declared order); empty: every column. Set by
        // the pushdown rule from the statement's column list, before the plan is built.
        void set_insert_columns(std::vector<std::size_t> columns) { insert_columns_ = std::move(columns); }
        const std::vector<std::size_t>& insert_columns() const noexcept { return insert_columns_; }

        // Row numbers. The scan numbers `count` rows starting at `offset` of chunk `chunk` of `rows`
        // and answers the first number (the rest follow it); row_values answers what the scan read
        // for a number, every declared column.
        std::int64_t number_rows(const std::shared_ptr<scan_rows_t>& rows,
                                 std::size_t chunk,
                                 std::uint64_t offset,
                                 std::uint64_t count);
        core::result_wrapper_t<std::vector<components::types::logical_value_t>>
        row_values(std::pmr::memory_resource* resource, std::int64_t number) const;

    private:
        components::logical_plan::storage_operator_t
        make_scan_impl(const services::context_storage_t& context) override;
        components::logical_plan::storage_operator_t
        make_insert_impl(const services::context_storage_t& context) override;
        components::logical_plan::storage_operator_t
        make_update_impl(const services::context_storage_t& context) override;
        components::logical_plan::storage_operator_t
        make_delete_impl(const services::context_storage_t& context) override;

        // Why this table's rows cannot be changed by number (ClickHouse, no primary key, a key column
        // whose value names no row exactly), or no_error.
        core::error_t check_row_changes(std::pmr::memory_resource* resource) const;

        struct numbered_range_t {
            std::int64_t first;
            std::shared_ptr<scan_rows_t> rows;
            std::size_t chunk;
            std::uint64_t offset;
            std::uint64_t count;
        };

        remote_table_t table_;
        std::string name_;
        std::string scan_sql_;
        qualified_name_t generator_table_;
        sql_gen::column_map_t column_map_;
        std::vector<std::size_t> insert_columns_;
        mutable std::mutex numbers_mutex_;
        std::vector<numbered_range_t> numbered_;
        std::int64_t next_number_{0};
    };

    // The rows a backend answered for a statement the pushdown rule made, held to the types the
    // engine gave the replaced subtree's output (an integer sum is BIGINT, avg DOUBLE, count(*)
    // UBIGINT): a column of another type is converted value by value — numbers between their types,
    // DECIMAL scaled, the decimal text MySQL answers a SUM / AVG of an integer in parsed — and named
    // by the declared column name. A column count that differs, or a value with no conversion, is
    // schema_error naming the statement.
    //
    // `discovered[i]`, when set, is the discovered type of the table column output i reads as it
    // is (a plain column reference): such a column answered as another type means the remote table
    // changed, schema_error "... changed since it was discovered" (which the session's rediscovery
    // reads; the storage's scan holds its rows to the declared columns this way). Empty span: no
    // output is a plain column.
    core::error_t conform_to_result(std::pmr::memory_resource* resource,
                                    std::string_view statement,
                                    std::string_view table_name,
                                    std::span<const components::types::complex_logical_type> declared,
                                    std::span<const std::optional<components::types::complex_logical_type>> discovered,
                                    core::date::timezone_offset_t timezone,
                                    components::vector::data_chunk_t& chunk);

} // namespace otterstax::federation
