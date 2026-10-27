// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
#pragma once

#include <components/logical_plan/node.hpp>

#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>

// Logical-plan carrier for a parsed CREATE EXTERNAL TABLE / COPY ... TO. Both the `s3` and `file`
// grammar extensions lower their AST into it; the Scheduler classifies it (s3 or local by location
// scheme, create or copy by `op`) and runs it on the matching transfer pool.
// Tagged node_type::unused so the engine never executes it. Header-only so the extension libs and
// otterbrix_local use it without a link edge.
namespace otterstax::external {

    enum class external_op_t {
        create_external_table, // CREATE EXTERNAL TABLE <db>.<table> WITH (location=...)
        copy_to                // COPY (<inner_sql>) TO '<location>' WITH (...)
    };

    class external_node_t final : public components::logical_plan::node_t {
    public:
        // Every field is copied onto `resource`, the plan's.
        external_node_t(std::pmr::memory_resource* resource,
                        external_op_t op,
                        std::string_view database,
                        std::string_view table,
                        std::string_view location,
                        std::string_view s3_alias,
                        std::string_view format,
                        std::string_view inner_sql)
            : components::logical_plan::node_t(resource, components::logical_plan::node_type::unused)
            , op_(op)
            , database_(database, resource)
            , table_(table, resource)
            , location_(location, resource)
            , s3_alias_(s3_alias, resource)
            , format_(format, resource)
            , inner_sql_(inner_sql, resource) {}

        external_op_t op() const noexcept { return op_; }

        // s3:// locations belong to the s3 manager; everything else is a local file.
        bool is_s3() const noexcept { return location_.starts_with("s3://"); }

        std::string_view database() const noexcept { return database_; }
        std::string_view table() const noexcept { return table_; }
        std::string_view location() const noexcept { return location_; }
        std::string_view s3_alias() const noexcept { return s3_alias_; }
        std::string_view format() const noexcept { return format_; }
        std::string_view inner_sql() const noexcept { return inner_sql_; }

        // "s3://bucket/key" -> "bucket/key" (the path form Arrow's S3FileSystem
        // and conn::s3::driver_t expect); local paths pass through. A view into location().
        std::string_view object_path() const noexcept {
            return is_s3() ? location().substr(5) : location();
        }

    private:
        components::logical_plan::hash_t hash_impl() const final { return 0; }
        std::string to_string_impl() const final { return "external_table"; }

        external_op_t op_;
        std::pmr::string database_;
        std::pmr::string table_;
        std::pmr::string location_;
        std::pmr::string s3_alias_;
        std::pmr::string format_;
        std::pmr::string inner_sql_;
    };

    using external_node_ptr = boost::intrusive_ptr<external_node_t>;

    inline external_node_ptr make_external_node(std::pmr::memory_resource* resource,
                                                external_op_t op,
                                                std::string_view database,
                                                std::string_view table,
                                                std::string_view location,
                                                std::string_view s3_alias,
                                                std::string_view format,
                                                std::string_view inner_sql) {
        return external_node_ptr{
            new external_node_t(resource, op, database, table, location, s3_alias, format, inner_sql)};
    }

} // namespace otterstax::external
