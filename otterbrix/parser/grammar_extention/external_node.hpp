// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/logical_plan/node.hpp>

#include <algorithm>
#include <cctype>
#include <memory_resource>
#include <string>
#include <utility>

// Logical-plan carrier for a parsed external-table statement (CREATE EXTERNAL
// TABLE / COPY ... TO). Both the `s3` and `file` grammar-extension transforms
// lower their AST into this single node type; the query session classifies it
// (s3 vs local by location scheme, create vs copy by `op`) and routes to the
// S3 / file managers.
//
// Tagged node_type::unused so the engine never tries to execute it. Header-only (all virtuals inline) so both the
// extension static libs and otterbrix_local can use it without a link edge.
namespace otterstax::external {
    // The most parts a table name is written with: [connection.][namespace.]name, or a local
    // database's [database.][schema.]name.
    inline constexpr int max_name_parts = 3;

    // A table name of CREATE EXTERNAL TABLE as written, its parts folded to lower
    // case as the engine folds an unquoted identifier and placed as otterbrix
    // places them: `t`, `s.t`, `x.s.t` (x a connection or a local database). The
    // empty slots are the session's place. `count` is 1..max_name_parts.
    inline qualified_name_t external_target(const char* const* parts, int count) {
        auto folded = [&](int i) {
            std::string part{parts[i]};
            std::transform(part.begin(), part.end(), part.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
            return part;
        };
        qualified_name_t name;
        name.collection = core::relname_t{folded(count - 1)};
        if (count >= 2) {
            name.schema = core::schema_t{folded(count - 2)};
        }
        if (count >= 3) {
            name.database = core::dbname_t{folded(count - 3)};
        }
        return name;
    }

    enum class external_op_t
    {
        create_external_table, // CREATE EXTERNAL TABLE [[[c.]d.]s.]t WITH (location=...)
        copy_to                // COPY (<inner_sql>) TO '<location>' WITH (...)
    };

    class external_node_t final : public components::logical_plan::node_t {
    public:
        external_node_t(std::pmr::memory_resource* resource,
                        external_op_t op,
                        qualified_name_t target,
                        std::string location,
                        std::string s3_alias,
                        std::string format,
                        std::string inner_sql)
            : components::logical_plan::node_t(resource, components::logical_plan::node_type::unused)
            , op_(op)
            , target_(std::move(target))
            , location_(std::move(location))
            , s3_alias_(std::move(s3_alias))
            , format_(std::move(format))
            , inner_sql_(std::move(inner_sql)) {}

        external_op_t op() const noexcept { return op_; }

        // s3:// locations belong to the s3 manager; everything else is a local file.
        bool is_s3() const noexcept { return location_.rfind("s3://", 0) == 0; }

        // CREATE EXTERNAL TABLE: the table loaded into, as written (external_target).
        const qualified_name_t& target() const noexcept { return target_; }
        const std::string& location() const noexcept { return location_; }
        const std::string& s3_alias() const noexcept { return s3_alias_; }
        const std::string& format() const noexcept { return format_; }
        const std::string& inner_sql() const noexcept { return inner_sql_; }

        // "s3://bucket/key" -> "bucket/key" (the path form Arrow's S3FileSystem
        // and conn::s3::ConnectorManager expect); local paths pass through.
        std::string object_path() const { return is_s3() ? location_.substr(5) : location_; }

    private:
        components::logical_plan::hash_t hash_impl() const final { return 0; }
        std::string to_string_impl() const final { return "external_table"; }

        external_op_t op_;
        qualified_name_t target_;
        std::string location_;
        std::string s3_alias_;
        std::string format_;
        std::string inner_sql_;
    };

    using external_node_ptr = boost::intrusive_ptr<external_node_t>;

    inline external_node_ptr make_external_node(std::pmr::memory_resource* resource,
                                                external_op_t op,
                                                qualified_name_t target,
                                                std::string location,
                                                std::string s3_alias,
                                                std::string format,
                                                std::string inner_sql) {
        return external_node_ptr{new external_node_t(resource,
                                                     op,
                                                     std::move(target),
                                                     std::move(location),
                                                     std::move(s3_alias),
                                                     std::move(format),
                                                     std::move(inner_sql))};
    }
} // namespace otterstax::external
