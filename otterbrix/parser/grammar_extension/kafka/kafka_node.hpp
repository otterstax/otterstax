// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/logical_plan/node.hpp>
#include <components/types/types.hpp>

namespace kafka_grammar {
    struct kafka_stmt;
} // namespace kafka_grammar

namespace components::logical_plan {
    class node_aggregate_t;
} // namespace components::logical_plan

namespace otterstax::kafka {
    // The Kafka objects (a SOURCE's backing table, the name an INSERT into a
    // SOURCE or STREAM is written with) live in the schema `kafka` of otterbrix's
    // default database `main`: `kafka.x` from the default place main.public,
    // `main.kafka.x` in full.
    inline constexpr const char* KAFKA_DATABASE_NAME = "main";
    inline constexpr const char* KAFKA_SCHEMA_NAME = "kafka";
    // Kafka's bookkeeping (`__sources`, `<source>__offsets`) lives in the schema
    // `kafka` of OtterStax's service database `federation`
    // (otterbrix/federation/metadata.hpp): hidden from the listings, owned by
    // OtterStax, never a name a client writes.
    inline constexpr const char* KAFKA_SERVICE_DATABASE_NAME = "federation";
    inline constexpr const char* KAFKA_SOURCES_TABLE = "__sources";

    // main.kafka.<relname>: the table of a Kafka object.
    inline qualified_name_t kafka_table_name(std::string relname) {
        return qualified_name_t{core::dbname_t{std::string{KAFKA_DATABASE_NAME}},
                                core::schema_t{std::string{KAFKA_SCHEMA_NAME}},
                                core::relname_t{std::move(relname)}};
    }

    // federation.kafka.<relname>: a bookkeeping table.
    inline qualified_name_t kafka_service_table_name(std::string relname) {
        return qualified_name_t{core::dbname_t{std::string{KAFKA_SERVICE_DATABASE_NAME}},
                                core::schema_t{std::string{KAFKA_SCHEMA_NAME}},
                                core::relname_t{std::move(relname)}};
    }

    // federation.kafka.<source>__offsets: the consumed positions of a SOURCE.
    inline qualified_name_t kafka_offsets_table_name(const std::string& source) {
        return kafka_service_table_name(source + "__offsets");
    }

    enum class kafka_op
    {
        create_source,
        create_stream,
        drop_source,
        drop_stream
    };

    struct kafka_column_t {
        std::string name;
        components::types::complex_logical_type type;
    };

    // Carrier logical_plan node for one parsed kafka DDL statement
    //
    // It is tagged components::logical_plan::node_type::unused and is NEVER
    // handed to the otterbrix engine. The query session recognises it on the plan
    // root (ParsedQueryData::extension_kind) and routes it to the kafka runtime
    class kafka_node_t final : public components::logical_plan::node_t {
    public:
        kafka_node_t(std::pmr::memory_resource* resource, kafka_op op, std::string name);

        kafka_op op() const noexcept { return op_; }
        const std::string& name() const noexcept { return name_; }

        std::vector<kafka_column_t>& columns() noexcept { return columns_; }
        const std::vector<kafka_column_t>& columns() const noexcept { return columns_; }

        // WITH (...) options. Keys are stored upper-cased; use option() to read
        std::unordered_map<std::string, std::string>& options() noexcept { return options_; }
        const std::unordered_map<std::string, std::string>& options() const noexcept { return options_; }
        std::optional<std::string> option(std::string_view key) const;

        const std::string& as_select() const noexcept { return as_select_; }
        void set_as_select(std::string sql) { as_select_ = std::move(sql); }

        bool if_exists() const noexcept { return if_exists_; }
        void set_if_exists(bool value) noexcept { if_exists_ = value; }

        // DROP ... CASCADE; false is RESTRICT, the default when neither is written
        bool cascade() const noexcept { return cascade_; }
        void set_cascade(bool value) noexcept { cascade_ = value; }

    private:
        components::expressions::hash_t hash_impl() const final;
        std::string to_string_impl() const final;

        kafka_op op_;
        std::string name_;
        std::vector<kafka_column_t> columns_;
        std::unordered_map<std::string, std::string> options_;
        std::string as_select_;
        bool if_exists_ = false;
        bool cascade_ = false;
    };

    using kafka_node_ptr = boost::intrusive_ptr<kafka_node_t>;

    // Maps a (case-insensitive) SQL type keyword to an otterbrix logical type.
    // Only the JSON-ingestible types are accepted (INTEGER, BIGINT, DOUBLE, BOOLEAN,
    // STRING_LITERAL); std::nullopt for anything else, which CREATE rejects
    std::optional<components::types::complex_logical_type> map_column_type(std::string_view type_name);

    // Lowers a parsed kafka AST statement into a kafka_node_t built on `resource`
    // (the durable transformer resource, not the parse arena). Every column type
    // must already have been validated by kafka_ext::parse via map_column_type
    kafka_node_ptr lower_to_node(std::pmr::memory_resource* resource, const kafka_grammar::kafka_stmt& stmt);

    // An INSERT that may write a Kafka object: its target as written and the
    // insert's source subplan (the rows to publish — node_raw_data for VALUES or a
    // SELECT subplan), to be executed and produced to the object's topic
    //
    // `source_is_select` distinguishes the two ksqlDB INSERT forms:
    //   - false: `INSERT ... VALUES`  → source is a node_raw_data (one-shot produce)
    //   - true:  `INSERT ... SELECT`  → source is a SELECT subplan; this is a
    //            continuous "INSERT INTO query" (persistent fan-in into an existing
    //            stream), not a one-shot snapshot
    struct kafka_write_t {
        qualified_name_t target;
        components::logical_plan::node_ptr source;
        bool source_is_select = false;

        const std::string& relname() const noexcept { return target.collection.t; }
    };

    // If `root` is a plain-SQL INSERT whose target, as written, can name a Kafka
    // object — no connection, its database (when written) `main`, its schema
    // (when written) `kafka` — return its target + source; else std::nullopt.
    // Whether it does is told by kafka_target_named once the session's place
    // fills the slots the name leaves out. The transformer wraps a write as a
    // sequence_t whose *direct* children are the target catalog_resolve (kind ==
    // table) and the insert_t (the source's own resolve nodes sit deeper, under the
    // insert), so only root + its direct children are inspected — `INSERT INTO
    // kafka.a SELECT FROM kafka.b` resolves to `a`.
    std::optional<kafka_write_t> kafka_write_target(const components::logical_plan::node_ptr& root);

    // Whether `written`, its empty database / schema slots filled from the
    // session's place (`database`, `schema`: current_database(), current_schema()),
    // is main.kafka.<relname>. A name written with its database needs no place.
    bool kafka_target_named(const qualified_name_t& written, std::string_view database, std::string_view schema);

    // A compiled `CREATE STREAM ... AS <select>` source binding: the source table
    // name (the SELECT's FROM) and the SELECT's operator nodes (the aggregate's
    // children — match/select/group/...), to be re-wrapped per batch as
    // `aggregate(empty) + [node_raw_data(batch), <operators>]`.
    struct kafka_stream_plan_t {
        std::string source_relname;
        std::vector<components::logical_plan::node_ptr> operators;
    };

    // Find the first aggregate_t in a parsed SELECT plan and return its source
    // relname + operator children; std::nullopt if there is no aggregate. EVERY
    // child is carried: the SELECT list rides in the group_t and the select_t is
    // what the validator moves it into when the query is not grouped — an operator
    // left out changes the projection. The operators are RE-HOMED to an empty
    // dbname/relname (rebuilt, reusing their expressions) so they chain over a
    // node_raw_data substitution — a parsed operator keeps its source table's
    // relname, which otherwise stops it from picking up the raw_data rows and
    // makes every batch resolve a table the swapped plan never reads.
    // (match/select/group and the ORDER BY / LIMIT / HAVING tail are rebuilt — every
    // operator type the transformer names the source table on; any other type is
    // returned as-is.)
    std::optional<kafka_stream_plan_t> kafka_stream_source(std::pmr::memory_resource* resource,
                                                           const components::logical_plan::node_ptr& root);

    // The first aggregate_t node in a parsed SELECT plan (the SELECT body), or
    // nullptr. Its target's relname is the SELECT's source table: the Kafka
    // object a STREAM or an INSERT INTO query reads
    const components::logical_plan::node_aggregate_t*
    kafka_find_aggregate(const components::logical_plan::node_ptr& root);

} // namespace otterstax::kafka
