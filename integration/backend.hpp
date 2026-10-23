// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/discovery.hpp"
#include "connectors/common/connection_pool.hpp"
#include "connectors/common/wait.hpp"
#include "integration/prepare_probe.hpp"
#include "otterbrix/parser/parser.hpp"
#include "otterbrix/query_generation/sql_query_generator.hpp"
#include "otterbrix/schema/schema_utils.hpp"
#include "otterbrix/translators/input/chunk_windows.hpp"
#include "types/otterbrix.hpp"
#include "utility/logger.hpp"
#include "utility/session.hpp"
#include "utility/tracy_profiler.hpp"

#include <actor-zeta.hpp>
#include <components/log/log.hpp>
#include <core/result_wrapper.hpp>

#include <cassert>
#include <memory_resource>
#include <string>
#include <utility>

namespace db {

    // The text of column `col` of every row of `chunk` — how discovery reads the catalog
    // listings (table names, system.columns) out of a decoded answer. A chunk without that
    // column, a column that is not text, or a NULL is `code`, naming `what`.
    inline core::result_wrapper_t<std::vector<std::string>> read_text_column(std::pmr::memory_resource* resource,
                                                                             const components::vector::data_chunk_t& chunk,
                                                                             std::size_t col,
                                                                             core::error_code_t code,
                                                                             const std::string& what) {
        const auto refuse = [&](const std::string& why) {
            return core::error_t(code, std::pmr::string{(what + ": " + why).c_str(), resource});
        };
        std::vector<std::string> out;
        if (chunk.size() == 0) {
            return out;
        }
        if (chunk.column_count() <= col) {
            return refuse("the answer has " + std::to_string(chunk.column_count()) + " column(s)");
        }
        if (chunk.types()[col].type() != components::types::logical_type::STRING_LITERAL) {
            return refuse("column " + std::to_string(col) + " is not text");
        }
        out.reserve(chunk.size());
        for (uint64_t row = 0; row < chunk.size(); ++row) {
            auto value = chunk.value(col, row);
            if (value.is_null()) {
                return refuse("row " + std::to_string(row) + " is NULL");
            }
            out.emplace_back(value.value<std::string_view>());
        }
        return out;
    }

    // The part of the query pipeline that reaches one remote database: generate each plan
    // slot's statement, run it on the connection pool, splice the answers into the plan.
    // One template for MySQL, PostgreSQL and ClickHouse; Traits holds what differs.
    //
    // Not an actor. Every Worker owns one of each, the catalog owns one of each for
    // discovery, and their coroutines run inside the owner's behavior on the owner's thread,
    // so awaiting a statement suspends the owner instead of blocking a thread, and two
    // Workers' statements run on two pool connections at once. actor-zeta allocates every
    // coroutine frame of this object on resource().
    //
    // Traits provides
    //   static constexpr backend_type_t type;
    //   static constexpr const char* name;          // in messages: "MySQL", "PostgreSQL", "ClickHouse"
    //   static constexpr std::string_view log_tag;
    //   using metadata_t;                            // decode metadata discovery fills
    //   static core::error_t check(const metadata_t&, const std::string& uid, std::pmr::memory_resource*);
    //   static conn::statement_t slot_statement(node_type slot, std::string sql, const metadata_t&,
    //                                           const std::string& uid, const std::string& table);
    //   static core::result_wrapper_t<data_chunk_t> slot_chunk(std::pmr::memory_resource*, node_type slot,
    //                                                          conn::statement_result_t answer,
    //                                                          const std::string& uid);
    //   static conn::statement_t probe_statement(std::string probe, const metadata_t&,
    //                                            const std::string& uid, const std::string& table);
    //   static std::string no_header(const std::string& probe); // describe's schema_error text
    //   static unique_future<result_wrapper_t<discovered_tables_t>> discover(backend_t&, qualified_name_t);
    template<typename Traits>
    class backend_t {
    public:
        using metadata_t = typename Traits::metadata_t;
        using data_chunk_t = components::vector::data_chunk_t;
        using node_type = components::logical_plan::node_type;

        // The statement one slot sends and the base table it reads, whose named types shape
        // its result columns (ClickHouse) — shared by execute (which runs it) and describe
        // (which probes its header), so both see one text.
        struct slot_statement_t {
            std::string sql;
            std::string table;
        };

        // A statement sent to the pool, with the ticket its owner waits on (null without an
        // owner).
        struct sent_t {
            conn::statement_future_t answer;
            conn::wait_ticket_ptr ticket;
        };

        // `pool` and `metadata` are owned by ComponentManager (or a test fixture) and
        // outlive every backend built on them. `owner` parks while a statement of this
        // backend is in flight and is woken by its answer — a Worker; an owner that polls
        // (the catalog) passes none (conn::wait_owner_t).
        backend_t(std::pmr::memory_resource* resource,
                  conn::connection_pool_t* pool,
                  metadata_t* metadata,
                  conn::wait_owner_t* owner = nullptr)
            : resource_(resource)
            , pool_(pool)
            , metadata_(metadata)
            , owner_(owner)
            , log_(get_logger(Traits::log_tag)) {
            assert(resource_ != nullptr);
            assert(pool_ != nullptr);
            assert(metadata_ != nullptr);
        }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        conn::connection_pool_t& pool() const noexcept { return *pool_; }
        metadata_t& metadata() const noexcept { return *metadata_; }
        const log_t& log() const noexcept { return log_; }

        // Sends one statement to the alias's pool; the owner is woken when it is answered.
        sent_t send(std::string_view uid, conn::statement_t statement) const {
            auto watched = conn::watch(owner_);
            return sent_t{pool_->execute(uid, std::move(statement), std::move(watched.waker)),
                          std::move(watched.ticket)};
        }

        // Awaits a sent statement's answer, telling the owner what it suspends on so it can
        // park instead of being polled.
        actor_zeta::unique_future<conn::statement_outcome_t> answer(sent_t sent) {
            conn::suspended_on_t suspended(owner_, sent.ticket, sent.answer.is_ready());
            auto outcome = co_await std::move(sent.answer);
            co_return outcome;
        }

        // send + answer: one statement, awaited at once.
        actor_zeta::unique_future<conn::statement_outcome_t> run(std::string_view uid, conn::statement_t statement) {
            return answer(send(uid, std::move(statement)));
        }

        core::error_t make_error(core::error_code_t code, const std::string& what) const {
            return core::error_t(code, std::pmr::string{what.c_str(), resource_});
        }

        // A statement sent for one slot of a batch, waiting for its answer.
        struct in_flight_t {
            std::size_t index; // the slot's position in its batch
            node_type slot;    // the slot's node type when it was sent
            sent_t sent;
        };
        using in_flight_list_t = std::pmr::vector<in_flight_t>;

        // Runs every slot of the statement this backend owns and replaces it with the rows
        // it answered (node_raw_data, cut to the engine's chunk bound). Batches run back to
        // front, innermost dependencies first; a batch's statements are all sent before the
        // first answer is awaited, so they run side by side on the alias's pool. For a Mixed
        // statement the slots of the other backends are left alone (node_backend_types);
        // backend_set_t::execute_mixed runs all three backends batch by batch instead.
        actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>> execute(session_hash_t id,
                                                                                      ParsedQueryDataPtr data) {
            assert(data);
            assert(data->otterbrix_params);
            for (std::size_t batch = data->otterbrix_params->external_nodes.size(); batch-- > 0;) {
                auto sent = send_batch(id, *data, batch);
                if (sent.has_error()) {
                    co_return sent.template convert_error<ParsedQueryDataPtr>();
                }
                auto answered = co_await answer_batch(*data, batch, std::move(sent.value()));
                if (answered.contains_error()) {
                    co_return std::move(answered);
                }
            }
            co_return std::move(data);
        }

        // Sends the statement of every slot of `batch` this backend owns; answers nothing yet.
        // A slot already holding rows (data_t) was answered by another backend. Refuses the
        // whole batch — before anything is sent — when a slot's connection or metadata is
        // missing or its SQL cannot be generated; a slot left symbolic would die later in
        // the engine with a misleading "database does not exist".
        core::result_wrapper_t<in_flight_list_t> send_batch(session_hash_t id, ParsedQueryData& data, std::size_t batch) {
            OTX_ZONE_N("db::backend::dispatch");
            const auto* parameters = &data.otterbrix_params->params_node->parameters();
            auto& nodes = data.otterbrix_params->external_nodes[batch];
            log_->debug("execute {}: batch {} of {} slot(s), session {}", Traits::name, batch, nodes.size(), id);

            // Generated first, sent after: a refusal leaves nothing running.
            struct ready_t {
                std::size_t index;
                node_type slot;
                std::string uid;
                conn::statement_t statement;
            };
            std::pmr::vector<ready_t> ready(resource_);
            for (std::size_t i = 0; i < nodes.size(); ++i) {
                auto& node = *nodes[i].node;
                const auto& target = nodes[i].target;
                const auto& uid = target.name.unique_identifier;
                if (node->type() == node_type::data_t || !owns(data, uid)) {
                    continue;
                }
                if (!pool_->contains(uid)) {
                    log_->error("execute: no {} connection registered for UID: {}", Traits::name, uid);
                    return make_error(core::error_code_t::do_not_exists,
                                      std::string{"no "} + Traits::name + " connection registered for '" + uid + "'");
                }
                if (auto refused = Traits::check(*metadata_, uid, resource_); refused.contains_error()) {
                    return refused;
                }
                auto generated = generate_slot_statement(node, target, nodes, parameters);
                if (generated.has_error()) {
                    log_->error("execute: SQL generation failed: {}", generated.error().what.c_str());
                    return generated.template convert_error<in_flight_list_t>();
                }
                auto& statement = generated.value();
                log_->debug("execute: {} statement: \"{}\"", Traits::name, statement.sql);
                auto slot_statement =
                    Traits::slot_statement(node->type(), std::move(statement.sql), *metadata_, uid, statement.table);
                if (!is_read(node->type())) {
                    slot_statement = std::move(slot_statement).once();
                }
                ready.push_back(ready_t{i, node->type(), uid, std::move(slot_statement)});
            }
            in_flight_list_t in_flight(resource_);
            in_flight.reserve(ready.size());
            for (auto& r : ready) {
                in_flight.push_back(in_flight_t{r.index, r.slot, send(r.uid, std::move(r.statement))});
            }
            return std::move(in_flight);
        }

        // Awaits the answers send_batch sent, in slot order, and splices each into its slot.
        // The first failed answer is the result; the statements still running are dropped —
        // the pool finishes them and their answers are discarded. Nothing they touch lives
        // in this frame.
        actor_zeta::unique_future<core::error_t>
        answer_batch(ParsedQueryData& data, std::size_t batch, in_flight_list_t in_flight) {
            auto& nodes = data.otterbrix_params->external_nodes[batch];
            for (auto& sent : in_flight) {
                auto outcome = co_await answer(std::move(sent.sent));
                if (outcome.has_error()) {
                    log_->error("execute: backend query failed: {}", outcome.error().what.c_str());
                    co_return std::move(outcome.error());
                }
                // No zone spans a co_await: the owning Worker resumes on any scheduler
                // thread (CLAUDE.md, "Tracy zones in coroutines"). This one ends before the
                // next answer is awaited.
                OTX_ZONE_N("db::backend::to_chunk");
                auto& slot = *nodes[sent.index].node;
                auto chunk = Traits::slot_chunk(resource_,
                                                sent.slot,
                                                std::move(outcome.value()),
                                                nodes[sent.index].target.name.unique_identifier);
                if (chunk.has_error()) {
                    log_->error("execute: {}", chunk.error().what.c_str());
                    co_return std::move(chunk.error());
                }
                // The slot being replaced is still typed: if it IS the statement's own
                // DML node, the whole statement ran on the backend and the chunk's
                // cardinality is its affected-row count. Record it before the slot
                // becomes a data_t.
                capture_remote_dml_count(*data.otterbrix_params, slot, chunk.value());
                // The engine takes at most DEFAULT_VECTOR_CAPACITY rows per chunk: a wider
                // result goes in as a run of such chunks (a count carrier stays whole).
                slot = components::logical_plan::make_node_raw_data(
                    resource_,
                    tsl::split_to_capacity(resource_, std::move(chunk.value())));
            }
            co_return core::error_t::no_error();
        }

        // The prepared schema of the statement's slots this backend owns, answered by the
        // backend: for every schema_node_t stub the statement execute would run for it is
        // wrapped as a LIMIT 0 probe (prepare_probe.hpp) and run, and the stub takes the
        // STRUCT of the answer's columns — decoded by the translator execute reads rows
        // with, so the prepared types are the executed chunk's by construction. A raw-SQL
        // stub is filled in place, so its text and qualifiers survive for execute. Nothing
        // is cached: every prepare asks the backend.
        actor_zeta::unique_future<core::result_wrapper_t<ParsedQueryDataPtr>> describe(session_hash_t id,
                                                                                       ParsedQueryDataPtr data) {
            assert(data);
            assert(data->otterbrix_params);
            {
                // No zone spans a co_await: the owning Worker resumes on any scheduler thread
                // (CLAUDE.md, "Tracy zones in coroutines").
                OTX_ZONE_N("db::backend::describe");
                log_->debug("describe {} started, session {}", Traits::name, id);
                // An extension root is `unused` like a stub; the Worker keeps it away from here.
                if (data->extension_kind != extension_kind_t::none) {
                    co_return make_error(core::error_code_t::invalid_parameter,
                                         std::string{"describe: an extension statement has no "} + Traits::name +
                                             " result schema");
                }
                // The binder holds every parameter of a parameterized statement, literals
                // included, until finalize: before Bind there is no SQL to probe with, and a
                // type read off the plan instead would be the guess this exists to replace.
                if (const std::size_t unbound = data->otterbrix_params->parameters_count; unbound > 0) {
                    co_return make_error(core::error_code_t::unimplemented_yet,
                                         std::string{"the result schema of a "} + Traits::name + " statement with " +
                                             std::to_string(unbound) +
                                             " unbound parameter(s) is known only once they are bound");
                }
            }

            const auto* parameters = &data->otterbrix_params->params_node->parameters();
            auto& batches = data->otterbrix_params->external_nodes;
            for (std::size_t batch = batches.size(); batch-- > 0;) {
                auto& nodes = batches[batch];
                for (std::size_t i = 0; i < nodes.size(); ++i) {
                    auto& node = *nodes[i].node;
                    const auto& target = nodes[i].target;
                    const auto& uid = target.name.unique_identifier;
                    // Only a stub carries a schema the backend can fill.
                    if (node->type() != node_type::unused || !owns(*data, uid)) {
                        continue;
                    }
                    auto& stub = static_cast<schema_utils::schema_node_t&>(*node);
                    std::string probe;
                    std::string table;
                    {
                        OTX_ZONE_N("db::backend::describe_slot"); // ends before the probe is awaited
                        if (!pool_->contains(uid)) {
                            log_->error("describe: no {} connection registered for UID: {}", Traits::name, uid);
                            co_return make_error(core::error_code_t::do_not_exists,
                                                 std::string{"no "} + Traits::name + " connection registered for '" +
                                                     uid + "'");
                        }
                        if (auto refused = Traits::check(*metadata_, uid, resource_); refused.contains_error()) {
                            co_return std::move(refused);
                        }
                        auto generated = generate_slot_statement(node, target, nodes, parameters);
                        if (generated.has_error()) {
                            log_->error("describe: SQL generation failed: {}", generated.error().what.c_str());
                            co_return generated.template convert_error<ParsedQueryDataPtr>();
                        }
                        // The wrap, its cost and what it cannot carry: prepare_probe.hpp.
                        probe = make_prepare_probe(generated.value().sql, Traits::type);
                        table = std::move(generated.value().table);
                        log_->debug("describe: probing \"{}\"", probe);
                    }
                    auto answer = co_await run(uid, Traits::probe_statement(probe, *metadata_, uid, table));
                    if (answer.has_error()) {
                        log_->error("describe: probe failed for UID {}: {}", uid, answer.error().what.c_str());
                        co_return answer.template convert_error<ParsedQueryDataPtr>();
                    }
                    if (!answer.value().schema) {
                        log_->error("describe: probe answered without columns: \"{}\"", probe);
                        co_return make_error(core::error_code_t::schema_error, Traits::no_header(probe));
                    }
                    // Filled in place rather than rebuilt: a raw-SQL stub keeps the text and
                    // qualifiers execute generates its statement from.
                    stub.set_schema(std::move(*answer.value().schema));
                }
            }
            co_return std::move(data);
        }

        // Discovery differs too much between the backends to share: each Traits writes its
        // own. Sent by the catalog (add_connection_schema) at startup.
        actor_zeta::unique_future<core::result_wrapper_t<catalog_ext::discovered_tables_t>>
        discover(qualified_name_t scope) {
            return Traits::discover(*this, std::move(scope));
        }

        // Whether a slot of this type only reads, so the driver may repeat its statement after
        // a stale connection lost the answer. Everything else — INSERT/UPDATE/DELETE, every
        // CREATE/DROP, a bare function call — may already have been applied when the answer
        // was lost, and is sent once. A stub (unused) is a raw SELECT subquery or the
        // catalog's aggregate.
        static constexpr bool is_read(node_type slot) noexcept {
            switch (slot) {
                case node_type::aggregate_t:
                case node_type::alias_t:
                case node_type::group_t:
                case node_type::intersect_t:
                case node_type::join_t:
                case node_type::limit_t:
                case node_type::match_t:
                case node_type::sort_t:
                case node_type::union_t:
                case node_type::unused:
                    return true;
                default:
                    return false;
            }
        }

        // Whether the slot of `uid` is this backend's: every slot of a single-backend
        // statement, the slots node_backend_types attributes to it in a Mixed one.
        bool owns(const ParsedQueryData& data, const std::string& uid) const {
            if (data.backend_type != backend_type_t::Mixed) {
                return true;
            }
            auto it = data.node_backend_types.find(uid);
            return it == data.node_backend_types.end() || it->second == Traits::type;
        }

        // What the slot holds decides the text: a raw-SQL subquery stub is its text with the
        // qualifiers rewritten for the backend, the catalog's aggregate stub and a plan node
        // are generated. The base table is the target's, or a raw subquery's first qualifier.
        core::result_wrapper_t<slot_statement_t>
        generate_slot_statement(const components::logical_plan::node_ptr& node,
                                const otterstax::names::resolved_target_t& target,
                                const std::pmr::vector<external_entry_t>& batch,
                                const components::logical_plan::storage_parameters* parameters) const {
            OTX_ZONE_N("db::backend::generate_slot_statement");
            slot_statement_t statement;
            statement.table = target.name.collection;
            if (node->type() == node_type::unused) {
                auto& stub = static_cast<schema_utils::schema_node_t&>(*node);
                if (stub.has_raw_sql()) {
                    auto rendered = sql_gen::replace_qualifiers(stub.raw_sql(), stub.qualifiers(), Traits::type, resource_);
                    if (rendered.has_error()) {
                        return rendered.template convert_error<slot_statement_t>();
                    }
                    statement.sql = std::move(rendered.value());
                    if (!stub.qualifiers().empty()) {
                        statement.table = stub.qualifiers().front().name.collection;
                    }
                    return std::move(statement);
                }
                auto generated =
                    sql_gen::generate_query(stub.agg_node(), parameters, Traits::type, target, batch, resource_);
                if (generated.has_error()) {
                    return generated.template convert_error<slot_statement_t>();
                }
                statement.sql = std::move(generated.value());
                return std::move(statement);
            }
            auto generated = sql_gen::generate_query(node, parameters, Traits::type, target, batch, resource_);
            if (generated.has_error()) {
                return generated.template convert_error<slot_statement_t>();
            }
            statement.sql = std::move(generated.value());
            return std::move(statement);
        }

    private:
        std::pmr::memory_resource* resource_;
        conn::connection_pool_t* pool_;
        metadata_t* metadata_;
        conn::wait_owner_t* owner_;
        log_t log_;
    };

} // namespace db
