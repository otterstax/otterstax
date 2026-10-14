// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "otterbrix/federation/metadata.hpp"

#include <actor-zeta.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// How the federation reaches a backend. The engine's hooks and rules are plain function pointers
// without state (components/planner/host_hooks.hpp), so the servers they may answer for live in ONE
// process-wide registry: built once at startup from config.yaml, installed before the engine opens,
// never changed while an engine runs, and reset (reset_registry) when the backends it points into go
// away. Lookups are lock-free reads of an immutable object.
namespace otterstax::federation {

    // The remote table column a result column reads as it is, backend spelling.
    struct column_origin_t {
        remote_table_name_t table;
        std::string column;
    };

    // One remote read: a SELECT in the backend's dialect — the full scan of a table
    // (remote_storage.cpp) or a subtree the pushdown rule made one statement of.
    struct scan_request_t {
        std::string uid; // the connection as config.yaml spells it
        std::string sql;
        // Per result column, by position: the table column it reads as it is (a column reference of
        // the SELECT list, of any table of the statement — a JOIN's second table included), nullopt for
        // a computed one. What the ClickHouse named-type overrides (system.columns) are looked up by:
        // a Bool, a named Tuple, a LowCardinality column is read as its declared type only through it.
        // Shorter than the result (empty: no column known): the rest read as their wire types.
        std::vector<std::optional<column_origin_t>> columns;
    };

    // Where a backend leaves the rows of one read. Shared between the reading operator and the
    // backend: the operator may die (a failed sibling source cancels the statement) while the fetch
    // is still in flight, so the backend never writes into the operator itself.
    struct scan_rows_t {
        // The rows as the input translators produced them (tsl::mysql_to_chunk, pg_to_chunk,
        // ch_to_chunk), in the statement's column order. Any width: the operator cuts them to the
        // engine's chunk bound. Their memory resource must outlive the statement's cursor (the
        // backend's own, thread-safe resource; never the resource scan() was handed).
        std::vector<components::vector::data_chunk_t> chunks;
    };

    enum class write_kind_t : std::uint8_t
    {
        insert,
        update,
        delete_
    };

    // One remote write: INSERT / UPDATE / DELETE in the backend's dialect.
    struct write_request_t {
        std::string uid;
        std::string sql;
        write_kind_t kind{write_kind_t::insert};
    };

    // The rows the backend reports it wrote.
    using write_result_t = core::result_wrapper_t<std::uint64_t>;

    // One table a discovery found on a backend: full qualified name, the STRUCT of column types
    // produced by the input translators, and the columns of its primary key (backend spelling, key
    // order; empty when the table has none — always for ClickHouse, whose sorting key names no row).
    struct discovered_table_t {
        qualified_name_t name;
        components::types::complex_logical_type schema;
        std::vector<std::string> primary_key{};
    };

    using discovered_tables_t = std::pmr::vector<discovered_table_t>;

    // A server of config.yaml: how its connections run one statement.
    //
    // Thread safety: every member is called from several threads at once and must be thread-safe
    // (scan / write from the engine's executors; discover / execute_ddl from the startup thread and
    // the query sessions). The implementation's own memory resource — the one the rows and the error
    // texts it answers live on — is used from its threads and read from the callers', so it must be a
    // thread-safe (synchronized) resource that outlives every statement.
    class backend_t {
    public:
        backend_t() = default;
        backend_t(const backend_t&) = delete;
        backend_t& operator=(const backend_t&) = delete;
        virtual ~backend_t() = default;

        // Starts `request` and answers at once with a future that a thread of the runtime's
        // backends pool fulfils (the integration backend posts the statement there as a job,
        // integration/statement_executor.hpp) once `rows` holds every row: no_error, or the backend's error (code and text reach the
        // cursor unchanged). The calling thread is an engine executor: never block it — the
        // connection checkout included. `resource` is the executor's, for the promise only:
        // allocate nothing else on it from another thread.
        virtual actor_zeta::unique_future<core::error_t>
        scan(std::pmr::memory_resource* resource, scan_request_t request, std::shared_ptr<scan_rows_t> rows) = 0;

        // Starts `request` the same way and answers the rows the backend reports it wrote — the
        // affected-row count of MySQL's OK packet (for an UPDATE the rows it matched, out of the
        // packet's info, as PostgreSQL and the engine count them) and PostgreSQL's command tag,
        // ClickHouse's written_rows for an INSERT and 0 for its mutations, which report nothing.
        virtual actor_zeta::unique_future<write_result_t> write(std::pmr::memory_resource* resource,
                                                                write_request_t request) = 0;

        // The tables of `scope` (uid, database / schema, table: empty → every base table of the
        // database / schema) with their columns and primary keys, the answer on the backend's own
        // resource; the connection's error, code kept. Run on the calling thread until the backend
        // answered — the startup thread (remote_backends_t::open) or a query session's rediscovery,
        // never an engine executor.
        virtual core::result_wrapper_t<discovered_tables_t> discover(const qualified_name_t& scope) = 0;

        // One DDL statement on `uid`, already in the backend's dialect (DDL never reaches the
        // engine's host hooks: the query session generates and runs it), on the calling thread like
        // discover; the connector's error, code kept.
        virtual core::error_t execute_ddl(const std::string& uid, const std::string& sql) = 0;
    };

    using backend_ptr = std::shared_ptr<backend_t>;

    // A connection of config.yaml as the federation sees it.
    struct server_t {
        std::string uid; // config.yaml's spelling
        backend_type_t kind;
        backend_ptr backend;
    };

    // Immutable map uid -> server. A uid is matched case-insensitively: an unquoted reference reaches
    // the engine lower-cased while config.yaml may spell the alias otherwise.
    class registry_t {
    public:
        registry_t() = default;
        explicit registry_t(std::vector<server_t> servers);

        // nullptr when no server is registered under `uid`.
        const server_t* find(std::string_view uid) const;

    private:
        std::vector<server_t> servers_;
        std::unordered_map<std::string, std::size_t> by_folded_uid_;
    };

    // The registry the hooks read. Install it before the engine opens; installing another one while
    // an engine runs is not supported (a hook may still answer with the old servers: every installed
    // registry is kept alive until reset_registry).
    void install_registry(std::shared_ptr<const registry_t> registry);
    // Drops every installed registry, and with them the backends they hold; registry() is empty
    // afterwards. Call it once no statement can run (the engine is gone, or no session is left)
    // and before the objects the backends point into are destroyed: nothing outlives them.
    void reset_registry() noexcept;
    // The installed registry; an empty one until install_registry is called or after reset_registry.
    const registry_t& registry() noexcept;

} // namespace otterstax::federation
