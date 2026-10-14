// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "catalog/discovery.hpp"

#include "otterbrix/federation/metadata.hpp"
#include "otterbrix/federation/provider.hpp"

#include <components/logical_plan/execution_plan.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string_view>
#include <vector>

// The writes that keep the host metadata in the engine (federation.remote_tables / remote_columns,
// layout in otterbrix/federation/metadata.hpp) in step with config.yaml and with what discovery
// (catalog/discovery.hpp, the backends' `discover`) answers.
//
// Every function only BUILDS execution plans; the caller sends them to the engine on one session,
// in order. An upsert is a delete followed by inserts, so run it inside BEGIN ... COMMIT: a statement
// resolving a name concurrently then sees the old rows or the new ones (need reads in the statement's
// snapshot), never neither. Plans are single-use: build them again for a retry.
//
// Written at startup (backends/, after the engine opened: bootstrap, the rows of another backend
// kind removed per connection, the startup discovery of every connection that opened) and by the query session's rediscovery
// (remote_rediscovery.hpp) and remote DDL.
namespace otterstax::catalog {

    // CREATE DATABASE IF NOT EXISTS federation, then CREATE TABLE IF NOT EXISTS for the two tables.
    // Idempotent: run it at every startup, before the first statement can name a remote table.
    std::pmr::vector<components::logical_plan::execution_plan_t>
    make_metadata_bootstrap(std::pmr::memory_resource* resource);

    // The remote_tables / remote_columns rows of one configured connection written for another
    // backend kind than `kind`, removed (the uid folded to lower case, fed::fold_identifier, the key
    // those rows are written under): the alias names a server of `kind` now, and those rows are
    // keyed in a way no discovery of `kind` replaces.
    std::pmr::vector<components::logical_plan::execution_plan_t>
    make_other_kind_removal(std::pmr::memory_resource* resource, std::string_view uid, backend_type_t kind);

    // The rows of a discovery reply. `scope` is the name discovery was asked for (the same name the
    // backend's `discover` took): an empty collection means "every table of the database /
    // schema", and then every row under that database (MySQL, ClickHouse) or schema (PostgreSQL)
    // that the reply does not hold is removed with the rest; a named collection replaces that one
    // table's rows (a reply without tables removes them). Each discovered column becomes a row:
    // column_name its name in lower case, remote_column its spelling, column_type its engine type
    // (otterbrix's type spec, components::catalog::encode_type_spec; a built-in scalar by its
    // pg_type name), key_position its position in the table's primary key (0: not a key column).
    // Of two discovered tables folding to one key (spellings differing only in case) the first is
    // kept.
    //
    // schema_error for a discovered column without a name, two columns of one table that fold to
    // the same name, or a primary key column that is not a column of the table; conversion_failure
    // for a column type the metadata cannot record.
    core::result_wrapper_t<std::pmr::vector<components::logical_plan::execution_plan_t>>
    make_discovery_upsert(std::pmr::memory_resource* resource,
                          backend_type_t kind,
                          const qualified_name_t& scope,
                          const federation::discovered_tables_t& tables);

    // The rows of federation.remote_tables under `key`: what with_remote_spelling
    // (remote_rediscovery.hpp) reads.
    components::logical_plan::execution_plan_t make_remote_table_read(std::pmr::memory_resource* resource,
                                                                      const federation::table_key_t& key);

    // A previous OtterStax kept one engine database per connection uid, mirroring the connection's
    // tables, with a manifest collection `<uid>.__otterstax_tables` in it. Such a database would
    // capture the connection's remote names (the engine resolves its own catalog first), so it is
    // dropped on the first start of this OtterStax. The probe answers rows when the manifest is
    // there; the drop is DROP DATABASE of the uid as the mirror spelled it (CASCADE).
    core::result_wrapper_t<components::logical_plan::execution_plan_t>
    make_legacy_mirror_probe(std::pmr::memory_resource* resource, std::string_view uid);
    core::result_wrapper_t<components::logical_plan::execution_plan_t>
    make_legacy_mirror_drop(std::pmr::memory_resource* resource, std::string_view uid);

} // namespace otterstax::catalog
