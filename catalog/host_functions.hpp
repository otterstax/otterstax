// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>

#include <memory_resource>
#include <span>
#include <string_view>

namespace otterstax::catalog {
    // The schema of local the host's functions go into: local.public, the place `local`.
    inline constexpr std::string_view host_function_schema = "public";

    // The name of every host function, in lower case (what host_function_registrations registers;
    // qualify_host_calls writes a call of one as local.public.<name>).
    [[nodiscard]] std::span<const std::string_view> host_function_names() noexcept;

    // version() -> TEXT, no argument: otterstax::version_text (utility/server_version.hpp) on every row. STABLE,
    // as PostgreSQL's.
    [[nodiscard]] components::compute::function_ptr make_version_function(std::pmr::memory_resource* resource);

    // The host's functions, each a register_udf node into local.public (the schema of the place `local`), which
    // catalog::register_connections runs at every startup once the database local exists. A function of the host is
    // the process's code: its pg_proc rows outlive a restart but not the kernel, so every start registers it again
    // (the engine rewrites the rows of the same name and inputs in place).
    //
    // otterbrix scopes a bare call to the built-ins and the functions of the session's place schema, and a
    // pg_catalog-qualified one to the built-ins only ([.engine-defect-pgcatalog-udf]): a host function answers a bare
    // call from a session placed in local.public, or a call written local.public.f().
    [[nodiscard]] std::pmr::vector<components::logical_plan::node_ptr>
    host_function_registrations(std::pmr::memory_resource* resource);
} // namespace otterstax::catalog
