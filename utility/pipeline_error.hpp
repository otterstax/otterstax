// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "result.hpp"

namespace otterstax {

    enum class error_code_t : int32_t
    {
        none = 0,
        parse_error,      // parser->parse() failed
        syntax_error,     // SQL syntax issue (frontend branches on this)
        unsupported_node, // unsupported node type (frontend branches on this)
        backend_unknown,  // cannot determine backend type
        connection_error, // cannot connect to MySQL/PG
        query_error,      // query execution failed (boost::mysql, libpq)
        schema_error,     // schema resolution failed
        catalog_error,    // catalog operation failed
        session_error,    // session missing/expired
        bind_error,       // prepared statement parameter binding failed
        internal_error    // unknown/unexpected
    };

    enum class error_tag_t : int32_t
    {
        none = 0,
        scheduler,
        catalog_manager,
        sql_connection_manager,
        pg_connection_manager,
        ch_connection_manager,
        otterbrix_manager,
        parser,
        frontend
    };

    using pipeline_error = basic_error<error_code_t, error_tag_t>;

    // Convenience alias: result<T> = result_t<pipeline_error, T>
    template<typename T>
    using result = result_t<pipeline_error, T>;

} // namespace otterstax
