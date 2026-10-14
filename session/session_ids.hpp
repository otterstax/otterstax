// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "utility/session.hpp"

#include <cstddef>

namespace otterstax::session {

    // One statement of a connection, as the frontend names it: the key of the
    // session's prepared-statement map. A frontend takes each one from its
    // session (query_session_t::next_statement); the value only has to be
    // unique within the session.
    enum class statement_id_t : std::size_t
    {
    };

    // The per-call id the integration actors (s3, file, kafka) still take: they
    // log it and key nothing on it. Kept equal to the statement id.
    constexpr session_hash_t to_hash(statement_id_t id) noexcept { return static_cast<session_hash_t>(id); }

} // namespace otterstax::session
