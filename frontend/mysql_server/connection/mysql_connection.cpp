// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 OtterStax

#include "mysql_connection.hpp"

using namespace components;
using namespace components::sql;

namespace mysql_front {
    mysql_connection::mysql_connection(std::pmr::memory_resource* resource,
                                       boost::asio::io_context& ctx,
                                       uint32_t connection_id,
                                       actor_zeta::address_t scheduler,
                                       std::function<void()> on_close)
        : resource_(resource)
        , socket_(ctx)
        , connection_id_(connection_id)
        , close_callback_(std::move(on_close))
        , read_buffer_(READ_BUFFER_SIZE)
        , statement_id_map_(resource_)
        , sequence_id_(0)
        , expected_sequence_id_(0)
        , next_statement_id_(0)
        , client_max_packet_size_(DEFAULT_MAX_PACKET_SIZE)
        , scheduler_(scheduler)
        , state_(State::HANDSHAKE) {}

    boost::asio::ip::tcp::socket& mysql_connection::socket() { return socket_; }

    mysql_connection::prepared_stmt_meta::prepared_stmt_meta(std::pmr::memory_resource* resource,
                                                             session_hash_t stmt_session,
                                                             uint32_t parameter_count)
        : stmt_session(stmt_session)
        , parameter_count(parameter_count)
        , param_types(resource) {}
} // namespace mysql_front