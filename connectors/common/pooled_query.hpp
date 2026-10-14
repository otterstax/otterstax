// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include "connection_pool.hpp"

#include "utility/wait_barrier.hpp"

#include <boost/asio/awaitable.hpp>

#include <string_view>
#include <utility>

namespace conn {

    // The awaitable ConnectorManager::executeQuery(uuid, ...) spawns: the
    // statement runs on a connection checked out of the uid's pool, and the
    // connection goes back to the pool when this frame ends — on the io thread,
    // the moment the statement has completed (or thrown), not when the consumer
    // picks the future up. The handle is a by-value coroutine parameter, like the
    // handler in run_with_owned_handler, so it lives exactly as long as the query.
    template<typename R, typename Arg, typename Handle, typename Callable>
    boost::asio::awaitable<otterstax::query_result_t<R>>
    run_on_checked_out(Handle handle, std::string_view query, Callable handler) {
        auto outcome = co_await otterstax::run_with_owned_handler<R, Arg>(*handle, query, std::move(handler));
        co_return std::move(outcome);
    }

} // namespace conn
