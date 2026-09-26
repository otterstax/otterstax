// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>

#include <memory_resource>
#include <string>
#include <utility>

namespace otterstax {

    // Picks up the outcome of an actor whose enqueue_impl runs the handler to
    // completion on the sending thread (CatalogManager, OtterbrixManager): by the
    // time actor_zeta::send returns, the future is settled. A future that is
    // neither ready nor failed here means the actor no longer honours that
    // contract; the caller must not treat the request as done, so that case is
    // reported as an error rather than waited on.
    inline core::error_t take_settled_error(actor_zeta::unique_future<core::error_t> future,
                                            std::pmr::memory_resource* resource) {
        if (future.failed()) {
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{("actor rejected the request: " + future.error().message()).c_str(), resource});
        }
        if (!future.is_ready()) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"actor did not settle the request synchronously", resource});
        }
        return std::move(future).take_ready();
    }

} // namespace otterstax
