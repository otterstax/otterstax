// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <utility>

namespace otterstax {

    // Return type of the error-only connector handlers (schema discovery,
    // metadata probes): a query whose outcome IS the error and carries no payload.
    //
    // It is a distinct type on purpose. `query_result_t` in wait_barrier.hpp
    // keys off it to marshal such a query as a bare core::error_t instead of a
    // result_wrapper_t<core::error_t> that would carry the same failure twice, and
    // the public default constructor spells "no error" for a handler that has
    // nothing to report — core::error_t's own default constructor is private.
    // The connector unwraps it to plain core::error_t via .release() before the
    // outcome leaves the io thread.
    struct asio_error_t {
        core::error_t error;

        asio_error_t() noexcept
            : error(core::error_t::no_error()) {}
        asio_error_t(core::error_t e) noexcept
            : error(std::move(e)) {}
        asio_error_t(const asio_error_t&) = default;
        asio_error_t(asio_error_t&&) noexcept = default;
        asio_error_t& operator=(const asio_error_t&) = default;
        asio_error_t& operator=(asio_error_t&&) noexcept = default;

        bool contains_error() const noexcept { return error.contains_error(); }
        core::error_t release() && { return std::move(error); }
    };

} // namespace otterstax
