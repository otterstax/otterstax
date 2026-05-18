// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <core/result_wrapper.hpp>

#include <utility>

namespace otterstax {

    // Tiny wrapper around core::error_t with a public default constructor.
    //
    // WHY: asio::co_spawn (used by all three backend connectors via
    // ConnectorManager::executeQuery → co_spawn(... use_future)) requires the
    // result type T to be default-constructible — asio internally does
    // `T()` when materializing exception completions. core::error_t's default
    // ctor is private (only error_t::no_error() is public), which makes it
    // unsuitable as the awaitable's result type directly.
    //
    // This wrapper exists solely as the awaitable result type for runQuery
    // overloads and the corresponding handler return type. Callers should
    // unwrap to plain core::error_t at their boundary via .release().
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
