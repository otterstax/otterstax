// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "transfer.hpp"

#include "utility/pmr_text.hpp"
#include "utility/tracy_profiler.hpp"

#include <cassert>
#include <utility>

namespace conn {

    namespace {

        core::result_wrapper_t<std::unique_ptr<transfer_pool_t>>
        open_pool(std::pmr::memory_resource* resource,
                  std::string_view name,
                  std::shared_ptr<ITransferDriver> driver,
                  pool_config_t config) {
            return transfer_pool_t::open(
                resource,
                name,
                [driver = std::move(driver)] { return std::make_unique<transfer_session_t>(driver); },
                config);
        }

    } // namespace

    transfer_pools_t::transfer_pools_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , s3_(resource) {
        assert(resource_ != nullptr);
    }

    // Each pool answers what is still queued, then joins its threads.
    transfer_pools_t::~transfer_pools_t() = default;

    core::error_t
    transfer_pools_t::add_s3(std::string_view alias, std::shared_ptr<ITransferDriver> driver, pool_config_t config) {
        OTX_ZONE_N("conn::transfer_pools::add_s3");
        if (s3_.contains(alias)) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 otterstax::concat(resource_, {"s3 alias '", alias, "' is already registered"}));
        }
        auto pool = open_pool(resource_, otterstax::concat(resource_, {"s3 '", alias, "'"}), std::move(driver), config);
        if (pool.has_error()) {
            return pool.error();
        }
        s3_.emplace(alias, std::move(pool.value()));
        return core::error_t::no_error();
    }

    core::error_t transfer_pools_t::set_files(std::shared_ptr<ITransferDriver> driver, pool_config_t config) {
        OTX_ZONE_N("conn::transfer_pools::set_files");
        if (files_) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{"the local-file transfer pool is already set", resource_});
        }
        auto pool = open_pool(resource_, "local files", std::move(driver), config);
        if (pool.has_error()) {
            return pool.error();
        }
        files_ = std::move(pool.value());
        return core::error_t::no_error();
    }

    bool transfer_pools_t::has_s3(std::string_view alias) const noexcept { return s3_.find(alias) != s3_.end(); }

    transfer_future_t transfer_pools_t::refuse(core::error_code_t code, std::pmr::string what) const {
        return actor_zeta::make_ready_future(resource_, transfer_outcome_t{core::error_t(code, std::move(what))});
    }

    transfer_future_t
    transfer_pools_t::s3(std::string_view op, std::string_view alias, transfer_t transfer, waker_t waker) {
        OTX_ZONE_N("conn::transfer_pools::s3");
        auto it = s3_.find(alias);
        if (it == s3_.end()) {
            return refuse(core::error_code_t::do_not_exists,
                          otterstax::concat(resource_, {op, ": unknown alias '", alias, "'"}));
        }
        return it->second->submit(std::move(transfer), std::move(waker));
    }

    transfer_future_t transfer_pools_t::files(std::string_view op, transfer_t transfer, waker_t waker) {
        OTX_ZONE_N("conn::transfer_pools::files");
        if (!files_) {
            return refuse(core::error_code_t::do_not_exists, otterstax::concat(resource_, {op, ": no local-file transfer pool"}));
        }
        return files_->submit(std::move(transfer), std::move(waker));
    }

} // namespace conn
