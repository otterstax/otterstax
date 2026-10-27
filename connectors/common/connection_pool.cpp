// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "connection_pool.hpp"

#include "utility/pmr_text.hpp"
#include "utility/tracy_profiler.hpp"

#include <cassert>
#include <utility>

namespace conn {

    driver_session_t::driver_session_t(std::unique_ptr<IDriver> driver, std::chrono::milliseconds check_after_idle)
        : driver_(std::move(driver))
        , check_after_idle_(check_after_idle)
        , last_used_(clock::now()) {
        assert(driver_ != nullptr);
    }

    core::error_t driver_session_t::open() {
        auto opened = driver_->connect();
        last_used_ = clock::now();
        return opened;
    }

    driver_session_t::outcome_t driver_session_t::run(const statement_t& statement) {
        OTX_ZONE_N("conn::driver_session::run");
        if (clock::now() - last_used_ > check_after_idle_) {
            // A dead connection is dropped here and execute() reconnects first; a live one
            // costs this one round trip after an idle period.
            [[maybe_unused]] auto pinged = driver_->ping();
        }
        auto outcome = driver_->execute(statement);
        last_used_ = clock::now();
        return outcome;
    }

    void driver_session_t::close() noexcept { driver_->close(); }

    connection_pool_t::connection_pool_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , aliases_(resource) {
        assert(resource_ != nullptr);
    }

    // The alias pools join their session threads; nothing else to release.
    connection_pool_t::~connection_pool_t() = default;

    core::error_t
    connection_pool_t::add(std::string_view alias, std::unique_ptr<IDriver> prototype, pool_config_t config) {
        OTX_ZONE_N("conn::connection_pool::add");
        assert(prototype != nullptr);
        if (aliases_.contains(alias)) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 otterstax::concat(resource_, {"connection '", alias, "' is already registered"}));
        }
        std::shared_ptr<const IDriver> shared_prototype{std::move(prototype)};
        auto factory = [shared_prototype, check = config.check_after_idle] {
            return std::make_unique<driver_session_t>(shared_prototype->clone_disconnected(), check);
        };
        auto opened = alias_pool_t::open(resource_, alias, std::move(factory), config);
        if (opened.has_error()) {
            return core::error_on(resource_, opened.error());
        }
        aliases_.emplace(alias, std::move(opened.value()));
        return core::error_t::no_error();
    }

    void connection_pool_t::drop(std::string_view alias) {
        if (auto it = aliases_.find(alias); it != aliases_.end()) {
            aliases_.erase(it);
        }
    }

    bool connection_pool_t::contains(std::string_view alias) const noexcept { return aliases_.contains(alias); }

    statement_future_t connection_pool_t::execute(std::string_view alias, statement_t statement, waker_t waker) {
        OTX_ZONE_N("conn::connection_pool::execute");
        auto it = aliases_.find(alias);
        if (it == aliases_.end()) {
            return actor_zeta::make_ready_future(
                resource_,
                statement_outcome_t{
                    core::error_t(core::error_code_t::do_not_exists,
                                  otterstax::concat(resource_, {"no connection registered for '", alias, "'"}))});
        }
        return it->second->submit(std::move(statement), std::move(waker));
    }

    const alias_pool_t* connection_pool_t::find(std::string_view alias) const noexcept {
        auto it = aliases_.find(alias);
        return it == aliases_.end() ? nullptr : it->second.get();
    }

} // namespace conn
