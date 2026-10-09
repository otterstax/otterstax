// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "otterbrix_engine.hpp"

#include <cstdio>
#include <cstdlib>

namespace db {

    actor_zeta::address_t otterbrix_engine_t::engine_dispatcher_address() const { return engine().dispatcher_address(); }

    otterbrix_engine_ptr make_otterbrix_engine(const configuration::config& config) {
        auto opened = otterbrix::base_otterbrix_t::open(config);
        if (opened.has_error()) {
            std::fprintf(stderr, "otterbrix engine refused to start: %s\n", opened.error().what.c_str());
            std::abort();
        }
        return otterbrix_engine_ptr(new otterbrix_engine_t(std::move(opened.value())));
    }

} // namespace db
