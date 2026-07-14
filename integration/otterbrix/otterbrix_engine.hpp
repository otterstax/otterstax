// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <integration/cpp/base_spaces.hpp>

#include <actor-zeta.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace db {
    class otterbrix_engine_t final
        : public otterbrix::base_otterbrix_t
        , public boost::intrusive_ref_counter<otterbrix_engine_t> {
    public:
        explicit otterbrix_engine_t(const configuration::config& config)
            : base_otterbrix_t(config) {}

        // Address of the async manager_dispatcher_t actor (protected in base)
        actor_zeta::address_t engine_dispatcher_address();
    };

    using otterbrix_engine_ptr = boost::intrusive_ptr<otterbrix_engine_t>;

    inline otterbrix_engine_ptr make_otterbrix_engine(const configuration::config& config) {
        return otterbrix_engine_ptr(new otterbrix_engine_t(config));
    }
} // namespace db
