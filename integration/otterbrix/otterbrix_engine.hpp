// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <integration/cpp/base_spaces.hpp>

#include <actor-zeta.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <core/pmr.hpp>

#include <memory_resource>
#include <type_traits>

namespace db {
    // core/pmr.hpp picks the TYPE of base_otterbrix_t's by-value resource member
    // off the including translation unit's sanitizer macros. The engine package
    // is built without ASAN, so this header must see the same layout it was
    // built with; a sanitizer build that leaks __SANITIZE_ADDRESS__ into an
    // engine header would otherwise derive otterbrix_engine_t from a base of a
    // different size and corrupt the engine's members.
    static_assert(std::is_same_v<core::pmr::otterbrix_resource, std::pmr::synchronized_pool_resource>,
                  "otterbrix_engine_t must see the engine package's resource layout (synchronized_pool_resource)");

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
