#pragma once
#include "handler_by_id.hpp"
#include <actor-zeta.hpp>

namespace ch_connection_manager {
    enum class route
    {
        execute,
        get_catalog,
    };

    constexpr auto handler_id(route type) { return handler_id(group_id_t::ch_connection_manager, type); }

} // namespace ch_connection_manager
