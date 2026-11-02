# The federation core: the backend actor of each connection of config.yaml (otterbrix's storage
# contract), its agents (one physical connection each, on the runtime's backends pool), and the host
# table of the backends' spelling. See CLAUDE.md in this directory.
set(OTTERSTAX_FEDERATION_HEADERS
    backend_actor.hpp
    backend_agent.hpp
    reachability.hpp
    conform.hpp
    metadata.hpp
)

set(OTTERSTAX_FEDERATION_SOURCES
    backend_actor.cpp
    backend_actor_offer.cpp
    backend_agent.cpp
    conform.cpp
    metadata.cpp
)

add_library(otterstax_federation
    ${OTTERSTAX_FEDERATION_HEADERS}
    ${OTTERSTAX_FEDERATION_SOURCES}
)

target_link_libraries(otterstax_federation PUBLIC
    catalog_discovery
    connectors
    otterbrix_local
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    Boost::boost
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
)

if(ENABLE_TRACY)
    target_link_libraries(otterstax_federation PUBLIC Tracy::TracyClient)
endif()

target_include_directories(otterstax_federation PUBLIC
    ${PROJECT_SOURCE_DIR}
)
