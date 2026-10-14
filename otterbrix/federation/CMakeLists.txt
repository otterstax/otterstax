# The federation provider: the name resolution primitives (need / decide), the pushdown rule, the
# storage of a remote table and its operators, and the process-wide registry of servers they read.
# See CLAUDE.md in this directory.
set(OTTERSTAX_FEDERATION_HEADERS
    metadata.hpp
    provider.hpp
    remote_storage.hpp
    name_resolution.hpp
    rules/pushdown.hpp
)

set(OTTERSTAX_FEDERATION_SOURCES
    metadata.cpp
    provider.cpp
    remote_storage.cpp
    name_resolution.cpp
    rules/pushdown.cpp
)

add_library(otterstax_federation
    ${OTTERSTAX_FEDERATION_HEADERS}
    ${OTTERSTAX_FEDERATION_SOURCES}
)

target_link_libraries(otterstax_federation PUBLIC
    otterbrix_local
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    Boost::boost
)

if(ENABLE_TRACY)
    target_link_libraries(otterstax_federation PUBLIC Tracy::TracyClient)
endif()

target_include_directories(otterstax_federation PUBLIC
    ${PROJECT_SOURCE_DIR}
)
