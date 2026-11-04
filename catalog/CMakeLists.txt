# The discovery queries the backend agents run (otterstax_federation links it); the registration
# of what they found builds on the federation.
add_library(catalog_discovery discovery.hpp discovery.cpp)

target_link_libraries(catalog_discovery PUBLIC
    otterbrix_local
    otterbrix::otterbrix
    Boost::boost
)

target_include_directories(catalog_discovery PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

set(CATALOG_HEADERS
    ownership.hpp
    registration.hpp
)

set(CATALOG_SOURCES
    ownership.cpp
    registration.cpp
)

add_library(catalog
    ${CATALOG_HEADERS}
    ${CATALOG_SOURCES}
)

target_link_libraries(catalog PUBLIC
    catalog_discovery
    otterstax_federation
    otterbrix_local
    Boost::boost
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    arrow::arrow
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
)

target_include_directories(catalog PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)
