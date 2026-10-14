set(CATALOG_HEADERS
    discovery.hpp
    introspection.hpp
    ownership.hpp
    remote_metadata.hpp
    remote_rediscovery.hpp
)

set(CATALOG_SOURCES
    discovery.cpp
    introspection.cpp
    ownership.cpp
    remote_metadata.cpp
    remote_rediscovery.cpp
)

add_library(catalog
    ${CATALOG_HEADERS}
    ${CATALOG_SOURCES}
)

target_link_libraries(catalog PUBLIC
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
