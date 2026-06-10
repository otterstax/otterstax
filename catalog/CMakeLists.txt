set(CATALOG_HEADERS
    catalog_manager.hpp
    schema_store.hpp
)

set(CATALOG_SOURCES
    catalog_manager.cpp
    schema_store.cpp
)

add_library(catalog
    ${CATALOG_HEADERS}
    ${CATALOG_SOURCES}
)

target_link_libraries(catalog PUBLIC
    Boost::boost
    otterbrix::otterbrix
    otterbrix_log
    actor-zeta::actor-zeta
    arrow::arrow
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
)

target_include_directories(catalog PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)