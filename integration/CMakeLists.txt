set(DB_INTEGRATION_HEADERS
    otterbrix/otterbrix_manager.hpp
    sql/connection_manager.hpp
    postgresql/connection_manager.hpp
    clickhouse/connection_manager.hpp
)

set(DB_INTEGRATION_SOURCES
    otterbrix/otterbrix_manager.cpp
    sql/connection_manager.cpp
    postgresql/connection_manager.cpp
    clickhouse/connection_manager.cpp
)

add_library(integration
    ${DB_INTEGRATION_HEADERS}
    ${DB_INTEGRATION_SOURCES}
)

target_link_libraries(integration PUBLIC
    Boost::boost
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
)

target_include_directories(integration PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)