add_subdirectory(s3)
add_subdirectory(file)
add_subdirectory(api_server)

set(CONNECTORS_HEADERS
    mysql/connector.hpp
    mysql/manager.hpp
    postgresql/types.hpp
    postgresql/connector.hpp
    postgresql/manager.hpp
    clickhouse/types.hpp
    clickhouse/connector.hpp
    clickhouse/manager.hpp
    api_connections/pg_connection_config.hpp
    api_connections/ch_connection_config.hpp
)

set(CONNECTORS_SOURCES
    mysql/connector.cpp
    mysql/manager.cpp
    postgresql/connector.cpp
    postgresql/manager.cpp
    clickhouse/connector.cpp
    clickhouse/manager.cpp
)

add_library(connectors
    ${CONNECTORS_HEADERS}
    ${CONNECTORS_SOURCES}
)

target_link_libraries(connectors PUBLIC
    Threads::Threads
    OpenSSL::Crypto
    OpenSSL::SSL
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    arrow::arrow
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
    s3
    file
    api_server
)

target_include_directories(connectors PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)