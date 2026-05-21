set(CONNECTORS_HEADERS
    mysql/connector.hpp
    mysql/manager.hpp
    postgresql/types.hpp
    postgresql/connector.hpp
    postgresql/manager.hpp
    clickhouse/types.hpp
    clickhouse/connector.hpp
    clickhouse/manager.hpp
    api_connections/connection_server.hpp
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
    api_connections/connection_server.cpp
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
)

target_include_directories(connectors PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)