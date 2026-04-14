set(CONNECTORS_HEADERS
    mysql/connector.hpp
    mysql/manager.hpp
    postgresql/types.hpp
    postgresql/connector.hpp
    postgresql/manager.hpp
    clickhouse/types.hpp
    clickhouse/connector.hpp
    clickhouse/manager.hpp
    file/types.hpp
    file/connector.hpp
    file/manager.hpp
    s3/types.hpp
    s3/connector.hpp
    s3/manager.hpp
    http_server/connection_server.hpp
    http_server/pg_connection_config.hpp
    http_server/ch_connection_config.hpp
)

set(CONNECTORS_SOURCES
    mysql/connector.cpp
    mysql/manager.cpp
    postgresql/connector.cpp
    postgresql/manager.cpp
    clickhouse/connector.cpp
    clickhouse/manager.cpp
    file/connector.cpp
    file/manager.cpp
    s3/connector.cpp
    s3/manager.cpp
    http_server/connection_server.cpp
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
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
    aws-sdk-cpp::s3
)

target_include_directories(connectors PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)