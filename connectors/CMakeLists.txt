set(CONNECTORS_HEADERS
    mysql/connector.hpp
    mysql/manager.hpp
    postgresql/types.hpp
    postgresql/connector.hpp
    postgresql/manager.hpp
    http_server/connection_server.hpp
    http_server/pg_connection_config.hpp
)

set(CONNECTORS_SOURCES
    mysql/connector.cpp
    mysql/manager.cpp
    postgresql/connector.cpp
    postgresql/manager.cpp
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
)

target_include_directories(connectors PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)