add_subdirectory(s3)
add_subdirectory(file)

set(CONNECTORS_HEADERS
    mysql/connector.hpp
    mysql/errors.hpp
    postgresql/types.hpp
    postgresql/connector.hpp
    postgresql/errors.hpp
    clickhouse/types.hpp
    clickhouse/connector.hpp
    clickhouse/errors.hpp
)

set(CONNECTORS_SOURCES
    mysql/connector.cpp
    postgresql/connector.cpp
    clickhouse/connector.cpp
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
)

target_include_directories(connectors PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)