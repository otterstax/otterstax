set(API_SERVER_HEADERS
    connection_server.hpp
)

set(API_SERVER_SOURCES
    connection_server.cpp
)

add_library(api_server
    ${API_SERVER_HEADERS}
    ${API_SERVER_SOURCES}
)

target_link_libraries(api_server PUBLIC
    Boost::boost
    Boost::json
    Threads::Threads
    OpenSSL::Crypto
    OpenSSL::SSL
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
    arrow::arrow
)

target_include_directories(api_server PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)
