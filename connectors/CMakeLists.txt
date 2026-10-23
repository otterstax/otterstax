add_subdirectory(s3)
add_subdirectory(file)

set(CONNECTORS_HEADERS
    common/statement.hpp
    common/driver.hpp
    common/pool_config.hpp
    common/work_pool.hpp
    common/connection_pool.hpp
    common/transfer.hpp
    common/wait.hpp
    mysql/driver.hpp
    mysql/errors.hpp
    postgresql/types.hpp
    postgresql/driver.hpp
    postgresql/errors.hpp
    clickhouse/types.hpp
    clickhouse/driver.hpp
    clickhouse/errors.hpp
)

set(CONNECTORS_SOURCES
    common/connection_pool.cpp
    common/transfer.cpp
    mysql/driver.cpp
    postgresql/driver.cpp
    clickhouse/driver.cpp
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