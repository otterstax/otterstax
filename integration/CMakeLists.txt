add_subdirectory(kafka)

set(DB_INTEGRATION_HEADERS
    prepare_probe.hpp
    otterbrix/otterbrix_engine.hpp
    otterbrix/otterbrix_manager.hpp
    backend.hpp
    backends.hpp
    sql/mysql_backend.hpp
    postgresql/postgres_backend.hpp
    clickhouse/clickhouse_backend.hpp
)

set(DB_INTEGRATION_SOURCES
    otterbrix/otterbrix_engine.cpp
    otterbrix/otterbrix_manager.cpp
    sql/mysql_backend.cpp
    postgresql/postgres_backend.cpp
    clickhouse/clickhouse_backend.cpp
)

add_library(integration
    ${DB_INTEGRATION_HEADERS}
    ${DB_INTEGRATION_SOURCES}
)

target_link_libraries(integration PUBLIC
    Boost::boost
    otterbrix::otterbrix
    arrow::arrow
    actor-zeta::actor-zeta
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
    OpenSSL::Crypto
    OpenSSL::SSL
)

target_include_directories(integration PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)