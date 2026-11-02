# The backend module of the process (runtime_t -> remote_backends_t ->
# engine_t, every module owned by main.cpp): remote_backends_t (a backend actor
# per MySQL / PostgreSQL / ClickHouse connection over its agents, the startup
# discovery, before the engine opens) and two startup steps after it
# opens (backends.hpp): the s3 aliases stored in the s3 connector and the
# discovered tables registered in the engine's catalog.
set(BACKENDS_HEADERS
    backends.hpp
    connection_retry.hpp
    remote_backends.hpp
)

set(BACKENDS_SOURCES
    backends.cpp
    remote_backends.cpp
)

add_library(backends
    ${BACKENDS_HEADERS}
    ${BACKENDS_SOURCES}
)

target_link_libraries(backends PUBLIC
    Boost::boost
    actor-zeta::actor-zeta
    otterbrix::otterbrix
    arrow::arrow
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
    config::config
    runtime
    catalog
    otterstax_federation
    connectors
    integration
    kafka_runtime
    s3
)

target_include_directories(backends PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)
