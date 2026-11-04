set(SESSION_HEADERS
    plan_inspection.hpp
    query_session.hpp
    session_factory.hpp
    session_ids.hpp
    session_payload.hpp
)

set(SESSION_SOURCES
    plan_inspection.cpp
    query_session.cpp
    session_factory.cpp
)

add_library(session
    ${SESSION_HEADERS}
    ${SESSION_SOURCES}
)

# The session sends to the engine dispatcher (otterbrix_local's
# engine_requests), the s3 manager, the file connector and the KafkaManager,
# registers a missed table of a connection through catalog (registration,
# ownership) and the backend actor of otterstax_federation, and reads plans
# through otterbrix_local (parser). The session factory stops the
# runtime's sessions pool in its shutdown (runtime_t::stop_sessions).
target_link_libraries(session PUBLIC
    Boost::boost
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    arrow::arrow
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
    Threads::Threads
    catalog
    otterstax_federation
    integration
    kafka_runtime
    file
    otterbrix_local
    runtime
)

target_include_directories(session PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)
