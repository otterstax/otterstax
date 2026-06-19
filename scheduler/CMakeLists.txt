set(SCHEDULER_HEADERS
    scheduler.hpp
    schema_utils.hpp
    session_data.hpp
    worker.hpp
)

set(SCHEDULER_SOURCES
    scheduler.cpp
    schema_utils.cpp
    worker.cpp
)

add_library(scheduler
    ${SCHEDULER_HEADERS}
    ${SCHEDULER_SOURCES}
)

target_link_libraries(scheduler PUBLIC
    Boost::boost
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    arrow::arrow
    PostgreSQL::PostgreSQL
    clickhouse-cpp-lib::clickhouse-cpp-lib
    # worker.cpp/scheduler.cpp own a std::thread event loop and Boost.Lockfree
    # queue (header-only via Boost::boost); link the thread runtime explicitly so
    # the scheduler library is self-contained rather than relying on the parent.
    Threads::Threads
)

target_include_directories(scheduler PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)