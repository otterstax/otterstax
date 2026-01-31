set(SCHEDULER_HEADERS
    scheduler.hpp
    schema_utils.hpp
)

set(SCHEDULER_SOURCES
    scheduler.cpp
    schema_utils.cpp
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
)

target_include_directories(scheduler PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)