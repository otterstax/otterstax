set(FLIGHT_SQL_HEADERS
    batch_reader.hpp
    server.hpp
)

set(FLIGHT_SQL_SOURCES
    batch_reader.cpp
    server.cpp
)

add_library(flight_sql_server
    ${FLIGHT_SQL_HEADERS}
    ${FLIGHT_SQL_SOURCES}
)

target_link_libraries(flight_sql_server PUBLIC
    arrow::arrow
    Boost::boost
    otterbrix::otterbrix
    actor-zeta::actor-zeta
)

target_include_directories(flight_sql_server PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)