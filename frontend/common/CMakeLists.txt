set(COMMON_SERVER_HEADERS
        protocol_config.hpp
    frontend_connection.hpp
    frontend_server.hpp
    packet_reader_base.hpp
    packet_writer_base.hpp
    utils.hpp
    resultset_utils.hpp
)

set(COMMON_SERVER_SOURCES
     frontend_connection.cpp
     packet_reader_base.cpp
     packet_writer_base.cpp
     frontend_connection.cpp
     utils.cpp
     resultset_utils.cpp
)

add_library(common_server
    ${COMMON_SERVER_HEADERS}
    ${COMMON_SERVER_SOURCES}
)

target_link_libraries(common_server PUBLIC
    Boost::boost
    otterbrix::otterbrix
    actor-zeta::actor-zeta
)

target_include_directories(common_server PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)