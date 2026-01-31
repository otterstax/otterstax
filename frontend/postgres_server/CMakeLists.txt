set(POSTGRES_SERVER_HEADERS
    postgres_defs/error.hpp
    postgres_defs/field_type.hpp
    postgres_defs/message_type.hpp
    packet/packet_reader.hpp
    packet/packet_writer.hpp
    packet/packet_utils.hpp
    connection/postgres_connection.hpp
    connection/transaction_manager.hpp
    connection/pipeline_state.hpp
    postgres_server.hpp
    resultset/field_description.hpp
    resultset/postgres_resultset.hpp
)

set(POSTGRES_SERVER_SOURCES
     packet/packet_reader.cpp
     packet/packet_writer.cpp
     packet/packet_utils.cpp
     connection/packet_processing.cpp
     connection/postgres_connection.cpp
     connection/transaction_manager.cpp
     connection/pipeline_state.cpp
     resultset/field_description.cpp
     resultset/postgres_resultset.cpp
)

add_library(postgres_server
    ${POSTGRES_SERVER_HEADERS}
    ${POSTGRES_SERVER_SOURCES}
)

target_link_libraries(postgres_server PUBLIC
    Boost::boost
    common_server
)

target_include_directories(postgres_server PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)