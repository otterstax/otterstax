set(MYSQL_SERVER_HEADERS
    mysql_defs/capabilities.hpp
    mysql_defs/character_set.hpp
    mysql_defs/column_flags.hpp
    mysql_defs/error.hpp
    mysql_defs/field_type.hpp
    mysql_defs/server_command.hpp
    mysql_defs/server_status.hpp
    packet/length_encoded.hpp
    packet/packet_reader.hpp
    packet/packet_writer.hpp
    packet/packet_utils.hpp
    resultset/column_definition_41.hpp
    resultset/mysql_resultset.hpp
    connection/mysql_connection.hpp
    mysql_server.hpp
    protocol_const.hpp
)

set(MYSQL_SERVER_SOURCES
     packet/length_encoded.cpp
     packet/packet_reader.cpp
     packet/packet_writer.cpp
     packet/packet_utils.cpp
     resultset/column_definition_41.cpp
     resultset/mysql_resultset.cpp
     connection/mysql_connection.cpp
     connection/packet_processing.cpp
)

add_library(mysql_server
    ${MYSQL_SERVER_HEADERS}
    ${MYSQL_SERVER_SOURCES}
)

target_link_libraries(mysql_server PUBLIC
    Boost::boost
    common_server
)

target_include_directories(mysql_server PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)