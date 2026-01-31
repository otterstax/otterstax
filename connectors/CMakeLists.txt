set(CONNECTORS_HEADERS
    mysql_connector.hpp
    mysql_manager.hpp
    http_server/connection_server.hpp
)

set(CONNECTORS_SOURCES
    mysql_connector.cpp
    mysql_manager.cpp
    http_server/connection_server.cpp
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
)

target_include_directories(connectors PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)