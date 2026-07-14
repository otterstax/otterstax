project(kafka_grammar CXX)

# The kafka parser extension is built like otterbrix's demo_extension: its own
# reentrant flex+bison pair (kafka_yy prefix), kept fully isolated from the core
# base_yy/core_yy parser. We invoke bison/flex directly rather than relying on
# otterbrix's otterbrix_add_parser_extension helper being exported by the package.
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

bison_target(kafka_parser
    ${CMAKE_CURRENT_SOURCE_DIR}/kafka_gram.y
    ${CMAKE_CURRENT_BINARY_DIR}/kafka_gram.cpp
    DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/kafka_gram.hpp)
flex_target(kafka_lexer
    ${CMAKE_CURRENT_SOURCE_DIR}/kafka_scan.l
    ${CMAKE_CURRENT_BINARY_DIR}/kafka_scan.cpp
    DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/kafka_scan.h)
add_flex_bison_dependency(kafka_lexer kafka_parser)

add_library(kafka_grammar STATIC
    ${BISON_kafka_parser_OUTPUTS}
    ${FLEX_kafka_lexer_OUTPUTS}
    kafka_extension.cpp
    kafka_node.cpp
)

target_include_directories(kafka_grammar PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}    # kafka_ast.hpp, kafka_node.hpp, kafka_extension.hpp
    ${CMAKE_CURRENT_BINARY_DIR}    # generated kafka_gram.hpp / kafka_scan.h
)

target_link_libraries(kafka_grammar PUBLIC
    otterbrix::otterbrix
    Boost::boost
)
