project(test_parser)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_subquery_extractor.cpp
    test_parser_integration.cpp
    test_replace_qualifiers.cpp
    test_query_gen.cpp
    test_parser_extras.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::otterbrix
    lib_otterstax
    Catch2::Catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
