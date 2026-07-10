project(test_parser)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_subquery_extractor.cpp
    test_parser_integration.cpp
    test_replace_qualifiers.cpp
    test_query_gen.cpp
    test_parser_extras.cpp
    test_s3_extension.cpp
    test_file_extension.cpp
    test_external_routing.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::otterbrix
    otterbrix::s3_extension
    otterbrix::file_extension
    lib_otterstax
    Catch2::Catch2
)

# Use the shared helper (tests/CMakeLists.txt): it bakes a parent-relative
# INSTALL_RPATH into the binary so it finds the Conan .so's in build/<type>/lib
# without needing LD_LIBRARY_PATH, then runs Catch discovery.
otterstax_discover_tests(${PROJECT_NAME})
