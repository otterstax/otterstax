project(test_mysql_front)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_resultset.cpp
    test_reader_writer.cpp
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