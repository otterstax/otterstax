project(test_mysql_front)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_resultset.cpp
    test_reader_writer.cpp
    test_wire_type_support.cpp
    # wire-level tests of the other two frontends: they need no running server
    test_pg_frontend_packets.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::otterbrix
    lib_otterstax
    Catch2::Catch2
)

otterstax_discover_tests(${PROJECT_NAME})
