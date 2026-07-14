project(test_kafka_grammar)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_kafka_grammar.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::otterbrix
    kafka_grammar
    lib_otterstax
    Catch2::Catch2
)

#otterstax_discover_tests(${PROJECT_NAME})
include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
