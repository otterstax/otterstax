project(test_s3)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_s3_connector.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::otterbrix
    lib_otterstax
    Catch2::Catch2
    arrow::arrow
    Parquet::parquet_shared
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
