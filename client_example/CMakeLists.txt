add_executable(client client.cpp)

target_link_libraries(client
    PRIVATE
    arrow::arrow
)

target_include_directories(client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)