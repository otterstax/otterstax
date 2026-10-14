project(runtime)

# runtime_t (memory resource + every thread pool) and engine_t (the embedded
# otterbrix engine over those pools): the first and the third module of the
# process (runtime_t -> remote_backends_t -> engine_t, owned by main.cpp).
set(${PROJECT_NAME}_HEADERS
    runtime.hpp
    engine.hpp
)

set(${PROJECT_NAME}_SOURCES
    runtime.cpp
    engine.cpp
)

add_library(${PROJECT_NAME} STATIC
    ${${PROJECT_NAME}_HEADERS}
    ${${PROJECT_NAME}_SOURCES}
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(${PROJECT_NAME} PUBLIC
    config::config
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    spdlog::spdlog
)

if(ENABLE_TRACY)
    target_link_libraries(${PROJECT_NAME} PUBLIC Tracy::TracyClient)
endif()
