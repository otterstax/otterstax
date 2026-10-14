set(config_HEADERS
    config.hpp
    yaml_scalar.hpp
    connections/connection_config.hpp
    connections/connection_config_reader.hpp
)

set(config_SOURCES
    config.cpp
    connections/connection_config_reader.cpp
)

add_library(config
    ${config_HEADERS}
    ${config_SOURCES}
)

add_library(config::config ALIAS config)

target_include_directories(config
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(config
    PUBLIC
    yaml-cpp::yaml-cpp
)

# The engine's core headers (core/result_wrapper.hpp, the error_t every reader answers)
target_link_libraries(config
    PUBLIC
    otterbrix::otterbrix
)

# Allow nearby project headers (e.g. utility/) to be found by targets linking config
target_include_directories(config
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)
