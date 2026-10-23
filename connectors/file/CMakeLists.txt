set(FILE_HEADERS
    types.hpp
    driver.hpp
)

set(FILE_SOURCES
    driver.cpp
)

add_library(file
    ${FILE_HEADERS}
    ${FILE_SOURCES}
)

target_link_libraries(file PUBLIC
    Threads::Threads
    OpenSSL::Crypto
    OpenSSL::SSL
    otterbrix::otterbrix
    actor-zeta::actor-zeta
    arrow::arrow
)

target_include_directories(file PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)
