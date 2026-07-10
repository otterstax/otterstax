project(file_extension CXX)

include(otterbrix_parser_extension)

# Builds otterbrix::file_extension (own flex+bison grammar, prefix fileyy).
otterbrix_add_parser_extension(file_extension
        PREFIX file
        SOURCES file_extension.cpp)
