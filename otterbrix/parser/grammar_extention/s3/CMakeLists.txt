project(s3_extension CXX)

include(otterbrix_parser_extension)

# Builds otterbrix::s3_extension (own flex+bison grammar, prefix s3yy).
otterbrix_add_parser_extension(s3_extension
        PREFIX s3
        SOURCES s3_extension.cpp)
