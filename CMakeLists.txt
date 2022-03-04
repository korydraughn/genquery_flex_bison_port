cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(genquery_flex_bison LANGUAGES C CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS TRUE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "-stdlib=libc++")
set(CMAKE_EXE_LINKER_FLAGS "-stdlib=libc++ -Wl,-rpath=/opt/irods-externals/clang13.0.0-0/lib")

find_package(FLEX 2.6.4 REQUIRED)
find_package(BISON 3.0.4 REQUIRED)

FLEX_TARGET(MyScanner lexer.l ${CMAKE_BINARY_DIR}/lexer.cpp)
BISON_TARGET(MyParser parser.y ${CMAKE_BINARY_DIR}/parser.cpp)
ADD_FLEX_BISON_DEPENDENCY(MyScanner MyParser)

include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    /opt/irods-externals/clang13.0.0-0/include/c++/v1
    /opt/irods-externals/boost1.78.0-0/include
    /opt/irods-externals/fmt8.1.1-0/include
)

add_executable(
    gql
    main.cpp
    genquery_sql.cpp
    genquery_wrapper.cpp
    ${FLEX_MyScanner_OUTPUTS}
    ${BISON_MyParser_OUTPUTS}
)

target_link_libraries(
    gql
    #${FLEX_LIBRARIES} # This causes a compiler error when using C++ (i.e. undefined reference to yylex()).
    /opt/irods-externals/clang13.0.0-0/lib/libc++.so
    /opt/irods-externals/fmt8.1.1-0/lib/libfmt.so
)
