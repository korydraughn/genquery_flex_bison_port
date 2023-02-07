find_package(FLEX 2.6.4 REQUIRED)
find_package(BISON 3.5.1 REQUIRED)

set(IRODS_FLEX_LEXER_NAME irods_lexer)
set(IRODS_BISON_PARSER_NAME irods_parser)

FLEX_TARGET(${IRODS_FLEX_LEXER_NAME} dsl/lexer.l ${CMAKE_CURRENT_BINARY_DIR}/lexer.cpp)
BISON_TARGET(${IRODS_BISON_PARSER_NAME} dsl/parser.y ${CMAKE_CURRENT_BINARY_DIR}/parser.cpp)
ADD_FLEX_BISON_DEPENDENCY(${IRODS_FLEX_LEXER_NAME} ${IRODS_BISON_PARSER_NAME})

set(IRODS_PARSER_NAME irods_genquery2_parser)
set(IRODS_FLEX_OUTPUTS "FLEX_${IRODS_FLEX_LEXER_NAME}_OUTPUTS")
set(IRODS_BISON_OUTPUTS "BISON_${IRODS_BISON_PARSER_NAME}_OUTPUTS")

add_library(
  ${IRODS_PARSER_NAME}
  OBJECT
  ${CMAKE_CURRENT_SOURCE_DIR}/src/genquery_sql.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/genquery_wrapper.cpp
  ${${IRODS_FLEX_OUTPUTS}}
  ${${IRODS_BISON_OUTPUTS}})

set_target_properties(${IRODS_PARSER_NAME} PROPERTIES INTERFACE_POSITION_INDEPENDENT_CODE TRUE)
set_target_properties(${IRODS_PARSER_NAME} PROPERTIES POSITION_INDEPENDENT_CODE TRUE)

target_include_directories(
  ${IRODS_PARSER_NAME}
  PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
  ${IRODS_EXTERNALS_FULLPATH_CLANG}/include/c++/v1
  ${IRODS_EXTERNALS_FULLPATH_FMT}/include)

target_link_libraries(
  ${IRODS_PARSER_NAME}
  PRIVATE
  #${FLEX_LIBRARIES} # This causes a compiler error when using C++ (i.e. undefined reference to yylex()).
  ${IRODS_EXTERNALS_FULLPATH_CLANG_RUNTIME}/lib/libc++.so
  ${IRODS_EXTERNALS_FULLPATH_FMT}/lib/libfmt.so)

# TODO Setup install directives.
install(
  FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/include/irods/genquery_sql.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/include/irods/genquery_wrapper.hpp
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/irods")
