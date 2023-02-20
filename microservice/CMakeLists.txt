set(
  IRODS_MICROSERVICE_OPERATIONS
  execute
  next_row
  column
  destroy)

foreach (IRODS_MICROSERVICE_OPERATION IN ITEMS ${IRODS_MICROSERVICE_OPERATIONS})
  set(IRODS_MICROSERVICE_NAME msi_genquery2_${IRODS_MICROSERVICE_OPERATION})

  add_library(
    ${IRODS_MICROSERVICE_NAME}
    MODULE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/${IRODS_MICROSERVICE_OPERATION}.cpp)
  
  target_compile_options(
    ${IRODS_MICROSERVICE_NAME}
    PRIVATE
    -Wall
    -Wextra
    #-Werror
    -Wconversion
    -Wshadow
    -Wpedantic)
  
  target_compile_definitions(
    ${IRODS_MICROSERVICE_NAME}
    PRIVATE
    #${IRODS_COMPILE_DEFINITIONS}
    ${IRODS_COMPILE_DEFINITIONS_PRIVATE}
    IRODS_ENABLE_RE
    IRODS_ENABLE_SYSLOG)
  
  target_include_directories(
    ${IRODS_MICROSERVICE_NAME}
    PRIVATE
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/api_plugin/include>
    ${IRODS_INCLUDE_DIRS}
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
    ${IRODS_EXTERNALS_FULLPATH_SPDLOG}/include
    ${IRODS_EXTERNALS_FULLPATH_FMT}/include)
  
  target_link_libraries(
    ${IRODS_MICROSERVICE_NAME}
    PRIVATE
    irods_server)
  
  add_dependencies(${IRODS_MICROSERVICE_NAME} irods_genquery2_server)
  
  install(
    TARGETS ${IRODS_MICROSERVICE_NAME}
    LIBRARY DESTINATION ${IRODS_PLUGINS_DIRECTORY}/microservices)
endforeach()
