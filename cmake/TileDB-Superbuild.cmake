include (ExternalProject)

############################################################
# Common variables
############################################################

# Build paths for external projects
set(TILEDB_EP_BASE "${CMAKE_BINARY_DIR}/externals")
set(TILEDB_EP_SOURCE_DIR "${TILEDB_EP_BASE}/src")
set(TILEDB_EP_INSTALL_PREFIX "${TILEDB_EP_BASE}/install")

# A variable that will hold the paths to all the dependencies that are built
# during the superbuild. These paths are passed to the regular non-superbuild
# build process as CMake arguments.
set(FORWARD_EP_CMAKE_ARGS)

# Variable that will hold a list of all the external projects added
# as a part of the superbuild.
set(TILEDB_EXTERNAL_PROJECTS)

# Forward any additional CMake args to the non-superbuild.
set(INHERITED_CMAKE_ARGS
  -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -DTILEDB_VERBOSE=${TILEDB_VERBOSE}
  -DTILEDB_S3=${TILEDB_S3}
  -DTILEDB_HDFS=${TILEDB_HDFS}
  -DTILEDB_WERROR=${TILEDB_WERROR}
  -DTILEDB_CPP_API=${TILEDB_CPP_API}
  -DTILEDB_FORCE_ALL_DEPS=${TILEDB_FORCE_ALL_DEPS}
  -DTILEDB_TESTS_AWS_S3_CONFIG=${TILEDB_TESTS_AWS_S3_CONFIG}
  -DSANITIZER=${SANITIZER}
  -DTILEDB_EP_BASE=${TILEDB_EP_BASE}
)

if (WIN32)
  list(APPEND INHERITED_CMAKE_ARGS
    -DMSVC_MP_FLAG=${MSVC_MP_FLAG}
  )
endif()

############################################################
# Set up external projects for dependencies
############################################################

if (TILEDB_FORCE_ALL_DEPS)
  message(STATUS "Forcing superbuild to build all dependencies as ExternalProjects.")
endif()

# These includes modify the TILEDB_EXTERNAL_PROJECTS and FORWARD_EP_CMAKE_ARGS
# variables.

include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindBlosc_EP.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindBzip2_EP.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindCatch_EP.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindLZ4_EP.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindSpdlog_EP.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindZlib_EP.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindZstd_EP.cmake)

if (TILEDB_S3)
  if (NOT WIN32)
    # AWS SDK uses builtin WinHTTP and BCrypt instead of these.
    include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindCurl_EP.cmake)
    include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindOpenSSL_EP.cmake)
  endif()

  include(${CMAKE_SOURCE_DIR}/cmake/Modules/FindAWSSDK_EP.cmake)
endif()

############################################################
# Set up the regular build (i.e. non-superbuild).
############################################################

ExternalProject_Add(tiledb
  SOURCE_DIR ${PROJECT_SOURCE_DIR}
  CMAKE_ARGS
    -DTILEDB_SUPERBUILD=OFF
    ${INHERITED_CMAKE_ARGS}
    ${FORWARD_EP_CMAKE_ARGS}
  INSTALL_COMMAND ""
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/tiledb
  DEPENDS ${TILEDB_EXTERNAL_PROJECTS}
)

############################################################
# "make check" target
############################################################

add_custom_target(check
  COMMAND ${CMAKE_COMMAND} --build . --target check --config ${CMAKE_BUILD_TYPE}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tiledb
)

############################################################
# "make examples" target
############################################################

add_custom_target(examples
  COMMAND ${CMAKE_COMMAND} --build . --target examples --config ${CMAKE_BUILD_TYPE}
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tiledb
)

############################################################
# "make format" and "make check-format" targets
############################################################

set(SCRIPTS_DIR "${CMAKE_SOURCE_DIR}/scripts")

find_package(ClangTools)
if (${CLANG_FORMAT_FOUND})
  # runs clang format and updates files in place.
  add_custom_target(format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1
    `find ${CMAKE_CURRENT_SOURCE_DIR}/tiledb
    ${CMAKE_CURRENT_SOURCE_DIR}/test/src
    ${CMAKE_CURRENT_SOURCE_DIR}/examples
    -name \\*.cc -or -name \\*.c -or -name \\*.h`)

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0
    `find ${CMAKE_CURRENT_SOURCE_DIR}/tiledb
    ${CMAKE_CURRENT_SOURCE_DIR}/test/src
    ${CMAKE_CURRENT_SOURCE_DIR}/examples
    -name \\*.cc -or -name \\*.c -or -name \\*.h`)
endif()

###########################################################
# Doxygen documentation
###########################################################

find_package(Doxygen)
if(DOXYGEN_FOUND)
  set(TILEDB_C_API_HEADERS "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/tiledb.h")
  file(GLOB TILEDB_CPP_API_HEADERS "${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/*.h")
  set(TILEDB_API_HEADERS ${TILEDB_C_API_HEADERS} ${TILEDB_CPP_API_HEADERS})
  add_custom_command(OUTPUT ${CMAKE_BINARY_DIR}/doxyfile.in
    COMMAND mkdir -p doxygen
    COMMAND echo INPUT = ${CMAKE_SOURCE_DIR}/doc/mainpage.dox
      ${TILEDB_API_HEADERS} > ${CMAKE_BINARY_DIR}/doxyfile.in
    COMMENT "Preparing for Doxygen documentation" VERBATIM
  )
  add_custom_target(doc
    ${DOXYGEN_EXECUTABLE} ${CMAKE_SOURCE_DIR}/doc/Doxyfile.mk >
      ${CMAKE_BINARY_DIR}/Doxyfile.log 2>&1
    COMMENT "Generating API documentation with Doxygen" VERBATIM
    DEPENDS ${CMAKE_BINARY_DIR}/doxyfile.in
  )
endif(DOXYGEN_FOUND)