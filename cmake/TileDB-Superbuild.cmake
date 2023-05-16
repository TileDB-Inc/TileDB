include (ExternalProject)

############################################################
# Common variables
############################################################

# Build paths for external projects
set(TILEDB_EP_BASE "${CMAKE_CURRENT_BINARY_DIR}/externals")
set(TILEDB_EP_SOURCE_DIR "${TILEDB_EP_BASE}/src")
set(TILEDB_EP_INSTALL_PREFIX "${TILEDB_EP_BASE}/install")

# A variable that will hold the paths to all the dependencies that are built
# during the superbuild. These paths are passed to the regular non-superbuild
# build process as CMake arguments.
set(FORWARD_EP_CMAKE_ARGS)

# Variable that will hold a list of all the external projects added
# as a part of the superbuild.
set(TILEDB_EXTERNAL_PROJECTS)

# Passing lists through ExternalProject_Add requires using a separator
# character other than a semicolon.
list(JOIN CMAKE_PREFIX_PATH "|" CMAKE_PREFIX_PATH_STR)

# Forward any additional CMake args to the non-superbuild.
set(INHERITED_CMAKE_ARGS
  -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
  -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH_STR}
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
  -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
  -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
  -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
  -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
  -DCOMPILER_SUPPORTS_AVX2=${COMPILER_SUPPORTS_AVX2}
  -DTILEDB_VCPKG=${TILEDB_VCPKG}
  -DTILEDB_VCPKG_DISABLE_MAGIC=${TILEDB_VCPKG_DISABLE_MAGIC}
  -DTILEDB_VERBOSE=${TILEDB_VERBOSE}
  -DTILEDB_ASSERTIONS=${TILEDB_ASSERTIONS}
  -DTILEDB_S3=${TILEDB_S3}
  -DTILEDB_AZURE=${TILEDB_AZURE}
  -DTILEDB_GCS=${TILEDB_GCS}
  -DTILEDB_HDFS=${TILEDB_HDFS}
  -DTILEDB_WERROR=${TILEDB_WERROR}
  -DTILEDB_CPP_API=${TILEDB_CPP_API}
  -DTILEDB_FORCE_ALL_DEPS=${TILEDB_FORCE_ALL_DEPS}
  -DTILEDB_REMOVE_DEPRECATIONS=${TILEDB_REMOVE_DEPRECATIONS}
  -DSANITIZER=${SANITIZER}
  -DTILEDB_EP_BASE=${TILEDB_EP_BASE}
  -DTILEDB_STATS=${TILEDB_STATS}
  -DTILEDB_STATIC=${TILEDB_STATIC}
  -DTILEDB_TESTS=${TILEDB_TESTS}
  -DTILEDB_TOOLS=${TILEDB_TOOLS}
  -DTILEDB_SERIALIZATION=${TILEDB_SERIALIZATION}
  -DTILEDB_ARROW_TESTS=${TILEDB_ARROW_TESTS}
  -DTILEDB_CRC32=${TILEDB_CRC32}
  -DTILEDB_WEBP=${TILEDB_WEBP}
  -DTILEDB_ABSEIL=${TILEDB_ABSEIL}
  -DTILEDB_INSTALL_LIBDIR=${TILEDB_INSTALL_LIBDIR}
  -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
  -DTILEDB_EXPERIMENTAL_FEATURES=${TILEDB_EXPERIMENTAL_FEATURES}
)

if (TILEDB_TESTS)
  list(APPEND INHERITED_CMAKE_ARGS
    -DTILEDB_TESTS_AWS_S3_CONFIG=${TILEDB_TESTS_AWS_S3_CONFIG}
    -DTILEDB_TESTS_ENABLE_REST=${TILEDB_TESTS_ENABLE_REST}
  )
endif()

# MSVC_MP_FLAG is defined by bootstrap.ps1
if (DEFINED MSVC_MP_FLAG)
  list(APPEND INHERITED_CMAKE_ARGS
    -DMSVC_MP_FLAG=${MSVC_MP_FLAG}
  )
endif()

if (TILEDB_CCACHE)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindCcache.cmake)
  list(APPEND INHERITED_CMAKE_ARGS
    -DCMAKE_CXX_COMPILER_LAUNCHER=${CCACHE_FOUND}
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

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindBzip2_EP.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindLZ4_EP.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindSpdlog_EP.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindZlib_EP.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindZstd_EP.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindMagic_EP.cmake)

if(TILEDB_CRC32)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindCrc32c_EP.cmake)
endif()

if(TILEDB_WEBP)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindWebp_EP.cmake)
endif()

if(TILEDB_ABSEIL)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/Findabsl_EP.cmake)
endif()

if (TILEDB_SERIALIZATION)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindCapnp_EP.cmake)
endif()

if (NOT WIN32)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindOpenSSL_EP.cmake)
endif()

if (TILEDB_GCS OR TILEDB_SERIALIZATION)
  # Need libcurl either with GCS or serialization support.
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindCurl_EP.cmake)
endif()

if (TILEDB_S3)
  # Note on Win32: AWS SDK uses builtin WinHTTP instead of libcurl,
  # and builtin BCrypt instead of OpenSSL.
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindAWSSDK_EP.cmake)
endif()

if (TILEDB_GCS)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindGCSSDK_EP.cmake)
endif()

if (TILEDB_TESTS)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindCatch_EP.cmake)
endif()

if (TILEDB_TOOLS)
  include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/FindClipp_EP.cmake)
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
  LIST_SEPARATOR "|"
)

############################################################
# Convenience superbuild targets that invoke TileDB targets
############################################################

# make install-tiledb
add_custom_target(install-tiledb
  COMMAND ${CMAKE_COMMAND} --build . --target install --config ${CMAKE_BUILD_TYPE}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tiledb
)

# make examples
add_custom_target(examples
  COMMAND ${CMAKE_COMMAND} --build . --target examples --config ${CMAKE_BUILD_TYPE}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tiledb
)

# make experimental/examples/
add_custom_target(experimental-examples
  COMMAND ${CMAKE_COMMAND} --build . --target experimental_examples --config ${CMAKE_BUILD_TYPE}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tiledb
)

# make check
if (TILEDB_TESTS)
  add_custom_target(check
    COMMAND ${CMAKE_COMMAND} --build . --target check --config ${CMAKE_BUILD_TYPE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tiledb
  )
endif()

############################################################
# "make format" and "make check-format" targets
############################################################

set(SCRIPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/scripts")

find_package(ClangTools)
if (NOT ${CLANG_FORMAT_FOUND})
  find_program(CLANG_FORMAT_BIN NAMES clang-format-14 clang-format-13 clang-format-12 clang-format-11 clang-format-10 clang-format-9 clang-format-8 clang-format-7 clang-format-6.0 clang-format-5.0 clang-format-4.0)
  if(CLANG_FORMAT_BIN)
    set(CLANG_FORMAT_FOUND TRUE)
  endif()
endif()
if (${CLANG_FORMAT_FOUND})
  message(STATUS "clang hunt, found ${CLANG_FORMAT_BIN}")
  # runs clang format and updates files in place.

  add_custom_target(format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1)

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0)
else()
  message(STATUS "was unable to find clang-format")
endif()

###########################################################
# Doxygen documentation
###########################################################

find_package(Doxygen)
if(DOXYGEN_FOUND)
  file(GLOB_RECURSE TILEDB_C_API_HEADERS "${CMAKE_SOURCE_DIR}/tiledb/*_api_external.h")
  list(APPEND TILEDB_C_API_HEADERS
      "${CMAKE_CURRENT_SOURCE_DIR}/tiledb/api/c_api/api_external_common.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/tiledb/sm/c_api/tiledb.h"
  )
  file(GLOB TILEDB_CPP_API_HEADERS
      "${CMAKE_CURRENT_SOURCE_DIR}/tiledb/sm/cpp_api/*.h"
  )
  set(TILEDB_API_HEADERS ${TILEDB_C_API_HEADERS} ${TILEDB_CPP_API_HEADERS})
  add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/doxyfile.in
    COMMAND mkdir -p doxygen
    COMMAND echo INPUT = ${CMAKE_CURRENT_SOURCE_DIR}/tiledb/doxygen/mainpage.dox
      ${TILEDB_API_HEADERS} > ${CMAKE_CURRENT_BINARY_DIR}/doxyfile.in
    COMMENT "Preparing for Doxygen documentation" VERBATIM
  )
  add_custom_target(doc
    ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tiledb/doxygen/Doxyfile.mk >
      ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile.log 2>&1
    COMMENT "Generating API documentation with Doxygen" VERBATIM
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/doxyfile.in
  )
else(DOXYGEN_FOUND)
  add_custom_target(doc
    _______doc
    COMMENT "!! Docs cannot be built. Please install Doxygen and re-run cmake. !!" VERBATIM
  )
endif(DOXYGEN_FOUND)
