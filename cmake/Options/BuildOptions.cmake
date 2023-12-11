############################################################
# TileDB Build options
############################################################

# Note: when adding options, make sure to forward them via INHERITED_CMAKE_ARGS
# in TileDB-Superbuild.cmake.

include(CMakeDependentOption)

option(TILEDB_SANITIZER "Sets the sanitizers to use. Only address is currently supported." "")
option(TILEDB_VCPKG_BASE_TRIPLET "Sets the base vcpkg triplet when building with sanitizers." "")
option(TILEDB_REMOVE_DEPRECATIONS "If true, do not build deprecated APIs." OFF)
option(TILEDB_VERBOSE "Prints TileDB errors with verbosity" OFF)
option(TILEDB_S3 "Enables S3/minio support using aws-cpp-sdk" OFF)
option(TILEDB_AZURE "Enables Azure Storage support using azure-storage-blobs-cpp" OFF)
option(TILEDB_GCS "Enables GCS Storage support using google-cloud-cpp" OFF)
option(TILEDB_HDFS "Enables HDFS support using the official Hadoop JNI bindings" OFF)
option(TILEDB_WERROR "Enables the -Werror flag during compilation." ON)
option(TILEDB_ASSERTIONS "Build with assertions enabled (default off for release, on for debug build)." OFF)
option(TILEDB_CPP_API "Enables building of the TileDB C++ API." ON)
option(TILEDB_CMAKE_IDE "(Used for CLion builds). Disables superbuild and sets the EP install dir." OFF)
option(TILEDB_STATS "Enables internal TileDB statistics gathering." ON)
option(BUILD_SHARED_LIBS "Enables building TileDB as a shared library." ON)
option(TILEDB_TESTS "If true, enables building the TileDB unit test suite" ON)
option(TILEDB_TOOLS "If true, enables building the TileDB tools" OFF)
option(TILEDB_SERIALIZATION "If true, enables building with support for query serialization" OFF)
option(TILEDB_CCACHE "If true, enables use of 'ccache' (if present)" OFF)
option(TILEDB_ARROW_TESTS "If true, enables building the arrow adapter unit tests" OFF)
option(TILEDB_WEBP "If true, enables building webp and a simple linkage test" ON)
option(TILEDB_LOG_OUTPUT_ON_FAILURE "If true, print error logs if dependency sub-project build fails" ON)
option(TILEDB_SKIP_S3AWSSDK_DIR_LENGTH_CHECK "If true, skip check needed path length for awssdk (TILEDB_S3) dependent builds" OFF)
option(TILEDB_EXPERIMENTAL_FEATURES "If true, build and include experimental features" OFF)
option(TILEDB_TESTS_AWS_S3_CONFIG "Use an S3 config appropriate for AWS in tests" OFF)

option(CMAKE_EXPORT_COMPILE_COMMANDS "cmake compile commands" ON)

set(TILEDB_INSTALL_LIBDIR "" CACHE STRING "If non-empty, install TileDB library to this directory instead of CMAKE_INSTALL_LIBDIR.")

if (DEFINED TILEDB_STATIC)
  message(DEPRECATION "TILEDB_STATIC is deprecated and will be removed in version 2.28, to be released in Q3 2024. Use BUILD_SHARED_LIBS INSTEAD. Building both static and shared libraries is no longer available.")
  if (TILEDB_STATIC)
    set(BUILD_SHARED_LIBS OFF)
  else()
    set(BUILD_SHARED_LIBS ON)
  endif()
endif()

# enable assertions by default for debug builds
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(TILEDB_ASSERTIONS TRUE)
endif()

include(TileDBAssertions)
