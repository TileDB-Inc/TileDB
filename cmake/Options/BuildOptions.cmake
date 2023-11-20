############################################################
# TileDB Build options
############################################################

# Note: when adding options, make sure to forward them via INHERITED_CMAKE_ARGS
# in TileDB-Superbuild.cmake.

include(CMakeDependentOption)

option(TILEDB_SUPERBUILD "If true, perform a superbuild (builds all missing dependencies)." ON)
option(TILEDB_VCPKG "If true, use vcpkg to download and build dependencies." ON)
cmake_dependent_option(TILEDB_FORCE_ALL_DEPS "If true, force superbuild to download and build all dependencies, even those installed on the system." OFF "NOT TILEDB_VCPKG" OFF)
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
option(TILEDB_STATIC "Enables building TileDB as a static library." OFF)
cmake_dependent_option(TILEDB_INSTALL_FIND_DEPENDENCIES "Enables calls to find_dependency in the exported config file. Should be disabled only when building a shared library with all dependencies statically linked." ON "NOT TILEDB_STATIC" ON)
option(TILEDB_TESTS "If true, enables building the TileDB unit test suite" ON)
option(TILEDB_TOOLS "If true, enables building the TileDB tools" OFF)
option(TILEDB_SERIALIZATION "If true, enables building with support for query serialization" OFF)
option(TILEDB_CCACHE "If true, enables use of 'ccache' (if present)" OFF)
option(TILEDB_ARROW_TESTS "If true, enables building the arrow adapter unit tests" OFF)
option(TILEDB_CRC32 "If true, enables building crc32 and a simple linkage test" OFF)
option(TILEDB_WEBP "If true, enables building webp and a simple linkage test" ON)
option(TILEDB_LOG_OUTPUT_ON_FAILURE "If true, print error logs if dependency sub-project build fails" ON)
option(TILEDB_SKIP_S3AWSSDK_DIR_LENGTH_CHECK "If true, skip check needed path length for awssdk (TILEDB_S3) dependent builds" OFF)
option(TILEDB_EXPERIMENTAL_FEATURES "If true, build and include experimental features" OFF)
option(TILEDB_TESTS_AWS_S3_CONFIG "Use an S3 config appropriate for AWS in tests" OFF)
option(TILEDB_TESTS_ENABLE_REST "Enables REST tests (requires running REST server)" OFF)

option(CMAKE_EXPORT_COMPILE_COMMANDS "cmake compile commands" ON)
option(_TILEDB_CMAKE_INIT_GIT_SUBMODULES "Check submodules during build" ON)

set(TILEDB_INSTALL_LIBDIR "" CACHE STRING "If non-empty, install TileDB library to this directory instead of CMAKE_INSTALL_LIBDIR.")

if (NOT TILEDB_VCPKG)
  message(DEPRECATION "Disabling TILEDB_VCPKG is deprecated and will be removed in a future version.")
endif()

# enable assertions by default for debug builds
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(TILEDB_ASSERTIONS TRUE)
endif()

include(TileDBAssertions)
