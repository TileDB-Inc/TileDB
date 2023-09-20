############################################################
# TileDB Toolchain Setup
############################################################

if (NOT TILEDB_VCPKG)
    return()
endif()

# We've already run vcpkg by the time the super build is finished
if (NOT TILEDB_SUPERBUILD)
    return()
endif()

if(DEFINED ENV{VCPKG_ROOT})
    set(CMAKE_TOOLCHAIN_FILE
        "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "Vcpkg toolchain file")
else()
    include(init-submodule)
    set(CMAKE_TOOLCHAIN_FILE
        "${CMAKE_CURRENT_SOURCE_DIR}/external/vcpkg/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "Vcpkg toolchain file")
endif()

if(APPLE AND NOT DEFINED VCPKG_TARGET_TRIPLET)
    if (CMAKE_OSX_ARCHITECTURES STREQUAL x86_64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64)|(AMD64|amd64)|(^i.86$)")
        set(VCPKG_TARGET_TRIPLET "x64-macos")
    elseif (CMAKE_OSX_ARCHITECTURES STREQUAL arm64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "^aarch64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
        set(VCPKG_TARGET_TRIPLET "arm64-macos")
    endif()
endif()

set(VCPKG_INSTALL_OPTIONS "--no-print-usage")

macro(tiledb_vcpkg_enable_if tiledb_feature vcpkg_feature)
    if(${tiledb_feature})
        list(APPEND VCPKG_MANIFEST_FEATURES ${vcpkg_feature})
    endif()
endmacro()

tiledb_vcpkg_enable_if(TILEDB_AZURE "azure")
tiledb_vcpkg_enable_if(TILEDB_GCS "gcs")
tiledb_vcpkg_enable_if(TILEDB_SERIALIZATION "serialization")
tiledb_vcpkg_enable_if(TILEDB_S3 "s3")
tiledb_vcpkg_enable_if(TILEDB_TESTS "tests")
tiledb_vcpkg_enable_if(TILEDB_TOOLS "tools")
tiledb_vcpkg_enable_if(TILEDB_WEBP "webp")

message("Using vcpkg features: ${VCPKG_MANIFEST_FEATURES}")
