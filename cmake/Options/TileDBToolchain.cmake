############################################################
# TileDB Toolchain Setup
############################################################

# Only enable vcpkg on Azure and GCS builds for now
if (NOT TILEDB_VCPKG AND NOT (TILEDB_AZURE OR TILEDB_GCS))
    return()
endif()

# For testing we're using --enable-gcs or --enable-azure
if((TILEDB_AZURE OR TILEDB_GCS) AND NOT TILEDB_VCPKG)
    set(TILEDB_VCPKG ON)
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

set(VCPKG_INSTALL_OPTIONS "--no-print-usage")

macro(tiledb_vcpkg_enable_if tiledb_feature vcpkg_feature)
    if(${tiledb_feature})
        list(APPEND VCPKG_MANIFEST_FEATURES ${vcpkg_feature})
    endif()
endmacro()

tiledb_vcpkg_enable_if(TILEDB_ABSEIL "abseil")
tiledb_vcpkg_enable_if(TILEDB_AZURE "azure")
tiledb_vcpkg_enable_if(TILEDB_GCS "gcs")
tiledb_vcpkg_enable_if(TILEDB_SERIALIZATION "serialization")
tiledb_vcpkg_enable_if(TILEDB_S3 "s3")
tiledb_vcpkg_enable_if(TILEDB_TESTS "tests")
tiledb_vcpkg_enable_if(TILEDB_TOOLS "tools")
tiledb_vcpkg_enable_if(TILEDB_WEBP "webp")

message("Using vcpkg features: ${VCPKG_MANIFEST_FEATURES}")
