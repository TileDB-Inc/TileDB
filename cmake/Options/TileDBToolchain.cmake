############################################################
# TileDB Toolchain Setup
############################################################

# Only enable vcpkg on GCS builds for now
if (NOT TILEDB_VCPKG AND NOT TILEDB_GCS)
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

set(VCPKG_INSTALL_OPTIONS "--no-print-usage")

if(TILEDB_ABSEIL)
    list(APPEND VCPKG_MANIFEST_FEATURES "with-abseil")
endif()

if(TILEDB_AZURE)
    list(APPEND VCPKG_MANIFEST_FEATURES "with-azure")
endif()

if(TILEDB_GCS)
    list(APPEND VCPKG_MANIFEST_FEATURES "with-gcs")
endif()

if(TILEDB_S3)
    list(APPEND VCPKG_MANIFEST_FEATURES "with-s3")
endif()
