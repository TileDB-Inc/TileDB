############################################################
# TileDB Toolchain Setup
############################################################

# Only enable vcpkg on GCS builds for now
if (NOT TILEDB_GCS)
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
list(APPEND VCPKG_MANIFEST_FEATURES "gcs")
