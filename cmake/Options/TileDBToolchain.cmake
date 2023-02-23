############################################################
# TileDB Toolchain Setup
############################################################


if(DEFINED ENV{VCPKG_ROOT})
    set(CMAKE_TOOLCHAIN_FILE
        "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "Vcpkg toolchain file"
    )
else()
    include(init-submodule.cmake)
endif()

set(VCPKG_INSTALL_OPTIONS "--no-print-usage")

if (TILEDB_GCS)
    list(APPEND VCPKG_MANIFEST_FEATURES "gcs")
endif()

if (TILEDB_S3)
    list(APPEND VCPKG_MANIFEST_FEATURES "s3")
endif()

if (TILEDB_SERIALIZATION)
    list(APPEND VCPKG_MANIFEST_FEATURES "serialization")
endif()
