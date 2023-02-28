############################################################
# TileDB Toolchain Setup
############################################################

if (TILEDB_GCS)
    if(DEFINED ENV{VCPKG_ROOT})
        set(CMAKE_TOOLCHAIN_FILE
            "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
            CACHE STRING "Vcpkg toolchain file"
        )
    else()
        include(init-submodule)
    endif()

    set(VCPKG_INSTALL_OPTIONS "--no-print-usage")
    list(APPEND VCPKG_MANIFEST_FEATURES "gcs")
endif()
