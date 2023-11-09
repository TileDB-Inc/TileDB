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

if (NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    if(DEFINED ENV{VCPKG_ROOT})
        set(CMAKE_TOOLCHAIN_FILE
            "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
            CACHE STRING "Vcpkg toolchain file")
    elseif(NOT DEFINED ENV{TILEDB_DISABLE_AUTO_VCPKG})
        # Inspired from https://github.com/Azure/azure-sdk-for-cpp/blob/azure-core_1.10.3/cmake-modules/AzureVcpkg.cmake
        message("TILEDB_DISABLE_AUTO_VCPKG is not defined. Fetch a local copy of vcpkg.")
        # To help with resolving conflicts, when you update the commit, also update its date.
        set(VCPKG_COMMIT_STRING 1b4d69f3028d74401a001aa316986a670ca6289a) # 2023-09-27
        message("Vcpkg commit string used: ${VCPKG_COMMIT_STRING}")
        include(FetchContent)
        FetchContent_Declare(
            vcpkg
            GIT_REPOSITORY https://github.com/microsoft/vcpkg.git
            GIT_TAG        ${VCPKG_COMMIT_STRING}
            )
        FetchContent_MakeAvailable(vcpkg)
        set(CMAKE_TOOLCHAIN_FILE "${vcpkg_SOURCE_DIR}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file")
    endif()
endif()

if(NOT DEFINED VCPKG_TARGET_TRIPLET AND SANITIZER STREQUAL "address")
    if(APPLE)
        if (CMAKE_OSX_ARCHITECTURES STREQUAL x86_64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64)|(AMD64|amd64)|(^i.86$)")
            set(VCPKG_TARGET_TRIPLET "x64-osx-asan")
        elseif (CMAKE_OSX_ARCHITECTURES STREQUAL arm64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "^aarch64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
            set(VCPKG_TARGET_TRIPLET "arm64-osx-asan")
        endif()
    elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        set(VCPKG_TARGET_TRIPLET "x64-linux-asan")
    elseif(WIN32)
        set(VCPKG_TARGET_TRIPLET "x64-windows-asan")
    else()
        message(WARNING "Unrecognized platform for sanitizer build. Dependencies will not be compiled with ASAN.")
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
