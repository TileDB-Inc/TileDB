#
# cmake/Options/TileDBToolchain.cmake
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
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
        set(VCPKG_COMMIT_STRING ac2a14f35fcd57d7a38f09af75dd5258e96dd6ac) # 2023-11-16
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

if(SANITIZER STREQUAL "address")
    if(NOT DEFINED VCPKG_TARGET_TRIPLET)
        message(FATAL_ERROR "VCPKG_TARGET_TRIPLET must be defined when building with ASAN.")
    elseif(NOT VCPKG_TARGET_TRIPLET MATCHES "-asan$")
        if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/../../ports/triplets/${VCPKG_TARGET_TRIPLET}-asan.cmake")
            message("Overriding vcpkg triplet to ${VCPKG_TARGET_TRIPLET}-asan")
            set(VCPKG_TARGET_TRIPLET "${VCPKG_TARGET_TRIPLET}-asan")
        else()
            message(FATAL_ERROR "Incompatible vcpkg triplet. It must end in -asan, or a triplet with the name ${VCPKG_TARGET_TRIPLET}-asan must exist in ports/triplets.")
        endif()
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
