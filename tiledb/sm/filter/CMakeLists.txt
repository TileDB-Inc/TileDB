#
# tiledb/sm/filter/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021-2022 TileDB, Inc.
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

include(common NO_POLICY_SCOPE)

#
# `filter` object library
#
add_library(filter OBJECT filter.cc filter_buffer.cc filter_storage.cc)
if (APPLE)
#  set_target_properties(filter PROPERTIES "OSX_ARCHITECTURES" "arm64;x86_64")
  set_target_properties(filter PROPERTIES "OSX_ARCHITECTURES" "x86_64;arm64")
endif()
target_link_libraries(filter PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(filter PUBLIC buffer $<TARGET_OBJECTS:buffer>)
target_link_libraries(filter PUBLIC crypto $<TARGET_OBJECTS:crypto>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_filter EXCLUDE_FROM_ALL)
target_link_libraries(compile_filter PRIVATE filter)
target_sources(compile_filter PRIVATE test/compile_filter_main.cc)

#
# `bitshuffle_filter` object library
#
# Dependency on external/bitshuffle is direct, not by add_subdirectory
cmake_path(APPEND TILEDB_SOURCE_ROOT "external/src/bitshuffle" OUTPUT_VARIABLE BITSHUFFLE_SOURCE_ROOT)
cmake_path(APPEND TILEDB_SOURCE_ROOT "external/include/bitshuffle" OUTPUT_VARIABLE BITSHUFFLE_INCLUDE)
list(APPEND BITSHUFFLE_SOURCES
    ${BITSHUFFLE_SOURCE_ROOT}/bitshuffle_core.cc
    ${BITSHUFFLE_SOURCE_ROOT}/iochain.cc
)
# The object library
add_library(bitshuffle_filter OBJECT bitshuffle_filter.cc ${BITSHUFFLE_SOURCES})
target_link_libraries(bitshuffle_filter PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(bitshuffle_filter PUBLIC buffer $<TARGET_OBJECTS:buffer>)
target_link_libraries(bitshuffle_filter PUBLIC filter $<TARGET_OBJECTS:filter>)
set_source_files_properties(
    bitshuffle_filter.cc ${BITSHUFFLE_SOURCES}
    PROPERTIES
    INCLUDE_DIRECTORIES "${BITSHUFFLE_INCLUDE}"
)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_bitshuffle_filter EXCLUDE_FROM_ALL)
target_link_libraries(compile_bitshuffle_filter PRIVATE bitshuffle_filter)
target_sources(compile_bitshuffle_filter PRIVATE test/compile_bitshuffle_filter_main.cc)

#
# `bitshuffle_filter` object library
#
# `bitshuffle_filter` depends on blosc, so we need to add its subdirectory. At
# the present time, that subdirectory is also being added in the main build.
# Adding the same directory twice causes an error. Thus, in the interim, we use
# `blosc-alt` for the binary. This also means that we're compiling blosc
# separately for the main build and for the unit. These separation is temporary
# for the duration of converting the main build into a unit-dependent one.
#
# Dependency on external/blosc
cmake_path(APPEND TILEDB_SOURCE_ROOT "external/blosc" OUTPUT_VARIABLE BLOSC_SOURCE_ROOT)
cmake_path(APPEND CMAKE_BINARY_DIR "external/blosc-alt" OUTPUT_VARIABLE BLOSC_BINARY_ROOT)
add_subdirectory(${BLOSC_SOURCE_ROOT} ${BLOSC_BINARY_ROOT})
# The object library
add_library(byteshuffle_filter OBJECT byteshuffle_filter.cc ${TileDB_blosc_SOURCES})
# Dependencies on other units
target_link_libraries(byteshuffle_filter PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(byteshuffle_filter PUBLIC buffer $<TARGET_OBJECTS:buffer>)
target_link_libraries(byteshuffle_filter PUBLIC filter $<TARGET_OBJECTS:filter>)
# [As of CMake 3.21] Setting private options on object libraries does not set
# the corresponding properties on sources in the object library.
set_source_files_properties(byteshuffle_filter.cc PROPERTIES INCLUDE_DIRECTORIES "${TILEDB_EXTERNAL_INCLUDE};${TileDB_blosc_INCLUDE_DIRS}")
set_source_files_properties(${TileDB_blosc_SOURCES} PROPERTIES INCLUDE_DIRECTORIES "${TileDB_blosc_INCLUDE_DIRS}")
set_source_files_properties(${TileDB_blosc_SOURCES} PROPERTIES COMPILE_OPTIONS "${TileDB_blosc_COMPILE_OPTIONS}")
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_byteshuffle_filter EXCLUDE_FROM_ALL)
target_link_libraries(compile_byteshuffle_filter PRIVATE byteshuffle_filter)
target_sources(compile_byteshuffle_filter PRIVATE test/compile_byteshuffle_filter_main.cc)

#
# `checksum_filters` object library
#
cmake_path(APPEND TILEDB_SOURCE_ROOT "external/src/md5" OUTPUT_VARIABLE MD5_SOURCE_ROOT)
add_library(checksum_filters OBJECT
    checksum_md5_filter.cc ${MD5_SOURCE_ROOT}/md5.cc
    checksum_sha256_filter.cc
)
target_link_libraries(checksum_filters PUBLIC config $<TARGET_OBJECTS:config>)
target_link_libraries(checksum_filters PUBLIC filter $<TARGET_OBJECTS:filter>)
set_source_files_properties(${MD5_SOURCE_ROOT}/md5.cc PROPERTIES INCLUDE_DIRECTORIES "${TILEDB_EXTERNAL_INCLUDE}")
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_checksum_filters EXCLUDE_FROM_ALL)
target_link_libraries(compile_checksum_filters PRIVATE checksum_filters)
target_sources(compile_checksum_filters PRIVATE test/compile_checksum_filters_main.cc)

#
# `compression_filter` object library
#
add_library(compression_filter OBJECT compression_filter.cc)
target_link_libraries(compression_filter PUBLIC filter $<TARGET_OBJECTS:filter>)
target_link_libraries(compression_filter PUBLIC compressors $<TARGET_OBJECTS:compressors>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_compression_filter EXCLUDE_FROM_ALL)
target_link_libraries(compile_compression_filter PRIVATE compression_filter)
target_sources(compile_compression_filter PRIVATE test/compile_compression_filter_main.cc)

#
# `encryption_filters` object library
#
add_library(encryption_filters OBJECT encryption_aes256gcm_filter.cc)
target_link_libraries(encryption_filters PUBLIC crypto $<TARGET_OBJECTS:crypto>)
target_link_libraries(encryption_filters PUBLIC filter $<TARGET_OBJECTS:filter>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_encryption_filters EXCLUDE_FROM_ALL)
target_link_libraries(compile_encryption_filters PRIVATE encryption_filters)
target_sources(compile_encryption_filters PRIVATE test/compile_encryption_filters_main.cc)

#
# `all_filters` object library
#
add_library(all_filters OBJECT
    filter_create.cc
    bit_width_reduction_filter.cc noop_filter.cc positive_delta_filter.cc
)
target_link_libraries(all_filters PUBLIC bitshuffle_filter $<TARGET_OBJECTS:bitshuffle_filter>)
target_link_libraries(all_filters PUBLIC byteshuffle_filter $<TARGET_OBJECTS:byteshuffle_filter>)
target_link_libraries(all_filters PUBLIC checksum_filters $<TARGET_OBJECTS:checksum_filters>)
target_link_libraries(all_filters PUBLIC compression_filter $<TARGET_OBJECTS:compression_filter>)
target_link_libraries(all_filters PUBLIC encryption_filters $<TARGET_OBJECTS:encryption_filters>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_all_filters EXCLUDE_FROM_ALL)
target_link_libraries(compile_all_filters PRIVATE all_filters)
target_sources(compile_all_filters PRIVATE test/compile_all_filters_main.cc)

#
# `filter_pipeline` object library
#
add_library(filter_pipeline OBJECT
    filter_pipeline.cc
)
target_link_libraries(filter_pipeline PUBLIC all_filters $<TARGET_OBJECTS:all_filters>)
target_link_libraries(filter_pipeline PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(filter_pipeline PUBLIC buffer $<TARGET_OBJECTS:buffer>)
target_link_libraries(filter_pipeline PUBLIC constants $<TARGET_OBJECTS:constants>)
target_link_libraries(filter_pipeline PUBLIC stats $<TARGET_OBJECTS:stats>)
target_link_libraries(filter_pipeline PUBLIC thread_pool $<TARGET_OBJECTS:thread_pool>)
target_link_libraries(filter_pipeline PUBLIC tile $<TARGET_OBJECTS:tile>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_filter_pipeline EXCLUDE_FROM_ALL)
target_link_libraries(compile_filter_pipeline PRIVATE filter_pipeline)
target_sources(compile_filter_pipeline PRIVATE test/compile_filter_pipeline_main.cc)



if (TILEDB_TESTS)
    add_executable(unit_filter_create EXCLUDE_FROM_ALL)
    target_link_libraries(unit_filter_create PUBLIC all_filters)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_filter_create PUBLIC Catch2::Catch2)

    add_executable(unit_filter_pipeline EXCLUDE_FROM_ALL)
    target_link_libraries(unit_filter_pipeline PUBLIC filter_pipeline)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_filter_pipeline PUBLIC Catch2::Catch2)

    # Sources for tests
    target_sources(unit_filter_create PUBLIC
            test/main.cc
            test/unit_filter_create.cc
            )

    target_sources(unit_filter_pipeline PUBLIC
            test/main.cc
            test/unit_filter_pipeline.cc
            )

    add_test(
            NAME "unit_filter_create"
            COMMAND $<TARGET_FILE:unit_filter_create>
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )

    add_test(
            NAME "unit_filter_pipeline"
            COMMAND $<TARGET_FILE:unit_filter_pipeline>
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
