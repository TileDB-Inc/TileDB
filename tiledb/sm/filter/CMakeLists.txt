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
include(object_library)

#
# `filter` object library
#
commence(object_library filter)
    this_target_sources(filter.cc filter_buffer.cc filter_storage.cc)
    this_target_object_libraries(baseline buffer crypto)
conclude(object_library)

#
# `bitshuffle_filter` object library
#
commence(object_library bitshuffle_filter)
    # Dependency on external/bitshuffle is direct, not by add_subdirectory
    cmake_path(APPEND TILEDB_SOURCE_ROOT "external/src/bitshuffle" OUTPUT_VARIABLE BITSHUFFLE_SOURCE_ROOT)
    cmake_path(APPEND TILEDB_SOURCE_ROOT "external/include/bitshuffle" OUTPUT_VARIABLE BITSHUFFLE_INCLUDE)
    list(APPEND BITSHUFFLE_SOURCES
        ${BITSHUFFLE_SOURCE_ROOT}/bitshuffle_core.cc
        ${BITSHUFFLE_SOURCE_ROOT}/iochain.cc
        )
    this_target_sources(bitshuffle_filter.cc ${BITSHUFFLE_SOURCES})
    this_target_object_libraries(baseline buffer filter)
    set_source_files_properties(
        bitshuffle_filter.cc ${BITSHUFFLE_SOURCES}
        PROPERTIES
        INCLUDE_DIRECTORIES "${BITSHUFFLE_INCLUDE}"
    )
conclude(object_library)

#
# `byteshuffle_filter` object library
#
# `byteshuffle_filter` depends on blosc, so we need to add its subdirectory. At
# the present time, that subdirectory is also being added in the main build.
# Adding the same directory twice causes an error. Thus, in the interim, we use
# `blosc-alt` for the binary. This also means that we're compiling blosc
# separately for the main build and for the unit. These separation is temporary
# for the duration of converting the main build into a unit-dependent one.
#
commence(object_library byteshuffle_filter)
    # Dependency on external/blosc
    cmake_path(APPEND TILEDB_SOURCE_ROOT "external/blosc" OUTPUT_VARIABLE BLOSC_SOURCE_ROOT)
    cmake_path(APPEND CMAKE_BINARY_DIR "external/blosc-alt" OUTPUT_VARIABLE BLOSC_BINARY_ROOT)
    add_subdirectory(${BLOSC_SOURCE_ROOT} ${BLOSC_BINARY_ROOT})
    this_target_sources(byteshuffle_filter.cc ${TileDB_blosc_SOURCES})
    this_target_object_libraries(baseline buffer filter)
    # [As of CMake 3.21] Setting private options on object libraries does not set
    # the corresponding properties on sources in the object library.
    set_source_files_properties(byteshuffle_filter.cc PROPERTIES INCLUDE_DIRECTORIES "${TILEDB_EXTERNAL_INCLUDE};${TileDB_blosc_INCLUDE_DIRS}")
    set_source_files_properties(${TileDB_blosc_SOURCES} PROPERTIES INCLUDE_DIRECTORIES "${TileDB_blosc_INCLUDE_DIRS}")
    set_source_files_properties(${TileDB_blosc_SOURCES} PROPERTIES COMPILE_OPTIONS "${TileDB_blosc_COMPILE_OPTIONS}")
conclude(object_library)

#
# `checksum_filters` object library
#
commence(object_library checksum_filters)
    cmake_path(APPEND TILEDB_SOURCE_ROOT "external/src/md5" OUTPUT_VARIABLE MD5_SOURCE_ROOT)
    this_target_sources(
        checksum_md5_filter.cc ${MD5_SOURCE_ROOT}/md5.cc checksum_sha256_filter.cc)
    this_target_object_libraries(config filter)
    set_source_files_properties(${MD5_SOURCE_ROOT}/md5.cc PROPERTIES INCLUDE_DIRECTORIES "${TILEDB_EXTERNAL_INCLUDE}")
conclude(object_library)

#
# `compression_filter` object library
#
commence(object_library compression_filter)
    this_target_sources(compression_filter.cc)
    this_target_object_libraries(constants compressors filter)
conclude(object_library)

#
# `encryption_filters` object library
#
commence(object_library encryption_filters)
    this_target_sources(encryption_aes256gcm_filter.cc)
    this_target_object_libraries(crypto filter)
conclude(object_library)

#
# `float_scaling_filter` object library
#
commence(object_library float_scaling_filter)
    this_target_sources(float_scaling_filter.cc)
    this_target_object_libraries(filter)
conclude(object_library)

#
# `xor_filter` object library
#
commence(object_library xor_filter)
    this_target_sources(xor_filter.cc)
    this_target_object_libraries(filter)
conclude(object_library)

#
# `bitsort_filters` object library
#
add_library(bitsort_filters OBJECT bitsort_filter.cc)
target_link_libraries(bitsort_filters PUBLIC filter $<TARGET_OBJECTS:filter>)
target_link_libraries(bitsort_filters PUBLIC tile $<TARGET_OBJECTS:tile>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_bitsort_filters EXCLUDE_FROM_ALL)
target_link_libraries(compile_bitsort_filters PRIVATE bitsort_filters)
target_sources(compile_bitsort_filters PRIVATE test/compile_bitsort_filter_main.cc)

#
# `webp_filters` object library.
#
add_library(webp_filters OBJECT webp_filter.cc)
target_link_libraries(webp_filters PUBLIC filter $<TARGET_OBJECTS:filter>)
target_link_libraries(webp_filters PUBLIC misc_types $<TARGET_OBJECTS:misc_types>)
target_link_libraries(webp_filters PUBLIC tile $<TARGET_OBJECTS:tile>)
if(TILEDB_WEBP)
    target_compile_definitions(webp_filters PUBLIC -DTILEDB_WEBP)
    find_package(Webp_EP REQUIRED)
    target_link_libraries(webp_filters PRIVATE WebP::webp)
endif()

#
# Test-compile of object library ensures link-completeness.
#
add_executable(compile_webp_filters EXCLUDE_FROM_ALL)
target_link_libraries(compile_webp_filters PRIVATE webp_filters)
target_sources(compile_webp_filters PRIVATE test/compile_webp_filter_main.cc)

#
# `all_filters` object library
#
commence(object_library all_filters)
    this_target_sources(filter_create.cc
        bit_width_reduction_filter.cc noop_filter.cc positive_delta_filter.cc)
    this_target_object_libraries(bitshuffle_filter byteshuffle_filter
        checksum_filters compression_filter encryption_filters float_scaling_filter
        xor_filter bitsort_filters webp_filters)
conclude(object_library)

#
# `filter_pipeline` object library
#
commence(object_library filter_pipeline)
    this_target_sources(filter_pipeline.cc)
    this_target_object_libraries(all_filters baseline buffer constants stats thread_pool tile)
conclude(object_library)

if (TILEDB_TESTS)
    add_executable(unit_filter_create EXCLUDE_FROM_ALL)
    target_link_libraries(unit_filter_create PUBLIC all_filters)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_filter_create PUBLIC Catch2::Catch2WithMain)

    add_executable(unit_filter_pipeline EXCLUDE_FROM_ALL)
    target_link_libraries(unit_filter_pipeline PUBLIC filter_pipeline)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_filter_pipeline PUBLIC Catch2::Catch2WithMain)

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
