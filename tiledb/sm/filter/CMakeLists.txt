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
    this_target_object_libraries(baseline buffer tiledb_crypto)
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
commence(object_library byteshuffle_filter)
    this_target_sources(byteshuffle_filter.cc)
    this_target_object_libraries(baseline blosc buffer filter)
conclude(object_library)

#
# `checksum_filters` object library
#
commence(object_library checksum_filters)
    this_target_sources(
        checksum_md5_filter.cc checksum_sha256_filter.cc)
    this_target_object_libraries(config filter)
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
    this_target_object_libraries(tiledb_crypto filter)
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
# `webp_filter` object library.
#
#add_library(webp_filter OBJECT webp_filter.cc)
#target_link_libraries(webp_filter PUBLIC filter $<TARGET_OBJECTS:filter>)
#target_link_libraries(webp_filter PUBLIC misc_types $<TARGET_OBJECTS:misc_types>)
#target_link_libraries(webp_filter PUBLIC tile $<TARGET_OBJECTS:tile>)
#if(TILEDB_WEBP)
#    target_compile_definitions(webp_filter PUBLIC -DTILEDB_WEBP)
#    find_package(WebP REQUIRED)
#    target_link_libraries(webp_filter PRIVATE WebP::webp)
#endif()

commence(object_library webp_filter)
    this_target_sources(webp_filter.cc)
    this_target_object_libraries(filter misc_types tile)
    if(TILEDB_WEBP)
        this_target_compile_definitions(-DTILEDB_WEBP)
        find_package(WebP REQUIRED)
        this_target_link_libraries(WebP::webp)
    endif()
conclude(object_library)

#
# Test-compile of object library ensures link-completeness.
#
#add_executable(compile_webp_filter EXCLUDE_FROM_ALL)
#target_link_libraries(compile_webp_filter PRIVATE webp_filter)
#target_sources(compile_webp_filter PRIVATE test/compile_webp_filter_main.cc)

#
# `all_filters` object library
#
commence(object_library all_filters)
    this_target_sources(filter_create.cc
        bit_width_reduction_filter.cc noop_filter.cc positive_delta_filter.cc)
    this_target_object_libraries(bitshuffle_filter byteshuffle_filter
        checksum_filters compression_filter encryption_filters float_scaling_filter
        xor_filter webp_filter)
conclude(object_library)

#
# `filter_pipeline` object library
#
commence(object_library filter_pipeline)
    this_target_sources(filter_pipeline.cc)
    this_target_object_libraries(all_filters baseline buffer constants stats thread_pool tile)
conclude(object_library)

add_test_subdirectory()
