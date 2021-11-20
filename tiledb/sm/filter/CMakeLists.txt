#
# tiledb/sm/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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
target_link_libraries(filter PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(filter PUBLIC buffer $<TARGET_OBJECTS:buffer>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_filter EXCLUDE_FROM_ALL)
target_link_libraries(compile_filter PRIVATE filter)
target_sources(compile_filter PRIVATE
    test/compile_filter_main.cc $<TARGET_OBJECTS:filter>
)

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
target_sources(compile_bitshuffle_filter PRIVATE
    test/compile_bitshuffle_filter_main.cc $<TARGET_OBJECTS:bitshuffle_filter>
)

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
target_sources(compile_byteshuffle_filter PRIVATE
    test/compile_byteshuffle_filter_main.cc $<TARGET_OBJECTS:byteshuffle_filter>
)

##
## `filter_pipeline` object library
##
#add_library(filter_pipeline OBJECT
#    filter_pipeline.cc filter_create.cc
#    bit_width_reduction_filter.cc
#    checksum_md5_filter.cc checksum_sha256_filter.cc compression_filter.cc
#    encryption_aes256gcm_filter.cc noop_filter.cc positive_delta_filter.cc
#    # for expediency---belongs in future unit
#
#    )
#target_link_libraries(filter_pipeline PUBLIC baseline $<TARGET_OBJECTS:baseline>)
#target_link_libraries(filter_pipeline PUBLIC buffer $<TARGET_OBJECTS:buffer>)
#target_link_libraries(filter_pipeline PUBLIC constants $<TARGET_OBJECTS:constants>)
#target_link_libraries(filter_pipeline PUBLIC filter $<TARGET_OBJECTS:filter>)
#target_link_libraries(filter_pipeline PUBLIC bitshuffle_filter $<TARGET_OBJECTS:bitshuffle_filter>)
#target_link_libraries(filter_pipeline PUBLIC byteshuffle_filter $<TARGET_OBJECTS:byteshuffle_filter>)
#target_link_libraries(filter_pipeline PUBLIC stats $<TARGET_OBJECTS:stats>)
#target_link_libraries(filter_pipeline PUBLIC thread_pool $<TARGET_OBJECTS:thread_pool>)
##
## Test-compile of object library ensures link-completeness
##
#add_executable(compile_filter_pipeline EXCLUDE_FROM_ALL)
#target_link_libraries(compile_filter_pipeline PRIVATE filter_pipeline)
#target_sources(compile_filter_pipeline PRIVATE
#    test/compile_filter_pipeline_main.cc $<TARGET_OBJECTS:filter_pipeline>
#)
