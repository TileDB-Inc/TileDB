#
# tiledb/sm/compressors/CMakeLists.txt
#
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
# `compressors` object library
#
commence(object_library compressors)
    this_target_sources(
        bzip_compressor.cc dd_compressor.cc delta_compressor.cc
        dict_compressor.cc gzip_compressor.cc lz4_compressor.cc
        rle_compressor.cc zstd_compressor.cc util/gzip_wrappers.cc)
    this_target_object_libraries(baseline buffer)
    find_package(Bzip2_EP REQUIRED)
    find_package(LZ4_EP REQUIRED)
    find_package(Zlib_EP REQUIRED)
    find_package(Zstd_EP REQUIRED)
    this_target_link_libraries(BZip2::BZip2 lz4::lz4 ZLIB::ZLIB ${ZSTD_TARGET})
conclude(object_library)

#
# tiledb crude gzip archiver initially for embedding magic.mgc
#
add_executable(tdb_gzip_embedded_data)
target_sources(tdb_gzip_embedded_data PRIVATE
        util/tdb_gzip_embedded_data.cc
)

target_link_libraries(
    tdb_gzip_embedded_data PUBLIC
    filter
    compressors
    )

target_include_directories(tdb_gzip_embedded_data PRIVATE
        $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS,INCLUDE_DIRECTORIES>
)

# Link to Threads::Threads if library or flag needed to enable threading.
find_package(Threads REQUIRED)

if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(tdb_gzip_embedded_data PRIVATE Threads::Threads)
endif()
# On Linux, must explicitly link -ldl for static linking to libzstd.
if (NOT WIN32)
  target_link_libraries(tdb_gzip_embedded_data PRIVATE dl)
endif()

add_test_subdirectory()
