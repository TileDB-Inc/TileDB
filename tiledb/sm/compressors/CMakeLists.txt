#
# tiledb/sm/compressors/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2021-2024 TileDB, Inc.
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
        rle_compressor.cc zstd_compressor.cc)
    this_target_object_libraries(baseline buffer)
    find_package(BZip2 REQUIRED)
    find_package(lz4 CONFIG REQUIRED)
    find_package(ZLIB REQUIRED)
    find_package(zstd CONFIG REQUIRED)
    this_target_link_libraries(BZip2::BZip2 LZ4::lz4 ZLIB::ZLIB zstd::libzstd)
conclude(object_library)

add_test_subdirectory()
