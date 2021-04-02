#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2018-2021 TileDB, Inc.
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

cmake_minimum_required(VERSION 2.8)
project(TileDBBenchmark)

# Set C++11 as required standard for all C++ targets (required to use the TileDB
# C++ API).
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


if (WIN32)
  if (CMAKE_BUILD_TYPE MATCHES "Debug")
    add_compile_options(/DDEBUG /Od /Zi)
  elseif (CMAKE_BUILD_TYPE MATCHES "Release")
    add_compile_options(/DNDEBUG /Ox)
  endif()
else()
  if (CMAKE_BUILD_TYPE MATCHES "Debug")
    add_compile_options(-DDEBUG -O0 -g3 -ggdb3 -gdwarf-3)
  elseif (CMAKE_BUILD_TYPE MATCHES "Release")
    add_compile_options(-DNDEBUG -O3)
  endif()
endif()

# Find TileDB
find_package(TileDB REQUIRED)

# Shared code
add_library(benchmark_core OBJECT
  benchmark.cc
)

# List of benchmarks
set(BENCHMARKS
  bench_dense_attribute_filtering
  bench_dense_read_large_tile
  bench_dense_read_small_tile
  bench_dense_tile_cache
  bench_dense_write_large_tile
  bench_dense_write_small_tile
  bench_large_io
  bench_selective_unfiltering
  bench_sparse_read_large_tile
  bench_sparse_read_small_tile
  bench_sparse_tile_cache
  bench_sparse_write_large_tile
  bench_sparse_write_small_tile
)

foreach(NAME IN LISTS BENCHMARKS)
  add_executable(${NAME}
    "${NAME}.cc"
    $<TARGET_OBJECTS:benchmark_core>
  )
  target_link_libraries(${NAME} TileDB::tiledb_shared pthread)
endforeach()
