#
# CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017 TileDB, Inc.
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

project(bzip2)
cmake_minimum_required(VERSION 3.21)

set(SOURCES "blocksort.c" "huffman.c" "crctable.c" "randtable.c"
"compress.c" "decompress.c" "bzlib.c")
file(GLOB HEADERS "bzlib.h")

# Set build flags
if (WIN32)
  set(CMAKE_C_FLAGS "/DWIN32 /D_FILE_OFFSET_BITS=64 /MD /Ox /nologo")
else()
  set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -Wall -Winline -O2 -g")
endif()

# Create libraries
add_library(bzip2_static STATIC ${SOURCES})
set_target_properties(bzip2_static PROPERTIES OUTPUT_NAME "libbz2static")
add_library(bzip2_shared SHARED ${SOURCES} libbz2.def)
set_target_properties(bzip2_shared PROPERTIES OUTPUT_NAME "libbz2")

# Install libraries
install(
  TARGETS bzip2_static bzip2_shared
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)
install(FILES ${HEADERS} DESTINATION include)
