#
# external/blosc/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2021 TileDB, Inc.
# Copyright (c) 2016 MIT and Intel Corporation
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
# Precondition
#   Expects caller to be <top>/tiledb/CMakeLists.txt. CMake (as of 3.20) does not have a way of retrieving the caller's
#   directories, nor of passing argument for the callee in add_subdirectory(). Here we avoid premature modularization
#   and hard-code the caller.
# Outputs
#   EXTERNAL_BLOSC_SOURCES -- a list of sources to compile
#   EXTERNAL_BLOSC_INCLUDES -- a list of directories to include
# Side effects
#   Sets outputs in PARENT_SCOPE
#   Sets compilation properties in ${CMAKE_SOURCE_DIR}/tiledb

list(APPEND SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/shuffle.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/shuffle-generic.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/bitshuffle-stub.c
)

try_compile(AVX2_DETECTED ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/cmake/detect-avx2.c)
if(AVX2_DETECTED)
  list(APPEND SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/shuffle-avx2.c
  )
endif()

try_compile(SSE2_DETECTED ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/cmake/detect-sse2.c)
if(SSE2_DETECTED)
  list(APPEND SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/shuffle-sse2.c
  )
endif()

# The blosc files are external and we ignore their warnings.
set_source_files_properties(
  ${SOURCES}
  DIRECTORY ${CMAKE_SOURCE_DIR}/tiledb    # set properties in the directory that compiles
  PROPERTIES COMPILE_FLAGS "-w"           # -w is common to gcc and MSVC
)
# Our modified shuffle.c needs a definition to trigger the modifications
# We need to repeat -w because CMake does not have a way to append to source file properties
set_source_files_properties(
    ${CMAKE_CURRENT_SOURCE_DIR}/src/shuffle.c
    DIRECTORY ${CMAKE_SOURCE_DIR}/tiledb    # set properties in the directory that compiles
    PROPERTIES COMPILE_FLAGS "-w -DINITIALIZE_WITHOUT_PTHREAD"
)

# Add initialize-shuffle.cc after ignoring warnings for the external files.
list(APPEND SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/initialize-shuffle.cc
)

# Return values to caller
set(EXTERNAL_BLOSC_SOURCES ${SOURCES} PARENT_SCOPE)
set(EXTERNAL_BLOSC_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/include PARENT_SCOPE)

