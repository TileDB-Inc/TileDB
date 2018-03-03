#
# examples/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2018 TileDB, Inc.
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

set(CMAKE_C_FLAGS "-std=c99 ${CMAKE_C_FLAGS}")

if (WIN32)
  # Disable MSVC warnings not present with gcc.
  add_compile_options("/wd4204") # C4204: non-constant aggregate initializer
  add_compile_options("/wd4221") # C4221: aggregate initializer with local variable
  add_compile_options("/wd4305") # C4305: implicit truncation of double to float
endif()

add_subdirectory(c_api)
add_subdirectory(cpp_api)

# Add custom target 'examples'
add_custom_target(examples DEPENDS tiledb-header-dir examples_c examples_cpp)

# Create directory for the TileDB headers
add_custom_target(
        tiledb-header-dir
        COMMAND ${CMAKE_COMMAND} -E make_directory
        "${CMAKE_CURRENT_BINARY_DIR}/tiledb")

# Copy C API TileDB headers
file(GLOB TILEDB_C_HEADERS "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/*.h")
foreach(HEADER ${TILEDB_C_HEADERS})
  STRING(REGEX REPLACE "^${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/" ""
         HEADER_STRIPPED ${HEADER}
  )
  configure_file(${HEADER} ${CMAKE_CURRENT_BINARY_DIR}/tiledb/${HEADER_STRIPPED} COPYONLY)
endforeach()

# Copy C++ API TileDB headers
file(GLOB TILEDB_CPP_HEADERS
        "${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/*.h"
        "${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/tiledb")
foreach(HEADER ${TILEDB_CPP_HEADERS})
  STRING(REGEX REPLACE "^${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/" ""
         HEADER_STRIPPED ${HEADER}
  )
  configure_file(${HEADER} ${CMAKE_CURRENT_BINARY_DIR}/tiledb/${HEADER_STRIPPED} COPYONLY)
endforeach()
