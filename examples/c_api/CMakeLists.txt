#
# c_api/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2018 TileDB, Inc.
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

# Function that builds an executable per example
function(build_TileDB_example_capi TARGET)
  add_executable(${TARGET}_c EXCLUDE_FROM_ALL ${TARGET}.c)
  if (WIN32)
    target_link_libraries(${TARGET}_c tiledb_static
            ${TILEDB_LIB_DEPENDENCIES} ${S3_LIB_DEPENDENCIES})
  else()
    target_link_libraries(${TARGET}_c tiledb_static -lpthread
            ${TILEDB_LIB_DEPENDENCIES} ${S3_LIB_DEPENDENCIES})
  endif()
endfunction()

# Get the example sources
file(GLOB TILEDB_EXAMPLE_SOURCES_CAPI "*.c")

# Include TileDB C API headers
include_directories(BEFORE 
    "${CMAKE_SOURCE_DIR}"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/"
    "${CMAKE_BINARY_DIR}/examples/"
)

# Initialize name for example binaries
set(EXAMPLE_C_BINS)

# Iterate over all example sources and call the build function
foreach(EXAMPLE_SOURCE ${TILEDB_EXAMPLE_SOURCES_CAPI})
  # Get the binary name
  STRING(REGEX REPLACE "^${CMAKE_SOURCE_DIR}/examples/c_api/" ""
         EXAMPLE_STRIPPED ${EXAMPLE_SOURCE}
  )
  STRING(REGEX REPLACE ".c$" "" EXAMPLE_BIN ${EXAMPLE_STRIPPED})
  set(EXAMPLE_C_BINS ${EXAMPLE_C_BINS} ${EXAMPLE_BIN}_c)

  # Build example executable
  build_TileDB_example_capi(${EXAMPLE_BIN})
endforeach()

# Add custom target 'examples'
add_custom_target(examples_c DEPENDS tiledb_static)
add_dependencies(examples_c ${EXAMPLE_C_BINS} tiledb_static)
