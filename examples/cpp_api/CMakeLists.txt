#
# cpp_api/CMakeLists.txt
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
function(build_TileDB_example_cppapi TARGET)
  add_executable(${TARGET}_cpp EXCLUDE_FROM_ALL ${TARGET}.cc)
  if (WIN32)
    target_link_libraries(${TARGET}_cpp tiledb_static
            ${TILEDB_LIB_DEPENDENCIES} ${S3_LIB_DEPENDENCIES})
  else()
    target_link_libraries(${TARGET}_cpp tiledb_static -lpthread
            ${TILEDB_LIB_DEPENDENCIES} ${S3_LIB_DEPENDENCIES})
  endif()
endfunction()

# Get the example sources
file(GLOB TILEDB_EXAMPLE_SOURCES_CPPAPI "*.cc")

# Include TileDB C API headers
include_directories(BEFORE 
    "${CMAKE_SOURCE_DIR}"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/c_api/"
    "${CMAKE_SOURCE_DIR}/tiledb/sm/cpp_api/"
    "${CMAKE_BINARY_DIR}/examples/"
)

# Initialize name for example binaries
set(EXAMPLE_CPP_BINS)

# Iterate over all example sources and call the build function
foreach(EXAMPLE_SOURCE ${TILEDB_EXAMPLE_SOURCES_CPPAPI})
  # Get the binary name
  STRING(REGEX REPLACE "^${CMAKE_SOURCE_DIR}/examples/cpp_api/" ""
          EXAMPLE_STRIPPED ${EXAMPLE_SOURCE}
          )
  STRING(REGEX REPLACE ".cc$" "" EXAMPLE_BIN ${EXAMPLE_STRIPPED})
  set(EXAMPLE_CPP_BINS ${EXAMPLE_CPP_BINS} ${EXAMPLE_BIN}_cpp)

  # Build example executable
  build_TileDB_example_cppapi(${EXAMPLE_BIN})
endforeach()

# Add custom target 'examples'
add_custom_target(examples_cpp DEPENDS tiledb_static)
add_dependencies(examples_cpp ${EXAMPLE_CPP_BINS} tiledb_static)
