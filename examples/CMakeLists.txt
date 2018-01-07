#
# examples/CMakeLists.txt
#
#
# The MIT License
#
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

# Function that builds an executable per example
function(build_TileDB_example_capi TARGET)
  add_executable(${TARGET}_c EXCLUDE_FROM_ALL src/c_api/${TARGET}.c)
  target_link_libraries(${TARGET}_c tiledb_static -lpthread ${TILEDB_LIB_DEPENDENCIES})
endfunction()

function(build_TileDB_example_cppapi TARGET)
  add_executable(${TARGET} EXCLUDE_FROM_ALL src/${TARGET}.cc)
  if (WIN32)
    target_link_libraries(${TARGET} tiledb_static ${TILEDB_LIB_DEPENDENCIES} ${S3_LIB_DEPENDENCIES})
  else()
    target_link_libraries(${TARGET} tiledb_static -lpthread ${TILEDB_LIB_DEPENDENCIES} ${S3_LIB_DEPENDENCIES})
  endif()
  target_link_libraries(${TARGET} tdbpp_static tiledb_static -lpthread ${TILEDB_LIB_DEPENDENCIES})
  target_link_libraries(${TARGET} tiledb_static -lpthread ${TILEDB_LIB_DEPENDENCIES})
  add_executable(${TARGET}_cpp EXCLUDE_FROM_ALL src/cpp_api/${TARGET}.cc)
  target_link_libraries(${TARGET}_cpp tiledb_static -lpthread ${TILEDB_LIB_DEPENDENCIES})
endfunction()

# Get the example sources
file(GLOB TILEDB_EXAMPLE_SOURCES_CAPI "src/c_api/*.c")
file(GLOB TILEDB_EXAMPLE_SOURCES_CPPAPI "src/cpp_api/*.cc")

# Include TileDB C API headers
include_directories(BEFORE "${CMAKE_SOURCE_DIR}/core/include/c_api/" "${CMAKE_SOURCE_DIR}/core/include/cpp_api/")

# Initialize name for example binaries
set(EXAMPLE_C_BINS)
set(EXAMPLE_CPP_BINS)

# Iterate over all example sources and call the build function
foreach(EXAMPLE_SOURCE ${TILEDB_EXAMPLE_SOURCES_CAPI})
  # Get the binary name
  STRING(REGEX REPLACE "^${CMAKE_SOURCE_DIR}/examples/src/c_api/" ""
         EXAMPLE_STRIPPED ${EXAMPLE_SOURCE}
  )
  STRING(REGEX REPLACE ".c$" "" EXAMPLE_BIN ${EXAMPLE_STRIPPED})
  set(EXAMPLE_C_BINS ${EXAMPLE_C_BINS} ${EXAMPLE_BIN}_c)

  # Build example executable
  build_TileDB_example_capi(${EXAMPLE_BIN})
endforeach()

foreach(EXAMPLE_SOURCE ${TILEDB_EXAMPLE_SOURCES_CPPAPI})
  # Get the binary name
  STRING(REGEX REPLACE "^${CMAKE_SOURCE_DIR}/examples/src/cpp_api/" ""
          EXAMPLE_STRIPPED ${EXAMPLE_SOURCE}
          )
  STRING(REGEX REPLACE ".cc$" "" EXAMPLE_BIN ${EXAMPLE_STRIPPED})
  set(EXAMPLE_CPP_BINS ${EXAMPLE_CPP_BINS} ${EXAMPLE_BIN}_cpp)

  # Build example executable
  build_TileDB_example_cppapi(${EXAMPLE_BIN})
endforeach()

# Add custom target 'examples'
add_custom_target(examples_c DEPENDS tiledb_static)
add_custom_target(examples_cpp DEPENDS tiledb_static)
add_dependencies(examples_c ${EXAMPLE_C_BINS} tiledb_static)
add_dependencies(examples_cpp ${EXAMPLE_CPP_BINS} tiledb_static)

add_custom_target(examples DEPENDS examples_c examples_cpp)
add_dependencies(examples ${EXAMPLE_C_BINS} ${EXAMPLE_CPP_BINS} tiledb_static)
