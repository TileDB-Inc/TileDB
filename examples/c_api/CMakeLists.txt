#
# c_api/CMakeLists.txt
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

# Use C99 as the standard for these examples.
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF) # Don't use GNU extensions

# Add custom target 'examples_c'
add_custom_target(examples_c)

# Function that builds an executable per example
function(build_TileDB_example_capi TARGET)
  add_executable(${TARGET}_c EXCLUDE_FROM_ALL ${TARGET}.c)
  target_link_libraries(${TARGET}_c PUBLIC local_install tiledb)
  if (NOT WIN32)
    # On Linux, must explicitly link -lpthread -ldl in order for static linking
    # to libzstd or libcurl to work.
    target_link_libraries(${TARGET}_c PUBLIC pthread dl)
  endif()
  add_dependencies(examples_c ${TARGET}_c)
endfunction()

# Get the example sources
file(GLOB TILEDB_EXAMPLE_SOURCES_CAPI "*.c")

if(TILEDB_REMOVE_DEPRECATIONS)
  list(FILTER TILEDB_EXAMPLE_SOURCES_CAPI EXCLUDE REGEX "async")
endif()

# Iterate over all example sources and call the build function
foreach(EXAMPLE_SOURCE ${TILEDB_EXAMPLE_SOURCES_CAPI})
  # Get the binary name
  string(REGEX
    REPLACE "^${CMAKE_CURRENT_SOURCE_DIR}/" ""
    EXAMPLE_STRIPPED ${EXAMPLE_SOURCE}
  )
  string(REGEX
    REPLACE ".c$" ""
    EXAMPLE_BIN ${EXAMPLE_STRIPPED}
  )

  # Build example executable
  build_TileDB_example_capi(${EXAMPLE_BIN})
endforeach()
