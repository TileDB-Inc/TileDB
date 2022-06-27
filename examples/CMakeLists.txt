#
# examples/CMakeLists.txt
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

#**#
set(CMAKE_VERBOSE_MAKEFILE ON)
#**#

if (WIN32)
  # Disable MSVC warnings not present with gcc.
  #   C4204: non-constant aggregate initializer
  #   C4221: aggregate initializer with local variable
  #   C4305: implicit truncation of double to float
  add_compile_options(/wd4204 /wd4221 /wd4305)
else()
  # Don't treat printf format warnings as errors.
  add_compile_options(-Wno-error=format)
  # TODO: Remove when KV API has been removed.
  add_compile_options(-Wno-deprecated-declarations)
endif()

############################################################
# Include directory
############################################################
# The examples assume they use an installed TileDB, for example:
#     '#include <tiledb/tiledb.h>'
#
# FILENAME headers copied to `${BASE}/tiledb`
# - All C++ headers use the FILENAME convention
# RELATIVE headers copied to `${BASE}`

set(TILEDB_EXAMPLES_INCLUDE_BASE "${CMAKE_CURRENT_BINARY_DIR}/include")

############################################################
# C API
############################################################

foreach(HEADER ${TILEDB_C_API_FILENAME_HEADERS})
  cmake_path(GET HEADER FILENAME HEADER_STRIPPED)
  configure_file(${HEADER} ${TILEDB_EXAMPLES_INCLUDE_BASE}/tiledb/${HEADER_STRIPPED} COPYONLY)
endforeach()
foreach(HEADER ${TILEDB_C_API_RELATIVE_HEADERS})
  cmake_path(RELATIVE_PATH HEADER
      BASE_DIRECTORY ${TILEDB_C_API_RELATIVE_HEADER_BASE}
      OUTPUT_VARIABLE HEADER_STRIPPED
  )
  configure_file(${HEADER} ${TILEDB_EXAMPLES_INCLUDE_BASE}/${HEADER_STRIPPED} COPYONLY)
endforeach()
configure_file(${TILEDB_EXPORT_HEADER} ${TILEDB_EXAMPLES_INCLUDE_BASE}/${TILEDB_EXPORT_HEADER_INSTALL_PATH} COPYONLY)

# Add the C examples
add_subdirectory(c_api)

############################################################
# C++ API
############################################################

if (TILEDB_CPP_API)
  file(GLOB TILEDB_CPP_HEADERS
    "${CMAKE_CURRENT_SOURCE_DIR}/../tiledb/sm/cpp_api/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/../tiledb/sm/cpp_api/tiledb"
    "${CMAKE_CURRENT_SOURCE_DIR}/../tiledb/sm/cpp_api/tiledb_experimental"
  )

  foreach(HEADER ${TILEDB_CPP_HEADERS})
    string(REGEX
      REPLACE "^${CMAKE_CURRENT_SOURCE_DIR}/../tiledb/sm/cpp_api/" ""
      HEADER_STRIPPED ${HEADER}
    )
    configure_file(${HEADER} ${TILEDB_EXAMPLES_INCLUDE_BASE}/tiledb/${HEADER_STRIPPED} COPYONLY)
  endforeach()

  # Add the C++ examples
  add_subdirectory(cpp_api)
endif()

############################################################
# `examples` target
############################################################
# The 'examples' target builds all examples.

add_custom_target(examples DEPENDS examples_c)
if (TILEDB_CPP_API)
  #**#
  # build C examples before C++ examples to see if error is configuration mismatch
  #**#
  add_dependencies(examples_cpp examples_c)

  add_dependencies(examples examples_cpp)
endif()
