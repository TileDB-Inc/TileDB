#
# Doxygen.cmake
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
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

###########################################################
# Doxygen documentation
###########################################################

find_package(Doxygen)
if(DOXYGEN_FOUND)
  file(GLOB_RECURSE TILEDB_C_API_HEADERS "${CMAKE_SOURCE_DIR}/tiledb/*_api_external.h")
  list(APPEND TILEDB_C_API_HEADERS
      "${CMAKE_CURRENT_SOURCE_DIR}/tiledb/api/c_api/api_external_common.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/tiledb/sm/c_api/tiledb.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/tiledb/sm/c_api/tiledb_deprecated.h"
  )
  file(GLOB TILEDB_CPP_API_HEADERS
      "${CMAKE_CURRENT_SOURCE_DIR}/tiledb/sm/cpp_api/*.h"
  )
  set(TILEDB_API_HEADERS ${TILEDB_C_API_HEADERS} ${TILEDB_CPP_API_HEADERS})
  add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/doxyfile.in
    COMMAND mkdir -p doxygen
    COMMAND echo INPUT = ${CMAKE_CURRENT_SOURCE_DIR}/tiledb/doxygen/mainpage.dox
      ${TILEDB_API_HEADERS} > ${CMAKE_CURRENT_BINARY_DIR}/doxyfile.in
    COMMENT "Preparing for Doxygen documentation" VERBATIM
  )
  add_custom_target(doc
    Doxygen::doxygen ${CMAKE_CURRENT_SOURCE_DIR}/tiledb/doxygen/Doxyfile.mk >
      ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile.log 2>&1
    COMMENT "Generating API documentation with Doxygen" VERBATIM
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/doxyfile.in
  )
else(DOXYGEN_FOUND)
  add_custom_target(doc
    _______doc
    COMMENT "!! Docs cannot be built. Please install Doxygen and re-run cmake. !!" VERBATIM
  )
endif(DOXYGEN_FOUND)
