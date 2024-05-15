#
# tiledb/api/c_api/query_field/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2023-2024 TileDB, Inc.
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

# The object library `capi_query_field` cannot be linked standalone at the
# present time. It depends upon member functions of `class Query`, which does
# not yet have a standalone object library of its own. Without such a
# dependency, `capi_query_field` would have to link (essentially) the whole 
# library to get a complete executable.
# 
# It is against policy to use the `object_library` environment to define object
# libraries that cannot stand alone. Therefore this definition is directly in
# CMake and does not include a link-completeness target.
include(common NO_POLICY_SCOPE)

list(APPEND SOURCES
  query_field_api.cc
  )
gather_sources(${SOURCES})

#
# Object library for C API query_field
#
# Maturity Note: object library `capi_query_field` cannot be linked stand-alone
#   at present.
#
add_library(capi_query_field OBJECT ${SOURCES})
target_link_libraries(capi_query_field PUBLIC export)
target_link_libraries(capi_query_field PUBLIC baseline $<TARGET_OBJECTS:baseline>)

add_test_subdirectory()
