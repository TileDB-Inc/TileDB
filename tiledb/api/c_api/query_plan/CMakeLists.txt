#
# tiledb/api/c_api/query_plan/CMakeLists.txt
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
include(common NO_POLICY_SCOPE)

list(APPEND SOURCES
  query_plan_api.cc
  )
gather_sources(${SOURCES})

#
# Object library for C API query plan
#
# Maturity:
#   This object library cannot link standalone because `query_plan` cannot link
#   standalone.
#
add_library(capi_query_plan OBJECT ${SOURCES})
target_link_libraries(capi_query_plan PUBLIC export)
target_link_libraries(capi_query_plan PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(capi_query_plan PUBLIC query_plan $<TARGET_OBJECTS:query_plan>)

add_test_subdirectory()
