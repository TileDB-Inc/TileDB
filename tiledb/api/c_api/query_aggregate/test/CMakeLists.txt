#
# tiledb/api/c_api/query_aggregate/test/CMakeLists.txt
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

include(unit_test)

commence(unit_test capi_query_aggregate)
  this_target_sources(unit_capi_query_aggregate.cc)
  this_target_object_libraries(capi_query_aggregate)
  if (NOT MSVC)
    target_compile_options(unit_capi_query_aggregate PRIVATE -Wno-deprecated-declarations)
  endif()
  this_target_link_libraries(tiledb_test_support_lib tiledb_core_objects_oxidize)
  
  if (TILEDB_SERIALIZATION)
    add_definitions(-DTILEDB_SERIALIZATION)
  endif()

conclude(unit_test)
