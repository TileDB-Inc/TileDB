#
# tiledb/api/c_api/context/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2022-2024 TileDB, Inc.
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
include(object_library)

list(APPEND SOURCES
    context_api.cc
    )
gather_sources(${SOURCES})

list(APPEND OTHER_SOURCES
    # We need to recompile sources that depend on StorageManager to use the stub
    ../../../sm/storage_manager/context.cc
    )

commence(object_library capi_context_stub)
  this_target_sources(${SOURCES} ${OTHER_SOURCES})
  this_target_link_libraries(export)
  this_target_object_libraries(capi_config_stub)
  this_target_object_libraries(storage_manager_stub)
  this_target_object_libraries(context_resources)
conclude(object_library)

add_test_subdirectory()
