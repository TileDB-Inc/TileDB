#
# tiledb/api/c_api/object/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2022 TileDB, Inc.
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
  object_api.cc
)
gather_sources(${SOURCES})

#
# Incomplete object library for object APIs. Moving the rest of
# the object API implementations over is blocked by the dependency
# on StorageManager that would require us to link the entire library
# for this to work.
#
# TODO: Remove StorageManager dependency from the object APIs and
# make this a real object library for unit test use.
#
add_library(capi_object OBJECT ${SOURCES})
target_link_libraries(capi_object PUBLIC export)
target_link_libraries(capi_object PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(capi_object PUBLIC constants $<TARGET_OBJECTS:constants>)

#
# Test-compile of object library ensures link-completeness
#
# add_executable(compile_capi_object EXCLUDE_FROM_ALL)
# target_link_libraries(compile_capi_object PRIVATE capi_object)
# target_sources(compile_capi_object PRIVATE test/compile_capi_object_main.cc)
# add_dependencies(all_link_complete compile_capi_object)

add_test_subdirectory()
