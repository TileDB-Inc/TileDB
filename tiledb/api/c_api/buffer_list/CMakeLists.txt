#
# tiledb/api/c_api/buffer_list/CMakeLists.txt
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
  buffer_list_api.cc
)
gather_sources(${SOURCES})

#
# Object library for other units to depend upon
#
add_library(capi_buffer_list_stub OBJECT ${SOURCES})
target_link_libraries(capi_buffer_list_stub PUBLIC export)
target_link_libraries(capi_buffer_list_stub PUBLIC buffer $<TARGET_OBJECTS:buffer>)
target_link_libraries(capi_buffer_list_stub PUBLIC buffer_list $<TARGET_OBJECTS:buffer_list>)

#
# We need to build this as a stub-based library because the API wrappers make
# reference to `context`, even though the functions in the buffer list section 
# of the API do not use a context for error handling.
#
target_link_libraries(capi_buffer_list_stub PUBLIC capi_context_stub $<TARGET_OBJECTS:capi_context_stub>)

 #
 # Test-compile of object library ensures link-completeness
 #
 add_executable(compile_capi_buffer_list EXCLUDE_FROM_ALL)
 target_sources(compile_capi_buffer_list PRIVATE test/compile_capi_buffer_list_main.cc)
 target_link_libraries(compile_capi_buffer_list PRIVATE capi_buffer_list_stub)
 add_dependencies(all_link_complete compile_capi_buffer_list)

 add_test_subdirectory()
