#
# tiledb/api/c_api/context/CMakeLists.txt
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
    context_api.cc
    )
gather_sources(${SOURCES})

list(APPEND OTHER_SOURCES
    # We need to recompile sources that depend on StorageManager to use the stub
    ../../../sm/storage_manager/context.cc
    ../../../sm/storage_manager/context_resources.cc
    )

#
# The stubbed object library is compiled with the `StorageManagerStub`. Anything
# that needs actual storage manager behavior will fail. If all that's needed is
# a reference, it will work fine.
#
add_library(capi_context_stub OBJECT ${SOURCES} ${OTHER_SOURCES})
target_link_libraries(capi_context_stub PUBLIC export)
target_link_libraries(capi_context_stub PUBLIC storage_manager_stub)
target_link_libraries(capi_context_stub PUBLIC capi_config_stub $<TARGET_OBJECTS:capi_config_stub>)
target_link_libraries(capi_context_stub PUBLIC vfs $<TARGET_OBJECTS:vfs>)
target_link_libraries(capi_context_stub PUBLIC stats $<TARGET_OBJECTS:stats>)
target_link_libraries(capi_context_stub PUBLIC thread_pool $<TARGET_OBJECTS:thread_pool>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_capi_context_stub EXCLUDE_FROM_ALL)
target_link_libraries(compile_capi_context_stub PRIVATE capi_context_stub)
target_sources(compile_capi_context_stub PRIVATE test/compile_capi_context_main.cc)
add_dependencies(all_link_complete compile_capi_context_stub)

add_test_subdirectory()
