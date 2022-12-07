#
# tiledb/api/c_api/config/CMakeLists.txt
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
    config_api.cc
)
gather_sources(${SOURCES})

#
# Object library designated as `_stub` version because it depends on the stub
# context library.
#
add_library(capi_config_stub OBJECT ${SOURCES})
target_link_libraries(capi_config_stub PUBLIC export)
target_link_libraries(capi_config_stub PUBLIC config $<TARGET_OBJECTS:config>)
target_link_libraries(capi_config_stub PUBLIC capi_error $<TARGET_OBJECTS:capi_error>)
#
# We need to build this as a stub-based library because the API wrappers make
# reference to `context`, even though the functions in the config section of the
# API do not use a context for error handling.
#
target_link_libraries(capi_config_stub PUBLIC storage_manager_stub)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_capi_config_stub EXCLUDE_FROM_ALL)
target_link_libraries(compile_capi_config_stub PRIVATE capi_config_stub)
target_sources(compile_capi_config_stub PRIVATE test/compile_capi_config_main.cc)
add_dependencies(all_link_complete compile_capi_config_stub)

add_test_subdirectory()
