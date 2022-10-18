#
# tiledb/storage_format/uri/CMakeLists.txt
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

include(common NO_POLICY_SCOPE)

#
# `uri_format` object library
#
add_library(uri_format OBJECT parse_uri.cc)
target_link_libraries(uri_format PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(uri_format PUBLIC vfs $<TARGET_OBJECTS:vfs>)
target_link_libraries(uri_format PUBLIC time $<TARGET_OBJECTS:time>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_uri_format EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_uri_format)
target_link_libraries(compile_uri_format PRIVATE uri_format)
target_sources(compile_uri_format PRIVATE test/compile_uri_format_main.cc)
