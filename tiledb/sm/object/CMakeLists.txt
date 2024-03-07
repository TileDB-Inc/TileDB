#
# tiledb/sm/object/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2024 TileDB, Inc.
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
include(object_library)

#
# `object` object library
#
# This object library does not support independent linkage at present. It has
# a dependency on `RestClient`, which at present is incapable of being
# linked standalone. As a result, this library is not defined with
# `commence(object_library)`, since that mandates a link-completeness test.
#
add_library(object OBJECT object.cc object_iter.cc)
target_link_libraries(object PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(object PUBLIC uri_format $<TARGET_OBJECTS:uri_format>)
target_link_libraries(object PUBLIC vfs $<TARGET_OBJECTS:vfs>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_object EXCLUDE_FROM_ALL)

# TODO: fix linkage of compile_object
#add_dependencies(all_link_complete compile_object)

target_link_libraries(compile_object PRIVATE object)
target_sources(compile_object PRIVATE
    test/compile_object_main.cc $<TARGET_OBJECTS:object>
)
