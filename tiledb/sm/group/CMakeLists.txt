#
# tiledb/sm/group/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021-2024 TileDB, Inc.
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

#
# `group` object library
#
# This object library does not support independent linkage at present. It has
# a dependency on `StorageManager`, which at present is incapable of being
# linked standalone. As a result, this library is not defined with
# `commence(object_library)`, since that mandates a link-completeness test.
#
add_library(group OBJECT EXCLUDE_FROM_ALL group_directory.cc group.cc group_details.cc group_details_v1.cc group_details_v2.cc group_member.cc group_member_v1.cc group_member_v2.cc)
target_link_libraries(group PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(group PUBLIC buffer $<TARGET_OBJECTS:buffer>)
target_link_libraries(group PUBLIC object $<TARGET_OBJECTS:object>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_group EXCLUDE_FROM_ALL)

# TODO: fix linkage of compile_group
#add_dependencies(all_link_complete compile_group)

target_link_libraries(compile_group PRIVATE group)
target_sources(compile_group PRIVATE
    test/compile_group_main.cc $<TARGET_OBJECTS:group>
)
