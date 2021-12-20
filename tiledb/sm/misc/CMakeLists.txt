#
# tiledb/sm/misc/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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
# `constants` object library
#
add_library(constants OBJECT constants.cc)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_constants EXCLUDE_FROM_ALL)
target_link_libraries(compile_constants PRIVATE constants)
target_sources(compile_constants PRIVATE test/compile_constants_main.cc)

#
# `misc_types` object library
#
add_library(misc_types OBJECT types.cc)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_misc_types EXCLUDE_FROM_ALL)
target_link_libraries(compile_misc_types PRIVATE misc_types)
target_sources(compile_misc_types PRIVATE test/compile_misc_types_main.cc)

#
# `parse_arguments` object library
#
add_library(parse_argument OBJECT parse_argument.cc)
target_link_libraries(parse_argument PUBLIC baseline $<TARGET_OBJECTS:baseline>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_parse_argument EXCLUDE_FROM_ALL)
target_link_libraries(compile_parse_argument PRIVATE parse_argument)
target_sources(compile_parse_argument PRIVATE test/compile_parse_argument_main.cc)
