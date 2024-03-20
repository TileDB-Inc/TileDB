#
# tiledb/sm/misc/CMakeLists.txt
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

include(unit_test)

commence(unit_test misc)
    this_target_object_libraries(math)
    this_target_link_libraries(tiledb_test_support_lib)
    # change to `this_target_include_directories` when available
    target_include_directories(unit_misc PRIVATE "${CMAKE_SOURCE_DIR}")
    this_target_sources(
        main.cc
        unit_bytevecvalue.cc
        unit_hilbert.cc
        unit_integral_type_casts.cc
        unit_math.cc
    )
conclude(unit_test)
