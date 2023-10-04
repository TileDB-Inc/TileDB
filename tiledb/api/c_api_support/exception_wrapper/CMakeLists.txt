#
# tiledb/api/exception_wrapper/CMakeLists.txt
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

#
# `exception_wrapper` object library
#
# Even though `exception_wrapper` is an object libary, it is actually header-
# only. `exception_wrapper.cc` is an empty source file needed to allow the
# OBJECT syntax.

# No actual source files at present.
list(APPEND SOURCES
    exception_wrapper.cc
)
gather_sources(${SOURCES})

add_library(exception_wrapper OBJECT exception_wrapper.cc)
target_link_libraries(exception_wrapper PUBLIC export)
target_link_libraries(exception_wrapper PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(exception_wrapper PUBLIC thread_pool $<TARGET_OBJECTS:thread_pool>)
target_link_libraries(exception_wrapper PUBLIC config $<TARGET_OBJECTS:config>)

add_test_subdirectory()
