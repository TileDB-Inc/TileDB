#
# tiledb/api/c_api_test_support/storage_manager_stub/CMakeLists.txt
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

#
# Object library for the stubbed storage manager.
#
# Use of this library will cause `StorageManager` to resolve to
# `StorageManagerStub`. This declaration must be made in every compilation unit.
# Including this library as an interface dependency accomplishes that. Objects
# not dependent upon this library may resolve to the canonical version. If
# compile or link references to `StorageManagerCanonical`, check that there's
# a dependency chain back to here.
#
add_library(storage_manager_stub OBJECT EXCLUDE_FROM_ALL
    ${CMAKE_SOURCE_DIR}/tiledb/api/c_api_test_support/storage_manager_stub/storage_manager_stub.cc)
# Presence of a particular header file causes the override. Add the directory
# that contains the overriding header to the include search path.
target_include_directories(storage_manager_stub PUBLIC
    ${CMAKE_SOURCE_DIR}/tiledb/api/c_api_test_support/storage_manager_stub)
target_include_directories(storage_manager_stub PRIVATE
    ${CMAKE_SOURCE_DIR}
)