#
# tiledb/sm/array/CMakeLists.txt
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
#

include(common NO_POLICY_SCOPE)
include(object_library)

#
# The data- and I/O-oriented operations of class `Array` have been
# intentionally separated to support two _future_ standalone object libraries:
#   - `array`: data-oriented
#   - `array_operations`: I/O-oriented
# 
# This design eliminates cyclic dependencies in the build, as each module has
# its own dependency chain.
#
# Note: The `array_operations` object library is very specifically blocked by 
# `StrategyBase::throw_if_cancelled`. At present, this function relies on 
# `StorageManagerCanonical`, which is slated for removal. The `Query` class is 
# undergoing maintenance to remove all dependencies on class `StorageManager`. 
# Once `StorageManager` is removed and a standalone `StrategyBase` object 
# library can be added, so should the `array_operations` object library. 
#

#
# `array_directory` object library
#
commence(object_library array_directory)
    this_target_sources(array_directory.cc)
    this_target_object_libraries(
        array_schema
        baseline
        context_resources
        fragment_identifier
        generic_tile_io
        vfs
    )
    if(TILEDB_STATS)
        this_target_compile_definitions(-DTILEDB_STATS)
    endif()
conclude(object_library)

#
# `array` object library
#
commence(object_library array)
    this_target_sources(array.cc consistency.cc)
    this_target_object_libraries(
        array_directory
        array_schema_operations
        fragment_metadata
        metadata
        object
    )
conclude(object_library)

add_test_subdirectory()
