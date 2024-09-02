#
# tiledb/sm/CMakeLists.txt
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

include(common-root)

add_subdirectory(array)
add_subdirectory(array_schema)
add_subdirectory(buffer)
add_subdirectory(compressors)
add_subdirectory(config)
add_subdirectory(consolidator)
add_subdirectory(crypto)
add_subdirectory(filesystem)
add_subdirectory(filter)
add_subdirectory(fragment)
add_subdirectory(group)
add_subdirectory(metadata)
add_subdirectory(misc)
add_subdirectory(object)
add_subdirectory(query)
add_subdirectory(query_plan)
add_subdirectory(rest)
add_subdirectory(rtree)
add_subdirectory(stats)
add_subdirectory(storage_manager)
add_subdirectory(subarray)
add_subdirectory(tile)


if (TILEDB_SERIALIZATION)
  add_subdirectory(serialization)
endif()
