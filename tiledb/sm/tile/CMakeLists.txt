#
# tiledb/sm/tile/CMakeLists.txt
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
include(object_library)

#
# `tile` object library
#
commence(object_library tile)
  this_target_sources(tile.cc)
  this_target_object_libraries(baseline buffer constants)
conclude(object_library)

#
# `generic_tile_io` object library
#
commence(object_library generic_tile_io)
  this_target_sources(generic_tile_io.cc)
  this_target_object_libraries(
      baseline
      buffer
      constants
      context_resources
      tiledb_crypto
      filter_pipeline
      tile
      vfs
  )
conclude(object_library)

#
# `writer_tile` object library
#
commence(object_library writer_tile)
  this_target_sources(tile_metadata_generator.cc writer_tile_tuple.cc)
  this_target_object_libraries(array_schema tile)
conclude(object_library)

add_test_subdirectory()
