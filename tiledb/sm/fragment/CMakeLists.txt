#
# tiledb/sm/fragment/CMakeLists.txt
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
#

include(common NO_POLICY_SCOPE)
include(object_library)

#
# Note: `fragment_identifier` and `fragment_metadata` have been separated in 
# lieu of a single `fragment` object library. There's a lot of overhead in the 
# `fragment_metadata` library that isn't necessary to build for modules 
# requiring only the `fragment_identifier` library. As such, they are kept 
# separate for the time being, though this may go away in the future.
#

#
# `fragment_identifier` object library
#
commence(object_library fragment_identifier)
    this_target_sources(fragment_identifier.cc)
    this_target_object_libraries(baseline vfs)
    if(TILEDB_STATS)
        this_target_compile_definitions(-DTILEDB_STATS)
    endif()
conclude(object_library)

#
# `fragment_metadata` object library
#
list(APPEND SOURCES
    column_fragment_writer.cc
    fragment_metadata.cc
    fragment_info.cc
    loaded_fragment_metadata.cc
    ondemand_fragment_metadata.cc
    v1v2preloaded_fragment_metadata.cc
)
gather_sources(${SOURCES})

commence(object_library fragment_metadata)
    this_target_sources(${SOURCES})
    this_target_object_libraries(
        array_directory    
        array_schema    
        fragment_identifier
        generic_tile_io 
        ndrectangle 
        rectangle
        rtree
        tiledb_crypto
        writer_tile
    )
conclude(object_library)

add_test_subdirectory()
