#
# tiledb/sm/array/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
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
# `array_directory` object library
#
commence(object_library array_directory)
    this_target_sources(array_directory.cc)
    this_target_object_libraries(
        array_schema
        baseline
        generic_tile_io
        uri_format
        uuid
        vfs
    )
conclude(object_library)

#
# `array` object library
#
list(APPEND SOURCES
    array.cc
    consistency.cc
    ../array_schema/array_schema_evolution.cc
    ../consolidator/array_meta_consolidator.cc
    ../consolidator/commits_consolidator.cc
    ../consolidator/fragment_consolidator.cc
    ../consolidator/fragment_meta_consolidator.cc
    ../consolidator/group_meta_consolidator.cc
    ../consolidator/consolidator.cc
    ../fragment/fragment_info.cc
    ../fragment/fragment_metadata.cc
    ../global_state/global_state.cc
    ../global_state/signal_handlers.cc
    ../global_state/watchdog.cc
    ../rest/rest_client.cc
    ../query/deletes_and_updates/deletes_and_updates.cc
    ../query/deletes_and_updates/serialization.cc
    ../query/dimension_label/array_dimension_label_queries.cc
    ../query/dimension_label/dimension_label_query.cc
    ../query/dimension_label/index_data.cc
    ../query/hilbert_order.cc
    ../query/legacy/cell_slab_iter.cc
    ../query/query.cc
    ../query/query_condition.cc
    ../query/query_remote_buffer_storage.cc
    ../query/readers/dense_reader.cc
    ../query/readers/ordered_dim_label_reader.cc
    ../query/readers/reader_base.cc
    ../query/readers/result_tile.cc
    ../query/readers/sparse_global_order_reader.cc
    ../query/readers/sparse_index_reader_base.cc
    ../query/readers/sparse_unordered_with_dups_reader.cc
    ../query/strategy_base.cc
    ../query/update_value.cc
    ../query/writers/dense_tiler.cc
    ../query/writers/global_order_writer.cc
    ../query/writers/ordered_writer.cc
    ../query/writers/unordered_writer.cc
    ../query/writers/writer_base.cc
    ../storage_manager/storage_manager.cc
    ../subarray/subarray_partitioner.cc
    ../subarray/subarray_tile_overlap.cc
    ../subarray/tile_cell_slab_iter.cc
    ../tile/tile_metadata_generator.cc
    ../tile/writer_tile_tuple.cc
    ../../storage_format/uri/generate_uri.cc
)
gather_sources(${SOURCES})

commence(object_library array)
    #this_target_sources(array.cc consistency.cc)
    #this_target_sources($<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
    #this_target_link_libraries(TILEDB_CORE_OBJECTS_ILIB)
    #this_target_object_libraries(array_directory)    
    this_target_sources(${SOURCES})
    this_target_object_libraries(
        array_directory
        group
        rtree
    )
    if(TILEDB_STATS)
        this_target_compile_definitions(-DTILEDB_STATS)
    endif()
conclude(object_library)

add_test_subdirectory()
