#
# tiledb/sm/subarray/CMakeLists.txt
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
include(object_library)

#
# `range_subset` object library
#
commence(object_library range_subset)
this_target_sources(range_subset.cc)
this_target_object_libraries(baseline buffer constants range thread_pool)
conclude(object_library)

find_package(Spdlog_EP REQUIRED)

add_library(subarray_melting_pot OBJECT
      ../fragment/fragment_metadata.cc
      ../fragment/fragment_info.cc
      ../misc/utils.cc
      ../array/array.cc
      ../array/array_directory.cc
      ../array/consistency.cc
      ../array_schema/array_schema_evolution.cc
      ../metadata/metadata.cc
      ../storage_manager/storage_manager.cc
      ../tile/generic_tile_io.cc
      ../tile/tile_metadata_generator.cc
      ../tile/writer_tile_tuple.cc
      ../consolidator/consolidator.cc
      ../consolidator/fragment_consolidator.cc
      ../consolidator/group_meta_consolidator.cc
      ../consolidator/fragment_meta_consolidator.cc
      ../consolidator/array_meta_consolidator.cc
      ../consolidator/commits_consolidator.cc
      ../global_state/global_state.cc
      ../global_state/signal_handlers.cc
      ../global_state/watchdog.cc
      ../query/query_condition.cc
      ../query/update_value.cc
      ../query/deletes_and_updates/deletes_and_updates.cc
      ../query/deletes_and_updates/serialization.cc
      ../query/query.cc
      ../query/hilbert_order.cc
      ../query/writers/global_order_writer.cc
      ../query/writers/writer_base.cc
      ../query/writers/ordered_writer.cc
      ../query/writers/unordered_writer.cc
      ../query/writers/dense_tiler.cc
      ../query/query_remote_buffer_storage.cc
      ../query/strategy_base.cc
      ../query/dimension_label/dimension_label_query.cc
      ../query/dimension_label/array_dimension_label_queries.cc
      ../query/dimension_label/index_data.cc
      ../query/readers/result_tile.cc
      ../query/readers/dense_reader.cc
      ../query/readers/ordered_dim_label_reader.cc
      ../query/readers/sparse_unordered_with_dups_reader.cc
      ../query/readers/sparse_global_order_reader.cc
      ../query/readers/reader_base.cc
      ../query/readers/sparse_index_reader_base.cc
      ../query/legacy/reader.cc
      ../query/legacy/cell_slab_iter.cc
      ../query/legacy/read_cell_slab_iter.cc
      ../query/ast/query_ast.cc
      ../rtree/rtree.cc
      ../rest/rest_client.cc
      ../../common/memory.cc
      ../../storage_format/uri/generate_uri.cc
)
target_compile_definitions(subarray_melting_pot PRIVATE $<TARGET_PROPERTY:spdlog::spdlog,INTERFACE_COMPILE_DEFINITIONS>)
# avoid errors of nature "warning C4189: 'timer_se': local variable is initialized but not referenced"
# from calls to <stats>->start_timer()
target_compile_definitions(subarray_melting_pot PRIVATE TILEDB_STATS)
target_include_directories(subarray_melting_pot PRIVATE
    "$<TARGET_PROPERTY:TILEDB_CORE_OBJECTS,INCLUDE_DIRECTORIES>")

#
# 'subarray' object library
#
commence(object_library subarray)
    this_target_sources(subarray.cc subarray_tile_overlap.cc relevant_fragment_generator.cc 
      subarray_partitioner.cc
      tile_cell_slab_iter.cc
    )
    this_target_object_libraries(subarray_melting_pot)
    this_target_object_libraries(uri_format)
    this_target_object_libraries( group range_subset domain dimension attribute array_schema vfs)
    target_compile_definitions(subarray PRIVATE $<TARGET_PROPERTY:spdlog::spdlog,INTERFACE_COMPILE_DEFINITIONS>)
    # avoid errors of nature "warning C4189: 'timer_se': local variable is initialized but not referenced"
    # from calls to <stats>->start_timer()
    target_compile_definitions(subarray PRIVATE TILEDB_STATS)
    target_include_directories(subarray PRIVATE
        "$<TARGET_PROPERTY:TILEDB_CORE_OBJECTS,INCLUDE_DIRECTORIES>")
conclude(object_library)


add_dependencies(all_link_complete subarray)

add_test_subdirectory()
