#
# tiledb/sm/filter/test/CMakeLists.txt
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

include(unit_test)

commence(unit_test filter_create)
    this_target_object_libraries(all_filters)
    this_target_sources(main.cc unit_filter_create.cc)
conclude(unit_test)

commence(unit_test filter_pipeline)
    this_target_object_libraries(filter_pipeline)
    this_target_sources(main.cc unit_filter_pipeline.cc)
conclude(unit_test)

commence(unit_test run_filter_pipeline)
    this_target_object_libraries(filter_pipeline mem_helpers)
    this_target_sources(
        main.cc
        add_1_in_place_filter.cc
        add_1_including_metadata_filter.cc
        add_1_out_of_place_filter.cc
        add_n_in_place_filter.cc
        filter_test_support.cc
        filtered_tile_checker.cc
        pseudo_checksum_filter.cc
        unit_checksum_pipeline.cc
        unit_bit_width_reduction_pipeline.cc
        unit_bitshuffle_pipeline.cc
        unit_byteshuffle_pipeline.cc
        unit_double_delta_pipeline.cc
        unit_encryption_pipeline.cc
        unit_positive_delta_pipeline.cc
        unit_run_filter_pipeline.cc
        unit_webp_pipeline.cc
        unit_xor_pipeline.cc
    )
    this_target_link_libraries(rapidcheck cxx_rust_std)
    target_include_directories(unit_run_filter_pipeline PRIVATE ${CMAKE_BINARY_DIR})
conclude(unit_test)

commence(unit_test float_scale_input_val)
    this_target_object_libraries(float_scaling_filter)
    this_target_sources(main.cc unit_float_scale_input_validation.cc)
conclude(unit_test)
