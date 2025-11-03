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
        unit_65154.cc
        ${CMAKE_SOURCE_DIR}/test/support/rapidcheck/show_datatype.cc
    )
    this_target_link_libraries(rapidcheck)
conclude(unit_test)

commence(unit_test float_scale_input_val)
    this_target_object_libraries(float_scaling_filter)
    this_target_sources(main.cc unit_float_scale_input_validation.cc)
conclude(unit_test)

commence(unit_test bitshuffle_compat)
    # Dependency on external/bitshuffle is direct, not by add_subdirectory
    cmake_path(APPEND TILEDB_SOURCE_ROOT "external/src/bitshuffle" OUTPUT_VARIABLE BITSHUFFLE_SOURCE_ROOT)
    cmake_path(APPEND TILEDB_SOURCE_ROOT "external/include/bitshuffle" OUTPUT_VARIABLE BITSHUFFLE_INCLUDE)
    list(APPEND BITSHUFFLE_SOURCES
        ${BITSHUFFLE_SOURCE_ROOT}/bitshuffle_core.cc
        ${BITSHUFFLE_SOURCE_ROOT}/iochain.cc
        )
    this_target_sources(unit_bitshuffle_compat.cc ${BITSHUFFLE_SOURCES})
    this_target_object_libraries(tiledb_crypto bitshuffle_filter)
    this_target_link_libraries(Blosc2::blosc2)
    set_source_files_properties(
        unit_bitshuffle_compat.cc ${BITSHUFFLE_SOURCES}
        PROPERTIES
        INCLUDE_DIRECTORIES "${BITSHUFFLE_INCLUDE}"
    )
conclude(unit_test)
