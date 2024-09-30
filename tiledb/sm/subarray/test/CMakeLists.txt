#
# tiledb/sm/subarray/CMakeLists.txt
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

include(unit_test)

commence(unit_test range_subset)
    this_target_object_libraries(range_subset)
    this_target_sources(main.cc unit_range_subset.cc)
conclude(unit_test)

commence(unit_test add_ranges_list)
    this_target_sources(main.cc unit_add_ranges_list.cc)
    this_target_link_libraries(TILEDB_CORE_OBJECTS)
    this_target_link_libraries(tiledb_test_support_lib)
    target_include_directories(unit_add_ranges_list PRIVATE
        "$<TARGET_PROPERTY:TILEDB_CORE_OBJECTS,INCLUDE_DIRECTORIES>")
    # We want tests to continue as normal even as the API is changing,
    # so don't warn for deprecations, since they'll be escalated to errors.
    if (NOT MSVC)
      target_compile_options(unit_add_ranges_list PRIVATE -Wno-deprecated-declarations)
    endif()
conclude(unit_test)

commence(unit_test subarray)
    this_target_sources(main.cc unit_subarray.cc unit_subarray_partitioner_dense.cc)
    this_target_link_libraries(subarray tiledb_test_support_lib)
conclude(unit_test)


commence(unit_test subarray_partitioner)
    this_target_sources(
        main.cc 
        unit_subarray_partitioner_dense.cc 
        unit_subarray_partitioner_error.cc 
        unit_subarray_partitioner_sparse.cc
    )
    this_target_link_libraries(subarray tiledb_test_support_lib)
conclude(unit_test)
