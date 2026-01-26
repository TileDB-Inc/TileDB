#
# tiledb/common/util/test/CMakeLists.txt
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

include(unit_test)

commence(unit_test alt_var_length_view)
    this_target_sources(
        unit_alt_var_length_view.cc
)
conclude(unit_test)

commence(unit_test permutation_sort)
    this_target_sources(
        unit_permutation_sort.cc
)
conclude(unit_test)

commence(unit_test permutation_view)
    this_target_sources(
        unit_permutation_view.cc
)
conclude(unit_test)

commence(unit_test proxy_sort)
    this_target_sources(
        unit_proxy_sort.cc
)
conclude(unit_test)

commence(unit_test sort_chunk)
    this_target_sources(
        unit_sort_chunk.cc
)
conclude(unit_test)

commence(unit_test sort_zip)
    this_target_sources(
        unit_sort_zip.cc
)
conclude(unit_test)

commence(unit_test var_length_util)
    this_target_sources(
        unit_var_length_util.cc
)
conclude(unit_test)

commence(unit_test var_length_view)
    this_target_sources(
        unit_var_length_view.cc
)
conclude(unit_test)

commence(unit_test block_view)
    this_target_sources(
        unit_block_view.cc
)
conclude(unit_test)

commence(unit_test view_combo)
this_target_sources(
        unit_view_combo.cc
)
conclude(unit_test)

commence(unit_test intercept)
    this_target_sources(
        unit_intercept.cc
        unit_no_intercept.cc
    )
conclude(unit_test)
