/**
 * @file   unit-cppapi-array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests the sparse refactored readers.
 * Notes:
 *  duplicated layout is only supported for int attrs
 *  full_domain must be divisible by num_fragments
 *  if using interleaved or duplicated order, full_domain must also be
 *   divisible by num_fragments * 2
 */

#include "catch.hpp"
#include "helpers-sparse_refactored_readers.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

/* ********************************* */
/*                TESTS              */
/* ********************************* */
TEST_CASE("Sparse refactored reader test", "[sparse][refactored_reader]") {
  uint64_t full_domain = 1000000;
  uint64_t num_fragments = 100;
  uint64_t read_buffer_size = 100000000;

  std::vector<TestAttribute> attrs;
  attrs.emplace_back("data", TILEDB_UINT64);
  attrs.emplace_back("data_var", TILEDB_STRING_ASCII);

  std::vector<bool> set_subarray = {false, true};
  std::string layout = "ordered";
  std::vector<uint64_t> which_attrs = {1, 2, 3};
  bool perf_analysis = false;

  for (auto which_attr : which_attrs) {
    for (auto subarray : set_subarray) {
      sparse_global_test(
          full_domain,
          num_fragments,
          read_buffer_size,
          attrs,
          subarray,
          layout,
          which_attr,
          perf_analysis);
    }
  }

  layout = "interleaved";
  full_domain = 999999;
  num_fragments = 99;
  for (auto which_attr : which_attrs) {
    for (auto subarray : set_subarray) {
      sparse_global_test(
          full_domain,
          num_fragments,
          read_buffer_size,
          attrs,
          subarray,
          layout,
          which_attr,
          perf_analysis);
    }
  }

  layout = "duplicated";
  full_domain = 1000000;
  num_fragments = 100;
  attrs.pop_back();
  for (auto subarray : set_subarray) {
    sparse_global_test(
        full_domain,
        num_fragments,
        read_buffer_size,
        attrs,
        subarray,
        layout,
        which_attrs[0],
        perf_analysis);
  }
}