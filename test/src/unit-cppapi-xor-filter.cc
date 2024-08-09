/**
 * @file   unit-cppapi-xor-filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests the C++ API for xor filter related functions.
 */

#include <random>
#include <vector>

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include "tiledb/common/common.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

std::string xor_array_name = "cpp_unit_array";
int xor_dim_hi = 10;

template <typename T>
void xor_filter_api_test(Context& ctx, tiledb_array_type_t array_type) {
  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "rows", {{1, xor_dim_hi}}, 4);
  auto d2 = Dimension::create<int>(ctx, "cols", {{1, xor_dim_hi}}, 4);
  domain.add_dimensions(d1, d2);

  Filter f(ctx, TILEDB_FILTER_XOR);

  FilterList filters(ctx);
  filters.add_filter(f);

  auto a = Attribute::create<T>(ctx, "a");
  a.set_filter_list(filters);

  ArraySchema schema(ctx, array_type);
  schema.set_domain(domain);
  schema.add_attribute(a);
  Array::create(xor_array_name, schema);

  // Setting up the random number generator for the XOR filter testing.
  std::mt19937_64 gen(0xADA65ED6);
  std::uniform_int_distribution<int64_t> dis(
      std::numeric_limits<T>::min(), std::numeric_limits<T>::max());

  std::vector<int> row_dims;
  std::vector<int> col_dims;
  std::vector<T> a_write;
  std::vector<T> expected_a;
  for (int i = 0; i < xor_dim_hi * xor_dim_hi; ++i) {
    int row = (i / xor_dim_hi) + 1;
    int col = (i % xor_dim_hi) + 1;
    row_dims.push_back(row);
    col_dims.push_back(col);

    T f = static_cast<T>(dis(gen));
    a_write.push_back(f);
    expected_a.push_back(f);
  }

  tiledb_layout_t layout_type =
      array_type == TILEDB_SPARSE ? TILEDB_UNORDERED : TILEDB_ROW_MAJOR;

  Array array_w(ctx, xor_array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(layout_type).set_data_buffer("a", a_write);

  if (array_type == TILEDB_SPARSE) {
    query_w.set_data_buffer("rows", row_dims).set_data_buffer("cols", col_dims);
  }

  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open and read the entire array.
  std::vector<T> a_data_read(xor_dim_hi * xor_dim_hi, 0);
  Array array_r(ctx, xor_array_name, TILEDB_READ);
  Query query_r(ctx, array_r);
  query_r.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", a_data_read);

  Subarray subarray_r(ctx, array_r);
  if (array_type == TILEDB_DENSE) {
    int range[] = {1, xor_dim_hi};
    subarray_r.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query_r.set_subarray(subarray_r);
  }

  query_r.submit();

  // Check for results.
  size_t total_num_elements = static_cast<size_t>(xor_dim_hi * xor_dim_hi);
  auto table = query_r.result_buffer_elements();
  REQUIRE(table.size() == 1);
  REQUIRE(table["a"].first == 0);
  REQUIRE(table["a"].second == total_num_elements);

  for (size_t i = 0; i < total_num_elements; ++i) {
    CHECK(a_data_read[i] == expected_a[i]);
  }

  query_r.finalize();
  array_r.close();
}

TEMPLATE_TEST_CASE(
    "C++ API: XOR Filter list on array",
    "[cppapi][filter][xor]",
    int8_t,
    int16_t,
    int32_t,
    int64_t) {
  // Setup.
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(xor_array_name))
    vfs.remove_dir(xor_array_name);

  auto array_type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  xor_filter_api_test<TestType>(ctx, array_type);

  // Teardown.
  if (vfs.is_dir(xor_array_name))
    vfs.remove_dir(xor_array_name);
}
