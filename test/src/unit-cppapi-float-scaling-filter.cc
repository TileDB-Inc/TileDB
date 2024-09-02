/**
 * @file   unit-cppapi-float-scaling-filter.cc
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
 * Tests the C++ API for float scaling filter related functions.
 */

#include <limits>
#include <random>
#include <vector>

#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

std::string array_name = "cpp_unit_array";
int dim_hi = 10;

TEST_CASE(
    "C++ API: Float Scaling Filter options",
    "[cppapi][filter][float-scaling]") {
  using namespace tiledb;
  Context ctx;

  // Test filter creation and option setting/getting
  Filter f(ctx, TILEDB_FILTER_SCALE_FLOAT);

  double scale = 2.53;
  double offset = 0.138;
  uint64_t byte_width = 8;

  double get_scale, get_offset;
  uint64_t get_byte_width;

  f.set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &byte_width);
  f.get_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &get_byte_width);
  CHECK(get_byte_width == byte_width);
  CHECK(get_byte_width == f.get_option<uint64_t>(TILEDB_SCALE_FLOAT_BYTEWIDTH));

  f.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  f.get_option(TILEDB_SCALE_FLOAT_FACTOR, &get_scale);
  CHECK(get_scale == scale);
  CHECK(get_scale == f.get_option<double>(TILEDB_SCALE_FLOAT_FACTOR));

  f.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);
  f.get_option(TILEDB_SCALE_FLOAT_OFFSET, &get_offset);
  CHECK(get_offset == offset);
  CHECK(get_offset == f.get_option<double>(TILEDB_SCALE_FLOAT_OFFSET));
}

template <typename T, typename W>
struct FloatScalingFilterTestStruct {
  void float_scaling_filter_api_test(
      Context& ctx, tiledb_array_type_t array_type) {
    Domain domain(ctx);
    auto d1 = Dimension::create<int>(ctx, "rows", {{1, dim_hi}}, 4);
    auto d2 = Dimension::create<int>(ctx, "cols", {{1, dim_hi}}, 4);
    domain.add_dimensions(d1, d2);

    Filter f(ctx, TILEDB_FILTER_SCALE_FLOAT);

    double scale = 2.53;
    double offset = 0.138;
    uint64_t byte_width = sizeof(W);

    f.set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &byte_width);
    f.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
    f.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);

    FilterList filters(ctx);
    filters.add_filter(f);

    auto a = Attribute::create<T>(ctx, "a");
    a.set_filter_list(filters);

    ArraySchema schema(ctx, array_type);
    schema.set_domain(domain);
    schema.add_attribute(a);
    Array::create(array_name, schema);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<T> dis(
        std::numeric_limits<W>::min(), std::numeric_limits<W>::max());

    std::vector<int> row_dims;
    std::vector<int> col_dims;
    std::vector<T> a_write;
    std::vector<T> expected_a;
    for (int i = 0; i < dim_hi * dim_hi; ++i) {
      int row = (i / dim_hi) + 1;
      int col = (i % dim_hi) + 1;
      row_dims.push_back(row);
      col_dims.push_back(col);

      T f = dis(gen);
      a_write.push_back(f);
      W val = static_cast<W>(
          round((f - static_cast<T>(offset)) / static_cast<T>(scale)));
      T val_float = static_cast<T>(scale * static_cast<T>(val) + offset);
      expected_a.push_back(val_float);
    }

    tiledb_layout_t layout_type =
        array_type == TILEDB_SPARSE ? TILEDB_UNORDERED : TILEDB_ROW_MAJOR;

    Array array_w(ctx, array_name, TILEDB_WRITE);
    Query query_w(ctx, array_w);
    query_w.set_layout(layout_type).set_data_buffer("a", a_write);

    if (array_type == TILEDB_SPARSE) {
      query_w.set_data_buffer("rows", row_dims)
          .set_data_buffer("cols", col_dims);
    }

    query_w.submit();
    query_w.finalize();
    array_w.close();

    // Open and read the entire array.
    std::vector<T> a_data_read(dim_hi * dim_hi, 0.0);
    Array array_r(ctx, array_name, TILEDB_READ);
    Query query_r(ctx, array_r);
    query_r.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", a_data_read);

    Subarray subarray_r(ctx, array_r);
    if (array_type == TILEDB_DENSE) {
      int range[] = {1, dim_hi};
      subarray_r.add_range("rows", range[0], range[1])
          .add_range("cols", range[0], range[1]);
      query_r.set_subarray(subarray_r);
    }

    query_r.submit();

    // Check for results.
    size_t total_num_elements = static_cast<size_t>(dim_hi * dim_hi);
    auto table = query_r.result_buffer_elements();
    REQUIRE(table.size() == 1);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == total_num_elements);

    for (size_t i = 0; i < total_num_elements; ++i) {
      CHECK(
          fabs(a_data_read[i] - expected_a[i]) <
          std::numeric_limits<T>::epsilon());
    }

    query_r.finalize();
    array_r.close();
  }
};

/**
,
     (float, int16_t),
     (double, int16_t),
     (float, int32_t),
     (double, int32_t),
     (float, int64_t),
     (double, int64_t)
 *
 */

TEMPLATE_PRODUCT_TEST_CASE(
    "C++ API: Float Scaling Filter list on array",
    "[cppapi][filter][float-scaling]",
    FloatScalingFilterTestStruct,
    ((float, int8_t), (double, int8_t))) {
  // Setup.
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  auto array_type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);

  TestType fs;
  fs.float_scaling_filter_api_test(ctx, array_type);

  // Teardown.
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}
