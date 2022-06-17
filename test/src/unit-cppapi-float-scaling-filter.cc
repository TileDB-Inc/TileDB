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

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"

std::string array_name = "cpp_unit_array";

TEST_CASE("C++ API: Float Scaling Filter options", "[cppapi][filter][float-scaling]") {
  using namespace tiledb;
  Context ctx;

  // Test filter creation and option setting/getting
  Filter f(ctx, TILEDB_FILTER_SCALE_FLOAT);

  double scale = 2.53;
  double offset = 0.138;
  uint64_t bit_width = 8;

  double get_scale, get_offset;
  uint64_t get_bit_width;

  f.set_option(TILEDB_SCALE_FLOAT_BITWIDTH, &bit_width);
  f.get_option(TILEDB_SCALE_FLOAT_BITWIDTH, &get_bit_width);
  CHECK(get_bit_width == bit_width);

  f.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  f.get_option(TILEDB_SCALE_FLOAT_FACTOR, &get_scale);
  CHECK(get_scale == scale);

  f.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);
  f.get_option(TILEDB_SCALE_FLOAT_OFFSET, &get_offset);
  CHECK(get_offset == offset);
}

template<typename T>
void float_scaling_filter_api_test(Context &ctx, uint64_t bit_width) {
  // Create an array with one attribute "a", run filter through it.
  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{0, 20}}, 4);
  auto d2 = Dimension::create<int>(ctx, "d2", {{0, 20}}, 4);
  domain.add_dimensions(d1, d2);

  // Test filter creation and option setting/getting
  Filter f(ctx, TILEDB_FILTER_SCALE_FLOAT);

  double scale = 2.53;
  double offset = 0.138;
  uint64_t bw = bit_width;

  f.set_option(TILEDB_SCALE_FLOAT_BITWIDTH, &bw);
  f.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  f.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);

  FilterList filters(ctx);
  filters.add_filter(f);

  auto a = Attribute::create<T>(ctx, "a");
  a.set_filter_list(filters);


  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: Float Scaling Filter list on array, sparse", "[cppapi][filter][float-scaling]") {
  // Setup.
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  auto bit_width = GENERATE(1, 2, 4, 8); 
  SECTION("Testing FLOAT32 attribute type.") {
      float_scaling_filter_api_test<float>(ctx, bit_width);
  }

  SECTION("Testing FLOAT64 attribute type.") {
      float_scaling_filter_api_test<double>(ctx, bit_width);
  }

  // Teardown.
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}