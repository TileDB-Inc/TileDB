/**
 * @file test-capi-subarray-labels.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * Tests the DimensionLabel API for subarrays
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>
#include <iostream>
#include <string>
#include <unordered_map>

using namespace tiledb::sm;
using namespace tiledb::test;

/**
 * Creates a sample test array with multiple dimension labels.
 *
 * Array summary:
 *  * Array type: Dense
 *  * Dimensions:
 *    - x: (type=UINT64, domain=[0, 63], tile=64)
 *  * Attributes:
 *    - a: (type=FLOAT64)
 *  * Dimension labels:
 *    - label: (dim_idx=0, type=FLOAT64)
 *    - id: (dim_idx=0, type=STRING_ASCII)
 *
 * @param array_name Array name relative to the fixture temporary directory.
 * @returns Full URI to the generated array.
 */
class SampleLabelledArrayTestFixture : public TemporaryDirectoryFixture {
 public:
  SampleLabelledArrayTestFixture() {
    auto ctx = get_ctx();

    // Create an array schema.
    uint64_t x_domain[2]{0, 63};
    uint64_t x_tile_extent{64};
    auto array_schema = create_array_schema(
        ctx,
        TILEDB_DENSE,
        {"dim"},
        {TILEDB_UINT64},
        {&x_domain[0]},
        {&x_tile_extent},
        {"a"},
        {TILEDB_FLOAT64},
        {1},
        {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        4096,
        false);

    // Add dimension labels.
    require_tiledb_ok(tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 0, "label", TILEDB_INCREASING_DATA, TILEDB_FLOAT64));
    require_tiledb_ok(tiledb_array_schema_add_dimension_label(
        ctx,
        array_schema,
        0,
        "id",
        TILEDB_INCREASING_DATA,
        TILEDB_STRING_ASCII));

    // Check array schema and number of dimension labels.
    require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));
    auto dim_label_num = array_schema->dim_label_num();
    REQUIRE(dim_label_num == 2);

    // Create array and free array schema.
    array_name = create_temporary_array(std::move(array_name), array_schema);
    tiledb_array_schema_free(&array_schema);
  }

 protected:
  std::string array_name;
};

TEST_CASE_METHOD(
    SampleLabelledArrayTestFixture,
    "Subarray with a fixed-length dimension label range",
    "[capi][subarray][DimensionLabel]") {
  auto ctx = get_ctx();

  // Create array and subarray.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));
  tiledb_subarray_t* subarray;
  require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));

  // Check label range num is zero for all labels.
  int32_t has_label_ranges{0};
  require_tiledb_ok(
      tiledb_subarray_has_label_ranges(ctx, subarray, 0, &has_label_ranges));
  CHECK(has_label_ranges == 0);
  uint64_t range_num{0};
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "label", &range_num));
  CHECK(range_num == 0);
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);

  // Add fixed ranges
  double r1[2]{-1.0, 1.0};
  require_tiledb_ok(tiledb_subarray_add_label_range(
      ctx, subarray, "label", &r1[0], &r1[1], nullptr));
  // Check has label ranges.
  require_tiledb_ok(
      tiledb_subarray_has_label_ranges(ctx, subarray, 0, &has_label_ranges));
  CHECK(has_label_ranges != 0);
  // Check no regular ranges set.
  require_tiledb_ok(
      tiledb_subarray_get_range_num(ctx, subarray, 0, &range_num));
  CHECK(range_num == 0);
  // Check 1 label range set by name.
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "label", &range_num));
  CHECK(range_num == 1);
  // Check 0 label range set by name to other label on dim.
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);
  // Get the name of the set label.
  const char* label_name;
  require_tiledb_ok(
      tiledb_subarray_get_label_name(ctx, subarray, 0, &label_name));
  std::string expected_label_name{"label"};
  CHECK(label_name == expected_label_name);

  // Check getting the range back
  const void* r1_start{};
  const void* r1_end{};
  const void* r1_stride{};
  require_tiledb_ok(tiledb_subarray_get_label_range(
      ctx, subarray, "label", 0, &r1_start, &r1_end, &r1_stride));
  CHECK(r1_stride == nullptr);
  CHECK(*static_cast<const double*>(r1_start) == r1[0]);
  CHECK(*static_cast<const double*>(r1_end) == r1[1]);

  // Check cannot set dimension range on same dimension
  uint64_t r2[2]{1, 10};
  auto rc =
      tiledb_subarray_add_range(ctx, subarray, 0, &r2[0], &r2[1], nullptr);
  CHECK(rc != TILEDB_OK);

  // Check cannot set label range for different label on same dimension
  std::string start{"alpha"};
  std::string end{"beta"};
  rc = tiledb_subarray_add_label_range_var(
      ctx, subarray, "id", start.data(), start.size(), end.data(), end.size());
  CHECK(rc != TILEDB_OK);

  // Free resources.
  tiledb_array_free(&array);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SampleLabelledArrayTestFixture,
    "Subarray with variable dimension label range",
    "[capi][subarray][DimensionLabel]") {
  auto ctx = get_ctx();
  // Create array and subarray.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));
  tiledb_subarray_t* subarray;
  require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));

  // Check label range num is zero for all labels.
  uint64_t range_num{0};
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "label", &range_num));
  CHECK(range_num == 0);
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);

  // Set range.
  std::string start{"alpha"};
  std::string end{"beta"};
  require_tiledb_ok(tiledb_subarray_add_label_range_var(
      ctx, subarray, "id", start.data(), start.size(), end.data(), end.size()));
  // Check no regular ranges set.
  require_tiledb_ok(
      tiledb_subarray_get_range_num(ctx, subarray, 0, &range_num));
  CHECK(range_num == 0);
  // Check 1 label range set by name.
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 1);
  // Check 0 label range set by name to other label on dim.
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "label", &range_num));
  CHECK(range_num == 0);

  // Check getting the range back from the subarray.
  uint64_t start_size{0};
  uint64_t end_size{0};
  require_tiledb_ok(tiledb_subarray_get_label_range_var_size(
      ctx, subarray, "id", 0, &start_size, &end_size));
  std::vector<char> start_data(start_size);
  std::vector<char> end_data(end_size);
  require_tiledb_ok(tiledb_subarray_get_label_range_var(
      ctx, subarray, "id", 0, start_data.data(), end_data.data()));
  CHECK(std::string(start_data.data(), start_data.size()) == start);
  CHECK(std::string(end_data.data(), end_data.size()) == end);

  // Check cannot set dimension range on same dimension
  uint64_t r2[2]{1, 10};
  auto rc =
      tiledb_subarray_add_range(ctx, subarray, 0, &r2[0], &r2[1], nullptr);
  CHECK(rc != TILEDB_OK);

  // Check cannot set dimension range to another label on same dimension
  double r1[2]{-1.0, 1.0};
  rc = tiledb_subarray_add_label_range(
      ctx, subarray, "label", &r1[0], &r1[1], nullptr);
  CHECK(rc != TILEDB_OK);

  // Free resources.
  tiledb_subarray_free(&subarray);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    SampleLabelledArrayTestFixture,
    "Subarray with dimension label ranges blocked",
    "[capi][subarray][DimensionLabel]") {
  auto ctx = get_ctx();
  // Create array and subarray.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));
  tiledb_subarray_t* subarray;
  require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));

  // Check label range num is zero for all labels.
  uint64_t range_num{0};
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "label", &range_num));
  CHECK(range_num == 0);
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);

  // Check error when adding range to non-existent label.
  double r0[2]{-1.0, 1.0};
  auto rc = tiledb_subarray_add_label_range(
      ctx, subarray, "fake_label", &r0[0], &r0[1], nullptr);
  CHECK(rc != TILEDB_OK);
  std::string start0{"start"};
  std::string end0{"end"};
  rc = tiledb_subarray_add_label_range_var(
      ctx,
      subarray,
      "fake_label",
      start0.data(),
      start0.size(),
      end0.data(),
      end0.size());
  CHECK(rc != TILEDB_OK);

  // Check cannot add dimension label range to dimension with standard ranges
  // explicitly set
  uint64_t r1[2]{1, 10};
  require_tiledb_ok(
      tiledb_subarray_add_range(ctx, subarray, 0, &r1[0], &r1[1], nullptr));
  // Check cannot set dimension range to another label on same dimension
  double r2[2]{-1.0, 1.0};
  rc = tiledb_subarray_add_label_range(
      ctx, subarray, "label", &r2[0], &r2[1], nullptr);
  CHECK(rc != TILEDB_OK);
  // Check cannot set label range for different label on same dimension
  std::string start{"alpha"};
  std::string end{"beta"};
  rc = tiledb_subarray_add_label_range_var(
      ctx, subarray, "id", start.data(), start.size(), end.data(), end.size());
  CHECK(rc != TILEDB_OK);
  tiledb_subarray_free(&subarray);
  tiledb_array_free(&array);
}
