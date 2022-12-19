/**
 * @file test-capi-dimension_labels.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Tests the DimensionLabel API
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/c_api/experimental/tiledb_dimension_label.h"
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
 * Fixture containing methods for creating arrays with dimension labels in a
 * temporary directory.
 *
 */
class DimensionLabelTestFixture : public TemporaryDirectoryFixture {
 public:
  DimensionLabelTestFixture() = default;
  ~DimensionLabelTestFixture() = default;

  /**
   * Creates a test array with a single dimension label with the specified type,
   * domain, and tile extent.
   *
   * Array summary:
   *  * Array type: Dense
   *  * Dimensions:
   *    - x: (type=UINT64, domain=[0, 63], tile=64)
   *    - y: (type=UINT64, domain=[0, 63], tile=64)
   *  * Attributes:
   *    - a: (type=FLOAT64)
   *  * Dimesnion labels:
   *    - x: (dim_idx=0, type=label_type)
   *
   * @param array_name Array name relative to the fixture temporary directory.
   * @param label_type The data type of the dimension label.
   * @returns Full URI to the generated array.
   */
  std::string create_single_label_array(
      std::string&& array_name, tiledb_datatype_t label_type);

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
   *    - x: (dim_idx=0, type=FLOAT64)
   *    - id: (dim_idx=0, type=STRING_ASCII)
   *
   * @param array_name Array name relative to the fixture temporary directory.
   * @returns Full URI to the generated array.
   */
  std::string create_multi_label_array(std::string&& array_name);
};

std::string DimensionLabelTestFixture::create_single_label_array(
    std::string&& array_name, tiledb_datatype_t label_type) {
  // Create an array schema
  uint64_t x_domain[2]{0, 63};
  uint64_t x_tile_extent{64};
  uint64_t y_domain[2]{0, 63};
  uint64_t y_tile_extent{64};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_DENSE,
      {"x", "y"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&x_domain[0], &y_domain[0]},
      {&x_tile_extent, &y_tile_extent},
      {"a"},
      {TILEDB_FLOAT64},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4096,
      false);

  require_tiledb_ok(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "x", TILEDB_INCREASING_DATA, label_type));

  // Check array schema and number of dimension labels.
  require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));
  auto dim_label_num = array_schema->array_schema_->dim_label_num();
  REQUIRE(dim_label_num == 1);

  // Create array
  auto array_uri = create_temporary_array(std::move(array_name), array_schema);
  tiledb_array_schema_free(&array_schema);
  return array_uri;
}

std::string DimensionLabelTestFixture::create_multi_label_array(
    std::string&& array_name) {
  // Create an array schema
  uint64_t x_domain[2]{0, 63};
  uint64_t x_tile_extent{64};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_DENSE,
      {"x"},
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
      ctx, array_schema, 0, "x", TILEDB_INCREASING_DATA, TILEDB_FLOAT64));
  require_tiledb_ok(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "id", TILEDB_INCREASING_DATA, TILEDB_STRING_ASCII));

  // Check array schema and number of dimension labels.
  require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));
  auto dim_label_num = array_schema->array_schema_->dim_label_num();
  REQUIRE(dim_label_num == 2);

  // Create array
  auto array_uri = create_temporary_array(std::move(array_name), array_schema);
  tiledb_array_schema_free(&array_schema);
  return array_uri;
}

TEST_CASE_METHOD(
    DimensionLabelTestFixture,
    "Write and read back TileDB array schema with dimension label",
    "[capi][ArraySchema][DimensionLabel]") {
  // Create and add dimension label schema (both fixed and variable length
  // examples).
  auto label_type = GENERATE(TILEDB_FLOAT64, TILEDB_STRING_ASCII);
  auto array_name = create_single_label_array("array0", label_type);

  // Load array schema and check number of labels.
  tiledb_array_schema_t* loaded_array_schema{nullptr};
  require_tiledb_ok(
      tiledb_array_schema_load(ctx, array_name.c_str(), &loaded_array_schema));

  // Check the array schema has the expected dimension label.
  int32_t has_label;
  check_tiledb_ok(tiledb_array_schema_has_dimension_label(
      ctx, loaded_array_schema, "x", &has_label));
  CHECK(has_label == 1);

  // Check the expected number of attributes, dimenions and labels.
  auto nattr = loaded_array_schema->array_schema_->attribute_num();
  CHECK(nattr == 1);
  auto ndim = loaded_array_schema->array_schema_->domain().dim_num();
  CHECK(ndim == 2);
  auto loaded_dim_label_num =
      loaded_array_schema->array_schema_->dim_label_num();
  CHECK(loaded_dim_label_num == 1);

  // Free remaining resources
  tiledb_array_schema_free(&loaded_array_schema);
}

TEST_CASE_METHOD(
    DimensionLabelTestFixture,
    "Write and read back TileDB array schema with dimension label with "
    "non-default filters",
    "[capi][ArraySchema][DimensionLabel]") {
  // Create and add dimension label schema (both fixed and variable length
  // examples).
  auto label_type = GENERATE(TILEDB_FLOAT64, TILEDB_STRING_ASCII);
  // Create an array schema
  uint64_t x_domain[2]{0, 63};
  uint64_t x_tile_extent{64};
  uint64_t y_domain[2]{0, 63};
  uint64_t y_tile_extent{64};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_DENSE,
      {"x", "y"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&x_domain[0], &y_domain[0]},
      {&x_tile_extent, &y_tile_extent},
      {"a"},
      {TILEDB_FLOAT64},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4096,
      false);

  // Add dimension label.
  require_tiledb_ok(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "x", TILEDB_INCREASING_DATA, label_type));

  // Set filter.
  tiledb_filter_list_t* filter_list;
  require_tiledb_ok(tiledb_filter_list_alloc(ctx, &filter_list));
  tiledb_filter_t* filter;
  int32_t level = 6;
  require_tiledb_ok(tiledb_filter_alloc(ctx, TILEDB_FILTER_BZIP2, &filter));
  require_tiledb_ok(
      tiledb_filter_set_option(ctx, filter, TILEDB_COMPRESSION_LEVEL, &level));
  require_tiledb_ok(tiledb_filter_list_add_filter(ctx, filter_list, filter));
  require_tiledb_ok(tiledb_array_schema_set_dimension_label_filter_list(
      ctx, array_schema, "x", filter_list));
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);

  // Check array schema and number of dimension labels.
  require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));
  auto dim_label_num = array_schema->array_schema_->dim_label_num();
  REQUIRE(dim_label_num == 1);

  // Create array
  auto array_name =
      create_temporary_array("array_with_label_modified_tile", array_schema);
  URI array_uri{array_name};
  tiledb_array_schema_free(&array_schema);

  // Get the schema for array containing the dimension label.
  tiledb_array_schema_t* loaded_array_schema{nullptr};
  require_tiledb_ok(
      tiledb_array_schema_load(ctx, array_uri.c_str(), &loaded_array_schema));
  auto dim_label_ref =
      loaded_array_schema->array_schema_->dimension_label_reference("x");
  auto dim_label_uri = dim_label_ref.uri(array_uri);
  tiledb_array_schema_t* loaded_dim_label_array_schema{nullptr};
  require_tiledb_ok(tiledb_array_schema_load(
      ctx, dim_label_uri.c_str(), &loaded_dim_label_array_schema));
  tiledb_array_schema_free(&loaded_array_schema);

  // Check the filter on the label attribute.
  tiledb_attribute_t* label_attr;
  require_tiledb_ok(tiledb_array_schema_get_attribute_from_index(
      ctx, loaded_dim_label_array_schema, 0, &label_attr));
  tiledb_filter_list_t* loaded_filter_list;
  require_tiledb_ok(
      tiledb_attribute_get_filter_list(ctx, label_attr, &loaded_filter_list));
  uint32_t nfilters;
  require_tiledb_ok(
      tiledb_filter_list_get_nfilters(ctx, loaded_filter_list, &nfilters));
  CHECK(nfilters == 1);
  tiledb_filter_t* loaded_filter;
  require_tiledb_ok(tiledb_filter_list_get_filter_from_index(
      ctx, loaded_filter_list, 0, &loaded_filter));
  REQUIRE(loaded_filter != nullptr);
  tiledb_filter_type_t loaded_filter_type{};
  require_tiledb_ok(
      tiledb_filter_get_type(ctx, loaded_filter, &loaded_filter_type));
  CHECK(loaded_filter_type == TILEDB_FILTER_BZIP2);
  int32_t loaded_level{};
  require_tiledb_ok(tiledb_filter_get_option(
      ctx, loaded_filter, TILEDB_COMPRESSION_LEVEL, &loaded_level));
  CHECK(loaded_level == level);
  tiledb_attribute_free(&label_attr);
  tiledb_filter_free(&loaded_filter);
  tiledb_filter_list_free(&loaded_filter_list);

  // Free remaining resources.
  tiledb_array_schema_free(&loaded_dim_label_array_schema);
}

TEST_CASE_METHOD(
    DimensionLabelTestFixture,
    "Write and read back TileDB array schema with dimension label with "
    "non-default tile",
    "[capi][ArraySchema][DimensionLabel]") {
  // Create and add dimension label schema (both fixed and variable length
  // examples).
  auto label_type = GENERATE(TILEDB_FLOAT64, TILEDB_STRING_ASCII);
  // Create an array schema
  uint64_t x_domain[2]{0, 63};
  uint64_t x_tile_extent{64};
  uint64_t y_domain[2]{0, 63};
  uint64_t y_tile_extent{64};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_DENSE,
      {"x", "y"},
      {TILEDB_UINT64, TILEDB_UINT64},
      {&x_domain[0], &y_domain[0]},
      {&x_tile_extent, &y_tile_extent},
      {"a"},
      {TILEDB_FLOAT64},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4096,
      false);

  // Add dimension label.
  require_tiledb_ok(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "x", TILEDB_INCREASING_DATA, label_type));

  // Set tile.
  uint64_t tile_extent{8};
  require_tiledb_ok(tiledb_array_schema_set_dimension_label_tile_extent(
      ctx, array_schema, "x", TILEDB_UINT64, &tile_extent));

  // Check array schema and number of dimension labels.
  require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));
  auto dim_label_num = array_schema->array_schema_->dim_label_num();
  REQUIRE(dim_label_num == 1);

  // Create array
  auto array_name =
      create_temporary_array("array_with_label_modified_tile", array_schema);
  URI array_uri{array_name};
  tiledb_array_schema_free(&array_schema);

  // Get the URI for the dimension label array schema.
  tiledb_array_schema_t* loaded_array_schema{nullptr};
  require_tiledb_ok(
      tiledb_array_schema_load(ctx, array_uri.c_str(), &loaded_array_schema));
  auto dim_label_ref =
      loaded_array_schema->array_schema_->dimension_label_reference("x");
  auto dim_label_uri = dim_label_ref.uri(array_uri);

  // Open the dimension label array schema and check the tile extent.
  tiledb_array_schema_t* loaded_dim_label_array_schema{nullptr};
  require_tiledb_ok(tiledb_array_schema_load(
      ctx, dim_label_uri.c_str(), &loaded_dim_label_array_schema));
  uint64_t loaded_tile_extent{
      loaded_dim_label_array_schema->array_schema_->dimension_ptr(0)
          ->tile_extent()
          .rvalue_as<uint64_t>()};
  REQUIRE(tile_extent == loaded_tile_extent);

  // Free remaining resources
  tiledb_array_schema_free(&loaded_array_schema);
  tiledb_array_schema_free(&loaded_dim_label_array_schema);
}

TEST_CASE_METHOD(
    DimensionLabelTestFixture,
    "Subarray with a fixed-length dimension label range",
    "[capi][subarray][DimensionLabel]") {
  auto array_name = create_multi_label_array("array1");

  // Create array and subarray.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));
  tiledb_subarray_t* subarray;
  require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));

  // Check label range num is zero for all labels.
  uint64_t range_num{0};
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
  CHECK(range_num == 0);
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);

  // Add fixed ranges
  double r1[2]{-1.0, 1.0};
  require_tiledb_ok(tiledb_subarray_add_label_range(
      ctx, subarray, "x", &r1[0], &r1[1], nullptr));
  // Check no regular ranges set.
  require_tiledb_ok(
      tiledb_subarray_get_range_num(ctx, subarray, 0, &range_num));
  CHECK(range_num == 0);
  // Check 1 label range set by name.
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
  CHECK(range_num == 1);
  // Check 0 label range set by name to other label on dim.
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);

  // Check getting the range back
  const void* r1_start{};
  const void* r1_end{};
  const void* r1_stride{};
  require_tiledb_ok(tiledb_subarray_get_label_range(
      ctx, subarray, "x", 0, &r1_start, &r1_end, &r1_stride));
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
    DimensionLabelTestFixture,
    "Subarray with variable dimension label range",
    "[capi][subarray][DimensionLabel]") {
  auto array_name = create_multi_label_array("array1");

  // Create array and subarray.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));
  tiledb_subarray_t* subarray;
  require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));

  // Check label range num is zero for all labels.
  uint64_t range_num{0};
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
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
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
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
      ctx, subarray, "x", &r1[0], &r1[1], nullptr);
  CHECK(rc != TILEDB_OK);

  // Free resources.
  tiledb_subarray_free(&subarray);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    DimensionLabelTestFixture,
    "Subarray with dimension label ranges blocked",
    "[capi][subarray][DimensionLabel]") {
  auto array_name = create_multi_label_array("array1");

  // Create array and subarray.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));
  tiledb_subarray_t* subarray;
  require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));

  // Check label range num is zero for all labels.
  uint64_t range_num{0};
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
  CHECK(range_num == 0);
  require_tiledb_ok(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);

  // Check error when adding range to non-existent label.
  double r0[2]{-1.0, 1.0};
  auto rc = tiledb_subarray_add_label_range(
      ctx, subarray, "label1", &r0[0], &r0[1], nullptr);
  CHECK(rc != TILEDB_OK);
  std::string start0{"start"};
  std::string end0{"end"};
  rc = tiledb_subarray_add_label_range_var(
      ctx,
      subarray,
      "label1",
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
      ctx, subarray, "x", &r2[0], &r2[1], nullptr);
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
