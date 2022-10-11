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

#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/c_api/experimental/tiledb_dimension_label.h"
#include "tiledb/sm/c_api/experimental/tiledb_struct_def.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
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
   *    - x: (dim_idx=0, type=label_type, domain=label_domain, tile=tile_extent)
   *
   * @param array_name Array name relative to the fixture temporary directory.
   * @param label_type The data type of the dimension label.
   * @param label_domain The domain of the dimension label.
   * @param tile_extent The tile extent of the dimension label.
   * @returns Full URI to the generated array.
   */
  std::string create_single_label_array(
      std::string&& array_name,
      tiledb_datatype_t label_type,
      void* label_domain,
      void* tile_extents);

  /**
   * Creates a sample test array with multiple dimension labels.
   *
   * Array summary:
   *  * Array type: Dense
   *  * Dimensions:
   *    - x: (type=UINT64, domain=[0, 63], tile=64)
   *  * Attributes:
   *    - a: (type=FLOAT64)
   *  * Dimesnion labels:
   *    - x: (dim_idx=0, type=FLOAT64, domain=[-10, 10], tile=4.0)
   *    - id: (dim_idx=0, type=STRING_ASCII, domain=null, tile=null)
   *
   * @param array_name Array name relative to the fixture temporary directory.
   * @returns Full URI to the generated array.
   */
  std::string create_multi_label_array(std::string&& array_name);
};

std::string DimensionLabelTestFixture::create_single_label_array(
    std::string&& array_name,
    tiledb_datatype_t label_type,
    void* label_domain,
    void* label_tile_extent) {
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

  tiledb_dimension_label_schema_t* dim_label_schema;
  REQUIRE_TILEDB_OK(tiledb_dimension_label_schema_alloc(
      ctx,
      TILEDB_UNORDERED_LABELS,
      TILEDB_UINT64,
      x_domain,
      &x_tile_extent,
      label_type,
      label_domain,
      label_tile_extent,
      &dim_label_schema));

  REQUIRE_TILEDB_OK(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "x", dim_label_schema));
  tiledb_dimension_label_schema_free(&dim_label_schema);

  // Check array schema and number of dimension labels.
  REQUIRE_TILEDB_OK(tiledb_array_schema_check(ctx, array_schema));
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

  // Create and add dimension labels to schema.
  tiledb_dimension_label_schema_t* dim_label_schema;
  // l0
  double l0_domain[] = {-10.0, 10.0};
  double l0_tile_extent = 4.0;
  REQUIRE_TILEDB_OK(tiledb_dimension_label_schema_alloc(
      ctx,
      TILEDB_INCREASING_LABELS,
      TILEDB_UINT64,
      x_domain,
      &x_tile_extent,
      TILEDB_FLOAT64,
      l0_domain,
      &l0_tile_extent,
      &dim_label_schema));
  REQUIRE_TILEDB_OK(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "x", dim_label_schema));
  tiledb_dimension_label_schema_free(&dim_label_schema);
  // l1
  REQUIRE_TILEDB_OK(tiledb_dimension_label_schema_alloc(
      ctx,
      TILEDB_UNORDERED_LABELS,
      TILEDB_UINT64,
      x_domain,
      &x_tile_extent,
      TILEDB_STRING_ASCII,
      nullptr,
      nullptr,
      &dim_label_schema));
  REQUIRE_TILEDB_OK(tiledb_array_schema_add_dimension_label(
      ctx, array_schema, 0, "id", dim_label_schema));
  tiledb_dimension_label_schema_free(&dim_label_schema);

  // Check array schema and number of dimension labels.
  REQUIRE_TILEDB_OK(tiledb_array_schema_check(ctx, array_schema));
  auto dim_label_num = array_schema->array_schema_->dim_label_num();
  REQUIRE(dim_label_num == 2);

  // Create array
  auto array_uri = create_temporary_array(std::move(array_name), array_schema);
  tiledb_array_schema_free(&array_schema);
  return array_uri;
}

TEST_CASE(
    "Create dimension label schema",
    "[capi][DimensionLabelSchema][DimensionLabel]") {
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);
  int64_t dim_domain[] = {1, 10};
  int64_t tile_extent = 5;
  double label_domain[] = {-10.0, 10.0};
  double label_tile_extent = 4.0;
  tiledb_dimension_label_schema_t* dim_label;
  REQUIRE_TILEDB_OK(tiledb_dimension_label_schema_alloc(
      ctx,
      TILEDB_INCREASING_LABELS,
      TILEDB_INT64,
      dim_domain,
      &tile_extent,
      TILEDB_FLOAT64,
      label_domain,
      &label_tile_extent,
      &dim_label));
  tiledb_dimension_label_schema_free(&dim_label);
}

TEST_CASE_METHOD(
    DimensionLabelTestFixture,
    "Write and read back TileDB array schema with dimension label",
    "[capi][ArraySchema][DimensionLabel]") {
  // Create and add dimension label schema (both fixed and variable length
  // examples).
  auto label_type = GENERATE(TILEDB_FLOAT64, TILEDB_STRING_ASCII);
  double label_domain[] = {-10.0, 10.0};
  double label_tile_extent = 4.0;
  auto array_name = create_single_label_array(
      "array0",
      label_type,
      (label_type == TILEDB_STRING_ASCII) ? nullptr : label_domain,
      (label_type == TILEDB_STRING_ASCII) ? nullptr : &label_tile_extent);

  // Load array schema and check number of labels.
  tiledb_array_schema_t* loaded_array_schema{nullptr};
  REQUIRE_TILEDB_OK(
      tiledb_array_schema_load(ctx, array_name.c_str(), &loaded_array_schema));
  auto nattr = loaded_array_schema->array_schema_->attribute_num();
  REQUIRE(nattr == 1);
  auto ndim = loaded_array_schema->array_schema_->domain().dim_num();
  REQUIRE(ndim == 2);
  auto loaded_dim_label_num =
      loaded_array_schema->array_schema_->dim_label_num();
  CHECK(loaded_dim_label_num == 1);
  const auto& dim_label_ref =
      loaded_array_schema->array_schema_->dimension_label_reference(0);
  auto x_label_uri = dim_label_ref.uri().c_str();

  // Check the dimension label schema.
  auto label_datatype = static_cast<Datatype>(label_type);
  URI label_uri{array_name + "/" + x_label_uri};
  DimensionLabel dim_label(label_uri, ctx->storage_manager());
  dim_label.open(QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);
  CHECK(dim_label.index_dimension()->type() == Datatype::UINT64);
  CHECK(dim_label.index_attribute()->type() == Datatype::UINT64);
  CHECK(dim_label.label_attribute()->type() == label_datatype);
  CHECK(dim_label.label_attribute()->type() == label_datatype);
  const auto& indexed_array_schema =
      dim_label.indexed_array()->array_schema_latest();
  CHECK(indexed_array_schema.domain().dim_num() == 1);
  CHECK(indexed_array_schema.attribute_num() == 1);
  CHECK(indexed_array_schema.dimension_ptr(0)->type() == Datatype::UINT64);
  CHECK(indexed_array_schema.attribute(0)->type() == label_datatype);
  const auto& labelled_array_schema =
      dim_label.labelled_array()->array_schema_latest();
  CHECK(labelled_array_schema.domain().dim_num() == 1);
  CHECK(labelled_array_schema.attribute_num() == 1);
  CHECK(labelled_array_schema.dimension_ptr(0)->type() == label_datatype);
  CHECK(labelled_array_schema.attribute(0)->type() == Datatype::UINT64);
  dim_label.close();

  // Free remaining resources
  tiledb_array_schema_free(&loaded_array_schema);
}

TEST_CASE_METHOD(
    DimensionLabelTestFixture,
    "Subarray with a fixed-length dimension label range",
    "[capi][subarray][DimensionLabel]") {
  auto array_name = create_multi_label_array("array1");

  // Create array and subarray.
  tiledb_array_t* array;
  REQUIRE_TILEDB_OK(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  REQUIRE_TILEDB_OK(tiledb_array_open(ctx, array, TILEDB_READ));
  tiledb_subarray_t* subarray;
  REQUIRE_TILEDB_OK(tiledb_subarray_alloc(ctx, array, &subarray));

  // Check label range num is zero for all labels.
  uint64_t range_num{0};
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
  CHECK(range_num == 0);
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);

  // Add fixed ranges
  double r1[2]{-1.0, 1.0};
  REQUIRE_TILEDB_OK(tiledb_subarray_add_label_range(
      ctx, subarray, "x", &r1[0], &r1[1], nullptr));
  // Check no regular ranges set.
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_range_num(ctx, subarray, 0, &range_num));
  CHECK(range_num == 0);
  // Check 1 label range set by name.
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
  CHECK(range_num == 1);
  // Check 0 label range set by name to other label on dim.
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);

  // Check getting the range back
  const void* r1_start{};
  const void* r1_end{};
  const void* r1_stride{};
  REQUIRE_TILEDB_OK(tiledb_subarray_get_label_range(
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
}

TEST_CASE_METHOD(
    DimensionLabelTestFixture,
    "Subarray with variable dimension label range",
    "[capi][subarray][DimensionLabel]") {
  auto array_name = create_multi_label_array("array1");

  // Create array and subarray.
  tiledb_array_t* array;
  REQUIRE_TILEDB_OK(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  REQUIRE_TILEDB_OK(tiledb_array_open(ctx, array, TILEDB_READ));
  tiledb_subarray_t* subarray;
  REQUIRE_TILEDB_OK(tiledb_subarray_alloc(ctx, array, &subarray));

  // Check label range num is zero for all labels.
  uint64_t range_num{0};
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
  CHECK(range_num == 0);
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 0);

  // Set range.
  std::string start{"alpha"};
  std::string end{"beta"};
  REQUIRE_TILEDB_OK(tiledb_subarray_add_label_range_var(
      ctx, subarray, "id", start.data(), start.size(), end.data(), end.size()));
  // Check no regular ranges set.
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_range_num(ctx, subarray, 0, &range_num));
  CHECK(range_num == 0);
  // Check 1 label range set by name.
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_label_range_num(ctx, subarray, "id", &range_num));
  CHECK(range_num == 1);
  // Check 0 label range set by name to other label on dim.
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
  CHECK(range_num == 0);

  // Check getting the range back from the subarray.
  uint64_t start_size{0};
  uint64_t end_size{0};
  REQUIRE_TILEDB_OK(tiledb_subarray_get_label_range_var_size(
      ctx, subarray, "id", 0, &start_size, &end_size));
  std::vector<char> start_data(start_size);
  std::vector<char> end_data(end_size);
  REQUIRE_TILEDB_OK(tiledb_subarray_get_label_range_var(
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
}

TEST_CASE_METHOD(
    DimensionLabelTestFixture,
    "Subarray with dimension label ranges blocked",
    "[capi][subarray][DimensionLabel]") {
  auto array_name = create_multi_label_array("array1");

  // Create array and subarray.
  tiledb_array_t* array;
  REQUIRE_TILEDB_OK(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  REQUIRE_TILEDB_OK(tiledb_array_open(ctx, array, TILEDB_READ));
  tiledb_subarray_t* subarray;
  REQUIRE_TILEDB_OK(tiledb_subarray_alloc(ctx, array, &subarray));

  // Check label range num is zero for all labels.
  uint64_t range_num{0};
  REQUIRE_TILEDB_OK(
      tiledb_subarray_get_label_range_num(ctx, subarray, "x", &range_num));
  CHECK(range_num == 0);
  REQUIRE_TILEDB_OK(
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
  REQUIRE_TILEDB_OK(
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
