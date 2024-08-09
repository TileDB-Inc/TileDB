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
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <iostream>
#include <string>
#include <unordered_map>

using namespace tiledb::sm;
using namespace tiledb::test;

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Write and read back TileDB array schema with dimension label",
    "[capi][ArraySchema][DimensionLabel]") {
  // Create and add dimension label schema (both fixed and variable length
  // examples).
  bool serialize = false;
#ifdef TILEDB_SERIALIZATION
  serialize = GENERATE(true, false);
#endif

  auto label_type = GENERATE(TILEDB_FLOAT64, TILEDB_STRING_ASCII);
  auto label_order = GENERATE(TILEDB_INCREASING_DATA, TILEDB_DECREASING_DATA);

  // Create an array schema.
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
      ctx, array_schema, 0, "label", label_order, label_type));

  // Check array schema and number of dimension labels.
  require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));
  uint64_t dim_label_num = 0;
  require_tiledb_ok(tiledb_array_schema_get_dimension_label_num(
      ctx, array_schema, &dim_label_num));
  REQUIRE(dim_label_num == 1);

  tiledb_dimension_label_t* dim_label{nullptr};
  SECTION("get dimension label from index -") {
    require_tiledb_ok(tiledb_array_schema_get_dimension_label_from_index(
        ctx, array_schema, 0, &dim_label));
  }
  SECTION("get dimension label from name -") {
    require_tiledb_ok(tiledb_array_schema_get_dimension_label_from_name(
        ctx, array_schema, "label", &dim_label));
  }

  const char* dim_label_name;
  require_tiledb_ok(
      tiledb_dimension_label_get_name(ctx, dim_label, &dim_label_name));
  REQUIRE(!strcmp(dim_label_name, "label"));

  // Check the dimension label properties.
  uint32_t label_cell_val_num;
  check_tiledb_ok(tiledb_dimension_label_get_label_cell_val_num(
      ctx, dim_label, &label_cell_val_num));
  tiledb_datatype_t label_type_;
  check_tiledb_ok(
      tiledb_dimension_label_get_label_type(ctx, dim_label, &label_type_));
  if (label_type == TILEDB_FLOAT64) {
    CHECK(label_cell_val_num == 1);
    CHECK(label_type_ == TILEDB_FLOAT64);
  } else {
    CHECK(label_cell_val_num == tiledb::sm::constants::var_num);
    CHECK(label_type_ == TILEDB_STRING_ASCII);
  }
  const char* dim_label_uri;
  check_tiledb_ok(
      tiledb_dimension_label_get_uri(ctx, dim_label, &dim_label_uri));
  std::string expected_dim_label_uri{"__labels/l0"};
  CHECK(dim_label_uri == expected_dim_label_uri);
  tiledb_dimension_label_free(&dim_label);

  // Create array.
  auto array_name = create_temporary_array(
      "simple_array_with_label", array_schema, serialize);
  tiledb_array_schema_free(&array_schema);

  // Load array schema and check number of labels.
  tiledb_array_schema_t* loaded_array_schema{nullptr};
  require_tiledb_ok(
      tiledb_array_schema_load(ctx, array_name.c_str(), &loaded_array_schema));

  // Check the array schema has the expected dimension label.
  require_tiledb_ok(tiledb_array_schema_get_dimension_label_num(
      ctx, loaded_array_schema, &dim_label_num));
  CHECK(dim_label_num == 1);
  int32_t has_label;
  check_tiledb_ok(tiledb_array_schema_has_dimension_label(
      ctx, loaded_array_schema, "label", &has_label));
  CHECK(has_label == 1);

  // Free remaining resources
  tiledb_array_schema_free(&loaded_array_schema);
}

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Write and read back TileDB array schema with dimension label for "
    "unordered labels",
    "[capi][ArraySchema][DimensionLabel]") {
  // Create and add dimension label schema (both fixed and variable length
  // examples).
  auto label_type = GENERATE(TILEDB_FLOAT64, TILEDB_STRING_ASCII);

  // Create an array schema.
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

  check_tiledb_error_with(
      tiledb_array_schema_add_dimension_label(
          ctx, array_schema, 0, "label", TILEDB_UNORDERED_DATA, label_type),
      "ArraySchema: Cannot add dimension label; Unordered dimension labels "
      "are not yet supported.");
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Write and read back TileDB array schema with dimension label with "
    "non-default filters",
    "[capi][ArraySchema][DimensionLabel]") {
  // Create and add dimension label schema (both fixed and variable length
  // examples).
  bool serialize = false;
#ifdef TILEDB_SERIALIZATION
  serialize = GENERATE(true, false);
#endif

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
      ctx, array_schema, 0, "label", TILEDB_INCREASING_DATA, label_type));

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
      ctx, array_schema, "label", filter_list));
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);

  // Check array schema and number of dimension labels.
  require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));
  uint64_t dim_label_num = 0;
  require_tiledb_ok(tiledb_array_schema_get_dimension_label_num(
      ctx, array_schema, &dim_label_num));
  REQUIRE(dim_label_num == 1);

  // Create array
  auto array_name = create_temporary_array(
      "array_with_label_modified_tile", array_schema, serialize);
  URI array_uri{array_name};
  tiledb_array_schema_free(&array_schema);

  // Get the schema for dimension label array.
  tiledb_array_schema_t* loaded_array_schema{nullptr};
  require_tiledb_ok(
      tiledb_array_schema_load(ctx, array_uri.c_str(), &loaded_array_schema));
  tiledb_dimension_label_t* loaded_dim_label{nullptr};
  require_tiledb_ok(tiledb_array_schema_get_dimension_label_from_name(
      ctx, loaded_array_schema, "label", &loaded_dim_label));
  if (array_name.find("tiledb://") == std::string::npos) {
    const char* dim_label_uri;
    require_tiledb_ok(
        tiledb_dimension_label_get_uri(ctx, loaded_dim_label, &dim_label_uri));
    const char* label_attr_name;
    require_tiledb_ok(tiledb_dimension_label_get_label_attr_name(
        ctx, loaded_dim_label, &label_attr_name));
    tiledb_array_schema_t* loaded_dim_label_array_schema{nullptr};
    // We can't open a dimension label by URI via REST.
    // The underlying 'Array' that is the dim label is not a registered asset.
    if (array_name.find("tiledb://") == std::string::npos) {
      require_tiledb_ok(tiledb_array_schema_load(
          ctx, dim_label_uri, &loaded_dim_label_array_schema));
    }
    // Check the filter on the label attribute.
    tiledb_attribute_t* label_attr;
    require_tiledb_ok(tiledb_array_schema_get_attribute_from_name(
        ctx, loaded_dim_label_array_schema, label_attr_name, &label_attr));
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
    tiledb_array_schema_free(&loaded_dim_label_array_schema);
  }

  // Free remaining resources.
  tiledb_dimension_label_free(&loaded_dim_label);
  tiledb_array_schema_free(&loaded_array_schema);
}

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Write and read back TileDB array schema with dimension label with "
    "non-default tile",
    "[capi][ArraySchema][DimensionLabel]") {
  // Create and add dimension label schema (both fixed and variable length
  // examples).
  bool serialize = false;
#ifdef TILEDB_SERIALIZATION
  serialize = GENERATE(true, false);
#endif

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
      ctx, array_schema, 0, "label", TILEDB_INCREASING_DATA, label_type));

  // Set tile.
  uint64_t tile_extent{8};
  require_tiledb_ok(tiledb_array_schema_set_dimension_label_tile_extent(
      ctx, array_schema, "label", TILEDB_UINT64, &tile_extent));

  // Check array schema and number of dimension labels.
  require_tiledb_ok(tiledb_array_schema_check(ctx, array_schema));
  auto dim_label_num = array_schema->array_schema_->dim_label_num();
  REQUIRE(dim_label_num == 1);

  // Create array
  auto array_name = create_temporary_array(
      "array_with_label_modified_tile", array_schema, serialize);
  URI array_uri{array_name};
  tiledb_array_schema_free(&array_schema);

  // Get the URI for the dimension label array schema.
  tiledb_array_schema_t* loaded_array_schema{nullptr};
  require_tiledb_ok(
      tiledb_array_schema_load(ctx, array_uri.c_str(), &loaded_array_schema));
  tiledb_dimension_label_t* loaded_dim_label{nullptr};
  require_tiledb_ok(tiledb_array_schema_get_dimension_label_from_name(
      ctx, loaded_array_schema, "label", &loaded_dim_label));

  if (array_name.find("tiledb://") == std::string::npos) {
    const char* dim_label_uri;
    require_tiledb_ok(
        tiledb_dimension_label_get_uri(ctx, loaded_dim_label, &dim_label_uri));
    // Open the dimension label array schema and check the tile extent.
    tiledb_array_schema_t* loaded_dim_label_array_schema{nullptr};
    require_tiledb_ok(tiledb_array_schema_load(
        ctx, dim_label_uri, &loaded_dim_label_array_schema));
    uint64_t loaded_tile_extent{
        loaded_dim_label_array_schema->array_schema_->dimension_ptr(0)
            ->tile_extent()
            .rvalue_as<uint64_t>()};
    REQUIRE(tile_extent == loaded_tile_extent);
    tiledb_array_schema_free(&loaded_dim_label_array_schema);
  }

  // Free remaining resources
  tiledb_dimension_label_free(&loaded_dim_label);
  tiledb_array_schema_free(&loaded_array_schema);
}
