/**
 * @file test-cppapi-dimension-label.cc
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
 * Tests the DimensionLabel C++ API
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_dimension_label.h"
#include "tiledb/sm/cpp_api/dimension_label.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <test/support/tdb_catch.h>

using namespace tiledb::test;

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Get dimension label from schema",
    "[cppapi][ArraySchema][DimensionLabel]") {
  // Use C-API to create an array with a dimension label.
  auto label_type = GENERATE(TILEDB_FLOAT64, TILEDB_STRING_ASCII);
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
      ctx, array_schema, 0, "label", TILEDB_INCREASING_DATA, label_type));
  auto array_name =
      create_temporary_array("simple_array_with_label", array_schema);
  tiledb_array_schema_free(&array_schema);

  // Allocate the C-API dimension label struct using the array schema.
  tiledb_dimension_label_t* c_dim_label{nullptr};
  tiledb_array_schema_t* loaded_array_schema{nullptr};
  require_tiledb_ok(
      tiledb_array_schema_load(ctx, array_name.c_str(), &loaded_array_schema));
  require_tiledb_ok(tiledb_array_schema_get_dimension_label_from_name(
      ctx, loaded_array_schema, "label", &c_dim_label));

  // Create C++ Dimension Label
  tiledb::Context context{ctx, false};
  tiledb::DimensionLabel dim_label{context, c_dim_label};

  // Check the values.
  CHECK(dim_label.dimension_index() == 0);
  CHECK(dim_label.label_order() == TILEDB_INCREASING_DATA);
  if (label_type == TILEDB_FLOAT64) {
    CHECK(dim_label.label_cell_val_num() == 1);
    CHECK(dim_label.label_type() == TILEDB_FLOAT64);
  } else {
    CHECK(dim_label.label_cell_val_num() == tiledb::sm::constants::var_num);
    CHECK(dim_label.label_type() == TILEDB_STRING_ASCII);
  }

  // Free the C-API.
  tiledb_dimension_label_free(&c_dim_label);
  tiledb_array_schema_free(&loaded_array_schema);
}
