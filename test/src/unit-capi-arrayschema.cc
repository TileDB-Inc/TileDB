/**
 * @file unit-capi-arrayschema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests for the C API array schema spec.
 */
#include "tiledb.h"
#include "utils.h"

#include <unistd.h>
#include <cassert>

#include "catch.hpp"

struct ArraySchemaFx {
  // Workspace folder name. */
  const std::string GROUP = ".__group/";

  // Array name Format:
  // (<domain_size_1>x<domain_size_2>_<tile_extent_1>x<tile_extent_2>).
  const std::string ARRAYNAME = "dense_test_100x100_10x10";

  // Array name
  std::string array_name_;

  // Array schema object under test
  tiledb_array_schema_t array_schema_;

  // True if the array schema is set
  bool array_schema_set_;

  // TileDB context
  tiledb_ctx_t* tiledb_ctx_;

  ArraySchemaFx() {
    // Error code
    int rc;

    // Array schema not set yet
    array_schema_set_ = false;

    // Initialize context
    rc = tiledb_ctx_init(&tiledb_ctx_, nullptr);
    assert(rc == TILEDB_OK);

    // Create group, delete it if it already exists
    std::string cmd = "test -d " + GROUP;
    rc = system(cmd.c_str());
    if (rc == 0) {
      cmd = "rm -rf " + GROUP;
      rc = system(cmd.c_str());
      assert(rc == 0);
    }
    rc = tiledb_group_create(tiledb_ctx_, GROUP.c_str());
    assert(rc == TILEDB_OK);

    // Set array name
    array_name_ = GROUP + ARRAYNAME;
  }

  ~ArraySchemaFx() {
    // Error code
    int rc;

    // Finalize TileDB context
    rc = tiledb_ctx_finalize(tiledb_ctx_);
    assert(rc == TILEDB_OK);

    // Remove the temporary group
    std::string cmd = "rm -rf " + GROUP;
    rc = system(cmd.c_str());
    assert(rc == 0);

    // Free array schema
    if (array_schema_set_) {
      rc = tiledb_array_free_schema(&array_schema_);
      assert(rc == TILEDB_OK);
    }
  }

  int create_dense_array() {
    // Initialization s
    int rc;
    const char* attributes[] = {"ATTR_INT32"};
    const char* dimensions[] = {"X", "Y"};
    int64_t domain[] = {0, 99, 0, 99};
    int64_t tile_extents[] = {10, 10};
    const tiledb_datatype_t types[] = {TILEDB_INT32, TILEDB_INT64};
    const tiledb_compressor_t compression[] = {TILEDB_NO_COMPRESSION,
                                               TILEDB_NO_COMPRESSION};

    // Set array schema
    rc = tiledb_array_set_schema(
        // The tiledb_ctx_t
        tiledb_ctx_,
        // The array schema structure
        &array_schema_,
        // Array name
        array_name_.c_str(),
        // Attributes
        attributes,
        // Number of attributes
        1,
        // Capacity
        1000,
        // Cell order
        TILEDB_COL_MAJOR,
        // Number of cell values per attribute (NULL means 1 everywhere)
        nullptr,
        // Compression
        compression,
        // Dense array
        1,
        // Dimensions
        dimensions,
        // Number of dimensions
        2,
        // Domain
        domain,
        // Domain length in bytes
        4 * sizeof(int64_t),
        // Tile extents (no regular tiles defined)
        tile_extents,
        // Tile extents in bytes
        2 * sizeof(int64_t),
        // Tile order
        TILEDB_ROW_MAJOR,
        // Types
        types);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Remember that the array schema is set
    array_schema_set_ = true;

    // Create the array
    return tiledb_array_create(tiledb_ctx_, &array_schema_);
  }
};

TEST_CASE_METHOD(ArraySchemaFx, "Test array schema creation and retrieval") {
  int rc = create_dense_array();
  REQUIRE(rc == TILEDB_OK);

  // Load array schema from the disk
  tiledb_array_schema_t array_schema_disk;
  rc = tiledb_array_load_schema(
      tiledb_ctx_, array_name_.c_str(), &array_schema_disk);
  REQUIRE(rc == TILEDB_OK);

  // For easy reference
  int64_t* tile_extents_disk =
      static_cast<int64_t*>(array_schema_disk.tile_extents_);
  int64_t* tile_extents = static_cast<int64_t*>(array_schema_.tile_extents_);

  // Get real array path
  std::string array_name_real = tiledb::utils::real_dir(array_name_);
  CHECK(array_name_real.c_str() != "");

  // Tests
  CHECK_THAT(array_schema_disk.array_name_, Catch::Equals(array_name_real));
  CHECK(array_schema_disk.attribute_num_ == array_schema_.attribute_num_);
  CHECK(array_schema_disk.dim_num_ == array_schema_.dim_num_);
  CHECK(array_schema_disk.capacity_ == array_schema_.capacity_);
  CHECK(array_schema_disk.cell_order_ == array_schema_.cell_order_);
  CHECK(array_schema_disk.tile_order_ == array_schema_.tile_order_);
  CHECK(array_schema_disk.dense_ == array_schema_.dense_);

  CHECK_THAT(
      array_schema_disk.attributes_[0],
      Catch::Equals(array_schema_.attributes_[0]));
  CHECK(array_schema_disk.compressor_[0] == array_schema_.compressor_[0]);
  CHECK(array_schema_disk.compressor_[1] == array_schema_.compressor_[1]);
  CHECK(array_schema_disk.types_[0] == array_schema_.types_[0]);
  CHECK(array_schema_disk.types_[1] == array_schema_.types_[1]);
  CHECK(tile_extents_disk[0] == tile_extents[0]);
  CHECK(tile_extents_disk[1] == tile_extents[1]);

  // Free array schema
  rc = tiledb_array_free_schema(&array_schema_disk);
  REQUIRE(rc == TILEDB_OK);
}
