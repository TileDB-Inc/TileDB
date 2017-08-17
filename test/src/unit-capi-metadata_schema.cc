/**
 * @file unit-capi-metadata_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * Tests for the C API tiledb_metadata_schema_t spec, along with
 * tiledb_attribute_iter_t.
 */

#include <unistd.h>
#include <cassert>
#include <cstring>
#include "../../core/include/vfs/filesystem.h"
#include "catch.hpp"
#include "tiledb.h"

struct MetadataSchemaFx {
  // Constant parameters
  const std::string GROUP = "test_group/";
  const std::string METADATA_NAME = "metadata";
  const std::string METADATA_PATH = GROUP + METADATA_NAME;
  const std::string METADATA_PATH_REAL = tiledb::vfs::real_dir(METADATA_PATH);
  const char* ARRAY_TYPE_STR = "sparse";
  const uint64_t CAPACITY = 500;
  const char* CAPACITY_STR = "500";
  const tiledb_layout_t CELL_ORDER = TILEDB_COL_MAJOR;
  const char* CELL_ORDER_STR = "col-major";
  const tiledb_layout_t TILE_ORDER = TILEDB_ROW_MAJOR;
  const char* TILE_ORDER_STR = "row-major";
  const char* ATTR_NAME = "a";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const char* ATTR_TYPE_STR = "INT32";
  const tiledb_compressor_t ATTR_COMPRESSOR = TILEDB_GZIP;
  const char* ATTR_COMPRESSOR_STR = "GZIP";
  const int ATTR_COMPRESSION_LEVEL = 5;
  const char* ATTR_COMPRESSION_LEVEL_STR = "5";
  const unsigned int CELL_VAL_NUM = 3;
  const char* CELL_VAL_NUM_STR = "3";

  // Metadata schema object under test
  tiledb_metadata_schema_t* metadata_schema_;

  // TileDB context
  tiledb_ctx_t* ctx_;

  MetadataSchemaFx() {
    // Error code
    int rc;

    // Metadata schema not set yet
    metadata_schema_ = nullptr;

    // Initialize context
    rc = tiledb_ctx_create(&ctx_);
    assert(rc == TILEDB_OK);

    // Create group, delete it if it already exists
    std::string cmd = "test -d " + GROUP;
    rc = system(cmd.c_str());
    if (rc == 0) {
      cmd = "rm -rf " + GROUP;
      rc = system(cmd.c_str());
      assert(rc == 0);
    }
    rc = tiledb_group_create(ctx_, GROUP.c_str());
    assert(rc == TILEDB_OK);
  }

  ~MetadataSchemaFx() {
    // Free metadata schema
    if (metadata_schema_ != nullptr)
      tiledb_metadata_schema_free(metadata_schema_);

    // Free TileDB context
    tiledb_ctx_free(ctx_);

    // Remove the temporary group
    std::string cmd = "rm -rf " + GROUP;
    int rc = system(cmd.c_str());
    assert(rc == 0);
  }

  void create_metadata() {
    int rc;

    // Attribute
    tiledb_attribute_t* attr;
    rc = tiledb_attribute_create(ctx_, &attr, ATTR_NAME, ATTR_TYPE);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_attribute_set_compressor(
        ctx_, attr, ATTR_COMPRESSOR, ATTR_COMPRESSION_LEVEL);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_attribute_set_cell_val_num(ctx_, attr, CELL_VAL_NUM);
    REQUIRE(rc == TILEDB_OK);

    // Create metadata schema
    rc = tiledb_metadata_schema_create(
        ctx_, &metadata_schema_, METADATA_PATH.c_str());
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_metadata_schema_set_capacity(ctx_, metadata_schema_, CAPACITY);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_metadata_schema_set_cell_order(
        ctx_, metadata_schema_, CELL_ORDER);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_metadata_schema_set_tile_order(
        ctx_, metadata_schema_, TILE_ORDER);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_metadata_schema_add_attribute(ctx_, metadata_schema_, attr);
    REQUIRE(rc == TILEDB_OK);

    // Clean up
    tiledb_attribute_free(attr);

    // Create the metadata
    rc = tiledb_metadata_create(ctx_, metadata_schema_);
    REQUIRE(rc == TILEDB_OK);
  }
};

TEST_CASE_METHOD(
    MetadataSchemaFx, "C API: Test metadata schema creation and retrieval") {
  create_metadata();

  // Load metadata schema from the disk
  tiledb_metadata_schema_t* metadata_schema;
  int rc = tiledb_metadata_schema_load(
      ctx_, &metadata_schema, METADATA_PATH.c_str());
  REQUIRE(rc == TILEDB_OK);

  // Check name
  const char* name;
  rc = tiledb_metadata_schema_get_metadata_name(ctx_, metadata_schema, &name);
  REQUIRE(rc == TILEDB_OK);
  std::string name_real = tiledb::vfs::real_dir(name);
  CHECK_THAT(name_real, Catch::Equals(METADATA_PATH_REAL));

  // Check capacity
  uint64_t capacity;
  rc = tiledb_metadata_schema_get_capacity(ctx_, metadata_schema, &capacity);
  REQUIRE(rc == TILEDB_OK);
  CHECK(capacity == CAPACITY);

  // Check cell order
  tiledb_layout_t cell_order;
  rc =
      tiledb_metadata_schema_get_cell_order(ctx_, metadata_schema, &cell_order);
  REQUIRE(rc == TILEDB_OK);
  CHECK(cell_order == CELL_ORDER);

  // Check tile order
  tiledb_layout_t tile_order;
  rc =
      tiledb_metadata_schema_get_tile_order(ctx_, metadata_schema, &tile_order);
  REQUIRE(rc == TILEDB_OK);
  CHECK(tile_order == TILE_ORDER);

  // Check attribute
  int attr_it_done;
  tiledb_attribute_iter_t* attr_it;
  rc = tiledb_attribute_iter_create(
      ctx_, metadata_schema, &attr_it, TILEDB_METADATA);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_attribute_iter_done(ctx_, attr_it, &attr_it_done);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!attr_it_done);

  const tiledb_attribute_t* attr;
  rc = tiledb_attribute_iter_here(ctx_, attr_it, &attr);
  REQUIRE(rc == TILEDB_OK);

  const char* attr_name;
  rc = tiledb_attribute_get_name(ctx_, attr, &attr_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(attr_name, Catch::Equals(ATTR_NAME));

  tiledb_datatype_t attr_type;
  rc = tiledb_attribute_get_type(ctx_, attr, &attr_type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(attr_type == ATTR_TYPE);

  tiledb_compressor_t attr_compressor;
  int attr_compression_level;
  rc = tiledb_attribute_get_compressor(
      ctx_, attr, &attr_compressor, &attr_compression_level);
  REQUIRE(rc == TILEDB_OK);
  CHECK(attr_compressor == ATTR_COMPRESSOR);
  CHECK(attr_compression_level == ATTR_COMPRESSION_LEVEL);

  unsigned int cell_val_num;
  rc = tiledb_attribute_get_cell_val_num(ctx_, attr, &cell_val_num);
  REQUIRE(rc == TILEDB_OK);
  CHECK(cell_val_num == CELL_VAL_NUM);

  rc = tiledb_attribute_iter_next(ctx_, attr_it);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_iter_done(ctx_, attr_it, &attr_it_done);
  REQUIRE(rc == TILEDB_OK);
  CHECK(attr_it_done);

  rc = tiledb_attribute_iter_first(ctx_, attr_it);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_iter_here(ctx_, attr_it, &attr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_get_name(ctx_, attr, &attr_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(attr_name, Catch::Equals(ATTR_NAME));

  // Check dump
  std::string dump_str =
      "- Array name: " + tiledb::vfs::real_dir(METADATA_PATH) + "\n" +
      "- Array type: " + ARRAY_TYPE_STR + "\n" +
      "- Cell order: " + CELL_ORDER_STR + "\n" +
      "- Tile order: " + TILE_ORDER_STR + "\n" + "- Capacity: " + CAPACITY_STR +
      "\n" + "\n" + "### Dimension ###\n" + "- Name: __key_dim_1\n" +
      "- Type: UINT32\n" + "- Compressor: NO_COMPRESSION\n" +
      "- Compression level: -1\n" + "- Domain: [0,4294967295]\n" +
      "- Tile extent: null\n" + "\n" + "### Dimension ###\n" +
      "- Name: __key_dim_2\n" + "- Type: UINT32\n" +
      "- Compressor: NO_COMPRESSION\n" + "- Compression level: -1\n" +
      "- Domain: [0,4294967295]\n" + "- Tile extent: null\n" + "\n" +
      "### Dimension ###\n" + "- Name: __key_dim_3\n" + "- Type: UINT32\n" +
      "- Compressor: NO_COMPRESSION\n" + "- Compression level: -1\n" +
      "- Domain: [0,4294967295]\n" + "- Tile extent: null\n" + "\n" +
      "### Dimension ###\n" + "- Name: __key_dim_4\n" + "- Type: UINT32\n" +
      "- Compressor: NO_COMPRESSION\n" + "- Compression level: -1\n" +
      "- Domain: [0,4294967295]\n" + "- Tile extent: null\n" + "\n" +
      "### Attribute ###\n" + "- Name: " + ATTR_NAME + "\n" +
      "- Type: " + ATTR_TYPE_STR + "\n" +
      "- Compressor: " + ATTR_COMPRESSOR_STR + "\n" +
      "- Compression level: " + ATTR_COMPRESSION_LEVEL_STR + "\n" +
      "- Cell val num: " + CELL_VAL_NUM_STR + "\n";
  FILE* gold_fout = fopen("gold_fout.txt", "w");
  const char* dump = dump_str.c_str();
  fwrite(dump, sizeof(char), strlen(dump), gold_fout);
  fclose(gold_fout);
  FILE* fout = fopen("fout.txt", "w");
  tiledb_metadata_schema_dump(ctx_, metadata_schema, fout);
  fclose(fout);
  CHECK(!system("diff gold_fout.txt fout.txt"));
  system("rm gold_fout.txt fout.txt");

  // Clean up
  tiledb_attribute_iter_free(attr_it);
  tiledb_metadata_schema_free(metadata_schema);
}
