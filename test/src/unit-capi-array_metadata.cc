/**
 * @file unit-capi-array_metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * Tests for the C API tiledb_array_metadata_t spec, along with
 * tiledb_attribute_iter_t and tiledb_dimension_iter_t.
 */

#include <unistd.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>

#include "catch.hpp"
#include "posix_filesystem.h"
#include "tiledb.h"
#include "uri.h"

struct ArraySchemaFx {
// Constant parameters
#ifdef HAVE_HDFS
  const std::string URI_PREFIX = "hdfs://";
  const std::string TEMP_DIR = "/tiledb_test/";
#else
  const std::string URI_PREFIX = "file://";
  const std::string TEMP_DIR = tiledb::posix::current_dir() + "/";
#endif
  const std::string GROUP = "test_group/";
  const std::string ARRAY_NAME = "dense_test_100x100_10x10";
  tiledb_array_type_t ARRAY_TYPE = TILEDB_DENSE;
  const char* ARRAY_TYPE_STR = "dense";
  const std::string ARRAY_PATH = URI_PREFIX + TEMP_DIR + GROUP + ARRAY_NAME;
  const std::string ARRAY_PATH_REAL = tiledb::URI(ARRAY_PATH).to_string();
  const uint64_t CAPACITY = 500;
  const char* CAPACITY_STR = "500";
  const tiledb_layout_t CELL_ORDER = TILEDB_COL_MAJOR;
  const char* CELL_ORDER_STR = "col-major";
  const tiledb_layout_t TILE_ORDER = TILEDB_ROW_MAJOR;
  const char* TILE_ORDER_STR = "row-major";
  const char* ATTR_NAME = "a";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const char* ATTR_TYPE_STR = "INT32";
  const tiledb_compressor_t ATTR_COMPRESSOR = TILEDB_NO_COMPRESSION;
  const char* ATTR_COMPRESSOR_STR = "NO_COMPRESSION";
  const int ATTR_COMPRESSION_LEVEL = -1;
  const char* ATTR_COMPRESSION_LEVEL_STR = "-1";
  const unsigned int CELL_VAL_NUM = 1;
  const char* CELL_VAL_NUM_STR = "1";
  const int DIM_NUM = 2;
  const char* DIM1_NAME = "d1";
  const char* DIM2_NAME = "d2";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const char* DIM_TYPE_STR = "INT64";
  const int64_t DIM_DOMAIN[4] = {0, 99, 20, 60};
  const char* DIM1_DOMAIN_STR = "[0,99]";
  const char* DIM2_DOMAIN_STR = "[20,60]";
  const uint64_t DIM_DOMAIN_SIZE = sizeof(DIM_DOMAIN) / DIM_NUM;
  const int64_t TILE_EXTENTS[2] = {10, 5};
  const char* DIM1_TILE_EXTENT_STR = "10";
  const char* DIM2_TILE_EXTENT_STR = "5";
  const uint64_t TILE_EXTENT_SIZE = sizeof(TILE_EXTENTS) / DIM_NUM;

  // Array metadata object under test
  tiledb_array_metadata_t* array_metadata_;

  // TileDB context
  tiledb_ctx_t* ctx_;

  ArraySchemaFx() {
    // Initialize context
    int rc = tiledb_ctx_create(&ctx_);
    if (rc != TILEDB_OK) {
      std::cerr << "ArraySchemaFx() Error creating tiledb_ctx_t" << std::endl;
      std::exit(1);
    }
    // Create group, delete it if it already exists
    if (dir_exists(TEMP_DIR + GROUP)) {
      bool success = remove_dir(TEMP_DIR + GROUP);
      if (!success) {
        tiledb_ctx_free(ctx_);
        std::cerr << "ArraySchemaFx() Error deleting existing test group"
                  << std::endl;
        std::exit(1);
      }
    }
    rc = tiledb_group_create(ctx_, (URI_PREFIX + TEMP_DIR + GROUP).c_str());
    if (rc != TILEDB_OK) {
      tiledb_ctx_free(ctx_);
      std::cerr << "ArraySchemaFx() Error creating test group" << std::endl;
      std::exit(1);
    }
  }

  ~ArraySchemaFx() {
    tiledb_array_metadata_free(ctx_, array_metadata_);
    tiledb_ctx_free(ctx_);
    // Remove the temporary group
    bool success = remove_dir(TEMP_DIR + GROUP);
    if (!success) {
      std::cerr << "~ArraySchemaFx() Error deleting test group" << std::endl;
      std::exit(1);
    }
  }

  bool dir_exists(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -test -d ") + path;
#else
    std::string cmd = std::string("test -d ") + path;
#endif
    return (system(cmd.c_str()) == 0);
  }

  bool remove_dir(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -rm -r -f ") + path;
#else
    std::string cmd = std::string("rm -r -f ") + path;
#endif
    return (system(cmd.c_str()) == 0);
  }

  void create_dense_array() {
    int rc;

    // Create array metadata with invalid URI
    rc =
        tiledb_array_metadata_create(ctx_, &array_metadata_, "file://my_array");
    REQUIRE(rc != TILEDB_OK);

    // Create array metadata
    rc = tiledb_array_metadata_create(
        ctx_, &array_metadata_, ARRAY_PATH.c_str());
    REQUIRE(rc == TILEDB_OK);

    // Set metadata members
    rc =
        tiledb_array_metadata_set_array_type(ctx_, array_metadata_, ARRAY_TYPE);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_set_capacity(ctx_, array_metadata_, CAPACITY);
    REQUIRE(rc == TILEDB_OK);
    rc =
        tiledb_array_metadata_set_cell_order(ctx_, array_metadata_, CELL_ORDER);
    REQUIRE(rc == TILEDB_OK);
    rc =
        tiledb_array_metadata_set_tile_order(ctx_, array_metadata_, TILE_ORDER);
    REQUIRE(rc == TILEDB_OK);

    // Check for invalid array metadata
    rc = tiledb_array_metadata_check(ctx_, array_metadata_);
    REQUIRE(rc != TILEDB_OK);
    rc = tiledb_array_create(ctx_, array_metadata_);
    REQUIRE(rc != TILEDB_OK);

    // Create dimensions
    tiledb_dimension_t* d1;
    rc = tiledb_dimension_create(
        ctx_, &d1, DIM1_NAME, TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0]);
    REQUIRE(rc == TILEDB_OK);
    tiledb_dimension_t* d2;
    rc = tiledb_dimension_create(
        ctx_, &d2, DIM2_NAME, TILEDB_INT64, &DIM_DOMAIN[2], &TILE_EXTENTS[1]);
    REQUIRE(rc == TILEDB_OK);

    // Set domain
    tiledb_domain_t* domain;
    rc = tiledb_domain_create(ctx_, &domain, DIM_TYPE);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_domain_add_dimension(ctx_, domain, d1);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_domain_add_dimension(ctx_, domain, d2);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_set_domain(ctx_, array_metadata_, domain);
    REQUIRE(rc == TILEDB_OK);

    // Check for invalid array metadata
    rc = tiledb_array_metadata_check(ctx_, array_metadata_);
    REQUIRE(rc != TILEDB_OK);
    rc = tiledb_array_create(ctx_, array_metadata_);
    REQUIRE(rc != TILEDB_OK);

    // Set attribute
    tiledb_attribute_t* attr;
    rc = tiledb_attribute_create(ctx_, &attr, ATTR_NAME, ATTR_TYPE);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata_, attr);
    REQUIRE(rc == TILEDB_OK);

    // Clean up
    tiledb_attribute_free(ctx_, attr);
    tiledb_dimension_free(ctx_, d1);
    tiledb_dimension_free(ctx_, d2);
    tiledb_domain_free(ctx_, domain);

    // Create the array
    rc = tiledb_array_create(ctx_, array_metadata_);
    REQUIRE(rc == TILEDB_OK);
  }
};

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array metadata creation and retrieval",
    "[metadata]") {
  create_dense_array();

  // Load array_metadata metadata from the disk
  tiledb_array_metadata_t* array_metadata;
  int rc =
      tiledb_array_metadata_load(ctx_, &array_metadata, ARRAY_PATH.c_str());
  REQUIRE(rc == TILEDB_OK);

  // Check name
  const char* name;
  rc = tiledb_array_metadata_get_array_name(ctx_, array_metadata, &name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(name, Catch::Equals(ARRAY_PATH_REAL));

  // Check capacity
  uint64_t capacity;
  rc = tiledb_array_metadata_get_capacity(ctx_, array_metadata, &capacity);
  REQUIRE(rc == TILEDB_OK);
  CHECK(capacity == CAPACITY);

  // Check cell order
  tiledb_layout_t cell_order;
  rc = tiledb_array_metadata_get_cell_order(ctx_, array_metadata, &cell_order);
  REQUIRE(rc == TILEDB_OK);
  CHECK(cell_order == CELL_ORDER);

  // Check tile order
  tiledb_layout_t tile_order;
  rc = tiledb_array_metadata_get_tile_order(ctx_, array_metadata, &tile_order);
  REQUIRE(rc == TILEDB_OK);
  CHECK(tile_order == TILE_ORDER);

  // Check array_metadata type
  tiledb_array_type_t type;
  rc = tiledb_array_metadata_get_array_type(ctx_, array_metadata, &type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(type == TILEDB_DENSE);

  // Check coordinates compression
  tiledb_compressor_t coords_compression;
  int coords_compression_level;
  rc = tiledb_array_metadata_get_coords_compressor(
      ctx_, array_metadata, &coords_compression, &coords_compression_level);
  REQUIRE(rc == TILEDB_OK);
  CHECK(coords_compression == TILEDB_DOUBLE_DELTA);
  CHECK(coords_compression_level == -1);

  // Check attribute
  int attr_it_done;
  tiledb_attribute_iter_t* attr_it;
  rc = tiledb_attribute_iter_create(ctx_, array_metadata, &attr_it);
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

  // Get domain
  tiledb_domain_t* domain;
  rc = tiledb_array_metadata_get_domain(ctx_, array_metadata, &domain);
  REQUIRE(rc == TILEDB_OK);

  // Check first dimension
  int dim_it_done;
  tiledb_dimension_iter_t* dim_it;
  rc = tiledb_dimension_iter_create(ctx_, domain, &dim_it);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_dimension_iter_done(ctx_, dim_it, &dim_it_done);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!dim_it_done);

  const tiledb_dimension_t* dim;
  rc = tiledb_dimension_iter_here(ctx_, dim_it, &dim);
  REQUIRE(rc == TILEDB_OK);

  const char* dim_name;
  rc = tiledb_dimension_get_name(ctx_, dim, &dim_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(dim_name, Catch::Equals(DIM1_NAME));

  void* dim_domain;
  rc = tiledb_dimension_get_domain(ctx_, dim, &dim_domain);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(dim_domain, &DIM_DOMAIN[0], DIM_DOMAIN_SIZE));

  void* tile_extent;
  rc = tiledb_dimension_get_tile_extent(ctx_, dim, &tile_extent);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(tile_extent, &TILE_EXTENTS[0], TILE_EXTENT_SIZE));

  rc = tiledb_dimension_iter_next(ctx_, dim_it);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_iter_done(ctx_, dim_it, &dim_it_done);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!dim_it_done);
  rc = tiledb_dimension_iter_here(ctx_, dim_it, &dim);
  REQUIRE(rc == TILEDB_OK);

  // Check second dimension
  rc = tiledb_dimension_get_name(ctx_, dim, &dim_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(dim_name, Catch::Equals(DIM2_NAME));

  rc = tiledb_dimension_get_domain(ctx_, dim, &dim_domain);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(dim_domain, &DIM_DOMAIN[2], DIM_DOMAIN_SIZE));

  rc = tiledb_dimension_get_tile_extent(ctx_, dim, &tile_extent);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(tile_extent, &TILE_EXTENTS[1], TILE_EXTENT_SIZE));

  rc = tiledb_dimension_iter_next(ctx_, dim_it);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_iter_done(ctx_, dim_it, &dim_it_done);
  REQUIRE(rc == TILEDB_OK);
  CHECK(dim_it_done);

  rc = tiledb_dimension_iter_first(ctx_, dim_it);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_iter_here(ctx_, dim_it, &dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_get_name(ctx_, dim, &dim_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(dim_name, Catch::Equals(DIM1_NAME));

  // Check dump
  std::string dump_str =
      "- Array name: " + tiledb::URI(ARRAY_PATH).to_string() + "\n" +
      "- Array type: " + ARRAY_TYPE_STR + "\n" +
      "- Cell order: " + CELL_ORDER_STR + "\n" +
      "- Tile order: " + TILE_ORDER_STR + "\n" + "- Capacity: " + CAPACITY_STR +
      "\n"
      "- Coordinates compressor: DOUBLE_DELTA\n" +
      "- Coordinates compression level: -1\n\n" +
      "=== Domain ===\n"
      "- Dimensions type: " +
      DIM_TYPE_STR + "\n\n" + "### Dimension ###\n" + "- Name: " + DIM1_NAME +
      "\n" + "- Domain: " + DIM1_DOMAIN_STR + "\n" +
      "- Tile extent: " + DIM1_TILE_EXTENT_STR + "\n" + "\n" +
      "### Dimension ###\n" + "- Name: " + DIM2_NAME + "\n" +
      "- Domain: " + DIM2_DOMAIN_STR + "\n" +
      "- Tile extent: " + DIM2_TILE_EXTENT_STR + "\n" + "\n" +
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
  tiledb_array_metadata_dump(ctx_, array_metadata, fout);
  fclose(fout);
  CHECK(!system("diff gold_fout.txt fout.txt"));
  CHECK(!system("rm gold_fout.txt fout.txt"));

  // Clean up
  rc = tiledb_attribute_iter_free(ctx_, attr_it);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_iter_free(ctx_, dim_it);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_free(ctx_, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_free(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_OK);
}
