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

#ifdef HAVE_S3
#include "s3.h"
#endif

struct ArrayMetadataFx {
// Filesystem related
#ifdef HAVE_HDFS
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
#endif
#ifdef HAVE_S3
  tiledb::S3 s3_;
  const char* S3_BUCKET = "tiledb";
  const std::string S3_TEMP_DIR = "s3://tiledb/tiledb_test/";
#endif
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::posix::current_dir() + "/tiledb_test/";

  // Constant parameters
  const std::string ARRAY_NAME = "dense_test_100x100_10x10";
  tiledb_array_type_t ARRAY_TYPE = TILEDB_DENSE;
  const char* ARRAY_TYPE_STR = "dense";
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

  // TileDB context
  tiledb_ctx_t* ctx_;

  // Functions
  ArrayMetadataFx();
  ~ArrayMetadataFx();
  void remove_temp_dir();
  void create_array(const std::string& path);
  void create_temp_dir();
  void delete_array(const std::string& path);
  bool is_array(const std::string& path);
  void load_and_check_array_metadata(const std::string& path);
};

ArrayMetadataFx::ArrayMetadataFx() {
  // Create TileDB context
  tiledb_config_t* config = nullptr;
  REQUIRE(tiledb_config_create(&config) == TILEDB_OK);
#ifdef HAVE_S3
  REQUIRE(
      tiledb_config_set(
          config, "tiledb.s3.endpoint_override", "localhost:9999") ==
      TILEDB_OK);
#endif
  REQUIRE(tiledb_ctx_create(&ctx_, config) == TILEDB_OK);
  REQUIRE(tiledb_config_free(config) == TILEDB_OK);

  // Connect to S3
#ifdef HAVE_S3
  // TODO: use tiledb_vfs_t instead of S3::*
  tiledb::S3::S3Config s3_config;
  s3_config.endpoint_override_ = "localhost:9999";
  REQUIRE(s3_.connect(s3_config).ok());

  // Create bucket if it does not exist
  if (!s3_.bucket_exists(S3_BUCKET))
    REQUIRE(s3_.create_bucket(S3_BUCKET).ok());
#endif
}

ArrayMetadataFx::~ArrayMetadataFx() {
  CHECK(tiledb_ctx_free(ctx_) == TILEDB_OK);
}

void ArrayMetadataFx::create_temp_dir() {
  remove_temp_dir();

#ifdef HAVE_S3
  REQUIRE(s3_.create_dir(tiledb::URI(S3_TEMP_DIR)).ok());
#endif
#ifdef HAVE_HDFS
  auto cmd_hdfs = std::string("hadoop fs -mkdir -p ") + HDFS_TEMP_DIR;
  REQUIRE(system(cmd_hdfs.c_str()) == 0);
#endif
  auto cmd_posix = std::string("mkdir -p ") + FILE_TEMP_DIR;
  REQUIRE(system(cmd_posix.c_str()) == 0);
}

void ArrayMetadataFx::remove_temp_dir() {
// Delete temporary directory
#ifdef HAVE_S3
  REQUIRE(s3_.remove_path(tiledb::URI(S3_TEMP_DIR)).ok());
#endif
#ifdef HAVE_HDFS
  auto cmd_hdfs = std::string("hadoop fs -rm -r -f ") + HDFS_TEMP_DIR;
  REQUIRE(system(cmd_hdfs.c_str()) == 0);
#endif
  auto cmd_posix = std::string("rm -rf ") + FILE_TEMP_DIR;
  REQUIRE(system(cmd_posix.c_str()) == 0);
}

bool ArrayMetadataFx::is_array(const std::string& path) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, path.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY;
}

void ArrayMetadataFx::delete_array(const std::string& path) {
  if (!is_array(path))
    return;

  CHECK(tiledb_delete(ctx_, path.c_str()) == TILEDB_OK);
}

void ArrayMetadataFx::create_array(const std::string& path) {
  // Create array metadata with invalid URI
  tiledb_array_metadata_t* array_metadata;
  int rc = tiledb_array_metadata_create(ctx_, &array_metadata, "file://array");
  REQUIRE(rc == TILEDB_ERR);

  // Create array metadata
  rc = tiledb_array_metadata_create(ctx_, &array_metadata, path.c_str());
  REQUIRE(rc == TILEDB_OK);

  // Set metadata members
  rc = tiledb_array_metadata_set_array_type(ctx_, array_metadata, ARRAY_TYPE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_capacity(ctx_, array_metadata, CAPACITY);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_cell_order(ctx_, array_metadata, CELL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_tile_order(ctx_, array_metadata, TILE_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Check for invalid array metadata
  rc = tiledb_array_metadata_check(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_array_create(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_ERR);

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
  rc = tiledb_array_metadata_set_domain(ctx_, array_metadata, domain);
  REQUIRE(rc == TILEDB_OK);

  // Check for invalid array metadata
  rc = tiledb_array_metadata_check(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_array_create(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_ERR);

  // Set attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_create(ctx_, &attr, ATTR_NAME, ATTR_TYPE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, attr);
  REQUIRE(rc == TILEDB_OK);

  // Create the array
  rc = tiledb_array_create(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_metadata_free(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_free(ctx_, attr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, d2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_free(ctx_, domain);
  REQUIRE(rc == TILEDB_OK);
}

void ArrayMetadataFx::load_and_check_array_metadata(const std::string& path) {
  // Load array_metadata metadata from the disk
  tiledb_array_metadata_t* array_metadata;
  int rc = tiledb_array_metadata_load(ctx_, &array_metadata, path.c_str());
  REQUIRE(rc == TILEDB_OK);

  // Check name
  const char* name;
  rc = tiledb_array_metadata_get_array_name(ctx_, array_metadata, &name);
  REQUIRE(rc == TILEDB_OK);
  auto real_path = tiledb::URI(path).to_string();
  CHECK_THAT(name, Catch::Equals(real_path));

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
  CHECK(coords_compression == TILEDB_BLOSC_ZSTD);
  CHECK(coords_compression_level == -1);

  // Check attribute

  // get first attribute by index
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_from_index(ctx_, array_metadata, 0, &attr);
  REQUIRE(rc == TILEDB_OK);

  const char* attr_name;
  rc = tiledb_attribute_get_name(ctx_, attr, &attr_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(attr_name, Catch::Equals(ATTR_NAME));
  tiledb_attribute_free(ctx_, attr);

  // get first attribute by name
  rc = tiledb_attribute_from_name(ctx_, array_metadata, ATTR_NAME, &attr);
  REQUIRE(rc == TILEDB_OK);
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

  unsigned int num_attributes = 0;
  rc = tiledb_array_metadata_get_num_attributes(
      ctx_, array_metadata, &num_attributes);
  REQUIRE(rc == TILEDB_OK);
  CHECK(num_attributes == 1);

  // Get domain
  tiledb_domain_t* domain;
  rc = tiledb_array_metadata_get_domain(ctx_, array_metadata, &domain);
  REQUIRE(rc == TILEDB_OK);

  // Check first dimension
  // get first dimension by name
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_from_name(ctx_, domain, DIM1_NAME, &dim);
  REQUIRE(rc == TILEDB_OK);

  const char* dim_name;
  rc = tiledb_dimension_get_name(ctx_, dim, &dim_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(dim_name, Catch::Equals(DIM1_NAME));

  rc = tiledb_dimension_free(ctx_, dim);
  REQUIRE(rc == TILEDB_OK);

  // get first dimension by index
  rc = tiledb_dimension_from_index(ctx_, domain, 0, &dim);
  REQUIRE(rc == TILEDB_OK);
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

  rc = tiledb_dimension_free(ctx_, dim);
  REQUIRE(rc == TILEDB_OK);

  // Check second dimension
  // get second dimension by name
  rc = tiledb_dimension_from_name(ctx_, domain, DIM2_NAME, &dim);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_dimension_get_name(ctx_, dim, &dim_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(dim_name, Catch::Equals(DIM2_NAME));

  rc = tiledb_dimension_free(ctx_, dim);
  REQUIRE(rc == TILEDB_OK);

  // get from index
  rc = tiledb_dimension_from_index(ctx_, domain, 1, &dim);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_dimension_get_name(ctx_, dim, &dim_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(dim_name, Catch::Equals(DIM2_NAME));

  rc = tiledb_dimension_get_domain(ctx_, dim, &dim_domain);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(dim_domain, &DIM_DOMAIN[2], DIM_DOMAIN_SIZE));

  rc = tiledb_dimension_get_tile_extent(ctx_, dim, &tile_extent);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(tile_extent, &TILE_EXTENTS[1], TILE_EXTENT_SIZE));

  // check that indexing > 1 returns an error for this domain
  rc = tiledb_dimension_from_index(ctx_, domain, 2, &dim);
  CHECK(rc != TILEDB_OK);

  // check that the rank of the domain is 2
  unsigned int rank = 0;
  rc = tiledb_domain_get_rank(ctx_, domain, &rank);
  REQUIRE(rc == TILEDB_OK);
  CHECK(rank == 2);

  // Check dump
  std::string dump_str =
      "- Array name: " + real_path + "\n" + "- Array type: " + ARRAY_TYPE_STR +
      "\n" + "- Cell order: " + CELL_ORDER_STR + "\n" +
      "- Tile order: " + TILE_ORDER_STR + "\n" + "- Capacity: " + CAPACITY_STR +
      "\n"
      "- Coordinates compressor: BLOSC_ZSTD\n" +
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
  rc = tiledb_attribute_free(ctx_, attr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_free(ctx_, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_free(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_OK);
}

TEST_CASE_METHOD(
    ArrayMetadataFx,
    "C API: Test array metadata creation and retrieval",
    "[capi], [array metadata]") {
  create_temp_dir();

  std::string array_name;

  // Posix
  array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY_NAME;
  create_array(array_name);
  load_and_check_array_metadata(array_name);
  delete_array(array_name);

#ifdef HAVE_S3
  // S3
  array_name = S3_TEMP_DIR + ARRAY_NAME;
  create_array(array_name);
  load_and_check_array_metadata(array_name);
  delete_array(array_name);
#endif

#ifdef HAVE_HDFS
  // HDFS
  array_name = HDFS_TEMP_DIR + ARRAY_NAME;
  create_array(array_name);
  load_and_check_array_metadata(array_name);
  delete_array(array_name);
#endif

  remove_temp_dir();
}

TEST_CASE_METHOD(
    ArrayMetadataFx,
    "C API: Test array metadata one anonymous dimension",
    "[capi], [array metadata]") {
  // Create dimensions
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_create(
      ctx_, &d1, "", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0]);
  REQUIRE(rc == TILEDB_OK);

  tiledb_dimension_t* d2;
  rc = tiledb_dimension_create(
      ctx_, &d2, "d2", TILEDB_INT64, &DIM_DOMAIN[2], &TILE_EXTENTS[1]);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_create(ctx_, &domain, DIM_TYPE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);

  tiledb_dimension_t* get_dim = nullptr;
  rc = tiledb_dimension_from_name(ctx_, domain, "", &get_dim);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, get_dim);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_dimension_from_name(ctx_, domain, "d2", &get_dim);
  const char* get_name = nullptr;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_dimension_get_name(ctx_, get_dim, &get_name);
  CHECK(rc == TILEDB_OK);
  CHECK_THAT(get_name, Catch::Equals("d2"));
  tiledb_dimension_free(ctx_, get_dim);

  // Clean up
  rc = tiledb_dimension_free(ctx_, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, d2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_free(ctx_, domain);
  CHECK(rc == TILEDB_OK);
}

TEST_CASE_METHOD(
    ArrayMetadataFx,
    "C API: Test array metadata multiple anonymous dimensions",
    "[capi], [array metadata]") {
  // Create dimensions
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_create(
      ctx_, &d1, "", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0]);
  REQUIRE(rc == TILEDB_OK);

  tiledb_dimension_t* d2;
  rc = tiledb_dimension_create(
      ctx_, &d2, "", TILEDB_INT64, &DIM_DOMAIN[2], &TILE_EXTENTS[1]);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_create(ctx_, &domain, DIM_TYPE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);

  tiledb_dimension_t* get_dim = nullptr;
  rc = tiledb_dimension_from_name(ctx_, domain, "", &get_dim);
  // getting multiple anonymous dimension by name is an error
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_dimension_from_index(ctx_, domain, 0, &get_dim);
  CHECK(rc == TILEDB_OK);
  CHECK(get_dim != nullptr);
  rc = tiledb_dimension_free(ctx_, get_dim);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_dimension_free(ctx_, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, d2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_free(ctx_, domain);
  CHECK(rc == TILEDB_OK);
}

TEST_CASE_METHOD(
    ArrayMetadataFx,
    "C API: Test array metadata one anonymous attribute",
    "[capi], [array metadata]") {
  // Create array metadata
  tiledb_array_metadata_t* array_metadata;
  int rc = tiledb_array_metadata_create(ctx_, &array_metadata, "my_meta");
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_create(
      ctx_, &d1, "", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0]);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_create(ctx_, &domain, DIM_TYPE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_domain(ctx_, array_metadata, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* attr1;
  rc = tiledb_attribute_create(ctx_, &attr1, "", ATTR_TYPE);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* attr2;
  rc = tiledb_attribute_create(ctx_, &attr2, "foo", ATTR_TYPE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, attr2);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_t* get_attr = nullptr;
  rc = tiledb_attribute_from_name(ctx_, array_metadata, "", &get_attr);
  // from name when there are multiple anon attributes is an error
  CHECK(rc == TILEDB_OK);
  CHECK(get_attr != nullptr);
  rc = tiledb_attribute_free(ctx_, get_attr);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_attribute_from_name(ctx_, array_metadata, "foo", &get_attr);
  CHECK(rc == TILEDB_OK);
  CHECK(get_attr != nullptr);
  const char* get_name = nullptr;
  rc = tiledb_attribute_get_name(ctx_, get_attr, &get_name);
  CHECK(rc == TILEDB_OK);
  CHECK_THAT(get_name, Catch::Equals("foo"));
  rc = tiledb_attribute_free(ctx_, get_attr);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_attribute_free(ctx_, attr1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_free(ctx_, attr2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_free(ctx_, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_metadata_free(ctx_, array_metadata);
  CHECK(rc == TILEDB_OK);
}

TEST_CASE_METHOD(
    ArrayMetadataFx,
    "C API: Test array metadata multiple anonymous attributes",
    "[capi], [array metadata]") {
  // Create array metadata
  tiledb_array_metadata_t* array_metadata;
  int rc = tiledb_array_metadata_create(ctx_, &array_metadata, "my_meta");
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_create(
      ctx_, &d1, "", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0]);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_create(ctx_, &domain, DIM_TYPE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_domain(ctx_, array_metadata, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* attr1;
  rc = tiledb_attribute_create(ctx_, &attr1, "", ATTR_TYPE);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* attr2;
  rc = tiledb_attribute_create(ctx_, &attr2, "", ATTR_TYPE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, attr2);
  CHECK(rc != TILEDB_OK);

  tiledb_attribute_t* get_attr = nullptr;
  rc = tiledb_attribute_from_name(ctx_, array_metadata, "", &get_attr);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_attribute_from_index(ctx_, array_metadata, 0, &get_attr);
  CHECK(rc == TILEDB_OK);
  CHECK(get_attr != nullptr);
  rc = tiledb_attribute_free(ctx_, get_attr);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_attribute_free(ctx_, attr1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_free(ctx_, attr2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_free(ctx_, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_metadata_free(ctx_, array_metadata);
  CHECK(rc == TILEDB_OK);
}
