/**
 * @file unit-capi-json.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * Tests for the C API tiledb_array_schema_to_json and
 * tiledb_array_schema_from_json.
 *
 */

#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>

#include "catch.hpp"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

struct ArraySchemaJson {
// Filesystem related
#ifdef _WIN32
  const std::string FILE_URI_PREFIX = "";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif

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

  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;

  // Functions
  ArraySchemaJson();
  ~ArraySchemaJson();
  void load_and_check_array_schema(const std::string& path);
  void set_supported_fs();
  tiledb_array_schema_t* create_array_schema();
};

ArraySchemaJson::ArraySchemaJson() {
  // Supported filesystems
  set_supported_fs();

  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  ctx_ = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);
}

ArraySchemaJson::~ArraySchemaJson() {
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void ArraySchemaJson::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  int is_supported = 0;
  int rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_S3, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_s3_ = (bool)is_supported;
  rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_HDFS, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_hdfs_ = (bool)is_supported;

  tiledb_ctx_free(&ctx);
}

tiledb_array_schema_t* ArraySchemaJson::create_array_schema() {
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, "", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0], &d1);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* attr1;
  rc = tiledb_attribute_alloc(ctx_, "", ATTR_TYPE, &attr1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* attr2;
  rc = tiledb_attribute_alloc(ctx_, "a1", ATTR_TYPE, &attr2);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr2);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_free(&attr1);
  tiledb_attribute_free(&attr2);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);

  return array_schema;
}

TEST_CASE_METHOD(
    ArraySchemaJson,
    "C API: Test array schema json serialization",
    "[capi], [array-schema]") {
  // Create array schema
  tiledb_array_schema_t* array_schema = create_array_schema();

  char* json_string;
  int rc = tiledb_array_schema_to_json(ctx_, array_schema, &json_string);
  REQUIRE(rc == TILEDB_OK);

  CHECK_THAT(
      json_string,
      Catch::Equals("{\"array_type\":\"dense\",\"attributes\":[{\"cell_val_"
                    "num\":1,\"compressor\":"
                    "\"NO_COMPRESSION\",\"compressor_level\":-1,\"name\":\"__"
                    "attr\",\"type\":\"INT32\"},"
                    "{\"cell_val_num\":1,\"compressor\":\"NO_COMPRESSION\","
                    "\"compressor_level\":-1,\"name\":"
                    "\"a1\",\"type\":\"INT32\"}],\"capacity\":10000,\"cell_"
                    "order\":\"row-major\","
                    "\"coords_compression\":\"ZSTD\",\"coords_"
                    "compression_level\":-1,\"domain\":"
                    "{\"cell_order\":\"row-major\",\"dimensions\":[{\"domain\":"
                    "[0,99],\"name\":\"__dim_0\","
                    "\"null_tile_extent\":false,\"tile_extent\":10,\"tile_"
                    "extent_type\":\"INT64\",\"type\":"
                    "\"INT64\"}],\"tile_order\":\"row-major\",\"type\":"
                    "\"INT64\"},\"offset_compression\":"
                    "\"ZSTD\",\"offset_compression_level\":-1,\"tile_"
                    "order\":\"row-major\",\"uri\":\"\","
                    "\"version\":[1,3,0]}"));

  tiledb_array_schema_free(&array_schema);

  rc = tiledb_array_schema_from_json(ctx_, &array_schema, json_string);
  REQUIRE(rc == TILEDB_OK);
  delete[] json_string;

  tiledb_attribute_t* attr2_check;
  rc = tiledb_array_schema_get_attribute_from_name(
      ctx_, array_schema, "a1", &attr2_check);
  REQUIRE(rc == TILEDB_OK);
  CHECK(attr2_check != nullptr);

  std::string malformedJson =
      "{\"array_type\":\"dense\",\"attributes\":[{\"cell_val_"
      "num\":1,\"compressor\":"
      "\"NO_COMPRESSION\",\"compressor_level\":-1,\"name\":\"__"
      "attr\",\"type\":\"INT32\"},"
      "{\"cell_val_num\":1,\"compressor\":\"NO_COMPRESSION\","
      "\"compressor_level\":-1,\"name\":"
      "\"a1\",\"type\":\"INT32\"}],\"capacity\":10000,\"cell_"
      "order\":\"row-major\","
      "\"coords_compression\":\"ZSTD\",\"coords_"
      "compression_level\":-1,\"domain\":"
      "{\"cell_order\":\"row-major\",\"dimensions\":[{\"domain\":"
      "[0,99],\"name\":\"__dim_0\","
      "\"null_tile_extent\":false,\"tile_extent\":10,\"tile_"
      "extent_type\":\"INT64\",\"type\":"
      "\"INT64\"}],\"tile_order\":\"row-major\",\"type\":"
      "\"INT64\"},\"offset_compression\":"
      "\"ZSTD\",\"offset_compression_level\":-1,\"tile_"
      "order\":\"row-major\",\"version\":[1,3,0]}";

  tiledb_array_schema_free(&array_schema);
  rc =
      tiledb_array_schema_from_json(ctx_, &array_schema, malformedJson.c_str());
  REQUIRE(rc == TILEDB_ERR);

  // Clean up
  tiledb_attribute_free(&attr2_check);
}
