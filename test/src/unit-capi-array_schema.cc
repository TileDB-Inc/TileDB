/**
 * @file unit-capi-array_schema.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * Tests for the C API tiledb_array_schema_t spec, along with
 * tiledb_attribute_iter_t and tiledb_dimension_iter_t.
 */

#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>

#include "catch.hpp"
#include "test/src/helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb::test;

struct ArraySchemaFx {
  // Filesystem related
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
  const std::string S3_PREFIX = "s3://";
  const std::string S3_BUCKET = S3_PREFIX + random_name("tiledb") + "/";
  const std::string S3_TEMP_DIR = S3_BUCKET + "tiledb_test/";
  const std::string AZURE_PREFIX = "azure://";
  const std::string bucket = AZURE_PREFIX + random_name("tiledb") + "/";
  const std::string AZURE_TEMP_DIR = bucket + "tiledb_test/";
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
  const char* ATTR_COMPRESSOR_STR = "NO_COMPRESSION";
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
  const uint32_t FILL_VALUE = 10;
  const char* FILL_VALUE_STR = "10";
  const uint32_t FILL_VALUE_SIZE = sizeof(uint32_t);
  const int64_t TILE_EXTENTS[2] = {10, 5};
  const char* DIM1_TILE_EXTENT_STR = "10";
  const char* DIM2_TILE_EXTENT_STR = "5";
  const uint64_t TILE_EXTENT_SIZE = sizeof(TILE_EXTENTS) / DIM_NUM;

  /**
   * If true, array schema is serialized before submission, to test the
   * serialization paths.
   */
  bool serialize_array_schema_ = false;

  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;
  bool supports_azure_;

  // Functions
  ArraySchemaFx();
  ~ArraySchemaFx();
  void remove_temp_dir(const std::string& path);
  void create_array(const std::string& path);
  void create_temp_dir(const std::string& path);
  void delete_array(const std::string& path);
  bool is_array(const std::string& path);
  void load_and_check_array_schema(const std::string& path);
  static std::string random_name(const std::string& prefix);
  void set_supported_fs();

  int array_create_wrapper(
      const std::string& path, tiledb_array_schema_t* array_schema);
  int array_schema_load_wrapper(
      const std::string& path, tiledb_array_schema_t** array_schema);
  int array_get_schema_wrapper(
      tiledb_array_t* array, tiledb_array_schema_t** array_schema);
  int array_schema_get_domain_wrapper(
      tiledb_array_schema_t* array_schema, tiledb_domain_t** domain);
  int tiledb_array_get_non_empty_domain_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      void* domain,
      int32_t* is_empty);
  int tiledb_array_get_non_empty_domain_from_index_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      uint32_t index,
      void* domain,
      int32_t* is_empty);
  int tiledb_array_get_non_empty_domain_from_name_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      const char* name,
      void* domain,
      int32_t* is_empty);
};

ArraySchemaFx::ArraySchemaFx() {
  // Supported filesystems
  set_supported_fs();

  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  if (supports_s3_) {
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.endpoint_override", "localhost:9999", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.s3.scheme", "https", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.use_virtual_addressing", "false", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.s3.verify_ssl", "false", &error) ==
        TILEDB_OK);
    REQUIRE(error == nullptr);
#endif
  }
  if (supports_azure_) {
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.storage_account_name",
            "devstoreaccount1",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.storage_account_key",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
            "K1SZFPTOtr/KBHBeksoGMGw==",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.blob_endpoint",
            "127.0.0.1:10000/devstoreaccount1",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.azure.use_https", "false", &error) ==
        TILEDB_OK);
  }

  ctx_ = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);

  // Connect to S3
  if (supports_s3_) {
    // Create bucket if it does not exist
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    REQUIRE(rc == TILEDB_OK);
    if (!is_bucket) {
      rc = tiledb_vfs_create_bucket(ctx_, vfs_, S3_BUCKET.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }

  // Connect to Azure
  if (supports_azure_) {
    int is_container = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, bucket.c_str(), &is_container);
    REQUIRE(rc == TILEDB_OK);
    if (!is_container) {
      rc = tiledb_vfs_create_bucket(ctx_, vfs_, bucket.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }
}

ArraySchemaFx::~ArraySchemaFx() {
  if (supports_s3_) {
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    CHECK(rc == TILEDB_OK);
    if (is_bucket) {
      CHECK(
          tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str()) == TILEDB_OK);
    }
  }

  if (supports_azure_) {
    int is_container = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, bucket.c_str(), &is_container);
    REQUIRE(rc == TILEDB_OK);
    if (is_container) {
      rc = tiledb_vfs_remove_bucket(ctx_, vfs_, bucket.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }

  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void ArraySchemaFx::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  get_supported_fs(&supports_s3_, &supports_hdfs_, &supports_azure_);

  tiledb_ctx_free(&ctx);
}

void ArraySchemaFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void ArraySchemaFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

bool ArraySchemaFx::is_array(const std::string& path) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, path.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY;
}

void ArraySchemaFx::delete_array(const std::string& path) {
  if (!is_array(path))
    return;

  CHECK(tiledb_object_remove(ctx_, path.c_str()) == TILEDB_OK);
}

int ArraySchemaFx::tiledb_array_get_non_empty_domain_wrapper(
    tiledb_ctx_t* ctx, tiledb_array_t* array, void* domain, int32_t* is_empty) {
  int ret = tiledb_array_get_non_empty_domain(ctx, array, domain, is_empty);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_array_schema_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_nonempty_domain(
          ctx,
          array,
          domain,
          *is_empty,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_array_nonempty_domain(
          ctx,
          array,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &domain,
          is_empty) == TILEDB_OK);

  tiledb_buffer_free(&buff);
  return TILEDB_OK;
}

int ArraySchemaFx::tiledb_array_get_non_empty_domain_from_index_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t index,
    void* domain,
    int32_t* is_empty) {
  int ret = tiledb_array_get_non_empty_domain_from_index(
      ctx, array, index, domain, is_empty);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_array_schema_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1) == TILEDB_OK);

  tiledb_buffer_free(&buff);
  return tiledb_array_get_non_empty_domain_from_index(
      ctx, array, index, domain, is_empty);
}

int ArraySchemaFx::tiledb_array_get_non_empty_domain_from_name_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* domain,
    int32_t* is_empty) {
  int ret = tiledb_array_get_non_empty_domain_from_name(
      ctx, array, name, domain, is_empty);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_array_schema_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1) == TILEDB_OK);

  tiledb_buffer_free(&buff);
  return tiledb_array_get_non_empty_domain_from_name(
      ctx, array, name, domain, is_empty);
}

int ArraySchemaFx::array_create_wrapper(
    const std::string& path, tiledb_array_schema_t* array_schema) {
#ifndef TILEDB_SERIALIZATION
  return tiledb_array_create(ctx_, path.c_str(), array_schema);
#endif

  if (!serialize_array_schema_) {
    return tiledb_array_create(ctx_, path.c_str(), array_schema);
  }

  // Serialize the array
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &buff) == TILEDB_OK);

  // Load array schema from the rest server
  tiledb_array_schema_t* new_array_schema = nullptr;
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &new_array_schema) == TILEDB_OK);

  // Create array from new schema
  int rc = tiledb_array_create(ctx_, path.c_str(), new_array_schema);

  // Serialize the new array schema and deserialize into the original array
  // schema.
  tiledb_buffer_t* buff2;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          new_array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff2) == TILEDB_OK);
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff2,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &array_schema) == TILEDB_OK);

  // Clean up.
  tiledb_array_schema_free(&new_array_schema);
  tiledb_buffer_free(&buff);
  tiledb_buffer_free(&buff2);

  return rc;
}

int ArraySchemaFx::array_schema_load_wrapper(
    const std::string& path, tiledb_array_schema_t** array_schema) {
#ifndef TILEDB_SERIALIZATION
  return tiledb_array_schema_load(ctx_, path.c_str(), array_schema);
#endif

  if (!serialize_array_schema_) {
    return tiledb_array_schema_load(ctx_, path.c_str(), array_schema);
  }

  // Load array.
  int rc = tiledb_array_schema_load(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Serialize the array
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          *array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &buff) == TILEDB_OK);

  // Load array schema from the rest server
  tiledb_array_schema_t* new_array_schema = nullptr;
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &new_array_schema) == TILEDB_OK);

  // Serialize the new array schema and deserialize into the original array
  // schema.
  tiledb_buffer_t* buff2;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          new_array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff2) == TILEDB_OK);
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff2,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          array_schema) == TILEDB_OK);

  // Clean up.
  tiledb_array_schema_free(&new_array_schema);
  tiledb_buffer_free(&buff);
  tiledb_buffer_free(&buff2);

  return rc;
}

int ArraySchemaFx::array_get_schema_wrapper(
    tiledb_array_t* array, tiledb_array_schema_t** array_schema) {
#ifndef TILEDB_SERIALIZATION
  return tiledb_array_get_schema(ctx_, array, array_schema);
#endif

  if (!serialize_array_schema_) {
    return tiledb_array_get_schema(ctx_, array, array_schema);
  }

  int rc = tiledb_array_get_schema(ctx_, array, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Serialize the array
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          *array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &buff) == TILEDB_OK);

  // Load array schema from the rest server
  tiledb_array_schema_t* new_array_schema = nullptr;
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &new_array_schema) == TILEDB_OK);

  // Serialize the new array schema and deserialize into the original array
  // schema.
  tiledb_buffer_t* buff2;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          new_array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff2) == TILEDB_OK);
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff2,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          array_schema) == TILEDB_OK);

  // Clean up.
  tiledb_array_schema_free(&new_array_schema);
  tiledb_buffer_free(&buff);
  tiledb_buffer_free(&buff2);

  return rc;
}

int ArraySchemaFx::array_schema_get_domain_wrapper(
    tiledb_array_schema_t* array_schema, tiledb_domain_t** domain) {
#ifndef TILEDB_SERIALIZATION
  return tiledb_array_schema_get_domain(ctx_, array_schema, domain);
#endif

  if (!serialize_array_schema_) {
    return tiledb_array_schema_get_domain(ctx_, array_schema, domain);
  }

  // Serialize the array
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &buff) == TILEDB_OK);

  // Load array schema from the rest server
  tiledb_array_schema_t* new_array_schema = nullptr;
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &new_array_schema) == TILEDB_OK);

  // Get domain from new array.
  int rc = tiledb_array_schema_get_domain(ctx_, new_array_schema, domain);

  // Serialize the new array schema and deserialize into the original array
  // schema.
  tiledb_buffer_t* buff2;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          new_array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff2) == TILEDB_OK);
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff2,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &array_schema) == TILEDB_OK);

  // Clean up.
  tiledb_array_schema_free(&new_array_schema);
  tiledb_buffer_free(&buff);
  tiledb_buffer_free(&buff2);

  return rc;
}

void ArraySchemaFx::create_array(const std::string& path) {
  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, ARRAY_TYPE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Set schema members
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, CAPACITY);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, CELL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILE_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Check for invalid array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_ERR);

  // Create dimensions
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, DIM1_NAME, TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0], &d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, DIM2_NAME, TILEDB_INT64, &DIM_DOMAIN[2], &TILE_EXTENTS[1], &d2);
  REQUIRE(rc == TILEDB_OK);
  int dim_domain_int[] = {0, 10};
  tiledb_dimension_t* d3;  // This will be an invalid dimension
  int tile_extent = 10000;
  rc = tiledb_dimension_alloc(  // This will not even be created
      ctx_,
      DIM2_NAME,
      TILEDB_INT32,
      dim_domain_int,
      &tile_extent,
      &d3);
  REQUIRE(rc == TILEDB_ERR);

  // Set up filters
  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx_, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);
  int level = 5;
  rc = tiledb_filter_set_option(ctx_, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx_, &filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_free(&filter);
  rc = tiledb_filter_alloc(ctx_, TILEDB_FILTER_BIT_WIDTH_REDUCTION, &filter);
  REQUIRE(rc == TILEDB_OK);
  int window = 1000;
  rc = tiledb_filter_set_option(
      ctx_, filter, TILEDB_BIT_WIDTH_MAX_WINDOW, &window);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);

  // Add filters to dimension
  rc = tiledb_dimension_set_filter_list(ctx_, d2, filter_list);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_datatype_t domain_type;
  rc = tiledb_domain_get_type(ctx_, domain, &domain_type);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(domain_type == TILEDB_INT64);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Check for invalid array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_ERR);

  // Set invalid attribute
  tiledb_attribute_t* inv_attr;
  rc = tiledb_attribute_alloc(ctx_, "__foo", ATTR_TYPE, &inv_attr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, inv_attr);
  REQUIRE(rc == TILEDB_ERR);
  tiledb_attribute_free(&inv_attr);

  // Set attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_, ATTR_NAME, ATTR_TYPE, &attr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_filter_list(ctx_, attr, filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_attribute_set_fill_value(ctx_, attr, &FILL_VALUE, FILL_VALUE_SIZE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr);
  REQUIRE(rc == TILEDB_OK);

  // Create array with invalid URI
  rc = array_create_wrapper("file://array", array_schema);
  REQUIRE(rc == TILEDB_ERR);

  // Create correct array
  rc = array_create_wrapper(path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create the array again - should fail
  rc = array_create_wrapper(path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_ERR);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_array_schema_free(&array_schema);
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
}

void ArraySchemaFx::load_and_check_array_schema(const std::string& path) {
  // Load array schema from the disk
  tiledb_array_schema_t* array_schema = nullptr;
  int rc = array_schema_load_wrapper(path.c_str(), &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Check capacity
  uint64_t capacity;
  rc = tiledb_array_schema_get_capacity(ctx_, array_schema, &capacity);
  REQUIRE(rc == TILEDB_OK);
  CHECK(capacity == CAPACITY);

  // Check cell order
  tiledb_layout_t cell_order;
  rc = tiledb_array_schema_get_cell_order(ctx_, array_schema, &cell_order);
  REQUIRE(rc == TILEDB_OK);
  CHECK(cell_order == CELL_ORDER);

  // Check tile order
  tiledb_layout_t tile_order;
  rc = tiledb_array_schema_get_tile_order(ctx_, array_schema, &tile_order);
  REQUIRE(rc == TILEDB_OK);
  CHECK(tile_order == TILE_ORDER);

  // Check array_schema type
  tiledb_array_type_t type;
  rc = tiledb_array_schema_get_array_type(ctx_, array_schema, &type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(type == TILEDB_DENSE);

  // Check coordinates compression
  tiledb_filter_list_t* coords_filters;
  rc = tiledb_array_schema_get_coords_filter_list(
      ctx_, array_schema, &coords_filters);
  REQUIRE(rc == TILEDB_OK);
  uint32_t nfilters;
  rc = tiledb_filter_list_get_nfilters(ctx_, coords_filters, &nfilters);
  CHECK(nfilters == 1);
  tiledb_filter_t* coords_filter;
  rc = tiledb_filter_list_get_filter_from_index(
      ctx_, coords_filters, 0, &coords_filter);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_type_t coords_compression;
  int32_t coords_compression_level;
  rc = tiledb_filter_get_type(ctx_, coords_filter, &coords_compression);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_get_option(
      ctx_, coords_filter, TILEDB_COMPRESSION_LEVEL, &coords_compression_level);
  REQUIRE(rc == TILEDB_OK);
  CHECK(coords_compression == TILEDB_FILTER_ZSTD);
  CHECK(coords_compression_level == -1);
  tiledb_filter_free(&coords_filter);
  tiledb_filter_list_free(&coords_filters);

  // Check attribute
  tiledb_attribute_t* attr;

  // check that getting an attribute fails when index is out of bounds
  rc = tiledb_array_schema_get_attribute_from_index(
      ctx_, array_schema, 1, &attr);
  REQUIRE(rc == TILEDB_ERR);

  // get first attribute by index
  rc = tiledb_array_schema_get_attribute_from_index(
      ctx_, array_schema, 0, &attr);
  REQUIRE(rc == TILEDB_OK);

  const char* attr_name;
  rc = tiledb_attribute_get_name(ctx_, attr, &attr_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(attr_name, Catch::Equals(ATTR_NAME));
  tiledb_attribute_free(&attr);

  // get first attribute by name
  rc = tiledb_array_schema_get_attribute_from_name(
      ctx_, array_schema, ATTR_NAME, &attr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_get_name(ctx_, attr, &attr_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(attr_name, Catch::Equals(ATTR_NAME));

  tiledb_datatype_t attr_type;
  rc = tiledb_attribute_get_type(ctx_, attr, &attr_type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(attr_type == ATTR_TYPE);

  tiledb_filter_list_t* attr_filters;
  rc = tiledb_attribute_get_filter_list(ctx_, attr, &attr_filters);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_get_nfilters(ctx_, attr_filters, &nfilters);
  CHECK(nfilters == 2);
  tiledb_filter_list_free(&attr_filters);

  unsigned int cell_val_num;
  rc = tiledb_attribute_get_cell_val_num(ctx_, attr, &cell_val_num);
  REQUIRE(rc == TILEDB_OK);
  CHECK(cell_val_num == CELL_VAL_NUM);

  const void* fill_value;
  uint64_t fill_value_size;
  rc = tiledb_attribute_get_fill_value(
      ctx_, attr, &fill_value, &fill_value_size);
  REQUIRE(rc == TILEDB_OK);
  CHECK(fill_value_size == FILL_VALUE_SIZE);
  CHECK(!memcmp(fill_value, &FILL_VALUE, FILL_VALUE_SIZE));

  unsigned int num_attributes = 0;
  rc = tiledb_array_schema_get_attribute_num(
      ctx_, array_schema, &num_attributes);
  REQUIRE(rc == TILEDB_OK);
  CHECK(num_attributes == 1);

  // Get domain
  tiledb_domain_t* domain;
  rc = tiledb_array_schema_get_domain(ctx_, array_schema, &domain);
  REQUIRE(rc == TILEDB_OK);

  // Check first dimension
  // get first dimension by name
  tiledb_dimension_t* dim;
  rc = tiledb_domain_get_dimension_from_name(ctx_, domain, DIM1_NAME, &dim);
  REQUIRE(rc == TILEDB_OK);

  const char* dim_name;
  rc = tiledb_dimension_get_name(ctx_, dim, &dim_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(dim_name, Catch::Equals(DIM1_NAME));

  tiledb_dimension_free(&dim);

  // get first dimension by index
  rc = tiledb_domain_get_dimension_from_index(ctx_, domain, 0, &dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_get_name(ctx_, dim, &dim_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(dim_name, Catch::Equals(DIM1_NAME));

  const void* dim_domain;
  rc = tiledb_dimension_get_domain(ctx_, dim, &dim_domain);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(dim_domain, &DIM_DOMAIN[0], DIM_DOMAIN_SIZE));

  const void* tile_extent;
  rc = tiledb_dimension_get_tile_extent(ctx_, dim, &tile_extent);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!memcmp(tile_extent, &TILE_EXTENTS[0], TILE_EXTENT_SIZE));
  tiledb_dimension_free(&dim);

  // Check second dimension
  // get second dimension by name
  rc = tiledb_domain_get_dimension_from_name(ctx_, domain, DIM2_NAME, &dim);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_dimension_get_name(ctx_, dim, &dim_name);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(dim_name, Catch::Equals(DIM2_NAME));
  tiledb_dimension_free(&dim);

  // get from index
  rc = tiledb_domain_get_dimension_from_index(ctx_, domain, 1, &dim);
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
  rc = tiledb_domain_get_dimension_from_index(ctx_, domain, 2, &dim);
  CHECK(rc != TILEDB_OK);

  // check that the ndim of the domain is 2
  unsigned int ndim = 0;
  rc = tiledb_domain_get_ndim(ctx_, domain, &ndim);
  REQUIRE(rc == TILEDB_OK);
  CHECK(ndim == 2);

  // Check dump
  std::string dump_str =
      std::string("- Array type: ") + ARRAY_TYPE_STR + "\n" +
      "- Cell order: " + CELL_ORDER_STR + "\n" +
      "- Tile order: " + TILE_ORDER_STR + "\n" + "- Capacity: " + CAPACITY_STR +
      "\n"
      "- Allows duplicates: " +
      "false\n"
      "- Coordinates filters: 1\n" +
      "  > ZSTD: COMPRESSION_LEVEL=-1\n" + "- Offsets filters: 1\n" +
      "  > ZSTD: COMPRESSION_LEVEL=-1\n\n" + "### Dimension ###\n" +
      "- Name: " + DIM1_NAME + "\n" + "- Type: INT64\n" +
      "- Cell val num: 1\n" + "- Domain: " + DIM1_DOMAIN_STR + "\n" +
      "- Tile extent: " + DIM1_TILE_EXTENT_STR + "\n" + "- Filters: 0\n\n" +
      "### Dimension ###\n" + "- Name: " + DIM2_NAME + "\n" +
      "- Type: INT64\n" + "- Cell val num: 1\n" +
      "- Domain: " + DIM2_DOMAIN_STR + "\n" +
      "- Tile extent: " + DIM2_TILE_EXTENT_STR + "\n" + "- Filters: 2\n" +
      "  > BZIP2: COMPRESSION_LEVEL=5\n" +
      "  > BitWidthReduction: BIT_WIDTH_MAX_WINDOW=1000\n\n" +
      "### Attribute ###\n" + "- Name: " + ATTR_NAME + "\n" +
      "- Type: " + ATTR_TYPE_STR + "\n" + "- Nullable: false\n" +
      "- Cell val num: " + CELL_VAL_NUM_STR + "\n" + "- Filters: 2\n" +
      "  > BZIP2: COMPRESSION_LEVEL=5\n" +
      "  > BitWidthReduction: BIT_WIDTH_MAX_WINDOW=1000\n" +
      "- Fill value: " + FILL_VALUE_STR + "\n";
  FILE* gold_fout = fopen("gold_fout.txt", "w");
  const char* dump = dump_str.c_str();
  fwrite(dump, sizeof(char), strlen(dump), gold_fout);
  fclose(gold_fout);
  FILE* fout = fopen("fout.txt", "w");
  tiledb_array_schema_dump(ctx_, array_schema, fout);
  fclose(fout);
#ifdef _WIN32
  CHECK(!system("FC gold_fout.txt fout.txt > nul"));
#else
  CHECK(!system("diff gold_fout.txt fout.txt"));
#endif
  CHECK(tiledb_vfs_remove_file(ctx_, vfs_, "gold_fout.txt") == TILEDB_OK);
  CHECK(tiledb_vfs_remove_file(ctx_, vfs_, "fout.txt") == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

std::string ArraySchemaFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema creation and retrieval",
    "[capi][array-schema]") {
  SECTION("- No serialization") {
    serialize_array_schema_ = false;
  }
  SECTION("- Serialization") {
    serialize_array_schema_ = true;
  }

  std::string array_name;

  // S3
  if (supports_s3_) {
    array_name = S3_TEMP_DIR + ARRAY_NAME;
    create_temp_dir(S3_TEMP_DIR);
    create_array(array_name);
    load_and_check_array_schema(array_name);
    delete_array(array_name);
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_azure_) {
    // Azure
    array_name = AZURE_TEMP_DIR + ARRAY_NAME;
    create_temp_dir(AZURE_TEMP_DIR);
    create_array(array_name);
    load_and_check_array_schema(array_name);
    delete_array(array_name);
    remove_temp_dir(AZURE_TEMP_DIR);
  } else if (supports_hdfs_) {
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY_NAME;
    create_temp_dir(HDFS_TEMP_DIR);
    create_array(array_name);
    load_and_check_array_schema(array_name);
    delete_array(array_name);
    remove_temp_dir(HDFS_TEMP_DIR);
  } else if (supports_azure_) {
    // Azure
    array_name = AZURE_TEMP_DIR + ARRAY_NAME;
    create_temp_dir(AZURE_TEMP_DIR);
    create_array(array_name);
    load_and_check_array_schema(array_name);
    delete_array(array_name);
    remove_temp_dir(AZURE_TEMP_DIR);
  } else {
    // File
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY_NAME;
    create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
    create_array(array_name);
    load_and_check_array_schema(array_name);
    delete_array(array_name);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema one anonymous dimension",
    "[capi], [array-schema]") {
  // Create dimensions
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0], &d1);
  REQUIRE(rc == TILEDB_OK);

  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_INT64, &DIM_DOMAIN[2], &TILE_EXTENTS[1], &d2);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);

  tiledb_dimension_t* get_dim = nullptr;
  rc = tiledb_domain_get_dimension_from_name(ctx_, domain, "", &get_dim);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_free(&get_dim);

  int32_t has_dim = 0;
  rc = tiledb_domain_has_dimension(ctx_, domain, "d2", &has_dim);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(has_dim == 1);
  has_dim = false;
  rc = tiledb_domain_has_dimension(ctx_, domain, "d3", &has_dim);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(has_dim == 0);

  rc = tiledb_domain_get_dimension_from_name(ctx_, domain, "d2", &get_dim);
  const char* get_name = nullptr;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_dimension_get_name(ctx_, get_dim, &get_name);
  CHECK(rc == TILEDB_OK);
  CHECK_THAT(get_name, Catch::Equals("d2"));
  tiledb_dimension_free(&get_dim);

  // Clean up
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema with invalid float dense domain",
    "[capi], [array-schema]") {
  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  double dim_domain[] = {0, 9};
  double tile_extent = 5;
  rc = tiledb_dimension_alloc(
      ctx_, "", TILEDB_FLOAT64, dim_domain, &tile_extent, &d1);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_ERR);

  // Clean up
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema with invalid dimension domain and tile extent",
    "[capi], [array-schema]") {
  // Domain range exceeds type range - error
  tiledb_dimension_t* d0;
  uint64_t dim_domain[] = {0, UINT64_MAX};
  int rc = tiledb_dimension_alloc(
      ctx_, "d0", TILEDB_UINT64, dim_domain, nullptr, &d0);
  CHECK(rc == TILEDB_ERR);

  // Create dimension with huge range and no tile extent - not ok
  tiledb_dimension_t* d1;
  dim_domain[1] = UINT64_MAX - 1;
  rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, dim_domain, nullptr, &d1);
  CHECK(rc == TILEDB_ERR);

  // Create dimension with huge range and tile extent - error
  tiledb_dimension_t* d2;
  uint64_t tile_extent = 7;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, dim_domain, &tile_extent, &d2);
  CHECK(rc == TILEDB_ERR);

  // Create dimension with tile extent exceeding domain - error
  tiledb_dimension_t* d3;
  dim_domain[1] = 10;
  tile_extent = 20;
  rc = tiledb_dimension_alloc(
      ctx_, "d3", TILEDB_UINT64, dim_domain, &tile_extent, &d3);
  CHECK(rc == TILEDB_ERR);

  // Create dimension with invalid domain - error
  tiledb_dimension_t* d4;
  dim_domain[0] = 10;
  dim_domain[1] = 1;
  rc = tiledb_dimension_alloc(
      ctx_, "d4", TILEDB_UINT64, dim_domain, &tile_extent, &d4);
  CHECK(rc == TILEDB_ERR);

  // Create dimension with 0 tile extent
  tiledb_dimension_t* d5;
  int64_t dim_domain_2[] = {0, 10};
  int64_t tile_extent_2 = 0;
  rc = tiledb_dimension_alloc(
      ctx_, "d5", TILEDB_INT64, dim_domain_2, &tile_extent_2, &d5);
  CHECK(rc == TILEDB_ERR);

  // Create dimension with negative tile extent
  tiledb_dimension_t* d6;
  tile_extent_2 = -1;
  rc = tiledb_dimension_alloc(
      ctx_, "d6", TILEDB_INT64, dim_domain_2, &tile_extent_2, &d6);
  CHECK(rc == TILEDB_ERR);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test NAN and INF in dimensions",
    "[capi][array-schema][array-schema-nan-inf]") {
  // Create dimension with INF
  tiledb_dimension_t* d;
  float dim_domain[] = {0, std::numeric_limits<float>::infinity()};
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_FLOAT32, dim_domain, nullptr, &d);
  CHECK(rc == TILEDB_ERR);

  // Create dimension with NAN
  dim_domain[0] = std::numeric_limits<float>::quiet_NaN();
  rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_FLOAT32, dim_domain, nullptr, &d);
  CHECK(rc == TILEDB_ERR);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema offsets/coords filter lists",
    "[capi], [array-schema], [filter]") {
  SECTION("- No serialization") {
    serialize_array_schema_ = false;
  }
  SECTION("- Serialization") {
    serialize_array_schema_ = true;
  }

  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
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
  rc = tiledb_attribute_alloc(ctx_, "foo", TILEDB_INT32, &attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, attr1, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr1);
  REQUIRE(rc == TILEDB_OK);

  // Set schema members
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, CAPACITY);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, CELL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILE_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Set up filter list
  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx_, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);
  int level = 5;
  rc = tiledb_filter_set_option(ctx_, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx_, &filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);

  // Set schema filters
  rc = tiledb_array_schema_set_coords_filter_list(
      ctx_, array_schema, filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_offsets_filter_list(
      ctx_, array_schema, filter_list);
  REQUIRE(rc == TILEDB_OK);

  // Check for invalid array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY_NAME;
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  rc = array_create_wrapper(array_name, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&attr1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_schema_t* read_schema;
  rc = array_get_schema_wrapper(array, &read_schema);
  REQUIRE(rc == TILEDB_OK);

  // Get filter lists
  tiledb_filter_list_t *coords_flist, *offsets_flist;
  rc = tiledb_array_schema_get_coords_filter_list(
      ctx_, read_schema, &coords_flist);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_get_offsets_filter_list(
      ctx_, read_schema, &offsets_flist);
  REQUIRE(rc == TILEDB_OK);

  unsigned nfilters;
  rc = tiledb_filter_list_get_nfilters(ctx_, coords_flist, &nfilters);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(nfilters == 1);
  rc = tiledb_filter_list_get_nfilters(ctx_, offsets_flist, &nfilters);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(nfilters == 1);

  // Check getting a filter
  tiledb_filter_t* read_filter;
  rc = tiledb_filter_list_get_filter_from_index(
      ctx_, coords_flist, 0, &read_filter);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_type_t type;
  rc = tiledb_filter_get_type(ctx_, read_filter, &type);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(type == TILEDB_FILTER_BZIP2);
  int read_level;
  rc = tiledb_filter_get_option(
      ctx_, read_filter, TILEDB_COMPRESSION_LEVEL, &read_level);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(read_level == level);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_filter_free(&read_filter);
  tiledb_filter_list_free(&coords_flist);
  tiledb_filter_list_free(&offsets_flist);
  tiledb_array_schema_free(&read_schema);
  tiledb_array_free(&array);
  delete_array(array_name);
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema load error condition",
    "[capi], [array-schema]") {
  SECTION("- No serialization") {
    serialize_array_schema_ = false;
  }
  SECTION("- Serialization") {
    serialize_array_schema_ = true;
  }

  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
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
  rc = tiledb_attribute_alloc(ctx_, "foo", TILEDB_INT32, &attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, attr1, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr1);
  REQUIRE(rc == TILEDB_OK);

  // Set schema members
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, CAPACITY);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, CELL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILE_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Check for invalid array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY_NAME;
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  rc = array_create_wrapper(array_name, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&attr1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);

  // Corrupt the array schema
  std::string schema_path = array_name + "/__array_schema.tdb";
  std::string to_write = "garbage";
  tiledb_vfs_fh_t* fh;
  rc = tiledb_vfs_open(ctx_, vfs_, schema_path.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_sync(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);

  // Check for failure opening the array.
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_ERR);

  // Clean up
  tiledb_array_free(&array);
  delete_array(array_name);
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema datetimes",
    "[capi][array-schema][datetime]") {
  SECTION("- No serialization") {
    serialize_array_schema_ = false;
  }
  SECTION("- Serialization") {
    serialize_array_schema_ = true;
  }

  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimension
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, "", TILEDB_DATETIME_MS, &DIM_DOMAIN[0], &TILE_EXTENTS[0], &d1);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attributes
  tiledb_attribute_t* attr1;
  rc = tiledb_attribute_alloc(ctx_, "attr1", ATTR_TYPE, &attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* attr2;
  rc = tiledb_attribute_alloc(ctx_, "attr2", TILEDB_DATETIME_DAY, &attr2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr2);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "datetime-dims";
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  rc = array_create_wrapper(array_name, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&attr1);
  tiledb_attribute_free(&attr2);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  delete_array(array_name);
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema setter/getter for allows_dups",
    "[capi][array-schema][allows-dups]") {
  SECTION("- No serialization") {
    serialize_array_schema_ = false;
  }
  SECTION("- Serialization") {
    serialize_array_schema_ = true;
  }

  // --- Test dense (should error out on allowing duplicates) ---

  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  int allows_dups = 1;
  rc = tiledb_array_schema_set_allows_dups(ctx_, array_schema, allows_dups);
  CHECK(rc == TILEDB_ERR);
  tiledb_array_schema_free(&array_schema);

  // --- Test sparse ---

  // Allocate array schema
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimension
  tiledb_dimension_t* d;
  rc = tiledb_dimension_alloc(
      ctx_, "d", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0], &d);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", ATTR_TYPE, &a);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  REQUIRE(rc == TILEDB_OK);

  // Set allows dups
  rc = tiledb_array_schema_set_allows_dups(ctx_, array_schema, allows_dups);
  CHECK(rc == TILEDB_OK);

  // Create array
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "duplicates";
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  rc = array_create_wrapper(array_name, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);

  // Load array schema
  rc = array_schema_load_wrapper(array_name.c_str(), &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Get allows dups
  int allows_dups_r = 0;
  rc = tiledb_array_schema_get_allows_dups(ctx_, array_schema, &allows_dups_r);
  CHECK(rc == TILEDB_OK);
  CHECK(allows_dups_r == 1);

  // Clean up
  tiledb_array_schema_free(&array_schema);
  delete_array(array_name);
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema setter/getter for dimension filters and cell val "
    "num",
    "[capi][array-schema][dimension]") {
  SECTION("- No serialization") {
    serialize_array_schema_ = false;
  }
  SECTION("- Serialization") {
    serialize_array_schema_ = true;
  }

  // Allocate array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Set up filter list
  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx_, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);
  int level = 5;
  rc = tiledb_filter_set_option(ctx_, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx_, &filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);

  // Create dimension
  tiledb_dimension_t* d;
  rc = tiledb_dimension_alloc(
      ctx_, "d", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0], &d);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_set_cell_val_num(ctx_, d, 3);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_dimension_set_cell_val_num(ctx_, d, 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_dimension_set_filter_list(ctx_, d, filter_list);
  CHECK(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", ATTR_TYPE, &a);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "dimension";
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  rc = array_create_wrapper(array_name, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);

  // Load array schema
  rc = array_schema_load_wrapper(array_name.c_str(), &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Get dimension
  rc = tiledb_array_schema_get_domain(ctx_, array_schema, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_get_dimension_from_index(ctx_, domain, 0, &d);
  REQUIRE(rc == TILEDB_OK);

  // Check cell val num
  unsigned num;
  rc = tiledb_dimension_get_cell_val_num(ctx_, d, &num);
  REQUIRE(rc == TILEDB_OK);
  CHECK(num == 1);

  // Check filter list
  rc = tiledb_dimension_get_filter_list(ctx_, d, &filter_list);
  REQUIRE(rc == TILEDB_OK);
  uint32_t nfilters;
  rc = tiledb_filter_list_get_nfilters(ctx_, filter_list, &nfilters);
  CHECK(nfilters == 1);
  rc = tiledb_filter_list_get_filter_from_index(ctx_, filter_list, 0, &filter);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_type_t type;
  rc = tiledb_filter_get_type(ctx_, filter, &type);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(type == TILEDB_FILTER_BZIP2);
  int read_level;
  rc = tiledb_filter_get_option(
      ctx_, filter, TILEDB_COMPRESSION_LEVEL, &read_level);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(read_level == level);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_dimension_free(&d);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  delete_array(array_name);
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema, set filter errors",
    "[capi][array-schema][filter-error]") {
  // Set up filter list
  tiledb_filter_t* filter;
  int rc = tiledb_filter_alloc(ctx_, TILEDB_FILTER_DOUBLE_DELTA, &filter);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx_, &filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);

  // Create real dimension and test double delta
  tiledb_dimension_t* d;
  float domain[] = {1.0f, 2.0f};
  float extent = .5f;
  rc = tiledb_dimension_alloc(ctx_, "d", TILEDB_FLOAT32, domain, &extent, &d);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_set_filter_list(ctx_, d, filter_list);
  CHECK(rc == TILEDB_ERR);

  // Create real attribute and test double delta
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_FLOAT64, &a);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_filter_list(ctx_, a, filter_list);
  CHECK(rc == TILEDB_ERR);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema, setting heterogeneous dimensions to dense array "
    "error",
    "[capi][array-schema][heter][error]") {
  tiledb_dimension_t* d1;
  float float32_domain[] = {1.0f, 2.0f};
  float float32_extent = .5f;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_FLOAT32, float32_domain, &float32_extent, &d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  int32_t int32_domain[] = {1, 2};
  int32_t int32_extent = 1;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_INT32, int32_domain, &int32_extent, &d2);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);

  // Set domain to a dense array schema should error out
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_ERR);

  // Clean up
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    ArraySchemaFx,
    "C API: Test array schema, setting heterogeneous dimensions to sparse "
    "array",
    "[capi][array-schema][heter][error]") {
  SECTION("- No serialization") {
    serialize_array_schema_ = false;
  }
  SECTION("- Serialization") {
    serialize_array_schema_ = true;
  }

  tiledb_dimension_t* d1;
  float float32_domain[] = {1.0f, 2.0f};
  float float32_extent = .5f;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_FLOAT32, float32_domain, &float32_extent, &d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  int32_t int32_domain[] = {1, 2};
  int32_t int32_extent = 1;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_INT32, int32_domain, &int32_extent, &d2);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);

  // Set domain to a dense array schema should error out
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  auto array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY_NAME;
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_schema_free(&array_schema);

  // Load array schema
  rc = tiledb_array_schema_load(ctx_, array_name.c_str(), &array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_t* read_dom;
  rc = tiledb_array_schema_get_domain(ctx_, array_schema, &read_dom);
  REQUIRE(rc == TILEDB_OK);
  tiledb_datatype_t type;
  rc = tiledb_domain_get_type(ctx_, read_dom, &type);
  REQUIRE(rc == TILEDB_ERR);

  // Check dimension types
  tiledb_dimension_t *r_d1, *r_d2;
  rc = tiledb_domain_get_dimension_from_name(ctx_, domain, "d1", &r_d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_get_dimension_from_name(ctx_, domain, "d2", &r_d2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_get_type(ctx_, r_d1, &type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(type == TILEDB_FLOAT32);
  rc = tiledb_dimension_get_type(ctx_, r_d2, &type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(type == TILEDB_INT32);

  // Get non-empty domain should error out
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  void* dom = nullptr;
  int is_empty = false;
  rc = tiledb_array_get_non_empty_domain_wrapper(ctx_, array, dom, &is_empty);
  REQUIRE(rc == TILEDB_ERR);
  void* subarray = nullptr;

  // Get non-empty domain per dimension
  dom = nullptr;
  is_empty = false;
  rc = tiledb_array_get_non_empty_domain_from_index_wrapper(
      ctx_, array, 0, dom, &is_empty);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_get_non_empty_domain_from_index_wrapper(
      ctx_, array, 1, dom, &is_empty);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_get_non_empty_domain_from_name_wrapper(
      ctx_, array, "d1", dom, &is_empty);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_get_non_empty_domain_from_name_wrapper(
      ctx_, array, "d2", dom, &is_empty);
  REQUIRE(rc == TILEDB_OK);

  // Query checks
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_ERR);
  void* buff = nullptr;
  uint64_t size = 1024;
  rc = tiledb_query_set_buffer(ctx_, query, "buff", buff, &size);
  REQUIRE(rc == TILEDB_ERR);

  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_dimension_free(&r_d1);
  tiledb_dimension_free(&r_d2);
  tiledb_domain_free(&domain);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_array_schema_free(&array_schema);
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}