/**
 * @file   unit-capi-dense_vector.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests of C API for dense vector operations.
 */

#include "catch.hpp"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include <sstream>
#include <thread>

struct DenseVectorFx {
  const char* ATTR_NAME = "val";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT64;
  const char* DIM0_NAME = "dim0";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
  const std::string S3_PREFIX = "s3://";
  const std::string S3_BUCKET = S3_PREFIX + random_bucket_name("tiledb") + "/";
  const std::string S3_TEMP_DIR = S3_BUCKET + "tiledb_test/";
#ifdef _WIN32
  const std::string FILE_URI_PREFIX = "";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  const std::string VECTOR = "vector";

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;

  // Functions
  DenseVectorFx();
  ~DenseVectorFx();
  void create_dense_vector(const std::string& path);
  void check_read(const std::string& path, tiledb_layout_t layout);
  void check_update(const std::string& path);
  void check_duplicate_coords(const std::string& path);
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  static std::string random_bucket_name(const std::string& prefix);
  void set_supported_fs();
};

DenseVectorFx::DenseVectorFx() {
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
        tiledb_config_set(config, "vfs.s3.scheme", "http", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.use_virtual_addressing", "false", &error) ==
        TILEDB_OK);
    REQUIRE(error == nullptr);
#endif
  }
  REQUIRE(tiledb_ctx_alloc(&ctx_, config) == TILEDB_OK);
  REQUIRE(error == nullptr);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, &vfs_, config) == TILEDB_OK);
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
}

DenseVectorFx::~DenseVectorFx() {
  if (supports_s3_) {
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    CHECK(rc == TILEDB_OK);
    if (is_bucket) {
      CHECK(
          tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str()) == TILEDB_OK);
    }
  }

  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void DenseVectorFx::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(&ctx, nullptr) == TILEDB_OK);

  int is_supported = 0;
  int rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_S3, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_s3_ = (bool)is_supported;
  rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_HDFS, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_hdfs_ = (bool)is_supported;

  tiledb_ctx_free(&ctx);
}

void DenseVectorFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void DenseVectorFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

std::string DenseVectorFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::timestamp_ms();
  return ss.str();
}

void DenseVectorFx::create_dense_vector(const std::string& path) {
  int rc;
  int64_t dim_domain[] = {0, 9};
  int64_t tile_extent = 10;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_, &dim, DIM0_NAME, TILEDB_INT64, dim_domain, &tile_extent);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim);
  REQUIRE(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_, &attr, ATTR_NAME, ATTR_TYPE);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, &array_schema, TILEDB_DENSE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_array_schema_free(&array_schema);

  const char* attributes[] = {ATTR_NAME};

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int64_t buffer_val[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  // Write array
  void* write_buffers[] = {buffer_val};
  uint64_t write_buffer_sizes[] = {sizeof(buffer_val)};
  tiledb_query_t* write_query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &write_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_,
      write_query,
      attributes[0],
      write_buffers[0],
      &write_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, write_query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, write_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, write_query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&write_query);
}

void DenseVectorFx::check_read(
    const std::string& path, tiledb_layout_t layout) {
  // Read subset of array val[0:2]
  uint64_t subarray[] = {0, 2};
  int64_t buffer[] = {0, 0, 0};
  void* read_buffers[] = {buffer};
  uint64_t read_buffer_sizes[] = {sizeof(buffer)};
  tiledb_query_t* read_query = nullptr;
  const char* attributes[] = {ATTR_NAME};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, read_query, attributes[0], read_buffers[0], &read_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, read_query, layout);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, read_query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);

  CHECK(buffer[0] == 0);
  CHECK(buffer[1] == 1);
  CHECK(buffer[2] == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&read_query);
}

void DenseVectorFx::check_update(const std::string& path) {
  uint64_t subarray[] = {0, 2};
  const char* attributes[] = {ATTR_NAME};
  int64_t update_buffer[] = {9, 8, 7};
  void* update_buffers[] = {update_buffer};
  uint64_t update_buffer_sizes[] = {sizeof(update_buffer)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Update
  tiledb_query_t* update_query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &update_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_,
      update_query,
      attributes[0],
      update_buffers[0],
      &update_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, update_query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, update_query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, update_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, update_query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&update_query);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Read
  int64_t buffer[] = {0, 0, 0};
  void* read_buffers[] = {buffer};
  uint64_t read_buffer_sizes[] = {sizeof(buffer)};
  tiledb_query_t* read_query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, read_query, attributes[0], read_buffers[0], &read_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, read_query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, read_query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&read_query);

  CHECK((buffer[0] == 9 && buffer[1] == 8 && buffer[2] == 7));
}

void DenseVectorFx::check_duplicate_coords(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  const int64_t num_writes = 5;
  for (int64_t write_num = 0; write_num < num_writes; write_num++) {
    const char* attributes[] = {ATTR_NAME, TILEDB_COORDS};
    int64_t update_buffer[] = {write_num, write_num, write_num};
    int64_t coords_buffer[] = {7, 8, 9};
    void* update_buffers[] = {update_buffer, coords_buffer};
    uint64_t update_buffer_sizes[] = {sizeof(update_buffer),
                                      sizeof(coords_buffer)};

    // Update
    tiledb_query_t* update_query;
    rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &update_query);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_buffer(
        ctx_,
        update_query,
        attributes[0],
        update_buffers[0],
        &update_buffer_sizes[0]);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_buffer(
        ctx_,
        update_query,
        attributes[1],
        update_buffers[1],
        &update_buffer_sizes[1]);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, update_query, TILEDB_UNORDERED);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_submit(ctx_, update_query);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_finalize(ctx_, update_query);
    REQUIRE(rc == TILEDB_OK);
    tiledb_query_free(&update_query);
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Read
  uint64_t subarray[] = {7, 9};
  const char* attributes[] = {ATTR_NAME};
  int64_t buffer[3] = {0};
  void* read_buffers[] = {buffer};
  uint64_t read_buffer_sizes[] = {sizeof(buffer)};
  tiledb_query_t* read_query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, read_query, attributes[0], read_buffers[0], &read_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, read_query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, read_query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&read_query);

  CHECK(
      (buffer[0] == num_writes - 1 && buffer[1] == num_writes - 1 &&
       buffer[2] == num_writes - 1));
}

TEST_CASE_METHOD(
    DenseVectorFx, "C API: Test 1d dense vector", "[capi], [dense-vector]") {
  std::string vector_name;

  if (supports_s3_) {
    // S3
    create_temp_dir(S3_TEMP_DIR);
    vector_name = S3_TEMP_DIR + VECTOR;
    create_dense_vector(vector_name);
    check_read(vector_name, TILEDB_ROW_MAJOR);
    check_read(vector_name, TILEDB_COL_MAJOR);
    check_update(vector_name);
    check_duplicate_coords(vector_name);
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_hdfs_) {
    // HDFS
    create_temp_dir(HDFS_TEMP_DIR);
    vector_name = HDFS_TEMP_DIR + VECTOR;
    create_dense_vector(vector_name);
    check_read(vector_name, TILEDB_ROW_MAJOR);
    check_read(vector_name, TILEDB_COL_MAJOR);
    check_update(vector_name);
    check_duplicate_coords(vector_name);
    remove_temp_dir(HDFS_TEMP_DIR);
  } else {
    // File
    create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
    vector_name = FILE_URI_PREFIX + FILE_TEMP_DIR + VECTOR;
    create_dense_vector(vector_name);
    check_read(vector_name, TILEDB_ROW_MAJOR);
    check_read(vector_name, TILEDB_COL_MAJOR);
    check_update(vector_name);
    check_duplicate_coords(vector_name);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}
