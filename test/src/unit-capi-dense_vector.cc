/**
 * @file   unit-capi-dense_vector.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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
#include "posix_filesystem.h"
#include "tiledb.h"

struct DenseVectorFx {
  const char* ATTR_NAME = "val";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT64;
  const char* DIM0_NAME = "dim0";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
#ifdef HAVE_HDFS
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
#endif
#ifdef HAVE_S3
  const tiledb::URI S3_BUCKET = tiledb::URI("s3://tiledb");
  const std::string S3_TEMP_DIR = "s3://tiledb/tiledb_test/";
#endif
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::posix::current_dir() + "/tiledb_test/";
  const std::string VECTOR = "vector";

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Functions
  DenseVectorFx();
  ~DenseVectorFx();
  void create_dense_vector(const std::string& path);
  void check_read(const std::string& path, tiledb_layout_t layout);
  void check_update(const std::string& path);
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
};

DenseVectorFx::DenseVectorFx() {
  // Create TileDB context
  tiledb_config_t* config = nullptr;
  REQUIRE(tiledb_config_create(&config) == TILEDB_OK);
#ifdef HAVE_S3
  REQUIRE(
      tiledb_config_set(config, "vfs.s3.endpoint_override", "localhost:9999") ==
      TILEDB_OK);
#endif
  REQUIRE(tiledb_ctx_create(&ctx_, config) == TILEDB_OK);
  REQUIRE(tiledb_config_free(config) == TILEDB_OK);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_create(ctx_, &vfs_, nullptr) == TILEDB_OK);

  // Connect to S3
#ifdef HAVE_S3
  // Create bucket if it does not exist
  int is_bucket = 0;
  int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
  REQUIRE(rc == TILEDB_OK);
  if (!is_bucket) {
    rc = tiledb_vfs_create_bucket(ctx_, vfs_, S3_BUCKET.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
#endif
}

DenseVectorFx::~DenseVectorFx() {
  CHECK(tiledb_vfs_free(ctx_, vfs_) == TILEDB_OK);
  CHECK(tiledb_ctx_free(ctx_) == TILEDB_OK);
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

void DenseVectorFx::create_dense_vector(const std::string& path) {
  int rc;
  int64_t dim_domain[] = {0, 9};
  int64_t tile_extent = 10;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_create(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_create(
      ctx_, &dim, DIM0_NAME, TILEDB_INT64, dim_domain, &tile_extent);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim);
  REQUIRE(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_create(ctx_, &attr, ATTR_NAME, ATTR_TYPE);
  REQUIRE(rc == TILEDB_OK);

  // Create array metadata
  tiledb_array_metadata_t* array_metadata;
  rc = tiledb_array_metadata_create(ctx_, &array_metadata, path.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_cell_order(
      ctx_, array_metadata, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_tile_order(
      ctx_, array_metadata, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_array_type(ctx_, array_metadata, TILEDB_DENSE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_domain(ctx_, array_metadata, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_metadata_check(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_free(ctx_, attr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_free(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_OK);

  const char* attributes[] = {ATTR_NAME};

  int64_t buffer_val[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  // Write array
  void* write_buffers[] = {buffer_val};
  uint64_t write_buffer_sizes[] = {sizeof(buffer_val)};
  tiledb_query_t* write_query;
  rc = tiledb_query_create(ctx_, &write_query, path.c_str(), TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, write_query, attributes, 1, write_buffers, write_buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, write_query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, write_query);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_free(ctx_, write_query);
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

  int rc = tiledb_query_create(ctx_, &read_query, path.c_str(), TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, read_query, attributes, 1, read_buffers, read_buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, read_query, layout);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, read_query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_free(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);

  CHECK((buffer[0] == 0 && buffer[1] == 1 && buffer[2] == 2));
}

void DenseVectorFx::check_update(const std::string& path) {
  uint64_t subarray[] = {0, 2};
  const char* attributes[] = {ATTR_NAME};
  int64_t update_buffer[] = {9, 8, 7};
  void* update_buffers[] = {update_buffer};
  uint64_t update_buffer_sizes[] = {sizeof(update_buffer)};

  // Update
  tiledb_query_t* update_query;
  int rc = tiledb_query_create(ctx_, &update_query, path.c_str(), TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, update_query, attributes, 1, update_buffers, update_buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, update_query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, update_query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, update_query);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_free(ctx_, update_query);

  // Read
  int64_t buffer[] = {0, 0, 0};
  void* read_buffers[] = {buffer};
  uint64_t read_buffer_sizes[] = {sizeof(buffer)};
  tiledb_query_t* read_query = nullptr;
  rc = tiledb_query_create(ctx_, &read_query, path.c_str(), TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, read_query, attributes, 1, read_buffers, read_buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, read_query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, read_query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_free(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);

  CHECK((buffer[0] == 9 && buffer[1] == 8 && buffer[2] == 7));
}

TEST_CASE_METHOD(
    DenseVectorFx, "C API: Test 1d dense vector", "[capi], [dense-vector]") {
  // File
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  std::string vector_name = FILE_URI_PREFIX + FILE_TEMP_DIR + VECTOR;
  create_dense_vector(vector_name);
  check_read(vector_name, TILEDB_ROW_MAJOR);
  check_read(vector_name, TILEDB_COL_MAJOR);
  check_update(vector_name);
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

#ifdef HAVE_S3
  // S3
  create_temp_dir(S3_TEMP_DIR);
  vector_name = S3_TEMP_DIR + VECTOR;
  create_dense_vector(vector_name);
  check_read(vector_name, TILEDB_ROW_MAJOR);
  check_read(vector_name, TILEDB_COL_MAJOR);
  check_update(vector_name);
  remove_temp_dir(S3_TEMP_DIR);
#endif

#ifdef HAVE_HDFS
  // HDFS
  create_temp_dir(HDFS_TEMP_DIR);
  vector_name = HDFS_TEMP_DIR + VECTOR;
  create_dense_vector(vector_name);
  check_read(vector_name, TILEDB_ROW_MAJOR);
  check_read(vector_name, TILEDB_COL_MAJOR);
  check_update(vector_name);
  remove_temp_dir(HDFS_TEMP_DIR);
#endif
}
