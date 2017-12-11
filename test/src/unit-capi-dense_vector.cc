/**
 * @file   unit-capi-dense_vector.cc
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
 * Tests of C API for dense vector operations.
 */

#include "catch.hpp"
#include "posix_filesystem.h"
#include "tiledb.h"

#include <cassert>
#include <iostream>

struct DenseVectorFx {
  // Constant parameters
  const char* ATTR_NAME = "val";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT64;
  const char* DIM0_NAME = "dim0";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;

  // Group folder name
#ifdef HAVE_HDFS
  const std::string URI_PREFIX = "hdfs://";
  const std::string TEMP_DIR = "/tiledb_test/";
#else
  const std::string URI_PREFIX = "file://";
  const std::string TEMP_DIR = tiledb::posix::current_dir() + "/";
#endif
  const std::string GROUP = "my_group/";

  // Array name
  std::string array_name_;

  // Array metadata object under test
  tiledb_array_metadata_t* array_metadata_;

  // TileDB context
  tiledb_ctx_t* ctx_;

  DenseVectorFx() {
    // Initialize context
    int rc = tiledb_ctx_create(&ctx_);
    if (rc != TILEDB_OK) {
      std::cerr << "DenseVectorFx() Error creating tiledb_ctx_t" << std::endl;
      std::exit(1);
    }
    // Create group, delete it if it already exists
    if (dir_exists(TEMP_DIR + GROUP)) {
      bool success = remove_dir(TEMP_DIR + GROUP);
      if (!success) {
        tiledb_ctx_free(ctx_);
        std::cerr << "DenseVectorFx() Error deleting existing test group"
                  << std::endl;
        std::exit(1);
      }
    }
    rc = tiledb_group_create(ctx_, (URI_PREFIX + TEMP_DIR + GROUP).c_str());
    if (rc != TILEDB_OK) {
      tiledb_ctx_free(ctx_);
      std::cerr << "DenseVectorFx() Error creating test group" << std::endl;
      std::exit(1);
    }
  }

  ~DenseVectorFx() {
    // Finalize TileDB context
    tiledb_ctx_free(ctx_);

    // Remove the temporary group
    bool success = remove_dir(TEMP_DIR + GROUP);
    if (!success) {
      std::cerr << "DenseVectorFx() Error deleting test group" << std::endl;
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

  /** Sets the array name for the current test. */
  void set_array_name(const char* name) {
    array_name_ = URI_PREFIX + TEMP_DIR + GROUP + name;
  }

  void write_dense_vector(const char* name) {
    int rc;
    int64_t dim_domain[] = {0, 9};
    int64_t tile_extent = 10;

    // Create domain
    tiledb_domain_t* domain;
    rc = tiledb_domain_create(ctx_, &domain, DIM_TYPE);
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
    set_array_name(name);
    tiledb_array_metadata_t* array_metadata;
    rc = tiledb_array_metadata_create(
        ctx_, &array_metadata, array_name_.c_str());
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_set_cell_order(
        ctx_, array_metadata, TILEDB_ROW_MAJOR);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_set_tile_order(
        ctx_, array_metadata, TILEDB_ROW_MAJOR);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_set_array_type(
        ctx_, array_metadata, TILEDB_DENSE);
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
    tiledb_attribute_free(ctx_, attr);
    tiledb_dimension_free(ctx_, dim);

    const char* attributes[] = {ATTR_NAME};

    int64_t buffer_val[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    // Write array
    void* write_buffers[] = {
        buffer_val,
    };
    uint64_t write_buffer_sizes[] = {
        sizeof(buffer_val),
    };
    tiledb_query_t* write_query;
    rc = tiledb_query_create(
        ctx_, &write_query, array_name_.c_str(), TILEDB_WRITE);
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
};

TEST_CASE_METHOD(
    DenseVectorFx,
    "C API: Test 1d dense vector read row-major",
    "[dense-vector]") {
  // Read subset of array val[0:2]
  int rc;
  uint64_t subarray[] = {0, 2};
  int64_t subarray_buffer[] = {0, 0, 0};

  void* read_buffers[] = {
      subarray_buffer,
  };
  uint64_t read_buffer_sizes[] = {
      sizeof(subarray_buffer),
  };

  write_dense_vector("foo");
  tiledb_query_t* read_query = nullptr;

  const char* attributes[] = {ATTR_NAME};

  SECTION("row-major") {
    rc = tiledb_query_create(
        ctx_, &read_query, array_name_.c_str(), TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_buffers(
        ctx_, read_query, attributes, 1, read_buffers, read_buffer_sizes);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, read_query, TILEDB_ROW_MAJOR);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_by_subarray(ctx_, read_query, subarray, TILEDB_INT64);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_submit(ctx_, read_query);
    REQUIRE(rc == TILEDB_OK);
    tiledb_query_free(ctx_, read_query);

    CHECK(
        (subarray_buffer[0] == 0 && subarray_buffer[1] == 1 &&
         subarray_buffer[2] == 2));
  }

  SECTION("col-major") {
    rc = tiledb_query_create(
        ctx_, &read_query, array_name_.c_str(), TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_buffers(
        ctx_, read_query, attributes, 1, read_buffers, read_buffer_sizes);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, read_query, TILEDB_COL_MAJOR);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_by_subarray(ctx_, read_query, subarray, TILEDB_INT64);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_query_submit(ctx_, read_query);
    REQUIRE(rc == TILEDB_OK);
    tiledb_query_free(ctx_, read_query);

    CHECK(
        (subarray_buffer[0] == 0 && subarray_buffer[1] == 1 &&
         subarray_buffer[2] == 2));
  }

  int64_t update_buffer[] = {9, 8, 7};
  void* update_buffers[] = {
      update_buffer,
  };
  uint64_t update_buffer_sizes[] = {
      sizeof(update_buffer),
  };

  SECTION("update") {
    tiledb_query_t* update_query;
    rc = tiledb_query_create(
        ctx_, &update_query, array_name_.c_str(), TILEDB_WRITE);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_buffers(
        ctx_, update_query, attributes, 1, update_buffers, update_buffer_sizes);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, update_query, TILEDB_ROW_MAJOR);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_by_subarray(ctx_, update_query, subarray, TILEDB_INT64);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_query_submit(ctx_, update_query);
    REQUIRE(rc == TILEDB_OK);
    tiledb_query_free(ctx_, update_query);

    rc = tiledb_query_create(
        ctx_, &read_query, array_name_.c_str(), TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_buffers(
        ctx_, read_query, attributes, 1, read_buffers, read_buffer_sizes);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, read_query, TILEDB_COL_MAJOR);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_by_subarray(ctx_, read_query, subarray, TILEDB_INT64);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_query_submit(ctx_, read_query);
    REQUIRE(rc == TILEDB_OK);
    tiledb_query_free(ctx_, read_query);

    CHECK(
        (subarray_buffer[0] == 9 && subarray_buffer[1] == 8 &&
         subarray_buffer[2] == 7));
  }
}
