/**
 * @file unit-capi-smoke-test.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * Smoke test that performs basic operations on the matrix of posssible
 * array schemas.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include <iostream>
#include <vector>

using namespace std;
using namespace tiledb::sm;
using namespace tiledb::test;

static const char encryption_key[] = "unittestunittestunittestunittest";

class SmokeTestFx {
 public:
#ifdef _WIN32
  const string FILE_URI_PREFIX = "";
  const string FILE_TEMP_DIR =
      tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  const string FILE_URI_PREFIX = "file://";
  const string FILE_TEMP_DIR =
      tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif

  struct test_dim_t {
    test_dim_t(
        const string& name,
        const tiledb_datatype_t type,
        const void* const domain,
        const uint64_t tile_extent)
        : name_(name)
        , type_(type)
        , domain_(domain)
        , tile_extent_(tile_extent) {
    }

    string name_;
    tiledb_datatype_t type_;
    const void* domain_;
    uint64_t tile_extent_;
  };

  struct test_attr_t {
    test_attr_t(
        const string& name,
        const tiledb_datatype_t type,
        const uint32_t cell_val_num)
        : name_(name)
        , type_(type)
        , cell_val_num_(cell_val_num) {
    }

    string name_;
    tiledb_datatype_t type_;
    uint32_t cell_val_num_;
  };

  struct test_query_buffer_t {
    test_query_buffer_t(
        const string& name,
        void* const buffer,
        uint64_t* const buffer_size,
        void* const buffer_offset,
        uint64_t* const buffer_offset_size)
        : name_(name)
        , buffer_(buffer)
        , buffer_size_(buffer_size)
        , buffer_offset_(buffer_offset)
        , buffer_offset_size_(buffer_offset_size) {
    }

    string name_;
    void* buffer_;
    uint64_t* buffer_size_;
    void* buffer_offset_;
    uint64_t* buffer_offset_size_;
  };

  /** Constructor. */
  SmokeTestFx();

  /** Destructor. */
  ~SmokeTestFx();

  /**
   * Create, write and read attributes to an array.
   *
   * @param test_attr The attribute to test.
   * @param test_dims The dimensions to test.
   * @param array_type The type of the array (dense/sparse).
   * @param cell_order The cell order of the array.
   * @param tile_order The tile order of the array.
   * @param write_order The write layout.
   * @param encryption_type The encryption type.
   */
  void smoke_test(
      const test_attr_t test_attr,
      const vector<test_dim_t>& test_dims,
      tiledb_array_type_t array_type,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order,
      tiledb_layout_t write_order,
      tiledb_encryption_type_t encryption_type);

 private:
  /** The C-API context object. */
  tiledb_ctx_t* ctx_;

  /** The C-API VFS object. */
  tiledb_vfs_t* vfs_;

  /**
   * Creates a directory using `vfs_`.
   *
   * @param path The directory path.
   */
  void create_dir(const string& path);

  /**
   * Removes a directory using `vfs_`.
   *
   * @param path The directory path.
   */
  void remove_dir(const string& path);

  /**
   * Creates a TileDB array.
   *
   * @param array_name The name of the array.
   * @param array_type The type of the array (dense/sparse).
   * @param test_dims The dimensions in the array.
   * @param test_attr The attribute in the array.
   * @param cell_order The cell order of the array.
   * @param tile_order The tile order of the array.
   * @param encryption_type The encryption type of the array.
   *
   */
  void create_array(
      const string& array_name,
      tiledb_array_type_t array_type,
      const vector<test_dim_t>& test_dims,
      const test_attr_t test_attr,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order,
      tiledb_encryption_type_t encryption_type);

  /**
   * Creates and executes a single write query.
   *
   * @param array_name The name of the array.
   * @param test_query_buffers The query buffers to write.
   * @param layout The write layout.
   * @param encryption_type The encryption type of the array.
   */
  void write(
      const string& array_name,
      const vector<test_query_buffer_t>& test_query_buffers,
      tiledb_layout_t layout,
      tiledb_encryption_type_t encryption_type);

  /**
   * Creates and executes a single read query.
   *
   * @param array_name The name of the array.
   * @param test_query_buffers The query buffers to read.
   * @param subarray The subarray to read.
   * @param encryption_type The encryption type of the array.
   */
  void read(
      const string& array_name,
      const vector<test_query_buffer_t>& test_query_buffers,
      const void* subarray,
      tiledb_encryption_type_t encryption_type);
};

SmokeTestFx::SmokeTestFx() {
  // Create a config.
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Create the context.
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Create the VFS.
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);

  tiledb_config_free(&config);
}

SmokeTestFx::~SmokeTestFx() {
  remove_dir(FILE_TEMP_DIR);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void SmokeTestFx::create_dir(const string& path) {
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SmokeTestFx::remove_dir(const string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SmokeTestFx::create_array(
    const string& array_name,
    tiledb_array_type_t array_type,
    const vector<test_dim_t>& test_dims,
    const test_attr_t test_attr,
    tiledb_layout_t cell_order,
    tiledb_layout_t tile_order,
    tiledb_encryption_type_t encryption_type) {
  remove_dir(FILE_TEMP_DIR);
  create_dir(FILE_TEMP_DIR);

  // Create the dimensions.
  vector<tiledb_dimension_t*> dims;
  dims.reserve(test_dims.size());
  for (const auto& test_dim : test_dims) {
    tiledb_dimension_t* dim;
    const int rc = tiledb_dimension_alloc(
        ctx_,
        test_dim.name_.c_str(),
        test_dim.type_,
        test_dim.domain_,
        &test_dim.tile_extent_,
        &dim);
    REQUIRE(rc == TILEDB_OK);
    dims.emplace_back(dim);
  }

  // Create the domain.
  tiledb_domain_t* domain;
  int rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  for (const auto& dim : dims) {
    rc = tiledb_domain_add_dimension(ctx_, domain, dim);
    REQUIRE(rc == TILEDB_OK);
  }

  // Create attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(
      ctx_, test_attr.name_.c_str(), test_attr.type_, &attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_attribute_set_cell_val_num(ctx_, attr, test_attr.cell_val_num_);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, array_type, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr);
  REQUIRE(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create the array with or without encryption
  if (encryption_type == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_create(
        ctx_, (FILE_TEMP_DIR + array_name).c_str(), array_schema);
    REQUIRE(rc == TILEDB_OK);
  } else {
    tiledb_array_create_with_key(
        ctx_,
        (FILE_TEMP_DIR + array_name).c_str(),
        array_schema,
        encryption_type,
        encryption_key,
        (uint32_t)strlen(encryption_key));
  }

  // Free attribute
  tiledb_attribute_free(&attr);

  // Free dimensions
  for (auto& dim : dims) {
    tiledb_dimension_free(&dim);
  }

  // Free the domain
  tiledb_domain_free(&domain);

  // Free the array schema
  tiledb_array_schema_free(&array_schema);
}

void SmokeTestFx::write(
    const string& array_name,
    const vector<test_query_buffer_t>& test_query_buffers,
    tiledb_layout_t layout,
    tiledb_encryption_type_t encryption_type) {
  // Open the array for writing (with or without encryption).
  tiledb_array_t* array;
  int rc =
      tiledb_array_alloc(ctx_, (FILE_TEMP_DIR + array_name).c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
    REQUIRE(rc == TILEDB_OK);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type,
        encryption_key,
        (uint32_t)strlen(encryption_key));
    REQUIRE(rc == TILEDB_OK);
  }

  // Create the write query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set the query layout.
  rc = tiledb_query_set_layout(ctx_, query, layout);
  REQUIRE(rc == TILEDB_OK);

  // Set the query buffers.
  for (const auto& test_query_buffer : test_query_buffers) {
    if (test_query_buffer.buffer_offset_ == nullptr) {
      rc = tiledb_query_set_buffer(
          ctx_,
          query,
          test_query_buffer.name_.c_str(),
          test_query_buffer.buffer_,
          test_query_buffer.buffer_size_);
      REQUIRE(rc == TILEDB_OK);
    } else {
      rc = tiledb_query_set_buffer_var(
          ctx_,
          query,
          test_query_buffer.name_.c_str(),
          static_cast<uint64_t*>(test_query_buffer.buffer_offset_),
          test_query_buffer.buffer_offset_size_,
          test_query_buffer.buffer_,
          test_query_buffer.buffer_size_);
      REQUIRE(rc == TILEDB_OK);
    }
  }

  // Submit the query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Check query status.
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize the query, a no-op for non-global writes.
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SmokeTestFx::read(
    const string& array_name,
    const vector<test_query_buffer_t>& test_query_buffers,
    const void* subarray,
    tiledb_encryption_type_t encryption_type) {
  // Open the array for reading (with or without encryption).
  tiledb_array_t* array;
  int rc =
      tiledb_array_alloc(ctx_, (FILE_TEMP_DIR + array_name).c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type,
        encryption_key,
        (uint32_t)strlen(encryption_key));
    REQUIRE(rc == TILEDB_OK);
  }

  // Create the read query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set the query buffers.
  for (size_t i = 0; i < test_query_buffers.size(); ++i) {
    const test_query_buffer_t& test_query_buffer = test_query_buffers[i];
    if (test_query_buffer.buffer_offset_ == nullptr) {
      rc = tiledb_query_set_buffer(
          ctx_,
          query,
          test_query_buffer.name_.c_str(),
          test_query_buffer.buffer_,
          test_query_buffer.buffer_size_);
      REQUIRE(rc == TILEDB_OK);
    } else {
      rc = tiledb_query_set_buffer_var(
          ctx_,
          query,
          test_query_buffer.name_.c_str(),
          static_cast<uint64_t*>(test_query_buffer.buffer_offset_),
          test_query_buffer.buffer_offset_size_,
          test_query_buffer.buffer_,
          test_query_buffer.buffer_size_);
      REQUIRE(rc == TILEDB_OK);
    }
  }

  // Set the subarray to read.
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);

  // Submit the query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Check query status.
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize the query, a no-op for non-global writes.
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SmokeTestFx::smoke_test(
    const test_attr_t test_attr,
    const vector<test_dim_t>& test_dims,
    tiledb_array_type_t array_type,
    tiledb_layout_t cell_order,
    tiledb_layout_t tile_order,
    tiledb_layout_t write_order,
    tiledb_encryption_type_t encryption_type) {
  const string array_name = "smoke_test_array";

  // Skip row-major and col-major writes for sparse arrays.
  if (array_type == TILEDB_SPARSE &&
      (write_order == TILEDB_ROW_MAJOR || write_order == TILEDB_COL_MAJOR)) {
    return;
  }

  // String_ascii, float32, and float64 types can only be
  // written to sparse arrays.
  if (array_type == TILEDB_DENSE && (test_attr.type_ == TILEDB_STRING_ASCII ||
                                     test_attr.type_ == TILEDB_FLOAT32 ||
                                     test_attr.type_ == TILEDB_FLOAT64)) {
    return;
  }

  // Create the array.
  create_array(
      array_name,
      array_type,
      test_dims,
      test_attr,
      cell_order,
      tile_order,
      encryption_type);

  // Define the write query buffers for "a".
  vector<test_query_buffer_t> write_query_buffers;
  uint64_t total_cells = 1;
  for (const auto& test_dim : test_dims) {
    const uint64_t max_range = ((uint64_t*)(test_dim.domain_))[1];
    const uint64_t min_range = ((uint64_t*)(test_dim.domain_))[0];
    const uint64_t full_range = max_range - min_range + 1;
    total_cells *= full_range;
  }

  uint64_t a_write_buffer_size;
  int32_t* a_write_buffer = nullptr;

  if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
    a_write_buffer_size =
        total_cells * 2 * tiledb_datatype_size(test_attr.type_);
    a_write_buffer = (int32_t*)malloc(a_write_buffer_size);
    for (uint64_t i = 0; i < (total_cells * 2); i++) {
      a_write_buffer[i] = i;
    }
  } else {
    a_write_buffer_size = total_cells * tiledb_datatype_size(test_attr.type_);
    a_write_buffer = (int32_t*)malloc(a_write_buffer_size);
    for (uint64_t i = 0; i < total_cells; i++) {
      a_write_buffer[i] = i;
    }
  }

  uint64_t a_write_buffer_offset_size = total_cells * sizeof(uint64_t);
  uint64_t* a_write_buffer_offset =
      (uint64_t*)malloc(a_write_buffer_offset_size);
  for (uint64_t i = 0; i < total_cells; i++) {
    a_write_buffer_offset[i] = i * tiledb_datatype_size(test_attr.type_) * 2;
  }

  // Variable buffer is set only when the attribute is var-sized.
  if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
    write_query_buffers.emplace_back(
        test_attr.name_,
        a_write_buffer,
        &a_write_buffer_size,
        a_write_buffer_offset,
        &a_write_buffer_offset_size);
  } else {
    write_query_buffers.emplace_back(
        test_attr.name_,
        a_write_buffer,
        &a_write_buffer_size,
        nullptr,
        nullptr);
  }

  // Define dimension query write vectors for either sparse arrays
  // or dense arrays with an unordered write order.
  vector<uint64_t*> d_write_buffers;
  if (array_type == TILEDB_SPARSE || write_order == TILEDB_UNORDERED) {
    // Calculate the write buffer lengths using the ranges of the dimensions.
    vector<uint64_t> dimension_ranges;
    uint64_t dim_buffer_len = 0;
    for (auto dims_iter = test_dims.begin(); dims_iter != test_dims.end();
         ++dims_iter) {
      const uint64_t max_range = ((uint64_t*)(dims_iter->domain_))[1];
      const uint64_t min_range = ((uint64_t*)(dims_iter->domain_))[0];
      dim_buffer_len = (max_range - min_range) + 1;
      dimension_ranges.emplace_back(dim_buffer_len);
    }

    // Create the dimension write buffers.
    for (const uint64_t range : dimension_ranges) {
      d_write_buffers.emplace_back(
          (uint64_t*)malloc(range * sizeof(uint64_t*)));
    }

    // Fill the write buffers with values.
    auto dims_iter = test_dims.begin();
    for (uint64_t* const d_write_buffer : d_write_buffers) {
      for (uint64_t i = 0; i < dimension_ranges[i]; i++) {
        d_write_buffer[i] = i;
      }
      uint64_t write_buffer_size =
          dim_buffer_len * tiledb_datatype_size(dims_iter->type_);

      write_query_buffers.emplace_back(
          dims_iter->name_,
          d_write_buffer,
          &(write_buffer_size),
          nullptr,
          nullptr);
      dims_iter = std::next(dims_iter, 1);
    }
  }

  // Execute the write query.
  write(array_name, write_query_buffers, write_order, encryption_type);

  // Define the read query buffers for "a".
  vector<test_query_buffer_t> read_query_buffers;

  uint64_t a_read_buffer_size;
  int32_t* a_read_buffer = nullptr;

  if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
    a_read_buffer_size =
        total_cells * 2 * tiledb_datatype_size(test_attr.type_);
    a_read_buffer = (int32_t*)malloc(a_read_buffer_size);
    for (uint64_t i = 0; i < total_cells * 2; i++) {
      a_read_buffer[i] = 0;
    }
  } else {
    a_read_buffer_size = total_cells * tiledb_datatype_size(test_attr.type_);
    a_read_buffer = (int32_t*)malloc(a_read_buffer_size);
    for (uint64_t i = 0; i < total_cells; i++) {
      a_read_buffer[i] = 0;
    }
  }

  uint64_t a_read_buffer_offset_size = total_cells * sizeof(uint64_t);
  uint64_t* a_read_buffer_offset = (uint64_t*)malloc(a_read_buffer_offset_size);
  for (uint64_t i = 0; i < total_cells; i++) {
    a_read_buffer_offset[i] = 0;
  }

  // Variable buffer is set only when the attribute is var-sized.
  if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
    read_query_buffers.emplace_back(
        test_attr.name_,
        a_read_buffer,
        &a_read_buffer_size,
        a_read_buffer_offset,
        &a_read_buffer_offset_size);
  } else {
    read_query_buffers.emplace_back(
        test_attr.name_, a_read_buffer, &a_read_buffer_size, nullptr, nullptr);
  }

  // This logic assumes that all dimensions are of type TILEDB_UINT64.
  uint64_t subarray_size = 2 * test_dims.size() * sizeof(uint64_t);
  uint64_t* subarray_full = (uint64_t*)malloc(subarray_size);
  for (uint64_t i = 0; i < test_dims.size(); ++i) {
    const uint64_t min_range = ((uint64_t*)(test_dims[i].domain_))[0];
    const uint64_t max_range = ((uint64_t*)(test_dims[i].domain_))[1];
    subarray_full[(i * 2)] = min_range;
    subarray_full[(i * 2) + 1] = max_range;
  }

  read(array_name, read_query_buffers, subarray_full, encryption_type);

  // Ensure that each value in `a_read_buffer` corresponds to its index in
  // the original `a_write_buffer`.
  REQUIRE(!memcmp(a_read_buffer, a_write_buffer, a_read_buffer_size));

  // Free the write buffers.
  free(a_write_buffer);
  free(a_write_buffer_offset);

  // Free the read buffers.
  free(a_read_buffer);
  free(a_read_buffer_offset);

  // Free the subarray_full.
  free(subarray_full);
}

TEST_CASE_METHOD(
    SmokeTestFx,
    "C API: Test a dynamic range of arrays",
    "[capi][smoke-test]") {
  vector<test_attr_t> attrs;
  attrs.emplace_back("a1", TILEDB_INT32, 1);
  attrs.emplace_back("a2", TILEDB_INT32, TILEDB_VAR_NUM);

  vector<test_dim_t> dims;
  const uint64_t d1_domain[] = {1, 2};
  const uint64_t d1_tile_extent = 1;
  dims.emplace_back("d1", TILEDB_UINT64, d1_domain, d1_tile_extent);
  const uint64_t d2_domain[] = {1, 2};
  const uint64_t d2_tile_extent = 1;
  dims.emplace_back("d2", TILEDB_UINT64, d2_domain, d2_tile_extent);
  const uint64_t d3_domain[] = {1, 3};
  const uint64_t d3_tile_extent = 1;
  dims.emplace_back("d3", TILEDB_UINT64, d3_domain, d3_tile_extent);

  const tiledb_layout_t write_order = TILEDB_ROW_MAJOR;

  for (const test_attr_t& attr : attrs) {
    for (const tiledb_array_type_t array_type : {TILEDB_DENSE, TILEDB_SPARSE}) {
      for (const tiledb_layout_t cell_order :
           {TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR}) {
        for (const tiledb_layout_t tile_order :
             {TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR}) {
          for (const tiledb_encryption_type_t encryption_type :
               {TILEDB_NO_ENCRYPTION, TILEDB_AES_256_GCM}) {
            vector<test_dim_t> test_dims;
            for (const test_dim_t& dim : dims) {
              test_dims.emplace_back(dim);

              smoke_test(
                  attr,
                  test_dims,
                  array_type,
                  cell_order,
                  tile_order,
                  write_order,
                  encryption_type);
            }
          }
        }
      }
    }
  }
}
