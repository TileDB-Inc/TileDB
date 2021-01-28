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
 * Dynamic test that performs basic operations on the matrix of posssible
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
      const uint32_t cell_val_num,
      const bool nullable)
      : name_(name)
      , type_(type)
      , cell_val_num_(cell_val_num)
      , nullable_(nullable) {
  }

  string name_;
  tiledb_datatype_t type_;
  uint32_t cell_val_num_;
  bool nullable_;
};

struct test_query_buffer_t {
  test_query_buffer_t(
      const string& name,
      void* const buffer,
      uint64_t* const buffer_size,
      void* const buffer_var,
      uint64_t* const buffer_var_size,
      uint8_t* const buffer_validity,
      uint64_t* const buffer_validity_size)
      : name_(name)
      , buffer_(buffer)
      , buffer_size_(buffer_size)
      , buffer_var_(buffer_var)
      , buffer_var_size_(buffer_var_size)
      , buffer_validity_(buffer_validity)
      , buffer_validity_size_(buffer_validity_size) {
  }

  string name_;
  void* buffer_;
  uint64_t* buffer_size_;
  void* buffer_var_;
  uint64_t* buffer_var_size_;
  uint8_t* buffer_validity_;
  uint64_t* buffer_validity_size_;
};

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
  vector<tiledb_attribute_t*> attrs;
  attrs.reserve(sizeof(test_attr));
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(
      ctx_, test_attr.name_.c_str(), test_attr.type_, &attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_attribute_set_cell_val_num(ctx_, attr, test_attr.cell_val_num_);
  REQUIRE(rc == TILEDB_OK);

  if (test_attr.nullable_) {
    rc = tiledb_attribute_set_nullable(ctx_, attr, 1);
    REQUIRE(rc == TILEDB_OK);
  }

  attrs.emplace_back(attr);

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
  for (const auto& attr : attrs) {
    rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr);
    REQUIRE(rc == TILEDB_OK);
  }

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

  // Free attributes
  for (auto& attr : attrs) {
    tiledb_attribute_free(&attr);
  }

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
        sizeof(encryption_key));
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
    if (test_query_buffer.buffer_validity_size_ == nullptr) {
      if (test_query_buffer.buffer_var_ == nullptr) {
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
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_,
            test_query_buffer.buffer_var_,
            test_query_buffer.buffer_var_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    } else {
      if (test_query_buffer.buffer_var_ == nullptr) {
        rc = tiledb_query_set_buffer_nullable(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_,
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_buffer_var_nullable(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_,
            test_query_buffer.buffer_var_,
            test_query_buffer.buffer_var_size_,
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    }
  }

  // Submit the query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

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
        sizeof(encryption_key));
    REQUIRE(rc == TILEDB_OK);
  }

  // Create the read query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set the query buffers.
  for (size_t i = 0; i < test_query_buffers.size(); ++i) {
    const test_query_buffer_t& test_query_buffer = test_query_buffers[i];
    if (test_query_buffer.buffer_validity_size_ == nullptr) {
      if (test_query_buffer.buffer_var_ == nullptr) {
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
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_,
            test_query_buffer.buffer_var_,
            test_query_buffer.buffer_var_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    } else {
      if (test_query_buffer.buffer_var_ == nullptr) {
        rc = tiledb_query_set_buffer_nullable(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_,
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_buffer_var_nullable(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_,
            test_query_buffer.buffer_var_,
            test_query_buffer.buffer_var_size_,
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    }
  }

  // Set the subarray to read.
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);

  // Submit the query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Get status of query
  tiledb_query_status_t status;
  tiledb_query_get_status(ctx_, query, &status);
  if (status == TILEDB_INCOMPLETE) {
    std::cerr << "status == TILEDB_INCOMPLETE" << std::endl;
  }

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
  const string array_name = "dynamic_array";

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
  uint64_t buffer_len = 1;
  for (const auto& test_dim : test_dims) {
    std::cerr << "DIM_NAME: " << test_dim.name_ << std::endl;
    const uint64_t max_range = ((uint64_t*)(test_dim.domain_))[1];
    const uint64_t min_range = ((uint64_t*)(test_dim.domain_))[0];
    const uint64_t full_range = max_range - min_range + 1;
    buffer_len *= full_range;
  }

  uint64_t a_write_buffer_size;
  auto a_write_buffer = nullptr;

  if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
    a_write_buffer_size = sizeof(uint64_t);
    uint64_t* a_write_buffer = (uint64_t*)malloc(a_write_buffer_size);
    for (uint64_t i = 0; i < a_write_buffer_size; i++) {
      a_write_buffer[i] = i * sizeof(int) * 2;
    }
  } else {
    a_write_buffer_size = buffer_len * tiledb_datatype_size(test_attr.type_);
    int32_t* a_write_buffer = (int32_t*)malloc(a_write_buffer_size);
    for (uint64_t i = 0; i < buffer_len; i++) {
      a_write_buffer[i] = i;
    }
  }

  uint64_t a_write_buffer_var_size =
      2 * buffer_len * tiledb_datatype_size(test_attr.type_);
  int32_t a_write_buffer_var_len = (int32_t)(
      a_write_buffer_var_size / tiledb_datatype_size(test_attr.type_));
  int32_t* a_write_buffer_var = (int32_t*)malloc(a_write_buffer_var_size);
  for (int32_t i = 0; i < a_write_buffer_var_len; i++) {
    a_write_buffer_var[i] = i;
  }

  std::cerr << "BUFFER_LEN: " << buffer_len << std::endl;
  std::cerr << "BUFFER_SIZE: " << a_write_buffer_size << std::endl;

  uint8_t a_write_buffer_validity[1] = {static_cast<uint8_t>(rand() % 2)};
  uint64_t a_write_buffer_validity_size = sizeof(a_write_buffer_validity);

  // Validity buffer is set only when the attribute is nullable.
  // Variable buffer is set only when the attribute is var-sized.
  if (test_attr.nullable_) {
    if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
      write_query_buffers.emplace_back(
          test_attr.name_,
          a_write_buffer,
          &a_write_buffer_size,
          a_write_buffer_var,
          &a_write_buffer_var_size,
          a_write_buffer_validity,
          &a_write_buffer_validity_size);
    } else {
      write_query_buffers.emplace_back(
          test_attr.name_,
          a_write_buffer,
          &a_write_buffer_size,
          nullptr,
          nullptr,
          a_write_buffer_validity,
          &a_write_buffer_validity_size);
    }

  } else {
    if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
      write_query_buffers.emplace_back(
          test_attr.name_,
          a_write_buffer,
          &a_write_buffer_size,
          a_write_buffer_var,
          &a_write_buffer_var_size,
          nullptr,
          nullptr);
    } else {
      write_query_buffers.emplace_back(
          test_attr.name_,
          a_write_buffer,
          &a_write_buffer_size,
          nullptr,
          nullptr,
          nullptr,
          nullptr);
    }
  }

  // Define dimension query write vectors for either sparse arrays
  // or dense arrays with an unordered write order.
  if (array_type == TILEDB_SPARSE || write_order == TILEDB_UNORDERED) {
    // Calculate the write buffer sizes using the ranges of the dimensions.
    vector<uint64_t> dimension_ranges;
    for (auto dims_iter = test_dims.begin(); dims_iter != test_dims.end();
         ++dims_iter) {
      const uint64_t max_range = ((uint64_t*)(dims_iter->domain_))[1];
      const uint64_t min_range = ((uint64_t*)(dims_iter->domain_))[0];

      // Domain {0, 1} is untested
      if (min_range == 0 && max_range == 1)
        return;

      const uint64_t buffer_size = (max_range - min_range) + 1;
      dimension_ranges.emplace_back(buffer_size);
    }

    // Create the dimension write buffers.
    // NOTE: if there is more than 1 dimension, the buffer shall be the previous
    // buffer multiplied by the current range
    vector<uint64_t*> d_write_buffers;
    for (const uint64_t range : dimension_ranges) {
      uint64_t* write_buffer;
      if (range == *dimension_ranges.begin()) {
        write_buffer = (uint64_t*)malloc(range * sizeof(uint64_t*));
      } else {
        write_buffer =
            (uint64_t*)malloc(range * (range - 1) * sizeof(uint64_t*));
      }
      d_write_buffers.emplace_back(write_buffer);
    }

    // Fill the write buffers with values.
    auto dims_iter = test_dims.begin();
    for (uint64_t* const d_write_buffer : d_write_buffers) {
      for (uint64_t i = 0; i < dimension_ranges[i]; i++) {
        d_write_buffer[i] = i;
      }
      uint64_t write_buffer_size = sizeof(d_write_buffer);

      write_query_buffers.emplace_back(
          dims_iter->name_,
          d_write_buffer,
          &(write_buffer_size),
          nullptr,
          nullptr,
          nullptr,
          nullptr);

      dims_iter = std::next(dims_iter, 1);
    }
  }

  // Execute the write query.
  write(array_name, write_query_buffers, write_order, encryption_type);

  // Define the read query buffers for "a".
  vector<test_query_buffer_t> read_query_buffers;
  uint64_t a_read_buffer_size =
      buffer_len * tiledb_datatype_size(test_attr.type_);
  int32_t* a_read_buffer = (int32_t*)malloc(a_read_buffer_size);
  for (uint64_t i = 0; i < buffer_len; i++) {
    a_read_buffer[i] = 0;
  }

  uint8_t a_read_buffer_validity[1] = {0};
  uint64_t a_read_buffer_validity_size = sizeof(a_read_buffer_validity);
  int a_read_buffer_var[2] = {0};
  uint64_t a_read_buffer_var_size = sizeof(a_read_buffer);

  // Validity buffer is set only when the attribute is nullable.
  // Variable buffer is set only when the attribute is var-sized.
  if (test_attr.nullable_) {
    if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
      read_query_buffers.emplace_back(
          test_attr.name_,
          a_read_buffer,
          &a_read_buffer_size,
          a_read_buffer_var,
          &a_read_buffer_var_size,
          a_read_buffer_validity,
          &a_read_buffer_validity_size);
    } else {
      read_query_buffers.emplace_back(
          test_attr.name_,
          a_read_buffer,
          &a_read_buffer_size,
          nullptr,
          nullptr,
          a_read_buffer_validity,
          &a_read_buffer_validity_size);
    }

  } else {
    if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
      read_query_buffers.emplace_back(
          test_attr.name_,
          a_read_buffer,
          &a_read_buffer_size,
          a_read_buffer_var,
          &a_read_buffer_var_size,
          nullptr,
          nullptr);
    } else {
      read_query_buffers.emplace_back(
          test_attr.name_,
          a_read_buffer,
          &a_read_buffer_size,
          nullptr,
          nullptr,
          nullptr,
          nullptr);
    }
  }

  uint64_t subarray_len = 2 * test_dims.size();
  uint64_t subarray_size = subarray_len * sizeof(uint64_t);
  uint64_t* subarray_full = (uint64_t*)malloc(subarray_size);
  for (uint64_t i = 0; i < subarray_len; i += 2) {
    const uint64_t min_range = ((uint64_t*)(test_dims[i / 2].domain_))[0];
    const uint64_t max_range = ((uint64_t*)(test_dims[i / 2].domain_))[1];
    subarray_full[i] = min_range;
    subarray_full[i + 1] = max_range;
  }

  read(array_name, read_query_buffers, subarray_full, encryption_type);

  // Each value in `a_read_buffer` corresponds to its index in
  // the original `a_write_buffer`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  // Validity buffer is set only when the attribute is nullable.
  int32_t expected_a_read_buffer_size =
      buffer_len * tiledb_datatype_size(test_attr.type_);
  int32_t* expected_a_read_buffer =
      (int32_t*)malloc(expected_a_read_buffer_size);

  if (test_attr.nullable_) {
    REQUIRE(a_read_buffer_size == a_write_buffer_size);
    REQUIRE(a_read_buffer_validity_size == a_write_buffer_validity_size);
    if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
      uint8_t expected_a_read_buffer_validity[2];
      REQUIRE(a_read_buffer_var_size == a_write_buffer_var_size);
      for (int i = 0; i < 2; ++i) {
        const uint64_t idx = a_read_buffer_var[i];
        expected_a_read_buffer_validity[i] = a_write_buffer_validity[idx];
      }
      REQUIRE(!memcmp(
          a_read_buffer_validity,
          expected_a_read_buffer_validity,
          a_read_buffer_validity_size));
    } else {
      uint8_t expected_a_read_buffer_validity[1];
      expected_a_read_buffer_validity[0] = a_write_buffer_validity[0];
      REQUIRE(!memcmp(
          a_read_buffer_validity,
          expected_a_read_buffer_validity,
          a_read_buffer_validity_size));
    }

  } else {
    REQUIRE(a_read_buffer_size == a_write_buffer_size);
    if (test_attr.cell_val_num_ == TILEDB_VAR_NUM) {
      uint8_t expected_a_read_buffer[buffer_len];
      REQUIRE(a_read_buffer_var_size == a_write_buffer_var_size);
      for (uint64_t i = 0; i < buffer_len; ++i) {
        const int32_t idx = a_read_buffer_var[i];
        expected_a_read_buffer[i] = a_write_buffer[idx];
      }
      REQUIRE(
          !memcmp(a_read_buffer, expected_a_read_buffer, a_read_buffer_size));
    } else {
      // int32_t expected_a_read_buffer_size =
      //    buffer_len * tiledb_datatype_size(test_attr.type_);
      // int32_t* expected_a_read_buffer =
      //    (int32_t*)malloc(expected_a_read_buffer_size);
      // uint64_t* expected_a_read_buffer = (uint64_t*)malloc(buffer_len / 2);
      REQUIRE(a_read_buffer_var_size == a_write_buffer_var_size);
      for (int32_t i = 0; i < expected_a_read_buffer_size; i++) {
        expected_a_read_buffer[i] = a_write_buffer[i];
      }
      REQUIRE(
          !memcmp(a_read_buffer, expected_a_read_buffer, a_read_buffer_size));
    }
  }

  // Free the expected buffer
  free(expected_a_read_buffer);

  // Free the write buffers vector
  write_query_buffers.clear();
  write_query_buffers.shrink_to_fit();
}

TEST_CASE_METHOD(
    SmokeTestFx,
    "C API: Test a dynamic range of arrays",
    "[capi][smoke-test]") {
  vector<test_dim_t> test_dims;
  const uint64_t d1_domain[] = {1, 2};
  const uint64_t d1_tile_extent = 1;
  test_dims.emplace_back("d1", TILEDB_UINT64, d1_domain, d1_tile_extent);
  // const uint64_t d2_domain[] = {1, 2};
  // const uint64_t d2_tile_extent = 1;
  // test_dims.emplace_back("d2", TILEDB_UINT64, d2_domain, d2_tile_extent);
  // const uint64_t d3_domain[] = {1, 2};
  // const uint64_t d3_tile_extent = 1;
  // test_dims.emplace_back("d3", TILEDB_UINT64, d3_domain, d3_tile_extent);

  const test_attr_t attr("a", TILEDB_INT32, TILEDB_VAR_NUM, false);

  const tiledb_array_type_t array_type = TILEDB_DENSE;
  const tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  const tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  const tiledb_layout_t write_order = TILEDB_ROW_MAJOR;
  tiledb_encryption_type_t encryption_type = TILEDB_NO_ENCRYPTION;

  smoke_test(
      attr,
      test_dims,
      array_type,
      cell_order,
      tile_order,
      write_order,
      encryption_type);
}
