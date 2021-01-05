/**
 * @file unit-capi-dynamic.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020 TileDB, Inc.
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
#include "test/src/vfs_helpers.h"
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

class DynamicArrayFx {
 public:
#ifdef _WIN32
  SupportedFsLocal windows_fs;
  const string temp_dir_ = windows_fs.temp_dir();
#else
  SupportedFsLocal posix_fs;
  const string temp_dir_ = posix_fs.temp_dir();
#endif

  /** Constructor. */
  DynamicArrayFx();

  /** Destructor. */
  ~DynamicArrayFx();

  /**
   * Create, write and read attributes to a 1D dense array.
   *
   * @param test_attrs The nullable attributes to test.
   * @param array_type The type of the array (dense/sparse).
   * @param cell_order The cell order of the array.
   * @param tile_order The tile order of the array.
   * @param write_order The write layout.
   */
  void dense_1d_test(
      const vector<test_attr_t>& test_attrs,
      tiledb_array_type_t array_type,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order,
      tiledb_layout_t write_order);

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
   * @param test_attrs The attributes in the array.
   * @param cell_order The cell order of the array.
   * @param tile_order The tile order of the array.
   */
  void create_array(
      const string& array_name,
      tiledb_array_type_t array_type,
      const vector<test_dim_t>& test_dims,
      const vector<test_attr_t>& test_attrs,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order);

  /**
   * Creates and executes a single write query.
   *
   * @param array_name The name of the array.
   * @param test_query_buffers The query buffers to write.
   * @param layout The write layout.
   */
  void write(
      const string& array_name,
      const vector<test_query_buffer_t>& test_query_buffers,
      tiledb_layout_t layout);

  /**
   * Creates and executes a single read query.
   *
   * @param array_name The name of the array.
   * @param test_query_buffers The query buffers to read.
   * @param subarray The subarray to read.
   */
  void read(
      const string& array_name,
      const vector<test_query_buffer_t>& test_query_buffers,
      const void* subarray);
};

DynamicArrayFx::DynamicArrayFx() {
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

DynamicArrayFx::~DynamicArrayFx() {
  remove_dir(temp_dir_);
}

void DynamicArrayFx::create_dir(const string& path) {
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void DynamicArrayFx::remove_dir(const string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void DynamicArrayFx::create_array(
    const string& array_name,
    tiledb_array_type_t array_type,
    const vector<test_dim_t>& test_dims,
    const vector<test_attr_t>& test_attrs,
    tiledb_layout_t cell_order,
    tiledb_layout_t tile_order) {
  remove_dir(temp_dir_);
  create_dir(temp_dir_);

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

  /*
  tiledb_ctx_alloc(NULL, &ctx_);
  int dim_domain[] = {0, 1};
  int tile_extent = 1;

  // Create the dimensions
  tiledb_dimension_t* dim;
  int rc = tiledb_dimension_alloc(ctx_, "dim", TILEDB_INT32, &dim_domain[0],
    &tile_extent, &dim);
  REQUIRE(rc == TILEDB_OK);
  */

  // Create the domain.
  tiledb_domain_t* domain;
  int rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  for (const auto& dim : dims) {
    rc = tiledb_domain_add_dimension(ctx_, domain, dim);
    REQUIRE(rc == TILEDB_OK);
  }

  /*
  // Create the domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_add_dimension(ctx_, domain, dim);
  */

  // Create attributes
  vector<tiledb_attribute_t*> attrs;
  attrs.reserve(test_attrs.size());
  for (const auto& test_attr : test_attrs) {
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
  }

  /*
  // Create the attributes
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_, "attr", TILEDB_INT32, &attr);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_set_cell_val_num(ctx_, attr, 1);
  */

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

  /*
  // Create array schema
  tiledb_array_schema_t* schema;
  tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &schema);
  tiledb_array_schema_set_domain(ctx_, schema, domain);
  tiledb_array_schema_add_attribute(ctx_, schema, attr);
  */

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc =
      tiledb_array_create(ctx_, (temp_dir_ + array_name).c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

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

void DynamicArrayFx::write(
    const string& array_name,
    const vector<test_query_buffer_t>& test_query_buffers,
    tiledb_layout_t layout) {
  // Open the array for writing.
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, (temp_dir_ + array_name).c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  /*
  // Prepare some data for the array.
  int data[] = {3};
  unsigned long long data_size = sizeof(data);

  // Set the subarray to write into.
  int subarray[] = {0,0};
  */

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
        std::cerr << "CASE 1" << std::endl;
        rc = tiledb_query_set_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        std::cerr << "CASE 2" << std::endl;
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
        std::cerr << "CASE 3" << std::endl;
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
        std::cerr << "CASE 4" << std::endl;
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

  /*
  // Set the query subarray.
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);


  // Set the query buffer.
  rc = tiledb_query_set_buffer(ctx_, query, "attr", data, &data_size);
  REQUIRE(rc == TILEDB_OK);
  */

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

void DynamicArrayFx::read(
    const string& array_name,
    const vector<test_query_buffer_t>& test_query_buffers,
    const void* subarray) {
  // Open the array for reading.
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, (temp_dir_ + array_name).c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Create the read query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  /*
  // Prepare read results
  int subarray[] = {0,0};
  int dim[1] = {0};
  uint64_t dim_size = sizeof(dim);
  int attr[6] = {0};
  uint64_t attr_size = sizeof(attr);


  // Set the query buffers
  rc = tiledb_query_set_buffer(ctx_, query, "dim", dim, &dim_size);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_set_buffer(ctx_, query, "attr", attr, &attr_size);
  REQUIRE(rc == TILEDB_OK);
  */

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

  // Finalize the query, a no-op for non-global writes.
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DynamicArrayFx::dense_1d_test(
    const vector<test_attr_t>& test_attrs,
    tiledb_array_type_t array_type,
    tiledb_layout_t cell_order,
    tiledb_layout_t tile_order,
    tiledb_layout_t write_order) {
  const string array_name = "1d_dense_array";

  // Define the dimensions.
  vector<test_dim_t> test_dims;
  const uint64_t d_domain[] = {0, 1};
  const uint64_t d_tile_extent = 1;
  test_dims.emplace_back("d", TILEDB_INT32, d_domain, d_tile_extent);

  // Create the array.
  create_array(
      array_name, array_type, test_dims, test_attrs, cell_order, tile_order);

  // Define the write query buffers for "a".
  vector<test_query_buffer_t> write_query_buffers;
  int a_write_buffer[1] = {0};
  uint64_t a_write_buffer_size = sizeof(a_write_buffer);
  uint8_t a_write_buffer_validity[1] = {static_cast<uint8_t>(rand() % 2)};
  // a_write_buffer_validity[0] = rand() % 2;
  uint64_t a_write_buffer_validity_size = sizeof(a_write_buffer_validity);
  write_query_buffers.emplace_back(
      "a",
      a_write_buffer,
      &a_write_buffer_size,
      nullptr,
      nullptr,
      a_write_buffer_validity,
      &a_write_buffer_validity_size);

  // Define dimension query buffers for either sparse arrays or dense arrays
  // with an unordered write order.
  uint64_t d_write_buffer[2];
  uint64_t d_write_buffer_size;
  if (array_type == TILEDB_SPARSE || write_order == TILEDB_UNORDERED) {
    vector<uint64_t> d_write_vec;
    d_write_vec = {0, 1};

    REQUIRE(d_write_vec.size() == 2);
    d_write_buffer[0] = d_write_vec[0];
    d_write_buffer[1] = d_write_vec[1];
    d_write_buffer_size = sizeof(d_write_buffer);

    write_query_buffers.emplace_back(
        "d",
        d_write_buffer,
        &d_write_buffer_size,
        nullptr,
        nullptr,
        nullptr,
        nullptr);
  }

  // Execute the write query.
  write(array_name, write_query_buffers, write_order);

  // Define the read query buffers for "a".
  vector<test_query_buffer_t> read_query_buffers;
  int a_read_buffer[1] = {0};
  uint64_t a_read_buffer_size = sizeof(a_read_buffer);
  uint8_t a_read_buffer_validity[1] = {0};
  uint64_t a_read_buffer_validity_size = sizeof(a_read_buffer_validity);
  read_query_buffers.emplace_back(
      "a",
      a_read_buffer,
      &a_read_buffer_size,
      nullptr,
      nullptr,
      a_read_buffer_validity,
      &a_read_buffer_validity_size);

  // Execute a read query over the entire domain.
  const uint64_t subarray_full[] = {0, 0};
  read(array_name, read_query_buffers, subarray_full);

  // Each value in `a_read_buffer` corresponds to its index in
  // the original `a_write_buffer`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  REQUIRE(a_read_buffer_size == a_write_buffer_size);
  REQUIRE(a_read_buffer_validity_size == a_write_buffer_validity_size);
  uint8_t expected_a_read_buffer_validity[1];
  expected_a_read_buffer_validity[0] = a_write_buffer_validity[0];
  REQUIRE(!memcmp(
      a_read_buffer_validity,
      expected_a_read_buffer_validity,
      a_read_buffer_validity_size));
}

TEST_CASE_METHOD(
    DynamicArrayFx,
    "C API: Test 1D dense array with 1 attribute",
    "[capi][1d][dense]") {
  vector<test_attr_t> attrs;
  attrs.emplace_back("a", TILEDB_INT32, 1, true);

  tiledb_array_type_t array_type = TILEDB_DENSE;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t write_order = TILEDB_UNORDERED;

  dense_1d_test(attrs, array_type, cell_order, tile_order, write_order);
}
