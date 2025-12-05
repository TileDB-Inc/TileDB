/**
 * @file unit-capi-nullable.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2024 TileDB, Inc.
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
 * Tests arrays with nullable attributes.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/temporary_local_directory.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/vfs/vfs_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/enums/array_type.h"

#include <iostream>
#include <optional>
#include <vector>

using namespace std;
using namespace tiledb::sm;
using namespace tiledb::test;

class NullableArrayFx {
 public:
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

  /** Constructor. */
  NullableArrayFx();

  /** Destructor. */
  ~NullableArrayFx();

  /**
   * Creates, writes, and reads nullable attributes.
   *
   * @param test_attrs The nullable attributes to test.
   * @param array_type The type of the array (dense/sparse).
   * @param cell_order The cell order of the array.
   * @param tile_order The tile order of the array.
   * @param write_order The write layout.
   */
  void do_2d_nullable_test(
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

  /** The unique local directory object. */
  TemporaryLocalDirectory temp_dir_;

  /**
   * Compute the full array path given an array name.
   *
   * @param array_name The array name.
   * @return The full array path.
   */
  const string array_path(const string& array_name);

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

NullableArrayFx::NullableArrayFx() {
  // Create a config.
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  throw_if_setup_failed(tiledb_config_alloc(&config, &error));
  throw_if_setup_failed(error == nullptr);

  // Create the context.
  throw_if_setup_failed(tiledb_ctx_alloc(config, &ctx_));
  throw_if_setup_failed(ctx_ != nullptr);

  // Create the VFS.
  throw_if_setup_failed(tiledb_vfs_alloc(ctx_, config, &vfs_));
  throw_if_setup_failed(vfs_ != nullptr);
  tiledb_config_free(&config);
}

NullableArrayFx::~NullableArrayFx() {
  vfs_test_remove_temp_dir(ctx_, vfs_, temp_dir_.path());
  CHECK(vfs_test_close(vfs_test_get_fs_vec(), ctx_, vfs_).ok());

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

const string NullableArrayFx::array_path(const string& array_name) {
  return vfs_array_uri(
      vfs_test_get_fs_vec()[0], temp_dir_.path() + array_name, ctx_);
}

void NullableArrayFx::create_array(
    const string& array_name,
    const tiledb_array_type_t array_type,
    const vector<test_dim_t>& test_dims,
    const vector<test_attr_t>& test_attrs,
    const tiledb_layout_t cell_order,
    const tiledb_layout_t tile_order) {
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

  // Create array
  rc = tiledb_array_create(ctx_, array_path(array_name).c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Free attributes.
  for (auto& attr : attrs) {
    tiledb_attribute_free(&attr);
  }

  // Free dimensions.
  for (auto& dim : dims) {
    tiledb_dimension_free(&dim);
  }

  // Free the domain.
  tiledb_domain_free(&domain);

  // Free the array schema.
  tiledb_array_schema_free(&array_schema);
}

void NullableArrayFx::write(
    const string& array_name,
    const vector<test_query_buffer_t>& test_query_buffers,
    const tiledb_layout_t layout) {
  // Open the array for writing.
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_path(array_name).c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

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
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_var_,
            test_query_buffer.buffer_var_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_offsets_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    } else {
      if (test_query_buffer.buffer_var_ == nullptr) {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_validity_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_var_,
            test_query_buffer.buffer_var_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_offsets_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_validity_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    }
  }

  // Submit query
  if (layout != TILEDB_GLOBAL_ORDER) {
    rc = tiledb_query_submit(ctx_, query);
  } else {
    rc = tiledb_query_submit_and_finalize(ctx_, query);
  }
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void NullableArrayFx::read(
    const string& array_name,
    const vector<test_query_buffer_t>& test_query_buffers,
    const void* const subarray) {
  // Open the array for reading.
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_path(array_name).c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Create the read query.
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set the query buffers.
  for (size_t i = 0; i < test_query_buffers.size(); ++i) {
    const test_query_buffer_t& test_query_buffer = test_query_buffers[i];
    if (test_query_buffer.buffer_validity_size_ == nullptr) {
      if (test_query_buffer.buffer_var_ == nullptr) {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_var_,
            test_query_buffer.buffer_var_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_offsets_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    } else {
      if (test_query_buffer.buffer_var_ == nullptr) {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_validity_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_var_,
            test_query_buffer.buffer_var_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_offsets_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_validity_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    }
  }

  // Set the subarray to read.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void NullableArrayFx::do_2d_nullable_test(
    const vector<test_attr_t>& test_attrs,
    const tiledb_array_type_t array_type,
    const tiledb_layout_t cell_order,
    const tiledb_layout_t tile_order,
    const tiledb_layout_t write_order) {
  const string array_name = "2d_nullable_array";

  // Skip row-major and col-major writes for sparse arrays.
  if (array_type == TILEDB_SPARSE &&
      (write_order == TILEDB_ROW_MAJOR || write_order == TILEDB_COL_MAJOR)) {
    return;
  }

  if (array_type == TILEDB_DENSE && (write_order == TILEDB_UNORDERED)) {
    return;
  }

  // Define the dimensions.
  vector<test_dim_t> test_dims;
  const uint64_t d1_domain[] = {1, 4};
  const uint64_t d1_tile_extent = 2;
  test_dims.emplace_back("d1", TILEDB_UINT64, d1_domain, d1_tile_extent);
  const uint64_t d2_domain[] = {1, 4};
  const uint64_t d2_tile_extent = 2;
  test_dims.emplace_back("d2", TILEDB_UINT64, d2_domain, d2_tile_extent);

  // Create the array.
  create_array(
      array_name, array_type, test_dims, test_attrs, cell_order, tile_order);

  // Define the write query buffers for "a1".
  vector<test_query_buffer_t> write_query_buffers;
  int a1_write_buffer[16];
  for (int i = 0; i < 16; ++i)
    a1_write_buffer[i] = i;
  uint64_t a1_write_buffer_size = sizeof(a1_write_buffer);
  uint8_t a1_write_buffer_validity[16];
  for (int i = 0; i < 16; ++i)
    a1_write_buffer_validity[i] = rand() % 2;
  uint64_t a1_write_buffer_validity_size = sizeof(a1_write_buffer_validity);
  write_query_buffers.emplace_back(
      "a1",
      a1_write_buffer,
      &a1_write_buffer_size,
      nullptr,
      nullptr,
      a1_write_buffer_validity,
      &a1_write_buffer_validity_size);

  // Define the write query buffers for "a2".
  int a2_write_buffer[16];
  for (int i = 0; i < 16; ++i)
    a2_write_buffer[i] = a1_write_buffer[16 - 1 - i];
  uint64_t a2_write_buffer_size = sizeof(a2_write_buffer);
  uint8_t a2_write_buffer_validity[16];
  for (uint64_t i = 0; i < 16; ++i)
    a2_write_buffer_validity[i] = a1_write_buffer_validity[16 - 1 - i];
  uint64_t a2_write_buffer_validity_size = sizeof(a2_write_buffer_validity);
  if (test_attrs.size() >= 2) {
    write_query_buffers.emplace_back(
        "a2",
        a2_write_buffer,
        &a2_write_buffer_size,
        nullptr,
        nullptr,
        a2_write_buffer_validity,
        &a2_write_buffer_validity_size);
  }

  // Define the write query buffers for "a3".
  uint64_t a3_write_buffer[16];
  for (uint64_t i = 0; i < 16; ++i)
    a3_write_buffer[i] = i * sizeof(int) * 2;
  uint64_t a3_write_buffer_size = sizeof(a3_write_buffer);
  int a3_write_buffer_var[32];
  for (int i = 0; i < 32; ++i)
    a3_write_buffer_var[i] = i;
  uint64_t a3_write_buffer_var_size = sizeof(a3_write_buffer_var);
  uint8_t a3_write_buffer_validity[16];
  for (int i = 0; i < 16; ++i)
    a3_write_buffer_validity[i] = rand() % 2;
  uint64_t a3_write_buffer_validity_size = sizeof(a3_write_buffer_validity);
  if (test_attrs.size() >= 3) {
    write_query_buffers.emplace_back(
        "a3",
        a3_write_buffer,
        &a3_write_buffer_size,
        a3_write_buffer_var,
        &a3_write_buffer_var_size,
        a3_write_buffer_validity,
        &a3_write_buffer_validity_size);
  }

  // Define dimension query buffers for either sparse arrays or dense arrays
  // with an unordered write order.
  uint64_t d1_write_buffer[16];
  uint64_t d2_write_buffer[16];
  uint64_t d1_write_buffer_size;
  uint64_t d2_write_buffer_size;
  if (array_type == TILEDB_SPARSE || write_order == TILEDB_UNORDERED) {
    vector<uint64_t> d1_write_vec;
    vector<uint64_t> d2_write_vec;

    // Coordinates for sparse arrays written in global order have unique
    // ordering when either/both cell and tile ordering is col-major.
    if (array_type == TILEDB_SPARSE && write_order == TILEDB_GLOBAL_ORDER &&
        (cell_order == TILEDB_COL_MAJOR || tile_order == TILEDB_COL_MAJOR)) {
      if (cell_order == TILEDB_ROW_MAJOR && tile_order == TILEDB_COL_MAJOR) {
        d1_write_vec = {1, 1, 2, 2, 3, 3, 4, 4, 1, 1, 2, 2, 3, 3, 4, 4};
        d2_write_vec = {1, 2, 1, 2, 1, 2, 1, 2, 3, 4, 3, 4, 3, 4, 3, 4};
      } else if (
          cell_order == TILEDB_COL_MAJOR && tile_order == TILEDB_ROW_MAJOR) {
        d1_write_vec = {1, 2, 1, 2, 1, 2, 1, 2, 3, 4, 3, 4, 3, 4, 3, 4};
        d2_write_vec = {1, 1, 2, 2, 3, 3, 4, 4, 1, 1, 2, 2, 3, 3, 4, 4};
      } else {
        REQUIRE(cell_order == TILEDB_COL_MAJOR);
        REQUIRE(tile_order == TILEDB_COL_MAJOR);
        d1_write_vec = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 3, 4};
        d2_write_vec = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4, 4};
      }
    } else {
      d1_write_vec = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4, 4};
      d2_write_vec = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 3, 4};
    }

    REQUIRE(d1_write_vec.size() == 16);
    for (size_t i = 0; i < 16; ++i)
      d1_write_buffer[i] = d1_write_vec[i];
    d1_write_buffer_size = sizeof(d1_write_buffer);
    write_query_buffers.emplace_back(
        "d1",
        d1_write_buffer,
        &d1_write_buffer_size,
        nullptr,
        nullptr,
        nullptr,
        nullptr);

    REQUIRE(d2_write_vec.size() == 16);
    for (size_t i = 0; i < 16; ++i)
      d2_write_buffer[i] = d2_write_vec[i];
    d2_write_buffer_size = sizeof(d2_write_buffer);
    write_query_buffers.emplace_back(
        "d2",
        d2_write_buffer,
        &d2_write_buffer_size,
        nullptr,
        nullptr,
        nullptr,
        nullptr);
  }

  // Execute the write query.
  write(array_name, write_query_buffers, write_order);

  // Define the read query buffers for "a1".
  vector<test_query_buffer_t> read_query_buffers;
  int a1_read_buffer[16] = {0};
  uint64_t a1_read_buffer_size = sizeof(a1_read_buffer);
  uint8_t a1_read_buffer_validity[16] = {0};
  uint64_t a1_read_buffer_validity_size = sizeof(a1_read_buffer_validity);
  read_query_buffers.emplace_back(
      "a1",
      a1_read_buffer,
      &a1_read_buffer_size,
      nullptr,
      nullptr,
      a1_read_buffer_validity,
      &a1_read_buffer_validity_size);

  // Define the read query buffers for "a2".
  int a2_read_buffer[16] = {0};
  uint64_t a2_read_buffer_size = sizeof(a2_read_buffer);
  uint8_t a2_read_buffer_validity[16] = {0};
  uint64_t a2_read_buffer_validity_size = sizeof(a2_read_buffer_validity);
  if (test_attrs.size() >= 2) {
    read_query_buffers.emplace_back(
        "a2",
        a2_read_buffer,
        &a2_read_buffer_size,
        nullptr,
        nullptr,
        a2_read_buffer_validity,
        &a2_read_buffer_validity_size);
  }

  // Define the read query buffers for "a3".
  uint64_t a3_read_buffer[16] = {0};
  uint64_t a3_read_buffer_size = sizeof(a3_read_buffer);
  int a3_read_buffer_var[32] = {0};
  uint64_t a3_read_buffer_var_size = sizeof(a3_read_buffer);
  uint8_t a3_read_buffer_validity[16] = {0};
  uint64_t a3_read_buffer_validity_size = sizeof(a3_read_buffer_validity);
  if (test_attrs.size() >= 3) {
    read_query_buffers.emplace_back(
        "a3",
        a3_read_buffer,
        &a3_read_buffer_size,
        a3_read_buffer_var,
        &a3_read_buffer_var_size,
        a3_read_buffer_validity,
        &a3_read_buffer_validity_size);
  }

  // Execute a read query over the entire domain.
  const uint64_t subarray_full[] = {1, 4, 1, 4};
  read(array_name, read_query_buffers, subarray_full);

  // Each value in `a1_read_buffer` corresponds to its index in
  // the original `a1_write_buffer`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  REQUIRE(a1_read_buffer_size == a1_write_buffer_size);
  REQUIRE(a1_read_buffer_validity_size == a1_write_buffer_validity_size);
  uint8_t expected_a1_read_buffer_validity[16];
  for (int i = 0; i < 16; ++i) {
    const uint64_t idx = a1_read_buffer[i];
    expected_a1_read_buffer_validity[i] = a1_write_buffer_validity[idx];
  }

  REQUIRE(!memcmp(
      a1_read_buffer_validity,
      expected_a1_read_buffer_validity,
      a1_read_buffer_validity_size));

  // Each value in `a2_read_buffer` corresponds to its reversed index in
  // the original `a2_write_buffer`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  if (test_attrs.size() >= 2) {
    REQUIRE(a2_read_buffer_size == a2_write_buffer_size);
    REQUIRE(a2_read_buffer_validity_size == a2_write_buffer_validity_size);
    uint8_t expected_a2_read_buffer_validity[16];
    for (int i = 0; i < 16; ++i) {
      const uint64_t idx = a2_read_buffer[16 - 1 - i];
      expected_a2_read_buffer_validity[i] = a2_write_buffer_validity[idx];
    }
    REQUIRE(!memcmp(
        a2_read_buffer_validity,
        expected_a2_read_buffer_validity,
        a2_read_buffer_validity_size));
  }

  // Each value in `a3_read_buffer_var` corresponds to its index in
  // the original `a3_write_buffer_var`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  if (test_attrs.size() >= 3) {
    REQUIRE(a3_read_buffer_size == a3_write_buffer_size);
    REQUIRE(a3_read_buffer_var_size == a3_write_buffer_var_size);
    REQUIRE(a3_read_buffer_validity_size == a3_write_buffer_validity_size);
    uint8_t expected_a3_read_buffer_validity[16];
    for (int i = 0; i < 16; ++i) {
      const uint64_t idx = a3_read_buffer_var[i * 2] / 2;
      expected_a3_read_buffer_validity[i] = a3_write_buffer_validity[idx];
    }
    REQUIRE(!memcmp(
        a3_read_buffer_validity,
        expected_a3_read_buffer_validity,
        a3_read_buffer_validity_size));
  }

  // Execute a read query over a partial domain.
  const uint64_t subarray_partial[] = {2, 3, 2, 3};
  read(array_name, read_query_buffers, subarray_partial);

  // Each value in `a1_read_buffer` corresponds to its index in
  // the original `a1_write_buffer`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  REQUIRE(a1_read_buffer_size == a1_write_buffer_size / 4);
  REQUIRE(a1_read_buffer_validity_size == a1_write_buffer_validity_size / 4);
  for (int i = 0; i < 4; ++i) {
    const uint64_t idx = a1_read_buffer[i];
    expected_a1_read_buffer_validity[i] = a1_write_buffer_validity[idx];
  }
  REQUIRE(!memcmp(
      a1_read_buffer_validity,
      expected_a1_read_buffer_validity,
      a1_read_buffer_validity_size));

  // Each value in `a2_read_buffer` corresponds to its reversed index in
  // the original `a2_write_buffer`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  if (test_attrs.size() >= 2) {
    REQUIRE(a2_read_buffer_size == a2_write_buffer_size / 4);
    REQUIRE(a2_read_buffer_validity_size == a2_write_buffer_validity_size / 4);
    uint8_t expected_a2_read_buffer_validity[4];
    for (int i = 0; i < 4; ++i) {
      const uint64_t idx = a2_read_buffer[4 - 1 - i];
      expected_a2_read_buffer_validity[i] = a2_write_buffer_validity[idx];
    }
    REQUIRE(!memcmp(
        a2_read_buffer_validity,
        expected_a2_read_buffer_validity,
        a2_read_buffer_validity_size));
  }

  // Each value in `a3_read_buffer_var` corresponds to its index in
  // the original `a3_write_buffer_var`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  if (test_attrs.size() >= 3) {
    REQUIRE(a3_read_buffer_size == a3_write_buffer_size / 4);
    REQUIRE(a3_read_buffer_var_size == a3_write_buffer_var_size / 4);
    REQUIRE(a3_read_buffer_validity_size == a3_write_buffer_validity_size / 4);
    uint8_t expected_a3_read_buffer_validity[4];
    for (int i = 0; i < 4; ++i) {
      const uint64_t idx = a3_read_buffer_var[i * 2] / 2;
      expected_a3_read_buffer_validity[i] = a3_write_buffer_validity[idx];
    }

    REQUIRE(!memcmp(
        a3_read_buffer_validity,
        expected_a3_read_buffer_validity,
        a3_read_buffer_validity_size));
  }
}

// TODO: Add [rest] tag and fix test issues with cleanup
// because of the use of dynamic section
TEST_CASE_METHOD(
    NullableArrayFx,
    "C API: Test 2D array with nullable attributes",
    "[capi][2d][nullable]") {
  // Define the attributes.
  vector<test_attr_t> attrs;
  attrs.emplace_back("a1", TILEDB_INT32, 1, true);
  attrs.emplace_back("a2", TILEDB_INT32, 1, true);
  attrs.emplace_back("a3", TILEDB_INT32, TILEDB_VAR_NUM, true);

  // Generate test conditions
  auto num_attrs{GENERATE(1, 2, 3)};
  auto array_type{GENERATE(TILEDB_DENSE, TILEDB_SPARSE)};
  auto cell_order{GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR)};
  auto tile_order{GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR)};
  auto write_order{GENERATE(
      TILEDB_ROW_MAJOR,
      TILEDB_COL_MAJOR,
      TILEDB_UNORDERED,
      TILEDB_GLOBAL_ORDER)};

  DYNAMIC_SECTION(
      array_type_str((ArrayType)array_type)
      << " array with " << num_attrs << " attribute(s). "
      << layout_str((Layout)cell_order) << " cells, "
      << layout_str((Layout)tile_order) << " tiles, "
      << layout_str((Layout)write_order) << " writes") {
    vector<test_attr_t> test_attrs(attrs.begin(), attrs.begin() + num_attrs);
    do_2d_nullable_test(
        test_attrs, array_type, cell_order, tile_order, write_order);
  }
}
