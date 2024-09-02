/**
 * @file unit-cppapi-nullable.cc
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
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <iostream>
#include <vector>

using namespace std;
using namespace tiledb;
using namespace tiledb::test;

class NullableArrayCppFx {
 public:
  template <typename T>
  struct test_dim_t {
    test_dim_t(
        const string& name,
        const array<T, 2>& domain,
        const uint64_t tile_extent)
        : name_(name)
        , domain_(domain)
        , tile_extent_(tile_extent) {
    }

    string name_;
    array<T, 2> domain_;
    uint64_t tile_extent_;
  };

  template <typename T>
  struct test_attr_t {
    test_attr_t(const string& name, const bool var, const bool nullable)
        : name_(name)
        , var_(var)
        , nullable_(nullable) {
    }

    string name_;
    bool var_;
    bool nullable_;
  };

  template <typename T>
  struct test_query_buffer_t {
    test_query_buffer_t(const string& name, vector<T>* const data)
        : name_(name)
        , offsets_(nullptr)
        , data_(data)
        , validity_bytemap_(nullptr) {
    }

    test_query_buffer_t(
        const string& name,
        vector<T>* const data,
        vector<uint8_t>* const validity_bytemap)
        : name_(name)
        , offsets_(nullptr)
        , data_(data)
        , validity_bytemap_(std::move(validity_bytemap)) {
    }

    test_query_buffer_t(
        const string& name,
        vector<uint64_t>* const offsets,
        vector<T>* const data,
        vector<uint8_t>* const validity_bytemap)
        : name_(name)
        , offsets_(offsets)
        , data_(data)
        , validity_bytemap_(validity_bytemap) {
    }

    string name_;
    vector<uint64_t>* offsets_;
    vector<T>* data_;
    vector<uint8_t>* validity_bytemap_;
  };

  /**
   * Creates, writes, and reads nullable attributes.
   *
   * @param test_attrs The nullable attributes to test.
   * @param array_type The type of the array (dense/sparse).
   * @param cell_order The cell order of the array.
   * @param tile_order The tile order of the array.
   * @param write_order The write layout.
   */
  template <typename ATTR_T>
  void do_2d_nullable_test(
      const vector<test_attr_t<ATTR_T>>& test_attrs,
      tiledb_array_type_t array_type,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order,
      tiledb_layout_t write_order);

 private:
  /** The C++ API context object. */
  Context ctx_;

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
  template <typename DIM_T, typename ATTR_T>
  void create_array(
      const string& array_name,
      tiledb_array_type_t array_type,
      const vector<test_dim_t<DIM_T>>& test_dims,
      const vector<test_attr_t<ATTR_T>>& test_attrs,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order);

  /**
   * Creates and executes a single write query.
   *
   * @param array_name The name of the array.
   * @param test_query_buffers The query buffers to write.
   * @param layout The write layout.
   */
  template <typename ATTR_T>
  void write(
      const string& array_name,
      const vector<test_query_buffer_t<ATTR_T>>& test_query_buffers,
      tiledb_layout_t layout);

  /**
   * Creates and executes a single read query.
   *
   * @param array_name The name of the array.
   * @param test_query_buffers The query buffers to read.
   * @param subarray The subarray to read.
   */
  template <typename ATTR_T>
  void read(
      const string& array_name,
      const vector<test_query_buffer_t<ATTR_T>>& test_query_buffers,
      const vector<uint64_t>& subarray);
};

template <typename DIM_T, typename ATTR_T>
void NullableArrayCppFx::create_array(
    const string& array_name,
    const tiledb_array_type_t array_type,
    const vector<test_dim_t<DIM_T>>& test_dims,
    const vector<test_attr_t<ATTR_T>>& test_attrs,
    const tiledb_layout_t cell_order,
    const tiledb_layout_t tile_order) {
  // Create the domain.
  Domain domain(ctx_);

  // Create the dimensions.
  for (const auto& test_dim : test_dims) {
    domain.add_dimension(Dimension::create<DIM_T>(
        ctx_, test_dim.name_, test_dim.domain_, test_dim.tile_extent_));
  }

  // Create the array schema.
  ArraySchema schema(ctx_, array_type);
  schema.set_domain(domain);
  schema.set_cell_order(cell_order);
  schema.set_tile_order(tile_order);

  // Create the attributes.
  for (const auto& test_attr : test_attrs) {
    Attribute attr =
        test_attr.var_ ?
            Attribute::create<vector<ATTR_T>>(ctx_, test_attr.name_) :
            Attribute::create<ATTR_T>(ctx_, test_attr.name_);
    attr.set_nullable(test_attr.nullable_);
    schema.add_attribute(attr);
  }

  // Check the array schema.
  schema.check();

  // Create the array.
  Array::create(array_name, schema);
}

template <typename ATTR_T>
void NullableArrayCppFx::write(
    const string& array_name,
    const vector<test_query_buffer_t<ATTR_T>>& test_query_buffers,
    const tiledb_layout_t layout) {
  // Open the array for writing.
  Array array(ctx_, array_name, TILEDB_WRITE);
  REQUIRE(array.is_open());

  // Create the write query.
  Query query(ctx_, array, TILEDB_WRITE);

  // Set the query layout.
  query.set_layout(layout);

  // Set the query buffers.
  for (const auto& test_query_buffer : test_query_buffers) {
    if (!test_query_buffer.validity_bytemap_) {
      if (!test_query_buffer.offsets_) {
        query.set_data_buffer(
            test_query_buffer.name_, *test_query_buffer.data_);
      } else {
        query.set_data_buffer(
            test_query_buffer.name_, *test_query_buffer.data_);
        query.set_offsets_buffer(
            test_query_buffer.name_, *test_query_buffer.offsets_);
      }
    } else {
      if (!test_query_buffer.offsets_) {
        query.set_data_buffer(
            test_query_buffer.name_, *test_query_buffer.data_);
        query.set_validity_buffer(
            test_query_buffer.name_, *test_query_buffer.validity_bytemap_);
      } else {
        query.set_data_buffer(
            test_query_buffer.name_, *test_query_buffer.data_);
        query.set_offsets_buffer(
            test_query_buffer.name_, *test_query_buffer.offsets_);
        query.set_validity_buffer(
            test_query_buffer.name_, *test_query_buffer.validity_bytemap_);
      }
    }
  }

  // Submit query
  if (layout == TILEDB_GLOBAL_ORDER) {
    query.submit_and_finalize();
  } else {
    query.submit();
  }

  // Clean up
  array.close();
}

template <typename ATTR_T>
void NullableArrayCppFx::read(
    const string& array_name,
    const vector<test_query_buffer_t<ATTR_T>>& test_query_buffers,
    const vector<uint64_t>& subarray) {
  // Open the array for reading.
  Array array(ctx_, array_name, TILEDB_READ);
  REQUIRE(array.is_open());

  // Create the read query.
  Query query(ctx_, array, TILEDB_READ);

  // Set the query buffers.
  for (const auto& test_query_buffer : test_query_buffers) {
    if (!test_query_buffer.validity_bytemap_) {
      if (!test_query_buffer.offsets_) {
        query.set_data_buffer(
            test_query_buffer.name_, *test_query_buffer.data_);
      } else {
        query.set_data_buffer(
            test_query_buffer.name_, *test_query_buffer.data_);
        query.set_offsets_buffer(
            test_query_buffer.name_, *test_query_buffer.offsets_);
      }
    } else {
      if (!test_query_buffer.offsets_) {
        query.set_data_buffer(
            test_query_buffer.name_, *test_query_buffer.data_);
        query.set_validity_buffer(
            test_query_buffer.name_, *test_query_buffer.validity_bytemap_);
      } else {
        query.set_data_buffer(
            test_query_buffer.name_, *test_query_buffer.data_);
        query.set_offsets_buffer(
            test_query_buffer.name_, *test_query_buffer.offsets_);
        query.set_validity_buffer(
            test_query_buffer.name_, *test_query_buffer.validity_bytemap_);
      }
    }
  }

  // Set the subarray to read.
  Subarray sub(ctx_, array);
  sub.set_subarray(subarray);
  query.set_subarray(sub);

  // Submit the query.
  REQUIRE(query.submit() == Query::Status::COMPLETE);

  // Finalize the query, a no-op for non-global reads.
  query.finalize();

  // Clean up
  array.close();
}

template <typename ATTR_T>
void NullableArrayCppFx::do_2d_nullable_test(
    const vector<test_attr_t<ATTR_T>>& test_attrs,
    const tiledb_array_type_t array_type,
    const tiledb_layout_t cell_order,
    const tiledb_layout_t tile_order,
    const tiledb_layout_t write_order) {
  VFSTestSetup vfs_test_setup{};
  ctx_ = vfs_test_setup.ctx();
  auto array_name{vfs_test_setup.array_uri("cpp_2d_nullable_array")};

  // Skip row-major and col-major writes for sparse arrays.
  if (array_type == TILEDB_SPARSE &&
      (write_order == TILEDB_ROW_MAJOR || write_order == TILEDB_COL_MAJOR)) {
    return;
  }

  // Skip unordered writes for dense arrays.
  if (array_type == TILEDB_DENSE && write_order == TILEDB_UNORDERED) {
    return;
  }

  // Define the dimensions.
  vector<test_dim_t<uint64_t>> test_dims;
  const array<uint64_t, 2> d1_domain = {1, 4};
  const uint64_t d1_tile_extent = 2;
  test_dims.emplace_back("d1", d1_domain, d1_tile_extent);
  const array<uint64_t, 2> d2_domain = {1, 4};
  const uint64_t d2_tile_extent = 2;
  test_dims.emplace_back("d2", d2_domain, d2_tile_extent);

  // Create the array.
  create_array(
      array_name, array_type, test_dims, test_attrs, cell_order, tile_order);

  // Define the write query buffers for "a1".
  vector<test_query_buffer_t<ATTR_T>> write_query_buffers;
  vector<ATTR_T> a1_write_buffer;
  for (int i = 0; i < 16; ++i)
    a1_write_buffer.emplace_back(i);
  vector<uint8_t> a1_write_buffer_validity;
  for (int i = 0; i < 16; ++i)
    a1_write_buffer_validity.emplace_back(rand() % 2);
  write_query_buffers.emplace_back(
      "a1", &a1_write_buffer, &a1_write_buffer_validity);

  // Define the write query buffers for "a2".
  vector<ATTR_T> a2_write_buffer(a1_write_buffer.size());
  for (size_t i = 0; i < a1_write_buffer.size(); ++i)
    a2_write_buffer[i] = a1_write_buffer[a1_write_buffer.size() - 1 - i];
  vector<uint8_t> a2_write_buffer_validity(a1_write_buffer_validity.size());
  for (size_t i = 0; i < a1_write_buffer_validity.size(); ++i)
    a2_write_buffer_validity[i] =
        a1_write_buffer_validity[a1_write_buffer_validity.size() - 1 - i];
  if (test_attrs.size() >= 2) {
    write_query_buffers.emplace_back(
        "a2", &a2_write_buffer, &a2_write_buffer_validity);
  }

  // Define the write query buffers for "a3".
  vector<uint64_t> a3_write_buffer;
  for (uint64_t i = 0; i < 16; ++i)
    a3_write_buffer.emplace_back(i * sizeof(ATTR_T) * 2);
  vector<ATTR_T> a3_write_buffer_var;
  for (int i = 0; i < 32; ++i)
    a3_write_buffer_var.emplace_back(i);
  vector<uint8_t> a3_write_buffer_validity;
  for (int i = 0; i < 16; ++i)
    a3_write_buffer_validity.emplace_back(rand() % 2);
  if (test_attrs.size() >= 3) {
    write_query_buffers.emplace_back(
        "a3",
        &a3_write_buffer,
        &a3_write_buffer_var,
        &a3_write_buffer_validity);
  }

  // Define dimension query buffers for either sparse arrays or dense arrays
  // with an unordered write order.
  vector<uint64_t> d1_write_buffer;
  vector<uint64_t> d2_write_buffer;
  if (array_type == TILEDB_SPARSE || write_order == TILEDB_UNORDERED) {
    // Coordinates for sparse arrays written in global order have unique
    // ordering when either/both cell and tile ordering is col-major.
    if (array_type == TILEDB_SPARSE && write_order == TILEDB_GLOBAL_ORDER &&
        (cell_order == TILEDB_COL_MAJOR || tile_order == TILEDB_COL_MAJOR)) {
      if (cell_order == TILEDB_ROW_MAJOR && tile_order == TILEDB_COL_MAJOR) {
        d1_write_buffer = {1, 1, 2, 2, 3, 3, 4, 4, 1, 1, 2, 2, 3, 3, 4, 4};
        d2_write_buffer = {1, 2, 1, 2, 1, 2, 1, 2, 3, 4, 3, 4, 3, 4, 3, 4};
      } else if (
          cell_order == TILEDB_COL_MAJOR && tile_order == TILEDB_ROW_MAJOR) {
        d1_write_buffer = {1, 2, 1, 2, 1, 2, 1, 2, 3, 4, 3, 4, 3, 4, 3, 4};
        d2_write_buffer = {1, 1, 2, 2, 3, 3, 4, 4, 1, 1, 2, 2, 3, 3, 4, 4};
      } else {
        REQUIRE(cell_order == TILEDB_COL_MAJOR);
        REQUIRE(tile_order == TILEDB_COL_MAJOR);
        d1_write_buffer = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 3, 4};
        d2_write_buffer = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4, 4};
      }
    } else {
      d1_write_buffer = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4, 4};
      d2_write_buffer = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 3, 4};
    }

    write_query_buffers.emplace_back("d1", &d1_write_buffer);
    write_query_buffers.emplace_back("d2", &d2_write_buffer);
  }

  // Execute the write query.
  write(array_name, write_query_buffers, write_order);

  // Define the read query buffers for "a1".
  vector<test_query_buffer_t<uint64_t>> read_query_buffers;
  vector<ATTR_T> a1_read_buffer(16);
  vector<uint8_t> a1_read_buffer_validity(16);
  read_query_buffers.emplace_back(
      "a1", &a1_read_buffer, &a1_read_buffer_validity);

  // Define the read query buffers for "a2".
  vector<ATTR_T> a2_read_buffer(16);
  vector<uint8_t> a2_read_buffer_validity(16);
  if (test_attrs.size() >= 2) {
    read_query_buffers.emplace_back(
        "a2", &a2_read_buffer, &a2_read_buffer_validity);
  }

  // Define the read query buffers for "a3".
  vector<uint64_t> a3_read_buffer(16);
  vector<ATTR_T> a3_read_buffer_var(32);
  vector<uint8_t> a3_read_buffer_validity(16);
  if (test_attrs.size() >= 3) {
    read_query_buffers.emplace_back(
        "a3", &a3_read_buffer, &a3_read_buffer_var, &a3_read_buffer_validity);
  }

  // Execute a read query over the entire domain.
  const vector<uint64_t> subarray_full = {1, 4, 1, 4};
  read(array_name, read_query_buffers, subarray_full);

  // Each value in `a1_read_buffer` corresponds to its index in
  // the original `a1_write_buffer`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  REQUIRE(a1_read_buffer.size() == a1_write_buffer.size());
  REQUIRE(a1_read_buffer_validity.size() == a1_write_buffer_validity.size());
  vector<uint8_t> expected_a1_read_buffer_validity(16);
  for (int i = 0; i < 16; ++i) {
    const uint64_t idx = a1_read_buffer[i];
    expected_a1_read_buffer_validity[i] = a1_write_buffer_validity[idx];
  }
  REQUIRE(!memcmp(
      a1_read_buffer_validity.data(),
      expected_a1_read_buffer_validity.data(),
      a1_read_buffer_validity.size()));

  // Each value in `a2_read_buffer` corresponds to its reversed index in
  // the original `a2_write_buffer`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  if (test_attrs.size() >= 2) {
    REQUIRE(a2_read_buffer.size() == a2_write_buffer.size());
    REQUIRE(a2_read_buffer_validity.size() == a2_write_buffer_validity.size());
    vector<uint8_t> expected_a2_read_buffer_validity(16);
    for (int i = 0; i < 16; ++i) {
      const uint64_t idx = a2_read_buffer[16 - 1 - i];
      expected_a2_read_buffer_validity[i] = a2_write_buffer_validity[idx];
    }
    REQUIRE(!memcmp(
        a2_read_buffer_validity.data(),
        expected_a2_read_buffer_validity.data(),
        a2_read_buffer_validity.size()));
  }

  // Each value in `a3_read_buffer_var` corresponds to its index in
  // the original `a3_write_buffer_var`. Check that the ordering of
  // the validity buffer matches the ordering in the value buffer.
  if (test_attrs.size() >= 3) {
    REQUIRE(a3_read_buffer.size() == a3_write_buffer.size());
    REQUIRE(a3_read_buffer_var.size() == a3_write_buffer_var.size());
    REQUIRE(a3_read_buffer_validity.size() == a3_write_buffer_validity.size());
    vector<uint8_t> expected_a3_read_buffer_validity(16);
    for (int i = 0; i < 16; ++i) {
      const uint64_t idx = a3_read_buffer_var[i * 2] / 2;
      expected_a3_read_buffer_validity[i] = a3_write_buffer_validity[idx];
    }

    REQUIRE(!memcmp(
        a3_read_buffer_validity.data(),
        expected_a3_read_buffer_validity.data(),
        a3_read_buffer_validity.size()));
  }
}

TEST_CASE_METHOD(
    NullableArrayCppFx,
    "C++ API: Test 2D array with nullable attributes",
    "[cppapi][2d][nullable][rest]") {
  vector<test_attr_t<uint64_t>> attrs;
  attrs.emplace_back("a1", false /* var */, true /* nullable */);
  attrs.emplace_back("a2", false /* var */, true /* nullable */);
  attrs.emplace_back("a3", true /* var */, true /* nullable */);
  for (auto attr_iter = attrs.begin(); attr_iter != attrs.end(); ++attr_iter) {
    vector<test_attr_t<uint64_t>> test_attrs(attrs.begin(), attr_iter + 1);
    for (const tiledb_array_type_t array_type : {TILEDB_DENSE, TILEDB_SPARSE}) {
      for (const tiledb_layout_t cell_order :
           {TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR}) {
        for (const tiledb_layout_t tile_order :
             {TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR}) {
          for (const tiledb_layout_t write_order :
               {TILEDB_ROW_MAJOR,
                TILEDB_COL_MAJOR,
                TILEDB_UNORDERED,
                TILEDB_GLOBAL_ORDER}) {
            do_2d_nullable_test(
                test_attrs, array_type, cell_order, tile_order, write_order);
          }
        }
      }
    }
  }
}
