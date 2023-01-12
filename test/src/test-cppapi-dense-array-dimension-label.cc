/**
 * @file test-cppapi-dense-array-dimension-label.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Test the dimension label C++ API with a dense array using fixed size
 * dimension labels.
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"

#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

#include <test/support/tdb_catch.h>
#include <iostream>
#include <string>

using namespace tiledb::sm;
using namespace tiledb::test;

/**
 * Create a small dense array with a dimension label.
 *
 * Array Summary:
 *  * Array Type: Dense
 *  * Dimensions:
 *    - x: (type=UINT64, domain=[0, 3], tile=4)
 *  * Attributes:
 *    - a: (type=FLOAT64)
 *  * Dimension labels:
 *    - x: (data_order=data_order, dim_idx=0, type=FLOAT64)
 */
template <tiledb_data_order_t data_order>
class DenseArrayExample {
 public:
  /**
   * Constructor: Create a temporary directory with the example array.
   */
  DenseArrayExample()
      : tmpdir_{}
      , ctx_{tmpdir_.get_ctx(), false}
      , array_name_{tmpdir_.fullpath("dense_array_with_label")}
      , index_domain_{0, 3} {
    tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
    tiledb::Domain domain(ctx_);
    auto d1 =
        tiledb::Dimension::create<uint64_t>(ctx_, "dim", index_domain_, 4);
    domain.add_dimension(d1);
    schema.set_domain(domain);
    auto a1 = tiledb::Attribute::create<double>(ctx_, "a");
    schema.add_attribute(a1);
    tiledb::ArraySchemaExperimental::add_dimension_label(
        ctx_, schema, 0, "x", data_order, TILEDB_FLOAT64);
    tiledb::Array::create(array_name_, schema);
  }

  /**
   * Write data to the array and dimension label.
   *
   * @param index_start Starting index value
   * @param index_end Ending index value
   * @param input_attr_data Data to write to the array attribute. If empty, the
   *     attribute data is not added to the query.
   * @param input_label_data Data to write to the dimension label. If empty, the
   *     dimension label data is not added to the query.
   * @param error_on_write If true, require the query returns a failed status.
   */
  void write_by_index(
      uint64_t index_start,
      uint64_t index_end,
      std::vector<double> input_attr_data,
      std::vector<double> input_label_data,
      bool error_on_write = false) {
    // Open array for writing.
    tiledb::Array array{ctx_, array_name_, TILEDB_WRITE};

    // Create the subarray.
    tiledb::Subarray subarray{ctx_, array};
    subarray.add_range(0, index_start, index_end);

    // Create the query.
    tiledb::Query query{ctx_, array, TILEDB_WRITE};
    query.set_layout(TILEDB_ROW_MAJOR).set_subarray(subarray);
    if (!input_attr_data.empty()) {
      query.set_data_buffer("a", input_attr_data);
    }
    if (!input_label_data.empty()) {
      query.set_data_buffer("x", input_label_data);
    }

    // Submit write query.
    if (error_on_write) {
      REQUIRE_THROWS(query.submit());
    } else {
      query.submit();
      REQUIRE(query.query_status() == tiledb::Query::Status::COMPLETE);
    }
  }

  /**
   * Write data to the array and dimension label by label.
   *
   * @param label_start Starting label value
   * @param label_end Ending label value
   * @param input_attr_data Data to write to the array attribute. If empty, the
   *     attribute data is not added to the query.
   * @param input_label_data Data to write to the dimension label. If empty, the
   *     dimension label data is not added to the query.
   * @param error_on_write If true, require the query returns a failed status.
   */
  void write_by_label(
      double label_start,
      double label_end,
      std::vector<double> input_attr_data,
      std::vector<double> input_label_data,
      bool error_on_write = false) {
    // Open array for writing.
    tiledb::Array array{ctx_, array_name_, TILEDB_WRITE};

    // Create the subarray.
    tiledb::Subarray subarray{ctx_, array};
    tiledb::SubarrayExperimental::add_label_range(
        ctx_, subarray, "x", label_start, label_end);

    // Create the query.
    tiledb::Query query{ctx_, array, TILEDB_WRITE};
    query.set_layout(TILEDB_ROW_MAJOR).set_subarray(subarray);
    if (!input_attr_data.empty()) {
      query.set_data_buffer("a", input_attr_data);
    }
    if (!input_label_data.empty()) {
      query.set_data_buffer("x", input_label_data);
    }

    // Submit write query.
    if (error_on_write) {
      REQUIRE_THROWS(query.submit());
    } else {
      query.submit();
      REQUIRE(query.query_status() == tiledb::Query::Status::COMPLETE);
    }
  }

  /**
   * Read back full array with a data query and check the values.
   *
   * @param index_start Starting value to read data from.
   * @param index_end Ending value to read data from.
   * @param expected_label_data A vector of the expected label values.
   * @param expected_attr_data A vector of the expected attribute values. If
   *     empty, do not read attribute data.
   */
  void read_and_check_values(
      uint64_t index_start,
      uint64_t index_end,
      const std::vector<double>& expected_label_data,
      const std::vector<double>& expected_attr_data) {
    // Open array for writing.
    tiledb::Array array{ctx_, array_name_, TILEDB_READ};

    // Create the subarray.
    tiledb::Subarray subarray{ctx_, array};
    subarray.add_range(0, index_start, index_end);

    // Create vectors for output data.
    std::vector<double> attr_data(expected_attr_data.size());
    std::vector<double> label_data(expected_label_data.size());

    // Create the query.
    tiledb::Query query{ctx_, array, TILEDB_READ};
    query.set_layout(TILEDB_ROW_MAJOR).set_subarray(subarray);
    if (!expected_attr_data.empty()) {
      query.set_data_buffer("a", attr_data);
    }
    if (!expected_label_data.empty()) {
      query.set_data_buffer("x", label_data);
    }

    // Submit the query.
    query.submit();
    CHECK(query.query_status() == tiledb::Query::Status::COMPLETE);

    // Check results.
    CHECK(label_data == expected_label_data);
    if (!expected_attr_data.empty()) {
      CHECK(attr_data == expected_attr_data);
    }
  }

 protected:
  /** Fixture for temporary directory. */
  TemporaryDirectoryFixture tmpdir_;

  /** TileDB Context. */
  tiledb::Context ctx_;

  /** Name of the example array. */
  std::string array_name_;

  /** Valid range for the index. */
  std::array<uint64_t, 2> index_domain_;
};

TEST_CASE(
    "CPP-API: Round trip increasing dimension label data for dense 1d array",
    "[cppapi][query][DimensionLabel]") {
  // Create array.
  DenseArrayExample<TILEDB_INCREASING_DATA> array_fixture{};

  // Create vectors for input data.
  std::vector<double> input_label_data{-1.0, 0.0, 0.5, 1.0};
  std::vector<double> input_attr_data{};
  SECTION("With array data") {
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }
  SECTION("Without array data") {
    input_attr_data = {};
  }

  // Create and write the array.
  array_fixture.write_by_index(0, 3, input_attr_data, input_label_data);

  // Read back the values and check as expected.
  array_fixture.read_and_check_values(0, 3, input_label_data, input_attr_data);
}

TEST_CASE(
    "CPP-API: Round trip decreasing dimension label data for dense 1d array",
    "[cppapi][query][DimensionLabel]") {
  // Create array.
  DenseArrayExample<TILEDB_DECREASING_DATA> array_fixture{};

  // Create vectors for input data.
  std::vector<double> input_label_data{1.0, 0.0, -0.5, -1.0};
  std::vector<double> input_attr_data{};
  SECTION("With array data") {
    input_attr_data = {0.5, 1.0, 1.5, 2.0};
  }
  SECTION("Without array data") {
    input_attr_data = {};
  }

  // Create and write the array.
  array_fixture.write_by_index(0, 3, input_attr_data, input_label_data);

  // Read back the values and check as expected.
  array_fixture.read_and_check_values(0, 3, input_label_data, input_attr_data);
}

TEST_CASE(
    "CPP-API: Test write array by label", "[cppapi][query][DimensionLabel]") {
  // Create the array in a temporary directory.
  DenseArrayExample<TILEDB_INCREASING_DATA> array_fixture{};

  // Vectors for input data.
  std::vector<double> input_label_data{-1.0, 0.0, 0.5, 1.0};
  std::vector<double> input_attr_data{0.5, 1.0, 1.5, 2.0};

  // Write only dimension label data and check.
  array_fixture.write_by_index(0, 3, {}, input_label_data);

  // Write array data using label.
  array_fixture.write_by_label(-1.0, 1.0, input_attr_data, {});

  // Check results.
  array_fixture.read_and_check_values(0, 3, input_label_data, input_attr_data);
}
