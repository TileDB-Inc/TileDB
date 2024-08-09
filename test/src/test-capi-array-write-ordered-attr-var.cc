/**
 * @file  test-capi-array-write-ordered-array-var.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests sort checks for writing variable length ordered attributes.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
using namespace Catch::Matchers;

#include <test/support/src/helpers.h>
#include <test/support/src/vfs_helpers.h>
#include <catch2/catch_test_macros.hpp>
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

#include <numeric>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

template <DataOrder order>
struct VarOrderedAttributeArrayFixture {
 public:
  VarOrderedAttributeArrayFixture()
      : temp_dir_{}
      , ctx{temp_dir_.get_ctx()} {
    // Allocate array schema.
    tiledb_array_schema_t* schema{nullptr};
    require_tiledb_ok(tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &schema));

    // Set the domian.
    uint32_t dim_domain_data[2]{0, 31};
    uint32_t tile{32};
    tiledb_dimension_t* dim;
    tiledb_dimension_alloc(
        ctx, "x", TILEDB_INT32, &dim_domain_data[0], &tile, &dim);
    tiledb_domain_t* domain;
    tiledb_domain_alloc(ctx, &domain);
    tiledb_domain_add_dimension(ctx, domain, dim);
    tiledb_array_schema_set_domain(ctx, schema, domain);
    tiledb_dimension_free(&dim);
    tiledb_domain_free(&domain);

    // Define the attribute: skip C-API since ordered attributes aren't
    // exposed in the C-API yet.
    auto attr = make_shared<sm::Attribute>(
        HERE(), "a", Datatype::STRING_ASCII, constants::var_num, order);
    auto st = schema->array_schema_->add_attribute(attr);
    REQUIRE(st.ok());

    // Create the array and clean-up.
    std::string base_name{"array_ordered_attr_ascii_" + data_order_str(order)};
    array_name = temp_dir_.create_temporary_array(std::move(base_name), schema);
    tiledb_array_schema_free(&schema);
  }

  /**
   * Read back data from an array and verify it matches the expected data.
   *
   * @param min_index Lower bound for range to read data from.
   * @param max_index Upper bound for range to read data from.
   * @param expected_data The expected output data.
   * @param expected_offsets The expected output offsets.
   */
  void check_array_data(
      uint32_t min_index,
      uint32_t max_index,
      const std::string& expected_data,
      const std::vector<uint64_t> expected_offsets) {
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_READ));

    // Define sizes for setting buffers.
    uint64_t data_size{expected_data.size() * sizeof(char)};
    uint64_t offsets_size{expected_offsets.size() * sizeof(uint64_t)};
    std::string output_data(expected_data.size(), ' ');
    std::vector<uint64_t> output_offsets(expected_offsets.size());

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 0, &min_index, &max_index, nullptr));

    // Create write query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(tiledb_query_set_data_buffer(
        ctx, query, "a", output_data.data(), &data_size));
    require_tiledb_ok(tiledb_query_set_offsets_buffer(
        ctx, query, "a", output_offsets.data(), &offsets_size));

    // Submit read query and verify it is successful.
    check_tiledb_ok(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    check_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
    CHECK(query_status == TILEDB_COMPLETED);

    CHECK(output_data == expected_data);
    CHECK(output_offsets == expected_offsets);

    // Clean-up.
    tiledb_subarray_free(&subarray);
    tiledb_query_free(&query);
    tiledb_array_free(&array);
  }

  /**
   * Write data to the ordered attribute.
   *
   * @param min_index Lower bound for range to write data to.
   * @param max_index Upper bound for range to write data to.
   * @param data Data to write to the attribute.
   * @param offsets Offsets for the variable length data to write to the
   *     attribute.
   * @param valid If ``true`` the write should succeed, otherwise the write
   *     should fail.
   */
  void write_fragment(
      uint32_t min_index,
      uint32_t max_index,
      std::string& data,
      std::vector<uint64_t>& offsets,
      bool valid = true) {
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

    // Define sizes for setting buffers.
    uint64_t data_size{data.size() * sizeof(char)};
    uint64_t offsets_size{offsets.size() * sizeof(uint64_t)};

    // Create subarray.
    tiledb_subarray_t* subarray;
    require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
    require_tiledb_ok(tiledb_subarray_add_range(
        ctx, subarray, 0, &min_index, &max_index, nullptr));

    // Create write query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR));
    require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
    require_tiledb_ok(
        tiledb_query_set_data_buffer(ctx, query, "a", data.data(), &data_size));
    require_tiledb_ok(tiledb_query_set_offsets_buffer(
        ctx, query, "a", offsets.data(), &offsets_size));

    // Submit query.
    if (valid) {
      // Submit write query and verify it is successful.
      check_tiledb_ok(tiledb_query_submit(ctx, query));
      tiledb_query_status_t query_status;
      check_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
      CHECK(query_status == TILEDB_COMPLETED);
    } else {
      // Submit write query and verify if fails.
      require_tiledb_error_with(
          tiledb_query_submit(ctx, query),
          "WriterBase: The data for attribute 'a' is not in the expected "
          "order.");
    }

    // Clean-up.
    tiledb_subarray_free(&subarray);
    tiledb_query_free(&query);
    tiledb_array_free(&array);
  }

  /** Check a TileDB return code is ok. */
  inline void check_tiledb_ok(int rc) const {
    temp_dir_.check_tiledb_ok(rc);
  }

  /** Require a TileDB return code is ok. */
  inline void require_tiledb_ok(int rc) const {
    temp_dir_.require_tiledb_ok(rc);
  }

  /** Require a TileDB return code is an error with the given message. */
  inline void require_tiledb_error_with(int rc, const std::string& msg) {
    temp_dir_.require_tiledb_error_with(rc, msg);
  }

 protected:
  /** Temporary directory and virtual file system. */
  test::TemporaryDirectoryFixture temp_dir_;

  /**
   * TileDB context.
   *
   * This context in managed by the above temporary directory fixture.
   */
  tiledb_ctx_t* ctx;

  /** Name of the array. */
  std::string array_name;
};

TEST_CASE_METHOD(
    VarOrderedAttributeArrayFixture<DataOrder::INCREASING_DATA>,
    "Write to increasing attributes with fixed length: valid",
    "[ordered-writer][var][ordered-data]") {
  std::string data{"aabbbccddd"};
  std::vector<uint64_t> offsets{0, 2, 5, 7};
  write_fragment(2, 5, data, offsets);
  check_array_data(2, 5, data, offsets);
}

TEST_CASE_METHOD(
    VarOrderedAttributeArrayFixture<DataOrder::DECREASING_DATA>,
    "Write to decreasing attributes with fixed length: valid",
    "[ordered-writer][var][ordered-data]") {
  std::string data{"zzyyyxxwww"};
  std::vector<uint64_t> offsets{0, 2, 5, 7};
  write_fragment(2, 5, data, offsets);
  check_array_data(2, 5, data, offsets);
}

TEST_CASE_METHOD(
    VarOrderedAttributeArrayFixture<DataOrder::INCREASING_DATA>,
    "Write to increasing attributes with variable length: Invalid order",
    "[ordered-writer][var][ordered-data]") {
  // Write initial data.
  std::string valid_data{"abcd"};
  std::vector<uint64_t> valid_offsets{0, 1, 2, 3};
  write_fragment(4, 7, valid_data, valid_offsets);

  // Try writing invalid data.
  std::string invalid_data{"aabbaadd"};
  std::vector<uint64_t> invalid_offsets{0, 2, 4, 6};
  write_fragment(4, 7, invalid_data, invalid_offsets, false);

  // Verify array data is unchanged for bad write.
  check_array_data(4, 7, valid_data, valid_offsets);
}

TEST_CASE_METHOD(
    VarOrderedAttributeArrayFixture<DataOrder::DECREASING_DATA>,
    "Write to decreasing attributes with variable length: Invalid order",
    "[ordered-writer][var][ordered-data]") {
  // Write initial data.
  std::string valid_data{"zzyyxxww"};
  std::vector<uint64_t> valid_offsets{0, 2, 4, 6};
  write_fragment(4, 7, valid_data, valid_offsets);

  // Try writing invalid data.
  std::string invalid_data{"zzyx"};
  std::vector<uint64_t> invalid_offsets{0, 1, 2, 3};
  write_fragment(4, 7, invalid_data, invalid_offsets, false);

  // Verify array data is unchanged for bad write.
  check_array_data(4, 7, valid_data, valid_offsets);
}
