/**
 * @file   cpp-integration-query-condition.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB Inc.
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
 * Tests the integration of query conditions by running queries.
 */

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>

#include <test/support/src/vfs_helpers.h>
#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

int num_rows = 20;
int a_fill_value = -1;
float b_fill_value = 0.0;
const std::string array_name = "cpp_integration_query_condition_array";

inline int index_from_row_col(int r, int c) {
  return ((r - 1) * num_rows) + (c - 1);
}

/**
 * @brief Create a TileDB array with the following characteristics.
 * - Two dimensions called rows and cols. Each dimension is of type
 * int, and has a lower bound of 1 and a higher bound of 20, inclusive.
 * - Two attributes called "a" (of type int) and "b" (of type float).
 * - Tile size of 4.
 *
 * The data in the array is set as follows. On attribute a, each cell's value
 * is 0 if the column index is 1 if the column dimension is odd, and 0 if the
 * column index is even. This makes the cell values on attribute a look like
 * the following:
 *
 * 1 0 1 0 1 0 1 0 1 0 1 0 1 0 1 0 1 0 1 0
 * .
 * . (for 20 rows total)
 * .
 * 1 0 1 0 1 0 1 0 1 0 1 0 1 0 1 0 1 0 1 0
 *
 * On attribute b, the cell values are based on a more complex system.
 * The values are set with likeness to the diagram below. Keep in mind
 * that each row is 20 cells long.
 *
 * 3.4  4.2  Z  4.2  Y    4.2  Z  4.2  ... 3.4  4.2  Z  4.2
 * Y    4.2  Z  4.2  3.4  4.2  Z  4.2  ... Y    4.2  Z  4.2
 * .
 * . (for 20 rows total)
 * .
 * Y    4.2  Z  4.2  3.4  4.2  Z  4.2  ... Y    4.2  Z  4.2
 *
 * Legend:
 * Y: 3.45 <= val <= 3.7
 * Z: val <= 3.2
 * Numbers are true to their cell value.
 *
 *
 * @param ctx Context.
 * @param array_type Type of array (sparse or dense).
 * @param set_dups Whether the array allows coordinate duplicates.
 * @param a_data_read Data buffer to store cell values on attribute a.
 * @param b_data_read Data buffer to store cell values on attribute b.
 * @param array_uri URI of array to create.
 */
void create_array(
    Context& ctx,
    tiledb_array_type_t array_type,
    bool set_dups,
    bool add_utf8_attr,
    std::vector<int>& a_data_read,
    std::vector<float>& b_data_read,
    const std::string& array_uri = array_name) {
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, num_rows}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, num_rows}}, 4));
  ArraySchema schema(ctx, array_type);
  if (set_dups) {
    schema.set_allows_dups(true);
  }
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  Attribute attr_a = Attribute::create<int>(ctx, "a");
  Attribute attr_b = Attribute::create<float>(ctx, "b");
  if (array_type == TILEDB_DENSE) {
    attr_a.set_fill_value(&a_fill_value, sizeof(int));
    attr_b.set_fill_value(&b_fill_value, sizeof(float));
  } else {
    schema.set_capacity(16);
  }
  schema.add_attribute(attr_a);
  schema.add_attribute(attr_b);
  if (add_utf8_attr) {
    Attribute attr_c = Attribute::create(ctx, "c", TILEDB_STRING_UTF8);
    attr_c.set_cell_val_num(TILEDB_VAR_NUM);
    attr_c.set_nullable(true);
    schema.add_attribute(attr_c);
  }
  Array::create(array_uri, schema);

  // Write some initial data and close the array.
  std::vector<int> row_dims;
  std::vector<int> col_dims;
  std::vector<int> a_data;
  std::vector<float> b_data;
  std::vector<char> c_data;
  std::vector<uint64_t> c_offsets;
  std::vector<uint8_t> c_validity;

  std::vector<std::string> c_choices = {
      std::string("bird"),
      std::string("bunny"),
      std::string("cat"),
      std::string("dog")};

  for (int i = 0; i < num_rows * num_rows; ++i) {
    int row = (i / num_rows) + 1;
    int col = (i % num_rows) + 1;
    int a = i % 2 == 1 ? 0 : 1;
    float b;
    std::string c_str;
    if (i % 8 == 0) {
      // b = 3.4
      b = 3.4f;
      c_str = c_choices[0];
    } else if (i % 4 == 0) {
      // 3.45 <= b <= 3.7
      b = static_cast<float>(rand()) / (static_cast<float>(RAND_MAX / 0.25));
      b += 3.45f;
      c_str = c_choices[1];
    } else if (i % 2 == 0) {
      // b <= 3.2
      b = static_cast<float>(rand()) / (static_cast<float>(RAND_MAX / 3.2));
      c_str = c_choices[2];
    } else {
      // b = 4.2
      b = 4.2f;
      c_str = c_choices[3];
    }

    row_dims.push_back(row);
    col_dims.push_back(col);
    a_data.push_back(a);
    b_data.push_back(b);

    c_offsets.push_back(c_data.size());
    for (size_t c_idx = 0; c_idx < c_str.size(); c_idx++) {
      c_data.push_back(c_str.at(c_idx));
    }

    c_validity.push_back(1);
  }

  if (array_type == TILEDB_SPARSE) {
    Array array_w(ctx, array_uri, TILEDB_WRITE);
    Query query_w(ctx, array_w);
    query_w.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("rows", row_dims)
        .set_data_buffer("cols", col_dims)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data);

    if (add_utf8_attr) {
      query_w.set_data_buffer("c", c_data)
          .set_offsets_buffer("c", c_offsets)
          .set_validity_buffer("c", c_validity);
    }

    query_w.submit();
    query_w.finalize();
    array_w.close();
  } else if (array_type == TILEDB_DENSE) {
    Array array_w(ctx, array_uri, TILEDB_WRITE);
    Query query_w(ctx, array_w);
    query_w.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data);

    if (add_utf8_attr) {
      query_w.set_data_buffer("c", c_data)
          .set_offsets_buffer("c", c_offsets)
          .set_validity_buffer("c", c_validity);
    }

    query_w.submit();
    query_w.finalize();
    array_w.close();
  }

  // Open and read the entire array to save data for future comparisons.
  Array array1(ctx, array_uri, TILEDB_READ);
  Query query1(ctx, array1);
  query1.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_data_read)
      .set_data_buffer("b", b_data_read);

  Subarray subarray1(ctx, array1);
  if (array_type == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray1.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query1.set_subarray(subarray1);
  }
  query1.submit();

  // Check the query for accuracy. The query results should contain all the
  // elements.
  size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
  auto table = query1.result_buffer_elements();
  REQUIRE(table.size() == 2);
  REQUIRE(table["a"].first == 0);
  REQUIRE(table["a"].second == total_num_elements);
  REQUIRE(table["b"].first == 0);
  REQUIRE(table["b"].second == total_num_elements);

  for (size_t i = 0; i < total_num_elements; ++i) {
    if (i % 2 == 0) {
      REQUIRE(a_data_read[i] == 1);
      REQUIRE(b_data_read[i] <= 3.8);
    } else {
      REQUIRE(a_data_read[i] == 0);
      REQUIRE(
          fabs(b_data_read[i] - 4.2f) < std::numeric_limits<float>::epsilon());
    }
  }
  query1.finalize();
  array1.close();
}

/**
 * @brief Function that performs a query and sets parameters as needed.
 *
 * @param a_data Query data buffer for attribute "a".
 * @param b_data Query data buffer for attribute "b".
 * @param qc Query condition.
 * @param layout_type Layout type for the query.
 * @param query The query to be submitted.
 */
static void perform_query(
    std::vector<int>& a_data,
    std::vector<float>& b_data,
    const QueryCondition& qc,
    tiledb_layout_t layout_type,
    Query& query) {
  query.set_layout(layout_type)
      .set_data_buffer("a", a_data)
      .set_data_buffer("b", b_data)
      .set_condition(qc);
  query.submit();
}

static void perform_query(
    std::vector<int>& a_data,
    std::vector<float>& b_data,
    std::string& c_data,
    std::vector<uint64_t>& c_offsets,
    std::vector<uint8_t>& c_validity,
    const QueryCondition& qc,
    tiledb_layout_t layout_type,
    Query& query) {
  query.set_layout(layout_type)
      .set_data_buffer("a", a_data)
      .set_data_buffer("b", b_data)
      .set_data_buffer("c", c_data)
      .set_offsets_buffer("c", c_offsets)
      .set_validity_buffer("c", c_validity)
      .set_condition(qc);
  query.submit();
}

struct TestParams {
  TestParams(
      tiledb_array_type_t array_type,
      tiledb_layout_t layout,
      bool set_dups,
      bool legacy,
      bool add_utf8_attr = false)
      : array_type_(array_type)
      , layout_(layout)
      , set_dups_(set_dups)
      , legacy_(legacy)
      , add_utf8_attr_(add_utf8_attr) {
  }

  tiledb_array_type_t array_type_;
  tiledb_layout_t layout_;
  bool set_dups_;
  bool legacy_;
  bool add_utf8_attr_;
};

TEST_CASE(
    "Testing read query with empty QC, with no range.",
    "[query][query-condition][empty][rest]") {
  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{vfs_test_setup.array_uri(array_name)};

  // Create an empty query condition
  QueryCondition qc(ctx);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(num_rows * num_rows);
  std::vector<float> b_data_read_2(num_rows * num_rows);

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read,
      array_uri);

  // Create the query, which reads over the entire array with query condition
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }

  auto vfs_test_setup2 = tiledb::test::VFSTestSetup(config.ptr().get(), false);
  auto ctx2 = vfs_test_setup2.ctx();
  Array array(ctx2, array_uri, TILEDB_READ);
  Query query(ctx2, array);

  // Set a subarray for dense.
  Subarray subarray(ctx2, array);
  if (params.array_type_ == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query.set_subarray(subarray);
  }

  // Perform query and validate.
  CHECK_THROWS_AS(
      perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query),
      std::exception);
}

TEST_CASE(
    "Testing read query with basic QC, with no range.",
    "[query][query-condition][rest]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{vfs_test_setup.array_uri(array_name)};

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(num_rows * num_rows);
  std::vector<float> b_data_read_2(num_rows * num_rows);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read,
      array_uri);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  auto vfs_test_setup2 = tiledb::test::VFSTestSetup(config.ptr().get(), false);
  auto ctx2 = vfs_test_setup2.ctx();
  Array array(ctx2, array_uri, TILEDB_READ);
  Query query(ctx2, array);

  // Set a subarray for dense.
  Subarray subarray(ctx2, array);
  if (params.array_type_ == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query.set_subarray(subarray);
  }

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);
  if (params.array_type_ == TILEDB_SPARSE) {
    // Check the query for accuracy. The query results should contain 200
    // elements. Each of these elements should have the cell value 1 on
    // attribute a and should match the original value in the array that reads
    // all elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 200);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 200);

    // The unordered query should return the results in global order. Therefore,
    // we iterate over each tile to collect our results.
    int i = 0;
    for (int tile_r = 1; tile_r <= num_rows; tile_r += 4) {
      for (int tile_c = 1; tile_c <= num_rows; tile_c += 4) {
        // Iterating over each tile.
        for (int r = tile_r; r < tile_r + 4; ++r) {
          for (int c = tile_c; c < tile_c + 4; c += 2) {
            int original_arr_i = index_from_row_col(r, c);
            REQUIRE(a_data_read_2[i] == 1);
            REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
            REQUIRE(
                fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
                std::numeric_limits<float>::epsilon());
            i += 1;
          }
        }
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain 400
    // elements. Elements that meet the query condition should have the cell
    // value 1 on attribute a and should match the original value in the array
    // on attribute b. Elements that do not should have the fill value for both
    // attributes.
    size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == total_num_elements);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == total_num_elements);

    for (int i = 0; i < num_rows * num_rows; ++i) {
      if (i % 2 == 0) {
        REQUIRE(a_data_read_2[i] == 1);
        REQUIRE(a_data_read_2[i] == a_data_read[i]);
        REQUIRE(
            fabs(b_data_read_2[i] - b_data_read[i]) <
            std::numeric_limits<float>::epsilon());
      } else {
        REQUIRE(a_data_read_2[i] == a_fill_value);
        REQUIRE(
            fabs(b_data_read_2[i] - b_fill_value) <
            std::numeric_limits<float>::epsilon());
      }
    }
  }

  query.finalize();
  array.close();
}

TEST_CASE(
    "Testing read query with basic negated QC, with no range.",
    "[query][query-condition][negation][rest]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{vfs_test_setup.array_uri(array_name)};

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  QueryCondition neg_qc = qc.negate();

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(num_rows * num_rows);
  std::vector<float> b_data_read_2(num_rows * num_rows);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read,
      array_uri);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }

  vfs_test_setup.update_config(config.ptr().get());
  Context ctx2 = vfs_test_setup.ctx();

  Array array(ctx2, array_uri, TILEDB_READ);
  Query query(ctx2, array);

  // Set a subarray for dense.
  Subarray subarray(ctx2, array);
  if (params.array_type_ == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query.set_subarray(subarray);
  }

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, neg_qc, params.layout_, query);
  if (params.array_type_ == TILEDB_SPARSE) {
    // Check the query for accuracy. The query results should contain 200
    // elements. Each of these elements should have the cell value 1 on
    // attribute a and should match the original value in the array that reads
    // all elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 200);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 200);

    // The unordered query should return the results in global order. Therefore,
    // we iterate over each tile to collect our results.
    int i = 0;
    for (int tile_r = 1; tile_r <= num_rows; tile_r += 4) {
      for (int tile_c = 1; tile_c <= num_rows; tile_c += 4) {
        // Iterating over each tile.
        for (int r = tile_r; r < tile_r + 4; ++r) {
          for (int c = tile_c + 1; c < tile_c + 4; c += 2) {
            int original_arr_i = index_from_row_col(r, c);
            REQUIRE(a_data_read_2[i] == 0);
            REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
            REQUIRE(
                fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
                std::numeric_limits<float>::epsilon());
            i += 1;
          }
        }
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain 400
    // elements. Elements that meet the query condition should have the cell
    // value 1 on attribute a and should match the original value in the array
    // on attribute b. Elements that do not should have the fill value for both
    // attributes.
    size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == total_num_elements);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == total_num_elements);

    for (int i = 0; i < num_rows * num_rows; ++i) {
      if (i % 2 == 1) {
        REQUIRE(a_data_read_2[i] == 0);
        REQUIRE(a_data_read_2[i] == a_data_read[i]);
        REQUIRE(
            fabs(b_data_read_2[i] - b_data_read[i]) <
            std::numeric_limits<float>::epsilon());
      } else {
        REQUIRE(a_data_read_2[i] == a_fill_value);
        REQUIRE(
            fabs(b_data_read_2[i] - b_fill_value) <
            std::numeric_limits<float>::epsilon());
      }
    }
  }

  query.finalize();
  array.close();
}

TEST_CASE(
    "Testing read query with basic QC, with range within a tile.",
    "[query][query-condition]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(4);
  std::vector<float> b_data_read_2(4);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Define range.
  int range[] = {2, 3};
  Subarray subarray(ctx2, array);
  subarray.add_range("rows", range[0], range[1])
      .add_range("cols", range[0], range[1]);
  query.set_subarray(subarray);

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);
  if (params.array_type_ == TILEDB_SPARSE) {
    // Check the query for accuracy. The query results should contain 2
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 2);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 2);

    // Testing (2,3), which should be in the result buffer.
    int ind_0 = index_from_row_col(2, 3);
    REQUIRE(a_data_read_2[0] == a_data_read[ind_0]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[ind_0]) <
        std::numeric_limits<float>::epsilon());

    // Testing (3,3), which should be in the result buffer.
    int ind_1 = index_from_row_col(3, 3);
    REQUIRE(a_data_read_2[1] == a_data_read[ind_1]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[ind_1]) <
        std::numeric_limits<float>::epsilon());
  } else {
    auto table = query.result_buffer_elements();

    // Check the query for accuracy. The query results should contain 4
    // elements.
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 4);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 4);

    // Testing (2,2), which should be filtered out.
    REQUIRE(a_data_read_2[0] == a_fill_value);
    REQUIRE(
        fabs(b_data_read_2[0] - b_fill_value) <
        std::numeric_limits<float>::epsilon());

    // Testing (2,3), which should be in the result buffer.
    int ind_1 = index_from_row_col(2, 3);
    REQUIRE(a_data_read_2[1] == a_data_read[ind_1]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[ind_1]) <
        std::numeric_limits<float>::epsilon());

    // Testing (3,2), which should be filtered out.
    REQUIRE(a_data_read_2[2] == a_fill_value);
    REQUIRE(
        fabs(b_data_read_2[2] - b_fill_value) <
        std::numeric_limits<float>::epsilon());

    // Testing (3,3), which should be in the result buffer.
    int ind_3 = index_from_row_col(3, 3);
    REQUIRE(a_data_read_2[3] == a_data_read[ind_3]);
    REQUIRE(
        fabs(b_data_read_2[3] - b_data_read[ind_3]) <
        std::numeric_limits<float>::epsilon());
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with basic QC, with range across tile for rows "
    "dimension, within tile for cols dimension.",
    "[query][query-condition]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(8);
  std::vector<float> b_data_read_2(8);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Define range.
  int range[] = {2, 3};
  int range1[] = {7, 10};
  Subarray subarray(ctx2, array);
  subarray.add_range("rows", range1[0], range1[1])
      .add_range("cols", range[0], range[1]);
  query.set_subarray(subarray);

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);
  if (params.array_type_ == TILEDB_SPARSE) {
    // Check the query for accuracy. The query results should contain 2
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 4);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 4);

    // Testing (7,3), which should be in the result buffer.
    int ind_0 = index_from_row_col(7, 3);
    REQUIRE(a_data_read_2[0] == a_data_read[ind_0]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[ind_0]) <
        std::numeric_limits<float>::epsilon());

    // Testing (8,3), which should be in the result buffer.
    int ind_1 = index_from_row_col(8, 3);
    REQUIRE(a_data_read_2[1] == a_data_read[ind_1]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[ind_1]) <
        std::numeric_limits<float>::epsilon());

    // Testing (9,3), which should be in the result buffer.
    int ind_2 = index_from_row_col(9, 3);
    REQUIRE(a_data_read_2[2] == a_data_read[ind_2]);
    REQUIRE(
        fabs(b_data_read_2[2] - b_data_read[ind_2]) <
        std::numeric_limits<float>::epsilon());

    // Testing (10,3), which should be in the result buffer.
    int ind_3 = index_from_row_col(10, 3);
    REQUIRE(a_data_read_2[3] == a_data_read[ind_3]);
    REQUIRE(
        fabs(b_data_read_2[3] - b_data_read[ind_3]) <
        std::numeric_limits<float>::epsilon());
  } else {
    // Check the query for accuracy. The query results should contain 8
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 8);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 8);
    for (int r = 7; r <= 10; ++r) {
      for (int c = 2; c <= 3; ++c) {
        int i = ((r - 7) * 2) + (c - 2);
        int original_arr_i = index_from_row_col(r, c);
        if (c == 3) {
          // If the column dimension value is 3, the cell should be in the
          // result buffer.
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
              std::numeric_limits<float>::epsilon());
        } else {
          // If the column dimension value is 2, the cell should be filtered
          // out.
          REQUIRE(a_data_read_2[i] == a_fill_value);
          REQUIRE(
              fabs(b_data_read_2[i] - b_fill_value) <
              std::numeric_limits<float>::epsilon());
        }
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with basic QC, with range within tile for rows "
    "dimension, across tile for cols dimension.",
    "[query][query-condition]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(8);
  std::vector<float> b_data_read_2(8);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Define range.
  int range[] = {2, 3};
  int range1[] = {7, 10};
  Subarray subarray(ctx2, array);
  subarray.add_range("rows", range[0], range[1])
      .add_range("cols", range1[0], range1[1]);
  query.set_subarray(subarray);

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);
  if (params.array_type_ == TILEDB_SPARSE) {
    // Check the query for accuracy. The query results should contain 4
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 4);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 4);

    // Ensure that the same data is in each array (global order).
    int i = 0;
    for (int c = range1[0]; c <= range1[1]; c += 2) {
      for (int r = range[0]; r <= range[1]; ++r) {
        int original_arr_i = index_from_row_col(r, c);
        REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
        REQUIRE(
            fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
            std::numeric_limits<float>::epsilon());
        i += 1;
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain 8
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 8);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 8);

    for (int r = 2; r <= 3; ++r) {
      for (int c = 7; c <= 10; ++c) {
        int i = ((r - 2) * 4) + (c - 7);
        int original_arr_i = index_from_row_col(r, c);
        // If the column dimension value is 7 or 9, the cell should be in the
        // result buffer.
        if (c == 7 || c == 9) {
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
              std::numeric_limits<float>::epsilon());
        } else {
          // If the column dimension value is 8 or 10, the cell should be
          // filtered out.
          REQUIRE(a_data_read_2[i] == a_fill_value);
          REQUIRE(
              fabs(b_data_read_2[i] - b_fill_value) <
              std::numeric_limits<float>::epsilon());
        }
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with basic QC, with ranges across tiles on both "
    "dimensions.",
    "[query][query-condition]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(64);
  std::vector<float> b_data_read_2(64);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Define range.
  int range[] = {7, 14};
  Subarray subarray(ctx2, array);
  subarray.add_range("rows", range[0], range[1])
      .add_range("cols", range[0], range[1]);
  query.set_subarray(subarray);

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);
  if (params.array_type_ == TILEDB_SPARSE) {
    // Check the query for accuracy. The query results should contain 32
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 32);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 32);

    int i = 0;
    std::vector<std::pair<int, int>> ranges_vec;
    ranges_vec.push_back(std::make_pair(7, 8));
    ranges_vec.push_back(std::make_pair(9, 12));
    ranges_vec.push_back(std::make_pair(13, 14));

    for (size_t a = 0; a < 3; ++a) {    // Iterating over rows.
      for (size_t b = 0; b < 3; ++b) {  // Iterating over cols.
        int row_lo = ranges_vec[a].first;
        int row_hi = ranges_vec[a].second;
        int col_lo = ranges_vec[b].first;
        int col_hi = ranges_vec[b].second;

        for (int r = row_lo; r <= row_hi; ++r) {
          for (int c = col_lo; c <= col_hi; ++c) {
            int original_arr_i = index_from_row_col(r, c);
            // The buffer should have kept elements that were originally
            // constructed to have values less than 4.0; this means that any
            // element whose original index is even should be in the result
            // buffer.
            if (original_arr_i % 2 == 0) {
              REQUIRE(a_data_read_2[i] == 1);
              REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
              REQUIRE(
                  fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
                  std::numeric_limits<float>::epsilon());
              i += 1;
            }
          }
        }
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain 64
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 64);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 64);

    for (int r = 7; r <= 14; ++r) {
      for (int c = 7; c <= 14; ++c) {
        int original_arr_i = index_from_row_col(r, c);
        int i = ((r - 7) * 8) + (c - 7);
        // The buffer should have kept elements that were originally constructed
        // to have values less than 4.0; this means that any element whose
        // original index is even should have been kept.
        if (original_arr_i % 2 == 0) {
          // Checking for original value.
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
              std::numeric_limits<float>::epsilon());
        } else {
          // Checking for fill value.
          REQUIRE(a_data_read_2[i] == a_fill_value);
          REQUIRE(
              fabs(b_data_read_2[i] - b_fill_value) <
              std::numeric_limits<float>::epsilon());
        }
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with complex QC, with ranges across tiles on both "
    "dimensions.",
    "[query][query-condition]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0f AND b <= 3.7f AND b >= 3.3f AND b
  // != 3.4f);
  QueryCondition qc1(ctx);
  float val1 = 4.0f;
  qc1.init("b", &val1, sizeof(float), TILEDB_LT);

  QueryCondition qc2(ctx);
  float val2 = 3.7f;
  qc2.init("b", &val2, sizeof(float), TILEDB_LE);

  QueryCondition qc3(ctx);
  float val3 = 3.3f;
  qc3.init("b", &val3, sizeof(float), TILEDB_GE);

  QueryCondition qc4(ctx);
  float val4 = 3.4f;
  qc4.init("b", &val4, sizeof(float), TILEDB_NE);

  QueryCondition qc5 = qc1.combine(qc2, TILEDB_AND);
  QueryCondition qc6 = qc5.combine(qc3, TILEDB_AND);
  QueryCondition qc = qc6.combine(qc4, TILEDB_AND);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(64);
  std::vector<float> b_data_read_2(64);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Define range.
  int range[] = {7, 14};
  Subarray subarray(ctx2, array);
  subarray.add_range("rows", range[0], range[1])
      .add_range("cols", range[0], range[1]);
  query.set_subarray(subarray);

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);
  if (params.array_type_ == TILEDB_SPARSE) {
    // Check the query for accuracy. The query results should contain 8
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 8);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 8);

    int i = 0;
    std::vector<std::pair<int, int>> ranges_vec;
    ranges_vec.push_back(std::make_pair(7, 8));
    ranges_vec.push_back(std::make_pair(9, 12));
    ranges_vec.push_back(std::make_pair(13, 14));

    for (size_t a = 0; a < 3; ++a) {    // Iterating over rows.
      for (size_t b = 0; b < 3; ++b) {  // Iterating over cols.
        int row_lo = ranges_vec[a].first;
        int row_hi = ranges_vec[a].second;
        int col_lo = ranges_vec[b].first;
        int col_hi = ranges_vec[b].second;

        for (int r = row_lo; r <= row_hi; ++r) {
          for (int c = col_lo; c <= col_hi; ++c) {
            int original_arr_i = index_from_row_col(r, c);
            // The buffer should have kept elements that were originally
            // constructed to have values that satisfy the range [3.3, 3.7] but
            // are not equal to 3.4. This means that any element whose original
            // index is 4 mod 8 should be in the result buffer.
            if (original_arr_i % 8 == 4) {
              REQUIRE(a_data_read_2[i] == 1);
              REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
              REQUIRE(
                  fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
                  std::numeric_limits<float>::epsilon());
              i += 1;
            }
          }
        }
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain 64
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 64);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 64);

    for (int r = 7; r <= 14; ++r) {
      for (int c = 7; c <= 14; ++c) {
        int original_arr_i = index_from_row_col(r, c);
        int i = ((r - 7) * 8) + (c - 7);
        // The buffer should have kept elements that were originally constructed
        // to have values that satisfy the range [3.3, 3.7] but are not equal
        // to 3.4. This means that any element whose original index is 4 mod 8
        // should have been kept.
        if (original_arr_i % 8 == 4) {
          // Checking for original value.
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
              std::numeric_limits<float>::epsilon());
        } else {
          // Checking for fill value.
          REQUIRE(a_data_read_2[i] == a_fill_value);
          REQUIRE(
              fabs(b_data_read_2[i] - b_fill_value) <
              std::numeric_limits<float>::epsilon());
        }
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with complex QC, with ranges across tiles on both "
    "dimensions and a UTF-8 attribute",
    "[query][query-condition][utf8]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0f AND b <= 3.7f AND b >= 3.3f AND b
  // != 3.4f);
  QueryCondition qc1(ctx);
  float val1 = 4.0f;
  qc1.init("b", &val1, sizeof(float), TILEDB_LT);

  QueryCondition qc2(ctx);
  float val2 = 3.7f;
  qc2.init("b", &val2, sizeof(float), TILEDB_LE);

  QueryCondition qc3(ctx);
  float val3 = 3.3f;
  qc3.init("b", &val3, sizeof(float), TILEDB_GE);

  QueryCondition qc4(ctx);
  float val4 = 3.4f;
  qc4.init("b", &val4, sizeof(float), TILEDB_NE);

  QueryCondition qc5(ctx);
  std::string val5("dog");
  qc5.init("c", val5.data(), val5.size(), TILEDB_LE);

  QueryCondition qc6 = qc1.combine(qc2, TILEDB_AND);
  QueryCondition qc7 = qc6.combine(qc3, TILEDB_AND);
  QueryCondition qc8 = qc7.combine(qc4, TILEDB_AND);
  QueryCondition qc = qc8.combine(qc5, TILEDB_AND);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(64);
  std::vector<float> b_data_read_2(64);
  std::string c_data_read_2;
  c_data_read_2.resize(64 * 5);
  std::vector<uint64_t> c_data_offsets_2(64);
  std::vector<uint8_t> c_data_validity_2(64);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false, true),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false, true));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Define range.
  int range[] = {7, 14};
  Subarray subarray(ctx2, array);
  subarray.add_range("rows", range[0], range[1])
      .add_range("cols", range[0], range[1]);
  query.set_subarray(subarray);

  // Perform query and validate.
  perform_query(
      a_data_read_2,
      b_data_read_2,
      c_data_read_2,
      c_data_offsets_2,
      c_data_validity_2,
      qc,
      params.layout_,
      query);
  if (params.array_type_ == TILEDB_SPARSE) {
    // Check the query for accuracy. The query results should contain 8
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 3);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 8);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 8);

    int i = 0;
    std::vector<std::pair<int, int>> ranges_vec;
    ranges_vec.push_back(std::make_pair(7, 8));
    ranges_vec.push_back(std::make_pair(9, 12));
    ranges_vec.push_back(std::make_pair(13, 14));

    for (size_t a = 0; a < 3; ++a) {    // Iterating over rows.
      for (size_t b = 0; b < 3; ++b) {  // Iterating over cols.
        int row_lo = ranges_vec[a].first;
        int row_hi = ranges_vec[a].second;
        int col_lo = ranges_vec[b].first;
        int col_hi = ranges_vec[b].second;

        for (int r = row_lo; r <= row_hi; ++r) {
          for (int c = col_lo; c <= col_hi; ++c) {
            int original_arr_i = index_from_row_col(r, c);
            // The buffer should have kept elements that were originally
            // constructed to have values that satisfy the range [3.3, 3.7] but
            // are not equal to 3.4. This means that any element whose original
            // index is 4 mod 8 should be in the result buffer.
            if (original_arr_i % 8 == 4) {
              REQUIRE(a_data_read_2[i] == 1);
              REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
              REQUIRE(
                  fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
                  std::numeric_limits<float>::epsilon());
              i += 1;
            }
          }
        }
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain 64
    // elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 3);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 64);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 64);

    for (int r = 7; r <= 14; ++r) {
      for (int c = 7; c <= 14; ++c) {
        int original_arr_i = index_from_row_col(r, c);
        int i = ((r - 7) * 8) + (c - 7);
        // The buffer should have kept elements that were originally constructed
        // to have values that satisfy the range [3.3, 3.7] but are not equal
        // to 3.4. This means that any element whose original index is 4 mod 8
        // should have been kept.
        if (original_arr_i % 8 == 4) {
          // Checking for original value.
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
              std::numeric_limits<float>::epsilon());
          REQUIRE(c_data_validity_2[i] == 1);
        } else {
          // Checking for fill value.
          REQUIRE(a_data_read_2[i] == a_fill_value);
          REQUIRE(
              fabs(b_data_read_2[i] - b_fill_value) <
              std::numeric_limits<float>::epsilon());
          REQUIRE(c_data_validity_2[i] == 0);
        }
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing dense query condition with overlapping fragment domains",
    "[query][query-condition][dense][overlapping-fd]") {
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Create a simple 1D vector with domain 1-10 and one attribute.
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{1, 10}}, 5));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  Attribute attr = Attribute::create<int>(ctx, "a");
  int fill_value = -1;
  attr.set_fill_value(&fill_value, sizeof(int));
  schema.add_attribute(attr);
  Array::create(array_name, schema);

  // Open array for write.
  Array array(ctx, array_name, TILEDB_WRITE);

  // Write all values as 3 in the array.
  std::vector<int> vals = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3};
  Query query_w(ctx, array, TILEDB_WRITE);
  int range[] = {1, 10};
  Subarray subarray_w(ctx, array);
  subarray_w.add_range("d", range[0], range[1]);
  query_w.set_subarray(subarray_w);
  query_w.set_layout(TILEDB_ROW_MAJOR);
  query_w.set_data_buffer("a", vals);
  REQUIRE(query_w.submit() == Query::Status::COMPLETE);

  // Write values from 3-6 as 7 in the array.
  std::vector<int> vals2 = {7, 7, 7, 7};
  Query query_w2(ctx, array, TILEDB_WRITE);
  int range2[] = {3, 6};
  Subarray subarray_w2(ctx, array);
  subarray_w2.add_range("d", range2[0], range2[1]);
  query_w2.set_subarray(subarray_w2);
  query_w2.set_layout(TILEDB_ROW_MAJOR);
  query_w2.set_data_buffer("a", vals2);
  REQUIRE(query_w2.submit() == Query::Status::COMPLETE);

  array.close();

  // Open the array for read.
  Array array_r(ctx, array_name, TILEDB_READ);

  // Query the data with query condition a > 4.
  QueryCondition qc(ctx);
  int val1 = 4;
  qc.init("a", &val1, sizeof(int), TILEDB_GT);

  std::vector<int> vals_read(10);
  Query query_r(ctx, array_r, TILEDB_READ);
  Subarray subarray_r(ctx, array_r);
  subarray_r.add_range("d", range[0], range[1]);
  query_r.set_subarray(subarray_r);
  query_r.set_layout(TILEDB_ROW_MAJOR);
  query_r.set_data_buffer("a", vals_read);
  query_r.set_condition(qc);
  REQUIRE(query_r.submit() == Query::Status::COMPLETE);

  std::vector<int> c_vals_read = {-1, -1, 7, 7, 7, 7, -1, -1, -1, -1};
  CHECK(0 == memcmp(vals_read.data(), c_vals_read.data(), 10 * sizeof(int)));

  array_r.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing sparse query condition with the same fragment domain.",
    "[query][query-condition][sparse][same-fd]") {
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Create a simple 1D vector with domain 1-10 and one attribute.
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "d", {{1, 10}}, 5));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.set_cell_order(TILEDB_HILBERT);
  schema.set_allows_dups(false);
  schema.set_capacity(2);

  Attribute attr = Attribute::create<int>(ctx, "a");
  Attribute attr2 = Attribute::create<int>(ctx, "a2");
  int fill_value = -1;
  attr.set_fill_value(&fill_value, sizeof(int));
  schema.add_attribute(attr);
  schema.add_attribute(attr2);
  Array::create(array_name, schema);

  // Open array for write.
  Array array(ctx, array_name, TILEDB_WRITE);

  std::vector<int> dim_vals = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  // Write all values as 3 in the array.
  std::vector<int> vals = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3};
  Query query_w(ctx, array, TILEDB_WRITE);
  query_w.set_data_buffer("d", dim_vals);
  query_w.set_data_buffer("a", vals);
  query_w.set_data_buffer("a2", vals);
  REQUIRE(query_w.submit() == Query::Status::COMPLETE);

  // Write values from 1-10 as 7 in the array.
  std::vector<int> vals2 = {7, 7, 7, 7, 7, 7, 7, 7, 7, 6};
  Query query_w2(ctx, array, TILEDB_WRITE);
  query_w2.set_data_buffer("d", dim_vals);
  query_w2.set_data_buffer("a", vals2);
  query_w2.set_data_buffer("a2", vals);
  REQUIRE(query_w2.submit() == Query::Status::COMPLETE);

  array.close();

  // Open the array for read.
  Array array_r(ctx, array_name, TILEDB_READ);

  // Query the data with query condition a < 7.
  QueryCondition qc(ctx);
  int val1 = 7;
  qc.init("a", &val1, sizeof(int), TILEDB_LT);

  std::vector<int> vals_read(10);
  std::vector<int> vals_read2(10);
  std::vector<int> dim_vals_read(10);
  Query query_r(ctx, array_r, TILEDB_READ);
  query_r.set_layout(TILEDB_GLOBAL_ORDER);
  query_r.set_data_buffer("a", vals_read);
  query_r.set_data_buffer("a2", vals_read2);
  query_r.set_data_buffer("d", dim_vals_read);
  query_r.set_condition(qc);
  REQUIRE(query_r.submit() == Query::Status::COMPLETE);

  array_r.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with basic QC, condition on dimension, with range "
    "within a tile.",
    "[query][query-condition][dimension]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Define ranges through query condition.
  int range[] = {2, 3};
  QueryCondition qc_range_rows_1(ctx);
  qc_range_rows_1.init("rows", &range[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_rows_1, TILEDB_AND);

  QueryCondition qc_range_rows_2(ctx);
  qc_range_rows_2.init("rows", &range[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_rows_2, TILEDB_AND);

  QueryCondition qc_range_cols_1(ctx);
  qc_range_cols_1.init("cols", &range[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_cols_1, TILEDB_AND);

  QueryCondition qc_range_cols_2(ctx);
  qc_range_cols_2.init("cols", &range[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_cols_2, TILEDB_AND);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(num_rows * num_rows);
  std::vector<float> b_data_read_2(num_rows * num_rows);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, false),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Set a subarray for dense.
  Subarray subarray(ctx2, array);
  if (params.array_type_ == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query.set_subarray(subarray);
  }

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);

  auto table = query.result_buffer_elements();
  if (params.array_type_ != TILEDB_DENSE) {
    // Check the query for accuracy. The query results should contain 2
    // elements.
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 2);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 2);

    // Testing (2,3), which should be in the result buffer.
    int ind_0 = index_from_row_col(2, 3);
    REQUIRE(a_data_read_2[0] == a_data_read[ind_0]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[ind_0]) <
        std::numeric_limits<float>::epsilon());

    // Testing (3,3), which should be in the result buffer.
    int ind_1 = index_from_row_col(3, 3);
    REQUIRE(a_data_read_2[1] == a_data_read[ind_1]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[ind_1]) <
        std::numeric_limits<float>::epsilon());
  } else {
    // Check the query for accuracy. The query results should contain all
    // elements.
    size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == total_num_elements);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == total_num_elements);

    // Ensure that the same data is in each array (global order).
    for (int c = 1; c <= num_rows; ++c) {
      for (int r = 1; r <= num_rows; ++r) {
        int i = index_from_row_col(r, c);
        int exp_a = -1;
        float exp_b = 0;
        if (r >= range[0] && r <= range[1] && c >= range[0] && c <= range[1] &&
            b_data_read[i] < 4.0) {
          exp_a = a_data_read[i];
          exp_b = b_data_read[i];
        }

        REQUIRE(a_data_read_2[i] == exp_a);
        REQUIRE(
            fabs(b_data_read_2[i] - exp_b) <
            std::numeric_limits<float>::epsilon());
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with basic QC, condition on dimension, with range "
    "across tile for rows dimension, within tile for cols dimension.",
    "[query][query-condition][dimension]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Define ranges through query condition.
  int range[] = {2, 3};
  int range1[] = {7, 10};
  QueryCondition qc_range_rows_1(ctx);
  qc_range_rows_1.init("rows", &range1[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_rows_1, TILEDB_AND);

  QueryCondition qc_range_rows_2(ctx);
  qc_range_rows_2.init("rows", &range1[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_rows_2, TILEDB_AND);

  QueryCondition qc_range_cols_1(ctx);
  qc_range_cols_1.init("cols", &range[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_cols_1, TILEDB_AND);

  QueryCondition qc_range_cols_2(ctx);
  qc_range_cols_2.init("cols", &range[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_cols_2, TILEDB_AND);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(num_rows * num_rows);
  std::vector<float> b_data_read_2(num_rows * num_rows);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, false),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Set a subarray for dense.
  Subarray subarray(ctx2, array);
  if (params.array_type_ == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query.set_subarray(subarray);
  }

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);

  auto table = query.result_buffer_elements();
  if (params.array_type_ != TILEDB_DENSE) {
    // Check the query for accuracy. The query results should contain 2
    // elements.
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 4);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 4);

    // Testing (7,3), which should be in the result buffer.
    int ind_0 = index_from_row_col(7, 3);
    REQUIRE(a_data_read_2[0] == a_data_read[ind_0]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[ind_0]) <
        std::numeric_limits<float>::epsilon());

    // Testing (8,3), which should be in the result buffer.
    int ind_1 = index_from_row_col(8, 3);
    REQUIRE(a_data_read_2[1] == a_data_read[ind_1]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[ind_1]) <
        std::numeric_limits<float>::epsilon());

    // Testing (9,3), which should be in the result buffer.
    int ind_2 = index_from_row_col(9, 3);
    REQUIRE(a_data_read_2[2] == a_data_read[ind_2]);
    REQUIRE(
        fabs(b_data_read_2[2] - b_data_read[ind_2]) <
        std::numeric_limits<float>::epsilon());

    // Testing (10,3), which should be in the result buffer.
    int ind_3 = index_from_row_col(10, 3);
    REQUIRE(a_data_read_2[3] == a_data_read[ind_3]);
    REQUIRE(
        fabs(b_data_read_2[3] - b_data_read[ind_3]) <
        std::numeric_limits<float>::epsilon());
  } else {
    // Check the query for accuracy. The query results should contain all
    // elements.
    size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == total_num_elements);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == total_num_elements);

    // Ensure that the same data is in each array (global order).
    for (int c = 1; c <= num_rows; ++c) {
      for (int r = 1; r <= num_rows; ++r) {
        int i = index_from_row_col(r, c);
        int exp_a = -1;
        float exp_b = 0;
        if (r >= range1[0] && r <= range1[1] && c >= range[0] &&
            c <= range[1] && b_data_read[i] < 4.0) {
          exp_a = a_data_read[i];
          exp_b = b_data_read[i];
        }

        REQUIRE(a_data_read_2[i] == exp_a);
        REQUIRE(
            fabs(b_data_read_2[i] - exp_b) <
            std::numeric_limits<float>::epsilon());
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with basic QC, condition on dimension, with range "
    "within tile for rows dimension, across tile for cols dimension.",
    "[query][query-condition][dimension]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Define ranges through query condition.
  int range[] = {2, 3};
  int range1[] = {7, 10};
  QueryCondition qc_range_rows_1(ctx);
  qc_range_rows_1.init("rows", &range[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_rows_1, TILEDB_AND);

  QueryCondition qc_range_rows_2(ctx);
  qc_range_rows_2.init("rows", &range[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_rows_2, TILEDB_AND);

  QueryCondition qc_range_cols_1(ctx);
  qc_range_cols_1.init("cols", &range1[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_cols_1, TILEDB_AND);

  QueryCondition qc_range_cols_2(ctx);
  qc_range_cols_2.init("cols", &range1[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_cols_2, TILEDB_AND);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(num_rows * num_rows);
  std::vector<float> b_data_read_2(num_rows * num_rows);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, false),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Set a subarray for dense.
  Subarray subarray(ctx2, array);
  if (params.array_type_ == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query.set_subarray(subarray);
  }

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);

  auto table = query.result_buffer_elements();
  if (params.array_type_ != TILEDB_DENSE) {
    // Check the query for accuracy. The query results should contain 4
    // elements.
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 4);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 4);

    // Ensure that the same data is in each array (global order).
    int i = 0;
    for (int c = range1[0]; c <= range1[1]; c += 2) {
      for (int r = range[0]; r <= range[1]; ++r) {
        int original_arr_i = index_from_row_col(r, c);
        REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
        REQUIRE(
            fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
            std::numeric_limits<float>::epsilon());
        i += 1;
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain all
    // elements.
    size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == total_num_elements);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == total_num_elements);

    // Ensure that the same data is in each array (global order).
    for (int c = 1; c <= num_rows; ++c) {
      for (int r = 1; r <= num_rows; ++r) {
        int i = index_from_row_col(r, c);
        int exp_a = -1;
        float exp_b = 0;
        if (r >= range[0] && r <= range[1] && c >= range1[0] &&
            c <= range1[1] && b_data_read[i] < 4.0) {
          exp_a = a_data_read[i];
          exp_b = b_data_read[i];
        }

        REQUIRE(a_data_read_2[i] == exp_a);
        REQUIRE(
            fabs(b_data_read_2[i] - exp_b) <
            std::numeric_limits<float>::epsilon());
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with basic QC, condition on dimension, with ranges "
    "across tiles on both dimensions.",
    "[query][query-condition][dimension]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Define ranges through query condition.
  int range[] = {7, 14};
  QueryCondition qc_range_rows_1(ctx);
  qc_range_rows_1.init("rows", &range[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_rows_1, TILEDB_AND);

  QueryCondition qc_range_rows_2(ctx);
  qc_range_rows_2.init("rows", &range[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_rows_2, TILEDB_AND);

  QueryCondition qc_range_cols_1(ctx);
  qc_range_cols_1.init("cols", &range[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_cols_1, TILEDB_AND);

  QueryCondition qc_range_cols_2(ctx);
  qc_range_cols_2.init("cols", &range[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_cols_2, TILEDB_AND);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(num_rows * num_rows);
  std::vector<float> b_data_read_2(num_rows * num_rows);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, false),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Set a subarray for dense.
  Subarray subarray(ctx2, array);
  if (params.array_type_ == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query.set_subarray(subarray);
  }

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);

  auto table = query.result_buffer_elements();
  if (params.array_type_ != TILEDB_DENSE) {
    // Check the query for accuracy. The query results should contain 32
    // elements.
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 32);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 32);

    int i = 0;
    std::vector<std::pair<int, int>> ranges_vec;
    ranges_vec.push_back(std::make_pair(7, 8));
    ranges_vec.push_back(std::make_pair(9, 12));
    ranges_vec.push_back(std::make_pair(13, 14));

    for (size_t a = 0; a < 3; ++a) {    // Iterating over rows.
      for (size_t b = 0; b < 3; ++b) {  // Iterating over cols.
        int row_lo = ranges_vec[a].first;
        int row_hi = ranges_vec[a].second;
        int col_lo = ranges_vec[b].first;
        int col_hi = ranges_vec[b].second;

        for (int r = row_lo; r <= row_hi; ++r) {
          for (int c = col_lo; c <= col_hi; ++c) {
            int original_arr_i = index_from_row_col(r, c);
            // The buffer should have kept elements that were originally
            // constructed to have values less than 4.0; this means that any
            // element whose original index is even should be in the result
            // buffer.
            if (original_arr_i % 2 == 0) {
              REQUIRE(a_data_read_2[i] == 1);
              REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
              REQUIRE(
                  fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
                  std::numeric_limits<float>::epsilon());
              i += 1;
            }
          }
        }
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain all
    // elements.
    size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == total_num_elements);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == total_num_elements);

    // Ensure that the same data is in each array (global order).
    for (int c = 1; c <= num_rows; ++c) {
      for (int r = 1; r <= num_rows; ++r) {
        int i = index_from_row_col(r, c);
        int exp_a = -1;
        float exp_b = 0;
        if (r >= range[0] && r <= range[1] && c >= range[0] && c <= range[1] &&
            b_data_read[i] < 4.0) {
          exp_a = a_data_read[i];
          exp_b = b_data_read[i];
        }

        REQUIRE(a_data_read_2[i] == exp_a);
        REQUIRE(
            fabs(b_data_read_2[i] - exp_b) <
            std::numeric_limits<float>::epsilon());
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with complex QC, condition on dimensions, with ranges "
    "across tiles on both dimensions.",
    "[query][query-condition][dimension]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0f AND b <= 3.7f AND b >= 3.3f AND b
  // != 3.4f);
  QueryCondition qc1(ctx);
  float val1 = 4.0f;
  qc1.init("b", &val1, sizeof(float), TILEDB_LT);

  QueryCondition qc2(ctx);
  float val2 = 3.7f;
  qc2.init("b", &val2, sizeof(float), TILEDB_LE);

  QueryCondition qc3(ctx);
  float val3 = 3.3f;
  qc3.init("b", &val3, sizeof(float), TILEDB_GE);

  QueryCondition qc4(ctx);
  float val4 = 3.4f;
  qc4.init("b", &val4, sizeof(float), TILEDB_NE);

  QueryCondition qc5 = qc1.combine(qc2, TILEDB_AND);
  QueryCondition qc6 = qc5.combine(qc3, TILEDB_AND);
  QueryCondition qc = qc6.combine(qc4, TILEDB_AND);

  // Define ranges through query condition.
  int range[] = {7, 14};
  QueryCondition qc_range_rows_1(ctx);
  qc_range_rows_1.init("rows", &range[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_rows_1, TILEDB_AND);

  QueryCondition qc_range_rows_2(ctx);
  qc_range_rows_2.init("rows", &range[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_rows_2, TILEDB_AND);

  QueryCondition qc_range_cols_1(ctx);
  qc_range_cols_1.init("cols", &range[0], sizeof(int), TILEDB_GE);
  qc = qc.combine(qc_range_cols_1, TILEDB_AND);

  QueryCondition qc_range_cols_2(ctx);
  qc_range_cols_2.init("cols", &range[1], sizeof(int), TILEDB_LE);
  qc = qc.combine(qc_range_cols_2, TILEDB_AND);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(num_rows * num_rows);
  std::vector<float> b_data_read_2(num_rows * num_rows);

  // Generate test parameters.
  // PJD: Why not dense here?
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, false),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Set a subarray for dense.
  Subarray subarray(ctx2, array);
  if (params.array_type_ == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query.set_subarray(subarray);
  }

  // Perform query and validate.
  perform_query(a_data_read_2, b_data_read_2, qc, params.layout_, query);

  auto table = query.result_buffer_elements();
  if (params.array_type_ != TILEDB_DENSE) {
    // Check the query for accuracy. The query results should contain 8
    // elements.
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 8);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == 8);

    int i = 0;
    std::vector<std::pair<int, int>> ranges_vec;
    ranges_vec.push_back(std::make_pair(7, 8));
    ranges_vec.push_back(std::make_pair(9, 12));
    ranges_vec.push_back(std::make_pair(13, 14));

    for (size_t a = 0; a < 3; ++a) {    // Iterating over rows.
      for (size_t b = 0; b < 3; ++b) {  // Iterating over cols.
        int row_lo = ranges_vec[a].first;
        int row_hi = ranges_vec[a].second;
        int col_lo = ranges_vec[b].first;
        int col_hi = ranges_vec[b].second;

        for (int r = row_lo; r <= row_hi; ++r) {
          for (int c = col_lo; c <= col_hi; ++c) {
            int original_arr_i = index_from_row_col(r, c);
            // The buffer should have kept elements that were originally
            // constructed to have values that satisfy the range [3.3, 3.7] but
            // are not equal to 3.4. This means that any element whose original
            // index is 4 mod 8 should be in the result buffer.
            if (original_arr_i % 8 == 4) {
              REQUIRE(a_data_read_2[i] == 1);
              REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
              REQUIRE(
                  fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
                  std::numeric_limits<float>::epsilon());
              i += 1;
            }
          }
        }
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain all
    // elements.
    size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
    REQUIRE(table.size() == 2);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == total_num_elements);
    REQUIRE(table["b"].first == 0);
    REQUIRE(table["b"].second == total_num_elements);

    // Ensure that the same data is in each array (global order).
    for (int c = 1; c <= num_rows; ++c) {
      for (int r = 1; r <= num_rows; ++r) {
        int i = index_from_row_col(r, c);
        int exp_a = -1;
        float exp_b = 0;
        if (r >= range[0] && r <= range[1] && c >= range[0] && c <= range[1] &&
            b_data_read[i] < 4.0 && b_data_read[i] <= 3.7 &&
            b_data_read[i] >= 3.3 &&
            (fabs(b_data_read[i] - 3.4) >
             std::numeric_limits<float>::epsilon())) {
          exp_a = a_data_read[i];
          exp_b = b_data_read[i];
        }

        REQUIRE(a_data_read_2[i] == exp_a);
        REQUIRE(
            fabs(b_data_read_2[i] - exp_b) <
            std::numeric_limits<float>::epsilon());
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with simple QC, condition on dimensions, string dim",
    "[query][query-condition][dimension]") {
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 4}}, 4))
      .add_dimension(Dimension::create(
          ctx, "cols", TILEDB_STRING_ASCII, nullptr, nullptr));
  ArraySchema schema(ctx, TILEDB_SPARSE);

  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  Attribute attr_a = Attribute::create<int>(ctx, "a");
  schema.set_capacity(16);
  schema.add_attribute(attr_a);
  Array::create(array_name, schema);

  // Write some initial data and close the array.
  std::vector<int> rows_dims = {1, 2};
  std::string cols_dims = "ab";
  std::vector<uint64_t> cols_dim_offs = {0, 1};
  std::vector<int> a_data = {3, 4};

  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("rows", rows_dims)
      .set_data_buffer("cols", cols_dims)
      .set_offsets_buffer("cols", cols_dim_offs)
      .set_data_buffer("a", a_data);

  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Read the data with query condition on the string dimension.
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);

  QueryCondition qc(ctx);
  std::string val = "b";
  qc.init("cols", val.data(), 1, TILEDB_EQ);

  std::vector<int> rows_read(1);
  std::string cols_read;
  cols_read.resize(1);
  std::vector<uint64_t> cols_offs_read(1);
  std::vector<int> a_read(1);

  query.set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("rows", rows_read)
      .set_data_buffer("cols", cols_read)
      .set_offsets_buffer("cols", cols_offs_read)
      .set_data_buffer("a", a_read)
      .set_condition(qc);
  query.submit();
  CHECK(query.query_status() == Query::Status::COMPLETE);

  // Check the query for accuracy. The query results should contain 1 element.
  auto table = query.result_buffer_elements();
  CHECK(table.size() == 3);
  CHECK(table["a"].first == 0);
  CHECK(table["a"].second == 1);

  CHECK(rows_read[0] == 2);
  CHECK(cols_read == "b");
  CHECK(cols_offs_read[0] == 0);
  CHECK(a_read[0] == 4);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with basic QC, with no range, query condition "
    "attribute not in buffers",
    "[query][query-condition][attr-not-in-buffers]") {
  // Initial setup.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx);
  float val = 4.0f;
  qc.init("b", &val, sizeof(float), TILEDB_LT);

  // Create buffers with size of the results of the two queries.
  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  // These buffers store the results of the second query made with the query
  // condition specified above.
  std::vector<int> a_data_read_2(num_rows * num_rows);

  // Generate test parameters.
  TestParams params = GENERATE(
      TestParams(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER, false, true),
      TestParams(TILEDB_SPARSE, TILEDB_UNORDERED, true, false),
      TestParams(TILEDB_DENSE, TILEDB_ROW_MAJOR, false, false));

  // Setup by creating buffers to store all elements of the original array.
  create_array(
      ctx,
      params.array_type_,
      params.set_dups_,
      params.add_utf8_attr_,
      a_data_read,
      b_data_read);

  // Create the query, which reads over the entire array with query condition
  // (b < 4.0).
  Config config;
  if (params.legacy_) {
    config.set("sm.query.sparse_global_order.reader", "legacy");
    config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");
  }
  Context ctx2 = Context(config);
  Array array(ctx2, array_name, TILEDB_READ);
  Query query(ctx2, array);

  // Set a subarray for dense.
  Subarray subarray(ctx2, array);
  if (params.array_type_ == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    subarray.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query.set_subarray(subarray);
  }

  // Perform query and validate.
  query.set_layout(params.layout_)
      .set_data_buffer("a", a_data_read_2)
      .set_condition(qc);
  query.submit();

  if (params.array_type_ == TILEDB_SPARSE) {
    // Check the query for accuracy. The query results should contain 200
    // elements. Each of these elements should have the cell value 1 on
    // attribute a and should match the original value in the array that reads
    // all elements.
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 1);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == 200);

    // The unordered query should return the results in global order. Therefore,
    // we iterate over each tile to collect our results.
    int i = 0;
    for (int tile_r = 1; tile_r <= num_rows; tile_r += 4) {
      for (int tile_c = 1; tile_c <= num_rows; tile_c += 4) {
        // Iterating over each tile.
        for (int r = tile_r; r < tile_r + 4; ++r) {
          for (int c = tile_c; c < tile_c + 4; c += 2) {
            int original_arr_i = index_from_row_col(r, c);
            REQUIRE(a_data_read_2[i] == 1);
            REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
            i += 1;
          }
        }
      }
    }
  } else {
    // Check the query for accuracy. The query results should contain 400
    // elements. Elements that meet the query condition should have the cell
    // value 1 on attribute a and should match the original value in the array
    // on attribute b. Elements that do not should have the fill value for both
    // attributes.
    size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
    auto table = query.result_buffer_elements();
    REQUIRE(table.size() == 1);
    REQUIRE(table["a"].first == 0);
    REQUIRE(table["a"].second == total_num_elements);
    for (int i = 0; i < num_rows * num_rows; ++i) {
      if (i % 2 == 0) {
        REQUIRE(a_data_read_2[i] == 1);
        REQUIRE(a_data_read_2[i] == a_data_read[i]);
      } else {
        REQUIRE(a_data_read_2[i] == a_fill_value);
      }
    }
  }

  query.finalize();
  array.close();

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Testing read query with simple QC, condition on attribute, bool attr",
    "[query][query-condition][dimension]") {
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 10}}, 10));

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain)
      .set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
      .add_attribute(Attribute::create<uint8_t>(ctx, "labs"));
  Array::create(array_name, schema);

  // Write some initial data and close the array.
  std::vector<int> wrows{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint8_t wlabs[10] = {
      false, true, false, true, true, false, false, true, false, false};

  Array warray(ctx, array_name, TILEDB_WRITE);
  Query wquery(ctx, warray, TILEDB_WRITE);
  wquery.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("rows", wrows)
      .set_data_buffer("labs", wlabs, 10);
  wquery.submit();
  warray.close();

  // Read the data with query condition on the Boolean attribute.
  Array rarray(ctx, array_name, TILEDB_READ);
  const std::vector<int> subarray = {1, 10};
  std::vector<int> rrows(10);
  std::vector<uint8_t> rlabs(10);

  Query rquery(ctx, rarray, TILEDB_READ);
  Subarray sub(ctx, rarray);
  sub.set_subarray(subarray);
  rquery.set_subarray(sub)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("rows", rrows)
      .set_data_buffer("labs", rlabs);
  rquery.submit();  // Submit the query and close the array.
  rarray.close();
  CHECK(rquery.query_status() == Query::Status::COMPLETE);

  // Check the query for accuracy.
  auto table = rquery.result_buffer_elements();
  CHECK(table.size() == 2);
  CHECK(table["rows"].first == 0);
  CHECK(table["rows"].second == 10);
  CHECK(table["labs"].first == 0);
  CHECK(table["labs"].second == 10);

  for (size_t i = 0; i < table["labs"].second; ++i) {
    CHECK(rlabs[i] == static_cast<uint8_t>(wlabs[i]));
  }

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}
