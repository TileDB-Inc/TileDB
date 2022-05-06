/**
 * @file   cpp-integration-query-condition.cc
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
 * Tests the integration of query conditions by running queries.
 */

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

int num_rows = 20;
int a_fill_value = -1;
float b_fill_value = 0.0;
const std::string array_name = "cpp_unit_array";

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
 */
void create_array(
    Context& ctx,
    tiledb_array_type_t array_type,
    bool set_dups,
    std::vector<int>& a_data_read,
    std::vector<float>& b_data_read) {
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
  }
  schema.add_attribute(attr_a);
  schema.add_attribute(attr_b);
  Array::create(array_name, schema);

  // Write some initial data and close the array.
  std::vector<int> row_dims;
  std::vector<int> col_dims;
  std::vector<int> a_data;
  std::vector<float> b_data;

  for (int i = 0; i < num_rows * num_rows; ++i) {
    int row = (i / num_rows) + 1;
    int col = (i % num_rows) + 1;
    int a = i % 2 == 1 ? 0 : 1;
    float b;
    if (i % 8 == 0) {
      // b = 3.4
      b = 3.4f;
    } else if (i % 4 == 0) {
      // 3.45 <= b <= 3.7
      b = static_cast<float>(rand()) / (static_cast<float>(RAND_MAX / 0.25));
      b += 3.45f;
    } else if (i % 2 == 0) {
      // b <= 3.2
      b = static_cast<float>(rand()) / (static_cast<float>(RAND_MAX / 3.2));
    } else {
      // b = 4.2
      b = 4.2f;
    }

    row_dims.push_back(row);
    col_dims.push_back(col);
    a_data.push_back(a);
    b_data.push_back(b);
  }

  if (array_type == TILEDB_SPARSE) {
    Array array_w(ctx, array_name, TILEDB_WRITE);
    Query query_w(ctx, array_w);
    query_w.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("rows", row_dims)
        .set_data_buffer("cols", col_dims)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data);

    query_w.submit();
    query_w.finalize();
    array_w.close();
  } else if (array_type == TILEDB_DENSE) {
    Array array_w(ctx, array_name, TILEDB_WRITE);
    Query query_w(ctx, array_w);
    query_w.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data);

    query_w.submit();
    query_w.finalize();
    array_w.close();
  }

  // Open and read the entire array to save data for future comparisons.
  Array array1(ctx, array_name, TILEDB_READ);
  Query query1(ctx, array1);
  query1.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_data_read)
      .set_data_buffer("b", b_data_read);

  if (array_type == TILEDB_DENSE) {
    int range[] = {1, num_rows};
    query1.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
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

TEST_CASE(
    "Test read for sparse arrays with query condition",
    "[query][query-condition][apply]") {
  // Setup by creating buffers to store all elements and creating the original
  // array.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  create_array(ctx, TILEDB_SPARSE, false, a_data_read, b_data_read);

  SECTION("No range.") {
    // Create the query, which reads over the entire array with query condition
    // (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);
    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(num_rows * num_rows);
    std::vector<float> b_data_read_2(num_rows * num_rows);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 200
    // elements. Each of these elements should have the cell value 1 on
    // attribute a and should match the original value in the array that reads
    // all elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 200);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 200);

    for (int i = 0; i < 200; i++) {
      int original_arr_i = 2 * i;
      REQUIRE(a_data_read_2[i] == 1);
      REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
      REQUIRE(
          fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
          std::numeric_limits<float>::epsilon());
    }

    query2.finalize();
    array2.close();
  }

  SECTION("Range within a tile.") {
    // Create the query, which reads over the range rows[2,3], cols[2,3] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(4);
    std::vector<float> b_data_read_2(4);
    int range[] = {2, 3};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 2
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 2);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 2);

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

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Range across tile for rows dimension, within tile for cols dimension.") {
    // Create the query, which reads over the range rows[7,10], cols[2,3] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(8);
    std::vector<float> b_data_read_2(8);
    int range[] = {2, 3};
    int range1[] = {7, 10};
    query2.add_range("rows", range1[0], range1[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 4
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 4);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 4);

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

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Range within tile for rows dimension, across tile for col dimension.") {
    // Create the query, which reads over the range rows[2,3], cols[7,10] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(8);
    std::vector<float> b_data_read_2(8);
    int range[] = {2, 3};
    int range1[] = {7, 10};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range1[0], range1[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 4
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 4);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 4);

    // Testing (2,7), which should be in the result buffer.
    int ind_0 = index_from_row_col(2, 7);
    REQUIRE(a_data_read_2[0] == a_data_read[ind_0]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[ind_0]) <
        std::numeric_limits<float>::epsilon());

    // Testing (2,9), which should be in the result buffer.
    REQUIRE(a_data_read_2[1] == a_data_read[28]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[28]) <
        std::numeric_limits<float>::epsilon());

    // Testing (3,7), which should be in the result buffer.
    REQUIRE(a_data_read_2[2] == a_data_read[46]);
    REQUIRE(
        fabs(b_data_read_2[2] - b_data_read[46]) <
        std::numeric_limits<float>::epsilon());

    // Testing (3,9), which should be in the result buffer.
    REQUIRE(a_data_read_2[3] == a_data_read[48]);
    REQUIRE(
        fabs(b_data_read_2[3] - b_data_read[48]) <
        std::numeric_limits<float>::epsilon());

    query2.finalize();
    array2.close();
  }

  SECTION("Ranges across tiles on both dimensions.") {
    // Create the query, which reads over the range rows[7,14], cols[7,14] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);
    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);
    std::vector<int> a_data_read_2(64);
    std::vector<float> b_data_read_2(64);
    int range[] = {7, 14};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 32
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 32);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 32);

    for (int r = 7; r <= 14; ++r) {
      for (int c = 7; c <= 14; ++c) {
        int original_arr_i = index_from_row_col(r, c);
        // The buffer should have kept elements that were originally constructed
        // to have values less than 4.0; this means that any element whose
        // original index is even should have been kept.
        if (original_arr_i % 2 == 0) {
          int i = ((r - 7) * 4) + ((c - 7) / 2);
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
              std::numeric_limits<float>::epsilon());
        }
      }
    }

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Ranges across tiles on both dimensions, with complex query condition.") {
    // Create the query, which reads over the range rows[7,14], cols[7,14] with
    // query condition (b < 4.0f AND b <= 3.7f AND b >= 3.3f AND b != 3.4f).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

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

    std::vector<int> a_data_read_2(64);
    std::vector<float> b_data_read_2(64);
    int range[] = {7, 14};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();
    auto table2 = query2.result_buffer_elements();

    // Check the query for accuracy. The query results should contain 8
    // elements.
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 8);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 8);

    for (int r = 7; r <= 14; ++r) {
      for (int c = 7; c <= 14; ++c) {
        int original_arr_i = index_from_row_col(r, c);
        // The buffer should have kept elements that were originally constructed
        // to have values that satisfy the range [3.3, 3.7] but are not equal
        // to 3.4. This means that any element whose original index is 4 mod 8
        // should be in the result buffer.
        if (original_arr_i % 8 == 4) {
          int i = r - 7;
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[original_arr_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[original_arr_i]) <
              std::numeric_limits<float>::epsilon());
        }
      }
    }

    query2.finalize();
    array2.close();
  }

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "Test read for sparse arrays (dups allowed), with query condition",
    "[query][query-condition][apply-sparse]") {
  // Setup by creating buffers to store all elements and creating the original
  // array.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  create_array(ctx, TILEDB_SPARSE, true, a_data_read, b_data_read);

  SECTION("No range.") {
    // Create the query, which reads over the entire array with query condition
    // (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);
    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(num_rows * num_rows);
    std::vector<float> b_data_read_2(num_rows * num_rows);
    query2.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 200
    // elements. Each of these elements should have the cell value 1 on
    // attribute a and should match the original value in the array that reads
    // all elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 200);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 200);

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

    query2.finalize();
    array2.close();
  }

  SECTION("Range within a tile.") {
    // Create the query, which reads over the range rows[2,3], cols[2,3] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(4);
    std::vector<float> b_data_read_2(4);
    int range[] = {2, 3};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 2
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 2);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 2);

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

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Range across tile for rows dimension, within tile for cols dimension.") {
    // Create the query, which reads over the range rows[7,10], cols[2,3] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(8);
    std::vector<float> b_data_read_2(8);
    int range[] = {2, 3};
    int range1[] = {7, 10};
    query2.add_range("rows", range1[0], range1[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 4
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 4);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 4);

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

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Range within tile for rows dimension, across tile for cols dimension.") {
    // Create the query, which reads over the range rows[2,3], cols[7,10] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(8);
    std::vector<float> b_data_read_2(8);
    int range[] = {2, 3};
    int range1[] = {7, 10};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range1[0], range1[1]);
    query2.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 4
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 4);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 4);

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

    query2.finalize();
    array2.close();
  }

  SECTION("Ranges across tiles on both dimensions.") {
    // Create the query, which reads over the range rows[7,14], cols[7,14] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);
    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);
    std::vector<int> a_data_read_2(64);
    std::vector<float> b_data_read_2(64);
    int range[] = {7, 14};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 32
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 32);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 32);

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

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Ranges across tiles on both dimensions, with complex query condition.") {
    // Create the query, which reads over the range rows[7,14], cols[7,14] with
    // query condition (b < 4.0f AND b <= 3.7f AND b >= 3.3f AND b != 3.4f).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

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

    std::vector<int> a_data_read_2(64);
    std::vector<float> b_data_read_2(64);
    int range[] = {7, 14};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 8
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 8);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 8);

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

    query2.finalize();
    array2.close();
  }

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "Test read for dense arrays, with query condition",
    "[query][query-condition][apply-dense]") {
  // Setup by creating buffers to store all elements and creating the original
  // array.
  std::srand(static_cast<uint32_t>(time(0)));
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  std::vector<int> a_data_read(num_rows * num_rows);
  std::vector<float> b_data_read(num_rows * num_rows);

  create_array(ctx, TILEDB_DENSE, false, a_data_read, b_data_read);

  SECTION("No range.") {
    // Create the query, which reads over the entire array with query condition
    // (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);
    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(num_rows * num_rows);
    std::vector<float> b_data_read_2(num_rows * num_rows);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    int range[] = {1, num_rows};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 400
    // elements. Elements that meet the query conditioe should have the cell
    // value 1 on attribute a and should match the original value in the array
    // on attribute b. Elements that do not should have the fill value for both
    // attributes.
    size_t total_num_elements = static_cast<size_t>(num_rows * num_rows);
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == total_num_elements);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == total_num_elements);

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

    query2.finalize();
    array2.close();
  }

  SECTION("Range within a tile.") {
    // Create the query, which reads over the range rows[2,3], cols[2,3] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(4);
    std::vector<float> b_data_read_2(4);
    int range[] = {2, 3};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();
    auto table2 = query2.result_buffer_elements();

    // Check the query for accuracy. The query results should contain 4
    // elements.
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 4);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 4);

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

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Range across tile for rows dimension, within tile for cols dimension.") {
    // Create the query, which reads over the range rows[7,10], cols[2,3] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(8);
    std::vector<float> b_data_read_2(8);
    int range[] = {2, 3};
    int range1[] = {7, 10};
    query2.add_range("rows", range1[0], range1[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 8
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 8);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 8);

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

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Range within tile for rows dimension, across tile for cols dimension.") {
    // Create the query, which reads over the range rows[2,3], cols[7,10] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(8);
    std::vector<float> b_data_read_2(8);
    int range[] = {2, 3};
    int range1[] = {7, 10};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range1[0], range1[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 8
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 8);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 8);

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

    query2.finalize();
    array2.close();
  }

  SECTION("Ranges across tiles on both dimensions.") {
    // Create the query, which reads over the range rows[7,14], cols[7,14] with
    // query condition (b < 4.0).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);
    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);
    std::vector<int> a_data_read_2(64);
    std::vector<float> b_data_read_2(64);
    int range[] = {7, 14};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();
    auto table2 = query2.result_buffer_elements();

    // Check the query for accuracy. The query results should contain 64
    // elements.
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 64);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 64);

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

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Ranges across tiles on both dimensions, with complex query condition.") {
    // Create the query, which reads over the range rows[7,14], cols[7,14] with
    // query condition (b < 4.0f AND b <= 3.7f AND b >= 3.3f AND b != 3.4f).
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);

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

    std::vector<int> a_data_read_2(64);
    std::vector<float> b_data_read_2(64);
    int range[] = {7, 14};
    query2.add_range("rows", range[0], range[1])
        .add_range("cols", range[0], range[1]);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();

    // Check the query for accuracy. The query results should contain 64
    // elements.
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 64);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 64);

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

    query2.finalize();
    array2.close();
  }

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}
