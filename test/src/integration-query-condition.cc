/**
 * @file   integration-query-condition.cc
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

TEST_CASE(
    "Test read for sparse arrays with added ranges and query "
    "condition (apply)",
    "[query][query-condition]") {
  std::srand(static_cast<uint32_t>(time(0)));
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create the array
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 20}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 20}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  schema.add_attribute(Attribute::create<float>(ctx, "b"));
  Array::create(array_name, schema);

  // Write some initial data and close the array.
  std::vector<int> row_dims;
  std::vector<int> col_dims;
  std::vector<int> a_data;
  std::vector<float> b_data;

  for (int i = 0; i < 400; ++i) {
    int row = (i / 20) + 1;
    int col = (i % 20) + 1;
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

  // Open and read the entire array.
  Array array1(ctx, array_name, TILEDB_READ);
  Query query1(ctx, array1);
  std::vector<int> a_data_read(400);
  std::vector<float> b_data_read(400);
  query1.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_data_read)
      .set_data_buffer("b", b_data_read);
  query1.submit();
  auto table = query1.result_buffer_elements();
  REQUIRE(table.size() == 2);
  REQUIRE(table["a"].first == 0);
  REQUIRE(table["a"].second == 400);
  REQUIRE(table["b"].first == 0);
  REQUIRE(table["b"].second == 400);

  for (size_t i = 0; i < 400; ++i) {
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

  SECTION("No range, with query condition") {
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);
    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(400);
    std::vector<float> b_data_read_2(400);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 200);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 200);

    for (int i = 0; i < 200; i++) {
      int og_i = 2 * i;
      REQUIRE(a_data_read_2[i] == 1);
      REQUIRE(a_data_read_2[i] == a_data_read[og_i]);
      REQUIRE(
          fabs(b_data_read_2[i] - b_data_read[og_i]) <
          std::numeric_limits<float>::epsilon());
    }

    query2.finalize();
    array2.close();
  }

  SECTION("Added range within a tile, with query condition") {
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
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 2);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 2);

    // testing (2,3)
    REQUIRE(a_data_read_2[0] == a_data_read[22]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[22]) <
        std::numeric_limits<float>::epsilon());

    // testing (3,3)
    REQUIRE(a_data_read_2[1] == a_data_read[42]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[42]) <
        std::numeric_limits<float>::epsilon());

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Added range across rows dimension, within tile for cols dimension, with "
      "query condition") {
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
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 4);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 4);

    // testing (7,3)
    REQUIRE(a_data_read_2[0] == a_data_read[122]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[122]) <
        std::numeric_limits<float>::epsilon());

    // testing (8,3)
    REQUIRE(a_data_read_2[1] == a_data_read[142]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[142]) <
        std::numeric_limits<float>::epsilon());

    // testing (9,3)
    REQUIRE(a_data_read_2[2] == a_data_read[162]);
    REQUIRE(
        fabs(b_data_read_2[2] - b_data_read[162]) <
        std::numeric_limits<float>::epsilon());

    // testing (10,3)
    REQUIRE(a_data_read_2[3] == a_data_read[182]);
    REQUIRE(
        fabs(b_data_read_2[3] - b_data_read[182]) <
        std::numeric_limits<float>::epsilon());

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Within tile for rows dimension, added range across col dimension, with "
      "query condition") {
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
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 4);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 4);

    // testing (2,7)
    REQUIRE(a_data_read_2[0] == a_data_read[26]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[26]) <
        std::numeric_limits<float>::epsilon());

    // testing (2,9)
    REQUIRE(a_data_read_2[1] == a_data_read[28]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[28]) <
        std::numeric_limits<float>::epsilon());

    // testing (3,7)
    REQUIRE(a_data_read_2[2] == a_data_read[46]);
    REQUIRE(
        fabs(b_data_read_2[2] - b_data_read[46]) <
        std::numeric_limits<float>::epsilon());

    // testing (3,9)
    REQUIRE(a_data_read_2[3] == a_data_read[48]);
    REQUIRE(
        fabs(b_data_read_2[3] - b_data_read[48]) <
        std::numeric_limits<float>::epsilon());

    query2.finalize();
    array2.close();
  }

  SECTION("Added range across tiles on both dimensions, with query condition") {
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

    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 32);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 32);

    for (int r = 7; r <= 14; ++r) {
      for (int c = 7; c <= 14; ++c) {
        int og_i = ((r - 1) * 20) + (c - 1);
        if (og_i % 2 == 0) {
          int i = ((r - 7) * 4) + ((c - 7) / 2);
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[og_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[og_i]) <
              std::numeric_limits<float>::epsilon());
        }
      }
    }

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Added range across tiles on both dimensions, with complex query "
      "condition") {
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

    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 8);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 8);

    for (int r = 7; r <= 14; ++r) {
      for (int c = 7; c <= 14; ++c) {
        int og_i = ((r - 1) * 20) + (c - 1);
        if (og_i % 8 == 4) {
          int i = r - 7;
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[og_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[og_i]) <
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
    "Test read for sparse arrays with added ranges and query "
    "condition (apply_sparse)",
    "[query][query-condition]") {
  std::srand(static_cast<uint32_t>(time(0)));
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create the array
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, 20}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{1, 20}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_allows_dups(true);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  schema.add_attribute(Attribute::create<float>(ctx, "b"));
  Array::create(array_name, schema);

  // Write some initial data and close the array.
  std::vector<int> row_dims;
  std::vector<int> col_dims;
  std::vector<int> a_data;
  std::vector<float> b_data;

  for (int i = 0; i < 400; ++i) {
    int row = (i / 20) + 1;
    int col = (i % 20) + 1;
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

  // Open and read the entire array.
  Array array1(ctx, array_name, TILEDB_READ);
  Query query1(ctx, array1);
  std::vector<int> a_data_read(400);
  std::vector<float> b_data_read(400);
  query1.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a", a_data_read)
      .set_data_buffer("b", b_data_read);
  query1.submit();
  auto table = query1.result_buffer_elements();
  REQUIRE(table.size() == 2);
  REQUIRE(table["a"].first == 0);
  REQUIRE(table["a"].second == 400);
  REQUIRE(table["b"].first == 0);
  REQUIRE(table["b"].second == 400);

  for (size_t i = 0; i < 400; ++i) {
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

  SECTION("No range, with query condition") {
    Array array2(ctx, array_name, TILEDB_READ);
    Query query2(ctx, array2);
    QueryCondition qc(ctx);
    float val = 4.0f;
    qc.init("b", &val, sizeof(float), TILEDB_LT);

    std::vector<int> a_data_read_2(400);
    std::vector<float> b_data_read_2(400);
    query2.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data_read_2)
        .set_data_buffer("b", b_data_read_2);
    query2.set_condition(qc);
    query2.submit();
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 200);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 200);

    for (int i = 0; i < 200; i++) {
      int og_i = 2 * i;
      REQUIRE(a_data_read_2[i] == 1);
      REQUIRE(a_data_read_2[i] == a_data_read[og_i]);
      REQUIRE(
          fabs(b_data_read_2[i] - b_data_read[og_i]) <
          std::numeric_limits<float>::epsilon());
    }

    query2.finalize();
    array2.close();
  }

  SECTION("Added range within a tile, with query condition") {
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
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 2);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 2);

    // testing (2,3)
    REQUIRE(a_data_read_2[0] == a_data_read[22]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[22]) <
        std::numeric_limits<float>::epsilon());

    // testing (3,3)
    REQUIRE(a_data_read_2[1] == a_data_read[42]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[42]) <
        std::numeric_limits<float>::epsilon());

    query2.finalize();
    array2.close();
  }

  SECTION(
      "Added range across rows dimension, within tile for cols dimension, with "
      "query condition") {
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
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 4);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 4);

    // testing (7,3)
    REQUIRE(a_data_read_2[0] == a_data_read[122]);
    REQUIRE(
        fabs(b_data_read_2[0] - b_data_read[122]) <
        std::numeric_limits<float>::epsilon());

    // testing (8,3)
    REQUIRE(a_data_read_2[1] == a_data_read[142]);
    REQUIRE(
        fabs(b_data_read_2[1] - b_data_read[142]) <
        std::numeric_limits<float>::epsilon());

    // testing (9,3)
    REQUIRE(a_data_read_2[2] == a_data_read[162]);
    REQUIRE(
        fabs(b_data_read_2[2] - b_data_read[162]) <
        std::numeric_limits<float>::epsilon());

    // testing (10,3)
    REQUIRE(a_data_read_2[3] == a_data_read[182]);
    REQUIRE(
        fabs(b_data_read_2[3] - b_data_read[182]) <
        std::numeric_limits<float>::epsilon());

    query2.finalize();
    array2.close();
  }

  /*

  SECTION("Within tile for rows dimension, added range across col dimension,
  with query condition") { Array array2(ctx, array_name, TILEDB_READ); Query
  query2(ctx, array2);

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
    auto table2 = query2.result_buffer_elements();
    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 4);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 4);

    std::vector<float> unordered_read_vals;
    unordered_read_vals.push_back(b_data[26]);
    unordered_read_vals.push_back(b_data[28]);
    unordered_read_vals.push_back(b_data[46]);
    unordered_read_vals.push_back(b_data[48]);

    std::sort(b_data_read_2.begin(), b_data_read_2.end());
    std::sort(unordered_read_vals.begin(), unordered_read_vals.end());

    for (size_t i = 0; i < 4; ++i) {
        REQUIRE(a_data_read_2[i] == 1);
        REQUIRE(
            fabs(b_data_read_2[i] - unordered_read_vals[i]) <
            std::numeric_limits<float>::epsilon());
    }

    query2.finalize();
    array2.close();
  }

  SECTION("Added range across tiles on both dimensions, with query condition") {
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
    auto table2 = query2.result_buffer_elements();

    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 32);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 32);

    for (int r = 7; r <= 14; ++r) {
      for (int c = 7; c <= 14; ++c) {
        int og_i = ((r - 1) * 20) + (c - 1);
        if (og_i % 2 == 0) {
          int i = ((r - 7) * 4) + ((c - 7) / 2);
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[og_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[og_i]) <
              std::numeric_limits<float>::epsilon());
        }
      }
    }

    query2.finalize();
    array2.close();
  }

  SECTION("Added range across tiles on both dimensions, with complex query
  condition") { Array array2(ctx, array_name, TILEDB_READ); Query query2(ctx,
  array2);

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
    auto table2 = query2.result_buffer_elements();

    REQUIRE(table2.size() == 2);
    REQUIRE(table2["a"].first == 0);
    REQUIRE(table2["a"].second == 8);
    REQUIRE(table2["b"].first == 0);
    REQUIRE(table2["b"].second == 8);

    for (int r = 7; r <= 14; ++r) {
      for (int c = 7; c <= 14; ++c) {
        int og_i = ((r - 1) * 20) + (c - 1);
        if (og_i % 8 == 4) {
          int i = r - 7;
          REQUIRE(a_data_read_2[i] == 1);
          REQUIRE(a_data_read_2[i] == a_data_read[og_i]);
          REQUIRE(
              fabs(b_data_read_2[i] - b_data_read[og_i]) <
              std::numeric_limits<float>::epsilon());
        }
      }
    }

    query2.finalize();
    array2.close();
  }
  */

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}