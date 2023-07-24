/**
 * @file   unit-cppapi-schema.cc
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
 * Tests the C++ API for schema related functions.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/constants.h"

#include <limits>

TEST_CASE(
    "C++ API: SchemaEvolution, add and drop attributes",
    "[cppapi][schema][evolution][add][drop]") {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);

  std::string array_uri = "test_schema_evolution_array";

  Domain domain(ctx);
  auto id1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  auto id2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
  CHECK_THROWS(id1.set_cell_val_num(4));
  CHECK_NOTHROW(id1.set_cell_val_num(1));
  domain.add_dimension(id1).add_dimension(id2);

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<int>(ctx, "a2");

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.add_attribute(a2);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_COL_MAJOR);

  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }

  Array::create(array_uri, schema);

  auto evolution = ArraySchemaEvolution(ctx);

  // add a new attribute a3
  auto a3 = Attribute::create<int>(ctx, "a3");
  evolution.add_attribute(a3);

  // drop attribute a1
  evolution.drop_attribute("a1");

  uint64_t now = tiledb_timestamp_now_ms();
  now = now + 1;
  evolution.set_timestamp_range({now, now});

  // evolve array
  evolution.array_evolve(array_uri);

  // read schema
  auto read_schema = Array::load_schema(ctx, array_uri);

  auto attrs = read_schema.attributes();
  CHECK(attrs.count("a1") == 0);
  CHECK(attrs.count("a2") == 1);
  CHECK(attrs.count("a3") == 1);

  // Clean up
  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }
}

TEST_CASE(
    "C++ API: SchemaEvolution, add attributes and read",
    "[cppapi][schema][evolution][add]") {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);
  auto layout = GENERATE(
      TILEDB_ROW_MAJOR,
      TILEDB_COL_MAJOR,
      TILEDB_UNORDERED,
      TILEDB_GLOBAL_ORDER);
  bool duplicates = GENERATE(true, false);

  std::string array_uri = "test_schema_evolution_array_read";

  // Create
  {
    Domain domain(ctx);
    auto id1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
    auto id2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
    domain.add_dimension(id1).add_dimension(id2);

    auto a = Attribute::create<int>(ctx, "a");

    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.set_allows_dups(duplicates);
    CHECK(duplicates == schema.allows_dups());
    schema.add_attribute(a);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_COL_MAJOR);

    if (vfs.is_dir(array_uri)) {
      vfs.remove_dir(array_uri);
    }

    Array::create(array_uri, schema);
  }

  // Write data
  {
    // Write some simple data to cells (1, 1), (2, 4) and (2, 3).
    std::vector<int> d1_data = {1, 2, 2};
    std::vector<int> d2_data = {1, 4, 3};
    std::vector<int> data = {1, 2, 3};

    // Open the array for writing and create the query.
    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", data)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  // Read
  {
    // Prepare the array for reading
    Array array(ctx, array_uri, TILEDB_READ);

    // Prepare the vector that will hold the result.
    // We take an upper bound on the result size, as we do not
    // know a priori how big it is (since the array is sparse)
    std::vector<int> data(3);
    std::vector<int> d1_data(3);
    std::vector<int> d2_data(3);

    // Prepare the query
    Query query(ctx, array, TILEDB_READ);
    query.add_range(0, 1, 4)
        .add_range(1, 1, 4)
        .set_layout(layout)
        .set_data_buffer("a", data)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Compare the results.
    auto result_num = (int)query.result_buffer_elements()["a"].second;
    CHECK(result_num == 3);
    // Same result buffers for all layouts
    CHECK_THAT(data, Catch::Matchers::Equals(std::vector<int>{1, 3, 2}));
    CHECK_THAT(d1_data, Catch::Matchers::Equals(std::vector<int>{1, 2, 2}));
    CHECK_THAT(d2_data, Catch::Matchers::Equals(std::vector<int>{1, 3, 4}));
  }

  // Evolve
  {
    uint64_t now = tiledb_timestamp_now_ms() + 1;
    ArraySchemaEvolution schemaEvolution = ArraySchemaEvolution(ctx);
    schemaEvolution.set_timestamp_range(std::make_pair(now, now));

    // Add attribute b
    Attribute b = Attribute::create<uint32_t>(ctx, "b");
    uint32_t fill_value = 1;
    b.set_fill_value(&fill_value, sizeof(fill_value));
    schemaEvolution.add_attribute(b);

    Attribute c = Attribute::create<uint32_t>(ctx, "c");
    uint32_t fill_value_c = 2;
    c.set_nullable(true);
    c.set_fill_value(&fill_value_c, sizeof(fill_value_c), false);
    schemaEvolution.add_attribute(c);

    Attribute d = Attribute::create<std::string>(ctx, "d");
    std::string fill_value_d = "test";
    d.set_fill_value(fill_value_d.c_str(), fill_value_d.size());
    schemaEvolution.add_attribute(d);

    Attribute e = Attribute::create<std::string>(ctx, "e");
    std::string fill_value_e = "n";
    e.set_nullable(true);
    e.set_fill_value(fill_value_e.c_str(), fill_value_e.size(), false);
    schemaEvolution.add_attribute(e);

    // evolve array
    schemaEvolution.array_evolve(array_uri);

    // read schema
    auto read_schema = Array::load_schema(ctx, array_uri);

    auto attrs = read_schema.attributes();
    CHECK(attrs.count("a") == 1);
    CHECK(attrs.count("b") == 1);
    CHECK(attrs.count("c") == 1);
    CHECK(attrs.count("d") == 1);
    CHECK(attrs.count("e") == 1);
  }

  // Write again
  {
    // Write some simple data to cell (3, 1)
    std::vector<int> d1_data = {3};
    std::vector<int> d2_data = {1};
    std::vector<int> a_data = {4};
    std::vector<uint32_t> b_data = {4};
    std::vector<uint32_t> c_data = {40};
    std::vector<uint8_t> c_validity = {1};
    std::vector<char> d_data = {'d'};
    std::vector<uint64_t> d_offsets = {0};
    std::vector<char> e_data = {'e'};
    std::vector<uint64_t> e_offsets = {0};
    std::vector<uint8_t> e_validity = {1};

    // Open the array for writing and create the query.
    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data)
        .set_data_buffer("c", c_data)
        .set_validity_buffer("c", c_validity)
        .set_data_buffer("d", d_data)
        .set_offsets_buffer("d", d_offsets)
        .set_data_buffer("e", e_data)
        .set_validity_buffer("e", e_validity)
        .set_offsets_buffer("e", e_offsets)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  // Read Again
  {
    // Prepare the array for reading
    Array array(ctx, array_uri, TILEDB_READ);

    // Prepare the vector that will hold the result.
    // We take an upper bound on the result size, as we do not
    // know a priori how big it is (since the array is sparse)
    std::vector<int> a_data(4);
    std::vector<uint32_t> b_data(4);
    std::vector<uint32_t> c_data(4);
    std::vector<uint8_t> c_validity(4);
    std::vector<char> d_data(13);
    std::vector<uint64_t> d_offsets(4);
    std::vector<char> e_data(4);
    std::vector<uint8_t> e_validity(4);
    std::vector<uint64_t> e_offsets(4);
    std::vector<int> d1_data(4);
    std::vector<int> d2_data(4);

    // Prepare the query
    Query query(ctx, array, TILEDB_READ);
    query.add_range(0, 1, 4)
        .add_range(1, 1, 4)
        .set_layout(layout)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data)
        .set_data_buffer("c", c_data)
        .set_validity_buffer("c", c_validity)
        .set_data_buffer("d", d_data)
        .set_offsets_buffer("d", d_offsets)
        .set_data_buffer("e", e_data)
        .set_offsets_buffer("e", e_offsets)
        .set_validity_buffer("e", e_validity)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Compare the results.
    auto result_num = (int)query.result_buffer_elements()["a"].second;
    CHECK(result_num == 4);
    if (layout == TILEDB_COL_MAJOR) {
      CHECK_THAT(a_data, Catch::Matchers::Equals(std::vector<int>{1, 4, 3, 2}));
      CHECK_THAT(
          b_data, Catch::Matchers::Equals(std::vector<uint32_t>{1, 4, 1, 1}));
      CHECK_THAT(
          c_data, Catch::Matchers::Equals(std::vector<uint32_t>{2, 40, 2, 2}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{0, 1, 0, 0}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(std::vector<char>{
              't',
              'e',
              's',
              't',
              'd',
              't',
              'e',
              's',
              't',
              't',
              'e',
              's',
              't'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{0, 4, 5, 9}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(std::vector<char>{'n', 'e', 'n', 'n'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{0, 1, 2, 3}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{0, 1, 0, 0}));
      CHECK_THAT(
          d1_data, Catch::Matchers::Equals(std::vector<int>{1, 3, 2, 2}));
      CHECK_THAT(
          d2_data, Catch::Matchers::Equals(std::vector<int>{1, 1, 3, 4}));
    } else {
      // Check values for unordered, global, and row-major
      CHECK_THAT(a_data, Catch::Matchers::Equals(std::vector<int>{1, 3, 2, 4}));
      CHECK_THAT(
          b_data, Catch::Matchers::Equals(std::vector<uint32_t>{1, 1, 1, 4}));
      CHECK_THAT(
          c_data, Catch::Matchers::Equals(std::vector<uint32_t>{2, 2, 2, 40}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{0, 0, 0, 1}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(std::vector<char>{
              't',
              'e',
              's',
              't',
              't',
              'e',
              's',
              't',
              't',
              'e',
              's',
              't',
              'd'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{0, 4, 8, 12}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(std::vector<char>{'n', 'n', 'n', 'e'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{0, 1, 2, 3}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{0, 0, 0, 1}));
      CHECK_THAT(
          d1_data, Catch::Matchers::Equals(std::vector<int>{1, 2, 2, 3}));
      CHECK_THAT(
          d2_data, Catch::Matchers::Equals(std::vector<int>{1, 3, 4, 1}));
    }
  }

  // Read using overlapping multi-range query
  // + Global order does not support multi-range subarrays
  if (layout != TILEDB_GLOBAL_ORDER) {
    Array array(ctx, array_uri, TILEDB_READ);

    std::vector<int> a_data(8);
    std::vector<uint32_t> b_data(8);
    std::vector<uint32_t> c_data(8);
    std::vector<uint8_t> c_validity(8);
    std::vector<char> d_data(26);
    std::vector<uint64_t> d_offsets(8);
    std::vector<char> e_data(8);
    std::vector<uint8_t> e_validity(8);
    std::vector<uint64_t> e_offsets(8);
    std::vector<int> d1_data(8);
    std::vector<int> d2_data(8);

    // Resize buffers for multi-range unordered read
    if (layout == TILEDB_UNORDERED) {
      a_data.resize(16);
      b_data.resize(16);
      c_data.resize(16);
      c_validity.resize(16);
      d_data.resize(52);
      d_offsets.resize(16);
      e_data.resize(16);
      e_validity.resize(16);
      e_offsets.resize(16);
      d1_data.resize(16);
      d2_data.resize(16);
    }

    // Prepare the query
    Query query(ctx, array, TILEDB_READ);
    query.add_range(0, 1, 4)
        .add_range(0, 1, 4)
        .add_range(1, 1, 4)
        .add_range(1, 1, 4)
        .set_layout(layout)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data)
        .set_data_buffer("c", c_data)
        .set_validity_buffer("c", c_validity)
        .set_data_buffer("d", d_data)
        .set_offsets_buffer("d", d_offsets)
        .set_data_buffer("e", e_data)
        .set_offsets_buffer("e", e_offsets)
        .set_validity_buffer("e", e_validity)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Compare the results.
    if (layout == TILEDB_COL_MAJOR) {
      auto result_num = (int)query.result_buffer_elements()["a"].second;
      CHECK(result_num == 8);

      CHECK_THAT(
          a_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 4, 4, 3, 3, 2, 2}));
      CHECK_THAT(
          b_data,
          Catch::Matchers::Equals(
              std::vector<uint32_t>{1, 1, 4, 4, 1, 1, 1, 1}));
      CHECK_THAT(
          c_data,
          Catch::Matchers::Equals(
              std::vector<uint32_t>{2, 2, 40, 40, 2, 2, 2, 2}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(
              std::vector<uint8_t>{0, 0, 1, 1, 0, 0, 0, 0}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(
              std::vector<char>{'t', 'e', 's', 't', 't', 'e', 's', 't', 'd',
                                'd', 't', 'e', 's', 't', 't', 'e', 's', 't',
                                't', 'e', 's', 't', 't', 'e', 's', 't'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(
              std::vector<uint64_t>{0, 4, 8, 9, 10, 14, 18, 22}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(
              std::vector<char>{'n', 'n', 'e', 'e', 'n', 'n', 'n', 'n'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(
              std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(
              std::vector<uint8_t>{0, 0, 1, 1, 0, 0, 0, 0}));
      CHECK_THAT(
          d1_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 3, 3, 2, 2, 2, 2}));
      CHECK_THAT(
          d2_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 1, 1, 3, 3, 4, 4}));
    } else if (layout == TILEDB_ROW_MAJOR) {
      auto result_num = (int)query.result_buffer_elements()["a"].second;
      CHECK(result_num == 8);

      CHECK_THAT(
          a_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 3, 3, 2, 2, 4, 4}));
      CHECK_THAT(
          b_data,
          Catch::Matchers::Equals(
              std::vector<uint32_t>{1, 1, 1, 1, 1, 1, 4, 4}));
      CHECK_THAT(
          c_data,
          Catch::Matchers::Equals(
              std::vector<uint32_t>{2, 2, 2, 2, 2, 2, 40, 40}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(
              std::vector<uint8_t>{0, 0, 0, 0, 0, 0, 1, 1}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(
              std::vector<char>{'t', 'e', 's', 't', 't', 'e', 's', 't', 't',
                                'e', 's', 't', 't', 'e', 's', 't', 't', 'e',
                                's', 't', 't', 'e', 's', 't', 'd', 'd'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(
              std::vector<uint64_t>{0, 4, 8, 12, 16, 20, 24, 25}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(
              std::vector<char>{'n', 'n', 'n', 'n', 'n', 'n', 'e', 'e'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(
              std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(
              std::vector<uint8_t>{0, 0, 0, 0, 0, 0, 1, 1}));
      CHECK_THAT(
          d1_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 2, 2, 2, 2, 3, 3}));
      CHECK_THAT(
          d2_data,
          Catch::Matchers::Equals(std::vector<int>{1, 1, 3, 3, 4, 4, 1, 1}));
    } else if (layout == TILEDB_UNORDERED) {
      auto result_num = (int)query.result_buffer_elements()["a"].second;
      CHECK(result_num == 16);

      CHECK_THAT(
          a_data,
          Catch::Matchers::Equals(std::vector<int>{
              1, 1, 1, 1, 3, 3, 3, 3, 2, 2, 2, 2, 4, 4, 4, 4}));
      CHECK_THAT(
          b_data,
          Catch::Matchers::Equals(std::vector<uint32_t>{
              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 4, 4, 4}));
      CHECK_THAT(
          c_data,
          Catch::Matchers::Equals(std::vector<uint32_t>{
              2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 40, 40, 40, 40}));
      CHECK_THAT(
          c_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1}));
      CHECK_THAT(
          d_data,
          Catch::Matchers::Equals(std::vector<char>{
              't', 'e', 's', 't', 't', 'e', 's', 't', 't', 'e', 's',
              't', 't', 'e', 's', 't', 't', 'e', 's', 't', 't', 'e',
              's', 't', 't', 'e', 's', 't', 't', 'e', 's', 't', 't',
              'e', 's', 't', 't', 'e', 's', 't', 't', 'e', 's', 't',
              't', 'e', 's', 't', 'd', 'd', 'd', 'd'}));
      CHECK_THAT(
          d_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{
              0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 49, 50, 51}));
      CHECK_THAT(
          e_data,
          Catch::Matchers::Equals(std::vector<char>{
              'n',
              'n',
              'n',
              'n',
              'n',
              'n',
              'n',
              'n',
              'n',
              'n',
              'n',
              'n',
              'e',
              'e',
              'e',
              'e'}));
      CHECK_THAT(
          e_offsets,
          Catch::Matchers::Equals(std::vector<uint64_t>{
              0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}));
      CHECK_THAT(
          e_validity,
          Catch::Matchers::Equals(std::vector<uint8_t>{
              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1}));
      CHECK_THAT(
          d1_data,
          Catch::Matchers::Equals(std::vector<int>{
              1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3}));
      CHECK_THAT(
          d2_data,
          Catch::Matchers::Equals(std::vector<int>{
              1, 1, 1, 1, 3, 3, 3, 3, 4, 4, 4, 4, 1, 1, 1, 1}));
    }
  }

  // Clean up
  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }
}

TEST_CASE(
    "C++ API: SchemaEvolution, add and drop attributes",
    "[cppapi][schema][evolution][add][query-condition]") {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);
  auto layout = GENERATE(
      TILEDB_ROW_MAJOR,
      TILEDB_COL_MAJOR,
      TILEDB_UNORDERED,
      TILEDB_GLOBAL_ORDER);
  bool duplicates = GENERATE(true, false);

  const char* out_str = nullptr;
  tiledb_layout_to_str(layout, &out_str);
  std::string array_uri = "test_schema_evolution_query_condition";

  {
    Domain domain(ctx);
    auto id1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
    auto id2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
    domain.add_dimension(id1).add_dimension(id2);

    auto a = Attribute::create<int>(ctx, "a");

    ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain);
    schema.set_allows_dups(duplicates);
    CHECK(duplicates == schema.allows_dups());
    schema.add_attribute(a);
    schema.set_cell_order(TILEDB_ROW_MAJOR);
    schema.set_tile_order(TILEDB_COL_MAJOR);

    if (vfs.is_dir(array_uri)) {
      vfs.remove_dir(array_uri);
    }

    Array::create(array_uri, schema);
  }

  // Write data
  {
    // Write some simple data to cells (1, 1), (2, 2) and (3, 3).
    std::vector<int> d1_data = {1, 2, 3};
    std::vector<int> d2_data = {1, 2, 3};
    std::vector<int> data = {1, 2, 3};

    // Open the array for writing and create the query.
    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", data)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  // Evolve
  {
    uint64_t now = tiledb_timestamp_now_ms() + 1;
    ArraySchemaEvolution schemaEvolution = ArraySchemaEvolution(ctx);
    schemaEvolution.set_timestamp_range(std::make_pair(now, now));

    // Add attribute b
    Attribute b = Attribute::create<uint32_t>(ctx, "b");
    uint32_t fill_value = 1;
    b.set_fill_value(&fill_value, sizeof(fill_value));
    schemaEvolution.add_attribute(b);

    schemaEvolution.array_evolve(array_uri);
  }

  // Write again
  {
    // Write some simple data to cell (4, 1)
    std::vector<int> d1_data = {4};
    std::vector<int> d2_data = {1};
    std::vector<int> a_data = {4};
    std::vector<uint32_t> b_data = {4};

    // Open the array for writing and create the query.
    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array, TILEDB_WRITE);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  // Read with Query Condition
  {
    // Prepare the array for reading
    Array array(ctx, array_uri, TILEDB_READ);

    // Prepare the vector that will hold the result.
    // We take an upper bound on the result size, as we do not
    // know a priori how big it is (since the array is sparse)
    std::vector<int> a_data(4);
    std::vector<uint32_t> b_data(4);
    std::vector<int> d1_data(4);
    std::vector<int> d2_data(4);

    // Create Query Condition
    // Note: this query condition must be on the evolved attribute
    //       in order to reproduce SC-23671.
    int value = 4;
    QueryCondition query_condition(ctx);
    query_condition.init("b", &value, sizeof(value), TILEDB_EQ);

    // Prepare the query
    Query query(ctx, array, TILEDB_READ);
    query.set_condition(query_condition)
        .set_layout(layout)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data)
        .set_data_buffer("d1", d1_data)
        .set_data_buffer("d2", d2_data);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Compare the results
    auto result_num = (int)query.result_buffer_elements()["a"].second;
    CHECK(result_num == 1);
    a_data.resize(result_num);
    // should this be here?
    b_data.resize(result_num);
    d1_data.resize(result_num);
    d2_data.resize(result_num);
    CHECK_THAT(a_data, Catch::Matchers::Equals(std::vector<int>{4}));
    CHECK_THAT(b_data, Catch::Matchers::Equals(std::vector<uint32_t>{4}));
    CHECK_THAT(d1_data, Catch::Matchers::Equals(std::vector<int>{4}));
    CHECK_THAT(d2_data, Catch::Matchers::Equals(std::vector<int>{1}));
  }

  // Cleanup.
  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }
}

TEST_CASE(
    "SchemaEvolution Error Handling Tests",
    "[cppapi][schema][evolution][errors]") {
  auto ase = make_shared<tiledb::sm::ArraySchemaEvolution>(HERE());
  REQUIRE_THROWS(ase->evolve_schema(nullptr));
  REQUIRE_THROWS(ase->add_attribute(nullptr));

  auto attr = make_shared<tiledb::sm::Attribute>(
      HERE(), "attr", tiledb::sm::Datatype::STRING_ASCII);
  ase->add_attribute(attr.get());
  REQUIRE_THROWS(ase->add_attribute(attr.get()));

  ase->set_timestamp_range(std::make_pair(1, 1));

  auto schema = make_shared<tiledb::sm::ArraySchema>(
      HERE(), tiledb::sm::ArrayType::SPARSE);
  auto dim = make_shared<tiledb::sm::Dimension>(
      HERE(), "dim1", tiledb::sm::Datatype::INT32);
  int range[2] = {0, 1000};
  throw_if_not_ok(dim->set_domain(range));

  auto dom = make_shared<tiledb::sm::Domain>(HERE());
  throw_if_not_ok(dom->add_dimension(dim));
  throw_if_not_ok(schema->set_domain(dom));

  CHECK_NOTHROW(ase->evolve_schema(schema));
}
