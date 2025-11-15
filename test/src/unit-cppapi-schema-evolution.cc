/**
 * @file   unit-cppapi-schema-evolution.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2025 TileDB Inc.
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
 * Tests the C++ API for schema evolution.
 */

#include <test/support/src/vfs_helpers.h>
#include <test/support/tdb_catch.h>
#include "test/support/src/array_helpers.h"
#include "test/support/src/array_schema_helpers.h"
#include "test/support/src/mem_helpers.h"

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

using namespace tiledb;

/**
 * @return a simple schema with dimension d1 and attribute a1
 */
static ArraySchema simple_schema(Context& ctx) {
  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  domain.add_dimension(d1);

  auto a1 = Attribute::create<int>(ctx, "a1");

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);

  return schema;
}

TEST_CASE(
    "C++ API: SchemaEvolution, add and drop attributes",
    "[cppapi][schema][evolution][add][drop][rest]") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{vfs_test_setup.array_uri("test_schema_evolution_array")};

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
}

TEST_CASE(
    "C++ API: SchemaEvolution, check error when dropping dimension",
    "[cppapi][schema][evolution][drop][rest]") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{vfs_test_setup.array_uri("test_schema_evolution_array")};

  Domain domain(ctx);
  auto id1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  auto id2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 5);
  domain.add_dimension(id1).add_dimension(id2);

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<int>(ctx, "a2");

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.add_attribute(a2);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_COL_MAJOR);

  Array::create(array_uri, schema);

  auto evolution = ArraySchemaEvolution(ctx);

  // try to drop dimension d1
  evolution.drop_attribute("d1");

  // check that an exception is thrown
  CHECK_THROWS(evolution.array_evolve(array_uri));
}

TEST_CASE(
    "C++ API: SchemaEvolution, add attributes and read",
    "[cppapi][schema][evolution][add][rest]") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};

  auto layout = GENERATE(
      TILEDB_ROW_MAJOR,
      TILEDB_COL_MAJOR,
      TILEDB_UNORDERED,
      TILEDB_GLOBAL_ORDER);
  bool duplicates = GENERATE(true, false);
  auto array_uri{vfs_test_setup.array_uri("test_schema_evolution_array")};

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
    Subarray subarray(ctx, array);
    subarray.add_range(0, 1, 4).add_range(1, 1, 4);
    query.set_subarray(subarray)
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
    Subarray subarray(ctx, array);
    subarray.add_range(0, 1, 4).add_range(1, 1, 4);
    query.set_subarray(subarray)
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
  // Disable merge overlapping sparse ranges.
  // Support for returning multiplicities for overlapping ranges will be
  // deprecated in a few releases. Turning off this setting allows to still
  // test that the feature functions properly until we do so. Once support is
  // fully removed for overlapping ranges, this read can be removed from the
  // test case.
  Config cfg;
  cfg["sm.merge_overlapping_ranges_experimental"] = "false";
  vfs_test_setup.update_config(cfg.ptr().get());
  // + Global order does not support multi-range subarrays
  if (layout != TILEDB_GLOBAL_ORDER) {
    ctx = vfs_test_setup.ctx();

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
    Subarray subarray(ctx, array);
    subarray.set_config(cfg);
    subarray.add_range(0, 1, 4).add_range(0, 1, 4).add_range(1, 1, 4).add_range(
        1, 1, 4);
    subarray.set_config(cfg);
    query.set_subarray(subarray)
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
}

/**
 * Validates the behavior of query conditions after evolution changes on an
 * attribute with identical names across schemas.
 *
 * Initially the attribute is fixed-size and has an enumeration label set.
 * After evolution the attribute becomes var-sized and has no enumeration label.
 */
TEST_CASE(
    "C++ API: SchemaEvolution, drop and add attribute",
    "[cppapi][schema][evolution][drop][add][query-condition][rest]") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto layout = GENERATE(
      TILEDB_ROW_MAJOR,
      TILEDB_COL_MAJOR,
      TILEDB_UNORDERED,
      TILEDB_GLOBAL_ORDER);
  bool duplicates = GENERATE(true, false);
  bool nullable = GENERATE(true, false);
  auto array_type = GENERATE(TILEDB_SPARSE, TILEDB_DENSE);
  bool sparse = array_type == TILEDB_SPARSE;

  const char* layout_str = nullptr;
  tiledb_layout_to_str(layout, &layout_str);
  const char* type_str = nullptr;
  tiledb_array_type_to_str(array_type, &type_str);
  auto array_uri{
      vfs_test_setup.array_uri("test_schema_evolution_query_condition_v2")};

  DYNAMIC_SECTION(
      type_str << " " << layout_str << " array"
               << " with duplicates=" << (duplicates ? "true" : "false")
               << " nullable=" << (nullable ? "true" : "false")) {
    {
      ArraySchema schema(ctx, array_type);
      // Duplicates are not supported for dense arrays.
      if (sparse) {
        schema.set_allows_dups(duplicates);
        CHECK(duplicates == schema.allows_dups());
      }
      schema.set_cell_order(TILEDB_ROW_MAJOR);
      schema.set_tile_order(TILEDB_COL_MAJOR);

      Domain domain(ctx);
      auto id1 = Dimension::create<int>(ctx, "d1", {{1, 4}}, 2);
      auto id2 = Dimension::create<int>(ctx, "d2", {{1, 4}}, 2);
      domain.add_dimension(id1).add_dimension(id2);
      schema.set_domain(domain);

      std::vector<std::string> enum_values = {"A", "B", "C", "D"};
      auto e = Enumeration::create(ctx, "a_label", enum_values, false);
      ArraySchemaExperimental::add_enumeration(ctx, schema, e);

      auto a = Attribute::create<int>(ctx, "a");
      AttributeExperimental::set_enumeration_name(ctx, a, "a_label");
      a.set_nullable(nullable);
      schema.add_attribute(a);

      auto b = Attribute::create<float>(ctx, "b");
      float b_fill = -1.0f;
      uint64_t size = sizeof(b_fill);
      b.set_fill_value(&b_fill, size);
      schema.add_attribute(b);

      Array::create(array_uri, schema);
    }

    // Write data
    {
      Array array(ctx, array_uri, TILEDB_WRITE);
      Query query(ctx, array, TILEDB_WRITE);
      std::vector<int> d1_data = {1, 1, 2, 2};
      std::vector<int> d2_data = {1, 2, 1, 2};
      std::vector<int> a_data = {1, 2, 3, 4};
      std::vector<uint8_t> a_validity = {1, 1, 1, 1};
      std::vector<float> b_data = {1.1f, 2.2f, 3.3f, 4.4f};

      // Set coordinates.
      if (sparse) {
        query.set_data_buffer("d1", d1_data).set_data_buffer("d2", d2_data);
      } else {
        Subarray subarray(ctx, array);
        subarray.add_range<int>(0, 1, 2).add_range<int>(1, 1, 2);
        query.set_subarray(subarray);
      }

      // Set data buffers.
      query.set_data_buffer("a", a_data).set_data_buffer("b", b_data);

      if (nullable) {
        query.set_validity_buffer("a", a_validity);
      }

      // Perform the write and close the array.
      CHECK(query.submit() == Query::Status::COMPLETE);
      array.close();
    }

    // Evolve
    {
      // Drop attribute 'a'.
      uint64_t now = tiledb_timestamp_now_ms() + 1;
      ArraySchemaEvolution schemaEvolution = ArraySchemaEvolution(ctx);
      schemaEvolution.set_timestamp_range(std::make_pair(now, now));
      schemaEvolution.drop_attribute("a").array_evolve(array_uri);

      // Add attribute 'a' without an enumeration label.
      // Also modify it's datatype from fixed int to var-size string.
      schemaEvolution = ArraySchemaEvolution(ctx);
      now += 1;  // Ensure schema timestamps are unique.
      schemaEvolution.set_timestamp_range({now, now});
      Attribute a = Attribute::create<std::string>(ctx, "a");
      // Invert nullability of the attribute after evolution.
      a.set_nullable(!nullable);
      schemaEvolution.add_attribute(a);
      schemaEvolution.array_evolve(array_uri);
    }

    // Update the bool to reflect inverted nullability of 'a' during evolution.
    nullable = !nullable;

    // Write again
    {
      Array array(ctx, array_uri, TILEDB_WRITE);
      Query query(ctx, array, TILEDB_WRITE);

      std::vector<int> d1_data = {1, 1, 2, 2};
      std::vector<int> d2_data = {1, 2, 1, 2};
      std::string a_data = "ABCD";
      std::vector<uint64_t> a_offsets = {0, 1, 2, 3};
      std::vector<uint8_t> a_validity = {1, 1, 1, 1};
      std::vector<float> b_data = {5.5f, 6.6f, 7.7f, 8.8f};

      // Set coordinates.
      if (sparse) {
        query.set_data_buffer("d1", d1_data).set_data_buffer("d2", d2_data);
      } else {
        Subarray subarray(ctx, array);
        subarray.add_range<int>(0, 1, 2).add_range<int>(1, 1, 2);
        query.set_subarray(subarray);
      }

      query.set_data_buffer("a", a_data)
          .set_offsets_buffer("a", a_offsets)
          .set_data_buffer("b", b_data);

      if (nullable) {
        query.set_validity_buffer("a", a_validity);
      }

      CHECK(query.submit() == Query::Status::COMPLETE);
      array.close();
    }

    // Read with Query Condition
    {
      Array array(ctx, array_uri, TILEDB_READ);

      std::vector<int> d1_data(4);
      std::vector<int> d2_data(4);
      std::string a_data(4, 'Z');
      std::vector<uint64_t> a_offsets(4);
      std::vector<uint8_t> a_validity(4);
      std::vector<float> b_data(4);

      // Sparse array with column major layout returns INCOMPLETE with 4
      // elements.
      if (sparse && layout == TILEDB_COL_MAJOR) {
        d1_data.resize(8);
        d2_data.resize(8);
        a_offsets.resize(5);
        a_validity.resize(5);
        b_data.resize(8);
      }

      char value = 'C';
      QueryCondition query_condition(ctx);
      query_condition.init("a", &value, sizeof(value), TILEDB_EQ);

      Query query(ctx, array, TILEDB_READ);
      query.set_condition(query_condition)
          .set_data_buffer("d1", d1_data)
          .set_data_buffer("d2", d2_data)
          .set_data_buffer("a", a_data)
          .set_offsets_buffer("a", a_offsets)
          .set_data_buffer("b", b_data);
      if (nullable) {
        query.set_validity_buffer("a", a_validity);
      }

      Subarray subarray(ctx, array);
      subarray.add_range<int>(0, 1, 2).add_range<int>(1, 1, 2);
      query.set_subarray(subarray);

      if (sparse) {
        query.set_layout(layout);
      }

      CHECK(query.submit() == Query::Status::COMPLETE);
      array.close();

      size_t result_num = query.result_buffer_elements()["a"].second;
      CHECK(result_num == (sparse ? 1 : 4));
      // Resize data buffers to prune the unused elements with no result.
      d1_data.resize(result_num);
      d2_data.resize(result_num);
      a_data.resize(result_num);
      a_validity.resize(result_num);
      b_data.resize(result_num);

      if (sparse) {
        CHECK(d1_data == std::vector<int>{2});
        CHECK(d2_data == std::vector<int>{1});
        CHECK(a_data == "C");
        if (nullable) {
          CHECK(a_validity == std::vector<uint8_t>{1});
        }
        CHECK(b_data == std::vector<float>{7.7f});
      } else {
        CHECK(d1_data == std::vector<int>{1, 1, 2, 2});
        CHECK(d2_data == std::vector<int>{1, 2, 1, 2});
        // Dense reads return fill values for cells that do not satisfy the QC.
        CHECK(a_data[2] == 'C');
        if (nullable) {
          CHECK(a_validity == std::vector<uint8_t>{0, 0, 1, 0});
        }
        CHECK(b_data == std::vector<float>{-1.0f, -1.0f, 7.7f, -1.0f});
      }
    }
  }
}

TEST_CASE(
    "C++ API: SchemaEvolution, add attributes",
    "[cppapi][schema][evolution][add][query-condition][rest]") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto layout = GENERATE(
      TILEDB_ROW_MAJOR,
      TILEDB_COL_MAJOR,
      TILEDB_UNORDERED,
      TILEDB_GLOBAL_ORDER);
  bool duplicates = GENERATE(true, false);

  const char* out_str = nullptr;
  tiledb_layout_to_str(layout, &out_str);
  auto array_uri{
      vfs_test_setup.array_uri("test_schema_evolution_query_condition")};

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
}

/**
 * C++ API: SchemaEvolution, drop fixed attribute and add back as var-sized
 *
 * Wrapper function for the following test case of the same name.
 * This logic has been moved into a function to resolve intermittent failures
 * when using Catch2's GENERATE statements inline with code under test
 * (https://app.shortcut.com/tiledb-inc/story/61528).
 * We should re-evaluate when Catch2 is upgraded.
 */
void test_schema_evolution_drop_fixed_add_var(
    tiledb_array_type_t array_type, tiledb_layout_t layout) {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{
      vfs_test_setup.array_uri("test_schema_evolution_drop_fixed_add_var")};

  // Create array
  Domain domain(ctx);
  auto d = Dimension::create<int>(ctx, "d", {{1, 10}}, 1);
  domain.add_dimension(d);
  auto a = Attribute::create<int>(ctx, "a");
  auto b = Attribute::create<int>(ctx, "b");
  ArraySchema schema(ctx, array_type);
  schema.set_domain(domain);
  schema.set_allows_dups(false);
  schema.add_attribute(a);
  schema.add_attribute(b);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_COL_MAJOR);
  Array::create(array_uri, schema);

  // Write a fragment to the array
  std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  Array array_w(ctx, array_uri, TILEDB_WRITE);
  Query query_w(ctx, array_w, TILEDB_WRITE);
  query_w.set_layout(TILEDB_GLOBAL_ORDER)
      .set_data_buffer("a", data)
      .set_data_buffer("b", data);
  if (array_type == TILEDB_SPARSE) {
    query_w.set_data_buffer("d", data);
  }
  query_w.submit_and_finalize();
  array_w.close();
  uint64_t initial_ts = tiledb_timestamp_now_ms();

  // Evolve schema to drop attribute "a"
  ArraySchemaEvolution schema_evolution = ArraySchemaEvolution(ctx);
  uint64_t now = initial_ts + 1;
  schema_evolution.set_timestamp_range(std::make_pair(now, now));
  schema_evolution.drop_attribute("a");
  schema_evolution.array_evolve(array_uri);

  // Evolve schema to add back attribute "a" as a string
  auto a_new = Attribute::create<std::string>(ctx, "a");
  now = initial_ts + 2;
  schema_evolution.set_timestamp_range(std::make_pair(now, now));
  schema_evolution.add_attribute(a_new);
  schema_evolution.array_evolve(array_uri);

  // Read the array with evolved schema
  std::string buffer;
  std::vector<uint64_t> offsets(10);
  TemporalPolicy temporal_latest(TimeTravel, initial_ts + 2);
  Array array_r(ctx, array_uri, TILEDB_READ, temporal_latest);
  Subarray subarray_r(ctx, array_r);
  subarray_r.add_range(0, 1, 10);
  Query query_r(ctx, array_r, TILEDB_READ);
  query_r.set_layout(layout)
      .set_subarray(subarray_r)
      .set_data_buffer("a", buffer)
      .set_offsets_buffer("a", offsets);
  query_r.submit();
  array_r.close();
  CHECK(buffer.size() == 0);
  CHECK_THAT(buffer, Catch::Matchers::Equals(std::string{""}));
  CHECK(offsets.size() == 10);
  CHECK_THAT(
      offsets,
      Catch::Matchers::Equals(
          std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

  // Read the original array
  std::vector<int> a_data(10);
  TemporalPolicy temporal_initial(TimeTravel, initial_ts);
  Array array_r2(ctx, array_uri, TILEDB_READ, temporal_initial);
  Subarray subarray_r2(ctx, array_r2);
  subarray_r2.add_range(0, 1, 10);
  Query query_r2(ctx, array_r2, TILEDB_READ);
  query_r2.set_layout(layout)
      .set_subarray(subarray_r2)
      .set_data_buffer("a", a_data);
  query_r2.submit();
  array_r2.close();
  auto result_num = (int)query_r2.result_buffer_elements()["a"].second;
  CHECK(result_num == 10);
  a_data.resize(result_num);
  CHECK_THAT(
      a_data,
      Catch::Matchers::Equals(std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
}

TEST_CASE(
    "C++ API: SchemaEvolution, drop fixed attribute and add back as var-sized",
    "[cppapi][schema][evolution][add][drop]") {
  test_schema_evolution_drop_fixed_add_var(TILEDB_DENSE, TILEDB_UNORDERED);
  test_schema_evolution_drop_fixed_add_var(TILEDB_DENSE, TILEDB_GLOBAL_ORDER);
  test_schema_evolution_drop_fixed_add_var(TILEDB_SPARSE, TILEDB_UNORDERED);
  test_schema_evolution_drop_fixed_add_var(TILEDB_SPARSE, TILEDB_GLOBAL_ORDER);
}

TEST_CASE(
    "SchemaEvolution Error Handling Tests",
    "[cppapi][schema][evolution][errors][rest]") {
  auto ase = make_shared<tiledb::sm::ArraySchemaEvolution>(
      HERE(), tiledb::test::create_test_memory_tracker());
  REQUIRE_THROWS(ase->evolve_schema(nullptr));
  REQUIRE_THROWS(ase->add_attribute(nullptr));

  auto attr = make_shared<tiledb::sm::Attribute>(
      HERE(), "attr", tiledb::sm::Datatype::STRING_ASCII);
  ase->add_attribute(attr);
  REQUIRE_THROWS(ase->add_attribute(attr));

  ase->set_timestamp_range(std::make_pair(1, 1));

  auto schema = make_shared<tiledb::sm::ArraySchema>(
      HERE(),
      tiledb::sm::ArrayType::SPARSE,
      tiledb::test::create_test_memory_tracker());
  auto dim = make_shared<tiledb::sm::Dimension>(
      HERE(),
      "dim1",
      tiledb::sm::Datatype::INT32,
      tiledb::test::get_test_memory_tracker());
  int range[2] = {0, 1000};
  dim->set_domain(range);

  auto dom = make_shared<tiledb::sm::Domain>(
      HERE(), tiledb::test::get_test_memory_tracker());
  dom->add_dimension(dim);
  schema->set_domain(dom);

  CHECK_NOTHROW(ase->evolve_schema(schema));
}

TEST_CASE(
    "C++ API: SchemaEvolution add multiple attributes",
    "[cppapi][schema][evolution][add]") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{vfs_test_setup.array_uri(
      "test_schema_evolution_add_multiple_attributes")};

  // create initial array
  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  domain.add_dimension(d1);

  auto a1 = Attribute::create<int>(ctx, "a1");

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);

  auto add_attributes = std::vector<Attribute>{
      Attribute::create<int>(ctx, "a2"),
      Attribute::create<int>(ctx, "a3"),
      Attribute::create<int>(ctx, "a4")};

  auto permutation = GENERATE(
      std::vector<int>{0, 1, 2},
      std::vector<int>{0, 2, 1},
      std::vector<int>{1, 0, 2},
      std::vector<int>{1, 2, 0},
      std::vector<int>{2, 0, 1},
      std::vector<int>{2, 1, 0});

  DYNAMIC_SECTION(
      "Add a1/a2/a3 in permutation: " + std::to_string(permutation[0]) + "/" +
      std::to_string(permutation[1]) + std::to_string(permutation[2])) {
    // create array
    Array::create(array_uri, schema);
    test::DeleteArrayGuard guard(ctx.ptr().get(), array_uri.c_str());

    // evolve it
    auto evolution = ArraySchemaEvolution(ctx);
    for (auto idx : permutation) {
      evolution.add_attribute(add_attributes[idx]);
    }
    evolution.array_evolve(array_uri);

    // check attribute order
    auto schema = Array::load_schema(ctx, array_uri);
    std::vector<Attribute> attributes;
    for (unsigned a = 0; a < schema.attribute_num(); a++) {
      attributes.push_back(schema.attribute(a));
    }

    CHECK(attributes.size() == 4);
    if (attributes.size() >= 1) {
      CHECK(test::is_equivalent_attribute(attributes[0], a1));
    }
    if (attributes.size() >= 2) {
      CHECK(test::is_equivalent_attribute(
          attributes[1], add_attributes[permutation[0]]));
    }
    if (attributes.size() >= 3) {
      CHECK(test::is_equivalent_attribute(
          attributes[2], add_attributes[permutation[1]]));
    }
    if (attributes.size() >= 4) {
      CHECK(test::is_equivalent_attribute(
          attributes[3], add_attributes[permutation[2]]));
    }
  }
}

TEST_CASE(
    "C++ API: SchemaEvolution add duplicate attribute",
    "[cppapi][schema][evolution][add]") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{vfs_test_setup.array_uri(
      "test_schema_evolution_add_duplicate_attribute")};

  // create initial array
  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{-100, 100}}, 10);
  domain.add_dimension(d1);

  auto a1 = Attribute::create<int>(ctx, "a1");

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);

  SECTION("Add attribute to evolution twice") {
    // try evolving
    auto evolution = ArraySchemaEvolution(ctx);
    evolution.add_attribute(Attribute::create<int>(ctx, "a2"));
    CHECK_THROWS(evolution.add_attribute(Attribute::create<int>(ctx, "a2")));

    evolution.drop_attribute("a2");
    evolution.add_attribute(Attribute::create<int>(ctx, "a2"));
    CHECK_THROWS(evolution.add_attribute(Attribute::create<int>(ctx, "a2")));
  }

  SECTION("Add attribute with same name as schema attribute") {
    // create array
    Array::create(array_uri, schema);
    test::DeleteArrayGuard guard(ctx.ptr().get(), array_uri.c_str());

    // try evolving
    auto evolution = ArraySchemaEvolution(ctx);
    evolution.add_attribute(Attribute::create<int>(ctx, "a1"));

    // should throw, cannot add an attribute with the same name
    CHECK_THROWS(evolution.array_evolve(array_uri));

    // load schema back should succeed
    CHECK_NOTHROW(Array::load_schema(ctx, array_uri));
  }
}

TEST_CASE(
    "C++ API: SchemaEvolution drop last attribute",
    "[cppapi][schema][evolution][drop]") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{
      vfs_test_setup.array_uri("test_schema_evolution_drop_last_attribute")};

  // create array
  ArraySchema schema = simple_schema(ctx);
  Array::create(array_uri, schema);
  test::DeleteArrayGuard guard(ctx.ptr().get(), array_uri.c_str());

  // try evolving
  auto evolution = ArraySchemaEvolution(ctx);
  evolution.drop_attribute("a1");

  // should throw, schema must have at least one attribute
  CHECK_THROWS(evolution.array_evolve(array_uri));

  // load schema back should succeed
  CHECK_NOTHROW(Array::load_schema(ctx, array_uri));
}

/**
 * Add an enumeration which is not used by any attribute.
 * This leaves behind a dangling enumeration which is expected behavior.
 */
TEST_CASE("C++ API: SchemaEvolution add unused enumeration") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{
      vfs_test_setup.array_uri("test_schema_evolution_add_unused_enumeration")};

  // create array
  ArraySchema schema = simple_schema(ctx);
  Array::create(array_uri, schema);
  test::DeleteArrayGuard guard(ctx.ptr().get(), array_uri.c_str());

  // evolve
  auto evolution = ArraySchemaEvolution(ctx);
  Enumeration enumeration_in = Enumeration::create_empty(
      ctx, "us_states", TILEDB_STRING_ASCII, tiledb::sm::constants::var_num);
  evolution.add_enumeration(enumeration_in);

  evolution.array_evolve(array_uri);

  auto schema_out = Array::load_schema(ctx, array_uri);

  Enumeration enumeration_out =
      ArraySchemaExperimental::get_enumeration_from_name(
          ctx, schema_out, "us_states");

  CHECK(test::is_equivalent_enumeration(enumeration_in, enumeration_out));
}

/**
 * Drop the last attribute which holds a reference to an enumeration.
 * This leaves behind a dangling enumeration which is expected behavior.
 */
TEST_CASE("C++ API: SchemaEvolution dangling enumeration") {
  test::VFSTestSetup vfs_test_setup;
  Context ctx{vfs_test_setup.ctx()};
  auto array_uri{
      vfs_test_setup.array_uri("test_schema_evolution_add_unused_enumeration")};

  // schema with enumeration
  ArraySchema schema = simple_schema(ctx);

  auto a2 = Attribute::create<int>(ctx, "a2");
  Enumeration enumeration = Enumeration::create_empty(
      ctx, "us_states", TILEDB_STRING_ASCII, tiledb::sm::constants::var_num);
  AttributeExperimental::set_enumeration_name(ctx, a2, "us_states");

  ArraySchemaExperimental::add_enumeration(ctx, schema, enumeration);
  schema.add_attribute(a2);

  // create array
  Array::create(array_uri, schema);
  test::DeleteArrayGuard guard(ctx.ptr().get(), array_uri.c_str());

  // evolve to drop last attribute referring to enumeration
  auto evolution = ArraySchemaEvolution(ctx);
  evolution.drop_attribute("a2");

  evolution.array_evolve(array_uri);

  auto schema_out = Array::load_schema(ctx, array_uri);

  // we can still find the enumeration
  Enumeration enumeration_out =
      ArraySchemaExperimental::get_enumeration_from_name(
          ctx, schema_out, "us_states");

  CHECK(test::is_equivalent_enumeration(enumeration, enumeration_out));

  // though no attributes reference it
  for (unsigned a = 0; a < schema_out.attribute_num(); a++) {
    CHECK(!AttributeExperimental::get_enumeration_name(
               ctx, schema_out.attribute(a))
               .has_value());
  }
}
