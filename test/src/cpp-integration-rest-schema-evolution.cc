/**
 * @file cpp-integration-rest-schema-evolution.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB Inc.
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
 * Tests the C++ schema evolution API integration with REST server.
 */

#include <test/support/src/vfs_helpers.h>
#include <test/support/tdb_catch.h>
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/rest/rest_client.h"

#include <climits>
#include <fstream>

using namespace tiledb;

static void create_array(const Context& ctx, const std::string& array_uri);
static void write_first_fragment(
    const Context& ctx, const std::string& array_uri);
static uint64_t time_travel_destination();
static void add_attr_b(const Context& ctx, const std::string& array_uri);
static void write_second_fragment(
    const Context& ctx, const std::string& array_uri);
static void read_without_time_travel(
    const Context& ctx, const std::string& array_uri);
static void read_with_time_travel(
    const Context& ctx, const std::string& array_uri, uint64_t when);

void create_array(const Context& ctx, const std::string& array_uri) {
  auto obj = tiledb::Object::object(ctx, array_uri);
  if (obj.type() != tiledb::Object::Type::Invalid) {
    tiledb::Object::remove(ctx, array_uri);
  }

  auto dim = tiledb::Dimension::create<int32_t>(ctx, "d", {{0, 1024}});

  tiledb::Domain dom(ctx);
  dom.add_dimension(dim);

  auto attr = tiledb::Attribute::create<int32_t>(ctx, "a");

  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
      .set_domain(dom)
      .add_attribute(attr);

  tiledb::Array::create(array_uri, schema);
}

void write_first_fragment(const Context& ctx, const std::string& array_uri) {
  std::vector<int32_t> d_data = {0, 1, 2, 3, 4};
  std::vector<int32_t> a_data = {5, 6, 7, 8, 9};

  tiledb::Array array(ctx, array_uri, TILEDB_WRITE);
  tiledb::Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d", d_data)
      .set_data_buffer("a", a_data);
  REQUIRE(query.submit() == tiledb::Query::Status::COMPLETE);
  array.close();
}

uint64_t time_travel_destination() {
  // We sleep for 5ms to ensure that our fragments are separated in time
  // and allowing us to grab a time guaranteed to be between them.
  auto delay = std::chrono::milliseconds(5);
  std::this_thread::sleep_for(delay);

  auto timepoint = tiledb_timestamp_now_ms();

  std::this_thread::sleep_for(delay);

  return timepoint;
}

void add_attr_b(const Context& ctx, const std::string& array_uri) {
  auto attr = tiledb::Attribute::create<int32_t>(ctx, "b");

  tiledb::ArraySchemaEvolution ase(ctx);
  ase.add_attribute(attr);
  ase.array_evolve(array_uri);
}

void write_second_fragment(const Context& ctx, const std::string& array_uri) {
  std::vector<int32_t> d_data = {5, 6, 7, 8, 9};
  std::vector<int32_t> a_data = {10, 11, 12, 13, 14};
  std::vector<int32_t> b_data = {15, 16, 17, 18, 19};

  tiledb::Array array(ctx, array_uri, TILEDB_WRITE);
  tiledb::Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d", d_data)
      .set_data_buffer("a", a_data)
      .set_data_buffer("b", b_data);
  REQUIRE(query.submit() == tiledb::Query::Status::COMPLETE);
  array.close();
}

void read_without_time_travel(
    const Context& ctx, const std::string& array_uri) {
  std::vector<int32_t> d_data(10);
  std::vector<int32_t> a_data(10);
  std::vector<int32_t> b_data(10);

  tiledb::Array array(ctx, array_uri, TILEDB_READ);
  tiledb::Query query(ctx, array, TILEDB_READ);
  query.set_data_buffer("d", d_data)
      .set_data_buffer("a", a_data)
      .set_data_buffer("b", b_data);

  REQUIRE(query.submit() == tiledb::Query::Status::COMPLETE);

  for (int32_t i = 0; i < 10; i++) {
    REQUIRE(d_data[i] == i);
    REQUIRE(a_data[i] == i + 5);

    if (i < 5) {
      REQUIRE(b_data[i] == INT32_MIN);
    } else {
      REQUIRE(b_data[i] == i + 10);
    }
  }
}

void read_with_time_travel(
    const Context& ctx, const std::string& array_uri, uint64_t when) {
  std::vector<int32_t> d_data(10, INT_MAX);
  std::vector<int32_t> a_data(10, INT_MAX);
  std::vector<int32_t> b_data(10, INT_MAX);

  tiledb::Array array(
      ctx,
      array_uri,
      TILEDB_READ,
      tiledb::TemporalPolicy(tiledb::TimeTravel, when));
  tiledb::Query query(ctx, array, TILEDB_READ);
  query.set_data_buffer("d", d_data).set_data_buffer("a", a_data);

  auto matcher = Catch::Matchers::ContainsSubstring("There is no field b");
  REQUIRE_THROWS_WITH(query.set_data_buffer("b", b_data), matcher);

  REQUIRE(query.submit() == tiledb::Query::Status::COMPLETE);

  for (int32_t i = 0; i < 10; i++) {
    if (i < 5) {
      REQUIRE(d_data[i] == i);
      REQUIRE(a_data[i] == i + 5);
      REQUIRE(b_data[i] == INT_MAX);
    } else {
      REQUIRE(d_data[i] == INT_MAX);
      REQUIRE(a_data[i] == INT_MAX);
      REQUIRE(b_data[i] == INT_MAX);
    }
  }
}

TEST_CASE(
    "Use the correct schema when time traveling",
    "[time-traveling][array-schema][bug][sc35424][rest]") {
  tiledb::test::VFSTestSetup vfs_test_setup;
  auto array_uri{vfs_test_setup.array_uri("test_time_traveling_schema")};
  auto ctx = vfs_test_setup.ctx();

  // Test setup
  create_array(ctx, array_uri);
  write_first_fragment(ctx, array_uri);
  auto timepoint = time_travel_destination();
  add_attr_b(ctx, array_uri);
  write_second_fragment(ctx, array_uri);

  // Check reads with and without time travel.
  read_without_time_travel(ctx, array_uri);
  read_with_time_travel(ctx, array_uri, timepoint);
}

TEST_CASE(
    "Bug test: Schema evolution open array schema",
    "[array-schema-evolution][rest]") {
  // Create the virtual file system.
  tiledb::test::VFSTestSetup vfs_test_setup{nullptr, true};
  auto array_uri = vfs_test_setup.array_uri("schema_evolution_array");

  auto config = vfs_test_setup.ctx().config();
  std::string qv3 = GENERATE("true", "false");
  config.set("rest.use_refactored_array_open_and_query_submit", qv3);
  INFO("Using rest.use_refactored_array_open_and_query_submit: " << qv3);
  vfs_test_setup.update_config(config.ptr().get());
  auto ctx = vfs_test_setup.ctx();

  // Create the array schema.
  tiledb::Domain domain(ctx);
  auto d1 = tiledb::Dimension::create<int64_t>(ctx, "d1", {{0, 100}}, 5);
  domain.add_dimension(d1);
  auto a1 = tiledb::Attribute::create<int64_t>(ctx, "a1");
  auto a2 = tiledb::Attribute::create<int8_t>(ctx, "a2");

  tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.add_attribute(a2);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_COL_MAJOR);

  // Create the array.
  tiledb::Array::create(array_uri, schema);

  // Evolve the array.
  // -- Drop attribute a1.
  // -- Add attribute a3.
  // -- Set timestamp to prevent both schemas from having the same timestamp.
  auto evolution = tiledb::ArraySchemaEvolution(ctx);
  auto a3 = tiledb::Attribute::create<int>(ctx, "a3");
  evolution.add_attribute(a3);
  evolution.drop_attribute("a1");
  uint64_t now{tiledb_timestamp_now_ms() + 1};
  evolution.set_timestamp_range({now, now});
  evolution.array_evolve(array_uri);

  // Open the array before the schema evolution.
  uint64_t timestamp{now - 1};
  tiledb::Array array(
      ctx,
      array_uri,
      TILEDB_READ,
      tiledb::TemporalPolicy(tiledb::TimestampStartEnd, 0, timestamp));

  // Get the internal TileDB array object.
  auto c_array = array.ptr();
  auto internal_array = c_array->array_;

  // Print timestamp information.
  auto timestamp_start = internal_array->timestamp_start();
  auto timestamp_end = internal_array->timestamp_end();
  INFO(
      "Array timestamp range [" << timestamp_start << ", " << timestamp_end
                                << "]");

  // Get the latest schema and print timestamp information.
  auto latest_schema = internal_array->array_schema_latest();
  auto schema_timestamps = latest_schema.timestamp_range();
  INFO(
      "Schema timestamp range [" << schema_timestamps.first << ", "
                                 << schema_timestamps.second << "]");

  CHECK(schema_timestamps.first < timestamp_end);

  // Get all schemas and print index.
  const auto all_schema = internal_array->array_schemas_all();
  for (auto& element : all_schema) {
    UNSCOPED_INFO("-- Schema key: " << element.first);
  }
  CHECK(all_schema.size() == 2);
}
