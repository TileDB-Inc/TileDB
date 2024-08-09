#include <chrono>
#include <climits>
#include <thread>

#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace tiledb;

static void create_array(const std::string& array_uri);
static void write_first_fragment(const std::string& array_uri);
static uint64_t time_travel_destination();
static void add_attr_b(const std::string& array_uri);
static void write_second_fragment(const std::string& array_uri);

static void read_without_time_travel(const std::string& array_uri);
static void read_with_time_travel(const std::string& array_uri, uint64_t when);

TEST_CASE(
    "Use the correct schema when time traveling",
    "[time-traveling][array-schema][bug][sc35424]") {
  std::string array_uri = "test_time_traveling_schema";

  // Test setup
  create_array(array_uri);
  write_first_fragment(array_uri);
  auto timepoint = time_travel_destination();
  add_attr_b(array_uri);
  write_second_fragment(array_uri);

  // Check reads with and without time travel.
  read_without_time_travel(array_uri);
  read_with_time_travel(array_uri, timepoint);
}

void create_array(const std::string& array_uri) {
  Context ctx;

  auto obj = Object::object(ctx, array_uri);
  if (obj.type() != Object::Type::Invalid) {
    Object::remove(ctx, array_uri);
  }

  auto dim = Dimension::create<int32_t>(ctx, "d", {{0, 1024}});

  Domain dom(ctx);
  dom.add_dimension(dim);

  auto attr = Attribute::create<int32_t>(ctx, "a");

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
      .set_domain(dom)
      .add_attribute(attr);

  Array::create(array_uri, schema);
}

void write_first_fragment(const std::string& array_uri) {
  std::vector<int32_t> d_data = {0, 1, 2, 3, 4};
  std::vector<int32_t> a_data = {5, 6, 7, 8, 9};

  Context ctx;
  Array array(ctx, array_uri, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d", d_data)
      .set_data_buffer("a", a_data);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
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

void add_attr_b(const std::string& array_uri) {
  Context ctx;
  auto attr = Attribute::create<int32_t>(ctx, "b");

  ArraySchemaEvolution ase(ctx);
  ase.add_attribute(attr);
  ase.array_evolve(array_uri);
}

void write_second_fragment(const std::string& array_uri) {
  std::vector<int32_t> d_data = {5, 6, 7, 8, 9};
  std::vector<int32_t> a_data = {10, 11, 12, 13, 14};
  std::vector<int32_t> b_data = {15, 16, 17, 18, 19};

  Context ctx;
  Array array(ctx, array_uri, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d", d_data)
      .set_data_buffer("a", a_data)
      .set_data_buffer("b", b_data);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
  array.close();
}

void read_without_time_travel(const std::string& array_uri) {
  std::vector<int32_t> d_data(10);
  std::vector<int32_t> a_data(10);
  std::vector<int32_t> b_data(10);

  Context ctx;
  Array array(ctx, array_uri, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  query.set_data_buffer("d", d_data)
      .set_data_buffer("a", a_data)
      .set_data_buffer("b", b_data);

  REQUIRE(query.submit() == Query::Status::COMPLETE);

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

void read_with_time_travel(const std::string& array_uri, uint64_t when) {
  std::vector<int32_t> d_data(10, INT_MAX);
  std::vector<int32_t> a_data(10, INT_MAX);
  std::vector<int32_t> b_data(10, INT_MAX);

  Context ctx;
  Array array(ctx, array_uri, TILEDB_READ, TemporalPolicy(TimeTravel, when));
  Query query(ctx, array, TILEDB_READ);
  query.set_data_buffer("d", d_data).set_data_buffer("a", a_data);

  auto matcher = Catch::Matchers::ContainsSubstring("There is no field b");
  REQUIRE_THROWS_WITH(query.set_data_buffer("b", b_data), matcher);

  REQUIRE(query.submit() == Query::Status::COMPLETE);

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
