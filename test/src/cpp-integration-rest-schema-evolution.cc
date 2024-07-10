#include <test/support/src/vfs_helpers.h>
#include <test/support/tdb_catch.h>
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/rest/rest_client.h"

#include <fstream>

using namespace tiledb;

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
