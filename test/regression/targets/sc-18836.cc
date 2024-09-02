#include <future>
#include <iostream>
#include <string>
#include <tiledb/tiledb>

#include <test/support/tdb_catch.h>

using namespace tiledb;

float a2_fill_value = 0.0f;
const std::string array_name = "cpp_qc_nullable_array";

void create_array() {
  Context ctx;

  // Create dimension
  auto dim = tiledb::Dimension::create<int32_t>(ctx, "dim", {{1, 9}}, 2);

  Domain domain(ctx);
  domain.add_dimension(dim);

  Attribute a1(ctx, "a1", TILEDB_INT32);
  a1.set_nullable(true);
  Attribute a2(ctx, "a2", TILEDB_FLOAT32);
  a2.set_fill_value(&a2_fill_value, sizeof(float));

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a1).add_attribute(a2);

  Array::create(array_name, schema);

  // Prepare some data for the array
  std::vector<int> data1 = {8, 9, 10, 11, 12, 13, 14, 15, 16};
  std::vector<uint8_t> a1_validity_buf = {0, 1, 1, 1, 1, 0, 1, 1, 0};

  std::vector<float> data2 = {
      13.2f, 14.1f, 14.2f, 15.1f, 15.2f, 15.3f, 16.1f, 18.3f, 19.1f};

  // Open array for writing
  Array array(ctx, array_name, TILEDB_WRITE);

  // Set the subarray to write into
  Subarray subarray(ctx, array);
  subarray.add_range<int>(0, 1, 9);

  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", data1)
      .set_validity_buffer("a1", a1_validity_buf)
      .set_data_buffer("a2", data2)
      .set_subarray(subarray);

  query.submit();
  array.close();
}

TEST_CASE(
    "Query Condition CPP API: Query Condition OR with nullable attributes "
    "(sc-18836)",
    "[QueryCondition]") {
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  create_array();

  // Prepare the array for reading
  Array array(ctx, array_name, TILEDB_READ);

  // Prepare the vectors that will hold the results
  std::vector<int> a1_buffer(9);
  std::vector<float> a2_buffer(9);
  std::vector<uint8_t> a1_validity_buf(9);

  // Set the subarray to write into
  Subarray subarray(ctx, array);
  subarray.add_range<int>(0, 1, 9);

  // Prepare the query
  Query query(ctx, array, TILEDB_READ);
  query.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", a1_buffer)
      .set_validity_buffer("a1", a1_validity_buf)
      .set_data_buffer("a2", a2_buffer);

  QueryCondition qc1(ctx);
  float val = 15.1f;
  qc1.init("a2", &val, sizeof(float), TILEDB_EQ);
  QueryCondition qc2(ctx);
  qc2.init("a1", nullptr, 0, TILEDB_EQ);
  QueryCondition qc(ctx);

  SECTION("Ordering q1, q2") {
    qc = qc1.combine(qc2, TILEDB_OR);
  }

  SECTION("Ordering q2, q1") {
    qc = qc2.combine(qc1, TILEDB_OR);
  }

  query.set_condition(qc);
  query.submit();
  array.close();

  // Print out the results.
  int m = std::numeric_limits<int>::min();
  std::vector<int> a1_expected = {8, m, m, 11, m, 13, m, m, 16};
  std::vector<float> a2_expected = {
      13.2f, 0.0f, 0.0f, 15.1f, 0.0f, 15.3f, 0.0f, 0.0f, 19.1f};
  auto result_num = (int)query.result_buffer_elements()["a1"].second;
  for (int r = 0; r < result_num; r++) {
    CHECK(a1_buffer[r] == a1_expected[r]);
    CHECK(a2_buffer[r] == a2_expected[r]);
  }

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}
