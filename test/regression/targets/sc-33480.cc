#include <tiledb/tiledb>

#include <catch2/catch_test_macros.hpp>

using namespace tiledb;

TEST_CASE("0 var chunks", "[unfiltering][bug][sc33480]") {
  // NOTE: This regression test will not fail on a Mac M1 because on that
  // platform, a division by 0 will not generate a segfault but return 0.

  // The following code created the zero_var_chunks_v10 array for this test.
  // #include <tiledb/tiledb>
  // using namespace tiledb;

  // int main() {
  //   Context ctx;

  //   Domain domain(ctx);
  //   domain.add_dimension(
  //       Dimension::create(ctx, "d", TILEDB_STRING_ASCII, nullptr, nullptr));
  //   ArraySchema schema(ctx, TILEDB_SPARSE);
  //   schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR,
  //   TILEDB_ROW_MAJOR}}); schema.add_attribute(Attribute::create<int32_t>(ctx,
  //   "a")); Array::create("zero_var_chunks_v10", schema);

  //   std::vector<char> d = {};
  //   std::vector<uint64_t> d_offsets = {0};
  //   std::vector<int32_t> data = {1};

  //   Array array(ctx, "zero_var_chunks_v10", TILEDB_WRITE);
  //   Query query(ctx, array, TILEDB_WRITE);
  //   query.set_layout(TILEDB_UNORDERED)
  //       .set_data_buffer("a", data)
  //       .set_data_buffer("d", d)
  //       .set_offsets_buffer("d", d_offsets);
  //   query.submit();
  //   array.close();
  //   return 0;
  // }

  // This array has a var file for the "d" dimension with 0 chunks. This was
  // only possible if a fragment had all empty values for a variable string in
  // versions earlier than v10.
  std::string array_name(
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays/zero_var_chunks_v10");
  Context ctx;

  // Prepare the array for reading.
  Array array(ctx, array_name, TILEDB_READ);

  // Prepare the query.
  Query query(ctx, array, TILEDB_READ);

  // Prepare the vector that will hold the result.
  std::vector<char> d(10);
  std::vector<uint64_t> d_offsets(10);
  std::vector<int32_t> a(10);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d", d)
      .set_offsets_buffer("d", d_offsets)
      .set_data_buffer("a", a);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Validate the results. This array has one cell at coordinate '' with
  // value 1.
  auto res = query.result_buffer_elements();
  CHECK(res["d"].first == 1);
  CHECK(res["d"].second == 0);
  CHECK(res["a"].first == 0);
  CHECK(res["a"].second == 1);
  CHECK(d_offsets[0] == 0);
  CHECK(a[0] == 1);
}
