#include <tiledb/tiledb>

#include <catch2/catch_test_macros.hpp>

using namespace tiledb;

static std::string array_name("load_var_sized_qc_fields");

static void remove_array() {
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

static void create_array() {
  // The bug we're testing is that if a dense array has a var sized attribute
  // that is referenced in a query condition, but not returned to the user,
  // the var tiles for that attribute we're not loaded properly leading to
  // a thrown exception.
  Context ctx;

  auto dim = Dimension::create<uint64_t>(ctx, "dim", {{1, 5}});

  Domain domain(ctx);
  domain.add_dimension(dim);

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_domain(domain);

  auto a1 = Attribute::create<std::string>(ctx, "a1");
  auto a2 = Attribute::create<std::string>(ctx, "a2");
  // The default zero byte value used as a fill value makes the query
  // results assertions a bit more difficult to assert. We just use
  // 'x' instead since its printable.
  a2.set_fill_value("x", 1);
  schema.add_attributes(a1, a2);

  Array::create(array_name, schema);
}

static void write_array() {
  Context ctx;

  std::string a1_data = "foobarbazbamcan";
  std::vector<uint64_t> a1_offsets = {0, 3, 6, 9, 12};

  std::string a2_data = "redorangegreenblueviolet";
  std::vector<uint64_t> a2_offsets = {0, 3, 9, 14, 18};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", a1_data)
      .set_offsets_buffer("a1", a1_offsets)
      .set_data_buffer("a2", a2_data)
      .set_offsets_buffer("a2", a2_offsets);

  REQUIRE(query.submit() == Query::Status::COMPLETE);
  query.finalize();
  array.close();
}

TEST_CASE(
    "DenseReader bug with query condition on var sized field not returned",
    "[dense-reder][bug][sc29682]") {
  remove_array();
  create_array();
  write_array();

  Context ctx;

  std::string a2_data;
  a2_data.resize(24);

  std::vector<uint64_t> a2_offsets;
  a2_offsets.resize(5);

  // Its important for the purposes of this test that the attribute we're
  // setting a condition on is *not* included in the user data buffers
  // passed to the query. I.e., we need to reference it in a query condition
  // but not ask to have its data returned.
  tiledb::QueryCondition qc(ctx);
  qc.init("a1", "baz", 3, TILEDB_EQ);

  tiledb::Array array(ctx, array_name, TILEDB_READ);
  tiledb::Subarray subarray(ctx, array);
  subarray.add_range("dim", (uint64_t)1, (uint64_t)5);
  tiledb::Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a2", a2_data)
      .set_offsets_buffer("a2", a2_offsets)
      .set_subarray(subarray)
      .set_condition(qc);

  // Before the fix included with this test, query.submit() would
  // have thrown an exception complaining that var sized tiles were
  // not loaded.
  REQUIRE(query.submit() == Query::Status::COMPLETE);

  std::string a2_expect = "xxgreenxx";
  a2_expect.resize(24);
  REQUIRE(a2_data == a2_expect);

  std::vector<uint64_t> expected_offsets = {0, 1, 2, 7, 8};
  REQUIRE(a2_offsets == expected_offsets);

  remove_array();
}
