#include <tiledb/tiledb>

#include <test/support/tdb_catch.h>

using namespace tiledb;

static std::string array_uri("sc-10782");

static void remove_array() {
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_uri)) {
    vfs.remove_dir(array_uri);
  }
}

static void create_array() {
  Context ctx;

  auto dim = tiledb::Dimension::create<int32_t>(ctx, "dim", {{1, 9}}, 2);

  Domain domain(ctx);
  domain.add_dimension(dim);

  Attribute a1(ctx, "a1", TILEDB_INT32);
  a1.set_nullable(true);

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);

  Array::create(array_uri, schema);
}

static void write_array() {
  Context ctx;

  std::vector<int> data1 = {8, 9, 10, 11, 12, 13, 14, 15, 16};
  std::vector<uint8_t> a1_validity_buf = {0, 1, 1, 1, 1, 0, 1, 1, 0};

  Array array(ctx, array_uri, TILEDB_WRITE);

  Query query(ctx, array);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_buffer("a1", data1)
      .set_validity_buffer("a1", a1_validity_buf);

  query.submit();
  array.close();
}

TEST_CASE(
    "FragmentInfo of consolidated array doesn't include prior fragment",
    "[!shouldfail][cppapi][FragmentInfo][bug][sc-10782]") {
  remove_array();
  create_array();
  write_array();
  write_array();

  Context ctx;
  FragmentInfo fi(ctx, array_uri);
  fi.load();
  REQUIRE(fi.fragment_num() == 2);

  tiledb::Array::consolidate(ctx, array_uri);
  // Normally, consolidation doesn't remove any fragments.
  // It just creates a new one.

  fi.load();
  REQUIRE(fi.fragment_num() == 3);

  remove_array();
}
