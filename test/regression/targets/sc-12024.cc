#include <tiledb/tiledb>

#include <test/support/tdb_catch.h>

#include <chrono>
#include <thread>

using namespace tiledb;

TEST_CASE("C++ API: Truncated values (ch12024)", "[cppapi][sparse]") {
  const std::string array_name_1d = "cpp_unit_array_1d";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name_1d))
    vfs.remove_dir(array_name_1d);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  Domain domain(ctx);
  auto fd1 = Dimension::create<double>(ctx, "Y", {{139200.34375, 140000.1875}});
  auto fd2 = Dimension::create<double>(ctx, "Z", {{-682.73999, 929.42999}});
  domain.add_dimensions(fd1, fd2);
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name_1d, schema);
  Array array(ctx, array_name_1d, TILEDB_WRITE);

  std::vector<int> a = {42};
  std::vector<double> coords;
  {
    Query q(ctx, array, TILEDB_WRITE);
    q.set_layout(TILEDB_GLOBAL_ORDER);
    q.set_data_buffer("a", a);

    std::vector<double> y = {139200.35};
    std::vector<double> z = {-682.75};
    q.set_data_buffer("Y", y);
    q.set_data_buffer("Z", z);
    REQUIRE_THROWS_WITH(
        q.submit(),
        "[TileDB::Dimension] Error: Coordinate -682.75 is out of domain "
        "bounds [-682.73999, 929.42999] on dimension 'Z'");
  }
  {
    Query q(ctx, array, TILEDB_WRITE);
    q.set_layout(TILEDB_GLOBAL_ORDER);
    q.set_data_buffer("a", a);

    std::vector<double> y = {139200.34};
    std::vector<double> z = {-682.73};
    q.set_data_buffer("Y", y);
    q.set_data_buffer("Z", z);
    REQUIRE_THROWS_WITH(
        q.submit(),
        "[TileDB::Dimension] Error: Coordinate 139200.34 is out of domain "
        "bounds [139200.34375, 140000.1875] on dimension 'Y'");
  }

  array.close();

  if (vfs.is_dir(array_name_1d))
    vfs.remove_dir(array_name_1d);
}
