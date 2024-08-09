#include <tiledb/tiledb>

#include <catch2/catch_test_macros.hpp>

using namespace tiledb;

static std::string array_name("reading_incomplete_array");

std::vector<std::vector<std::string>> permute(std::vector<std::string>& elems);

static void remove_array() {
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

static void create_array() {
  // Create a TileDB context.
  Context ctx;

  auto rows =
      Dimension::create(ctx, "rows", TILEDB_STRING_ASCII, nullptr, nullptr);
  auto cols =
      Dimension::create(ctx, "cols", TILEDB_STRING_ASCII, nullptr, nullptr);

  Domain domain(ctx);
  domain.add_dimensions(rows, cols);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.set_domain(domain);

  auto a1 = Attribute::create<int32_t>(ctx, "a1");
  schema.add_attribute(Attribute::create<int>(ctx, "a1"));

  Array::create(array_name, schema);
}

static void write_array() {
  Context ctx;

  std::string row_data = "abbcccddddeeeee";
  std::vector<uint64_t> row_offsets = {0, 1, 3, 6, 10};

  std::string col_data = "jjjjjiiiihhhggf";
  std::vector<uint64_t> col_offsets = {0, 5, 9, 12, 14};

  std::vector<int> a1_data = {1, 2, 3, 4, 5};

  Array array(ctx, array_name, TILEDB_WRITE);

  Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("rows", row_data)
      .set_offsets_buffer("rows", row_offsets)
      .set_offsets_buffer("cols", col_offsets)
      .set_data_buffer("cols", col_data)
      .set_data_buffer("a1", a1_data);

  REQUIRE(query.submit() == Query::Status::COMPLETE);
  query.finalize();
  array.close();
}

TEST_CASE(
    "C++ API: Bug in set_offsets_buffer call ordering",
    "[cppapi][regression][set-offsets-call-order][sc25116]") {
  remove_array();
  create_array();
  write_array();

  Context ctx;
  FragmentInfo fi(ctx, array_name);
  fi.load();
  REQUIRE(fi.fragment_num() == 1);

  remove_array();
}
