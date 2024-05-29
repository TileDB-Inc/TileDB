#include <climits>

#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

#include <test/support/tdb_catch.h>

using namespace tiledb;

static void create_array(Context& ctx, const std::string& array_uri);
static void write_array(Context& ctx, const std::string& array_uri);
static void read_array(Context& ctx, const std::string& array_uri);

TEST_CASE("Empty write breaks reads", "[hilbert][bug][sc48428]") {
  Context ctx;
  std::string array_uri = "test_empty_write";

  // Test setup
  create_array(ctx, array_uri);
  write_array(ctx, array_uri);
  read_array(ctx, array_uri);
}

void create_array(Context& ctx, const std::string& array_uri) {
  auto obj = Object::object(ctx, array_uri);
  if (obj.type() != Object::Type::Invalid) {
    Object::remove(ctx, array_uri);
  }

  auto dim = Dimension::create<uint8_t>(ctx, "d", {{69, 105}}, 1);

  Domain dom(ctx);
  dom.add_dimension(dim);

  auto filter = Filter(ctx, TILEDB_FILTER_GZIP);
  auto flist = FilterList(ctx).add_filter(filter);

  auto attr = Attribute::create(ctx, "a", TILEDB_STRING_UCS2)
                  .set_cell_val_num(std::numeric_limits<uint32_t>::max())
                  .set_filter_list(flist);

  auto schema = ArraySchema(ctx, TILEDB_DENSE)
                    .set_cell_order(TILEDB_ROW_MAJOR)
                    .set_tile_order(TILEDB_COL_MAJOR)
                    .set_domain(dom)
                    .add_attribute(attr);

  Array::create(array_uri, schema);
}

void write_array(Context& ctx, const std::string& array_uri) {
  auto data = std::vector<uint16_t>();
  auto offsets = std::vector<uint64_t>();
  offsets.push_back(0);

  Array array(ctx, array_uri, TILEDB_WRITE);
  auto subarray = Subarray(ctx, array);
  uint8_t start = 97;
  uint8_t end = 97;
  subarray.add_range(0, start, end);

  Query query(ctx, array, TILEDB_WRITE);

  query.set_layout(TILEDB_ROW_MAJOR)
      .set_subarray(subarray)
      .set_data_buffer("a", (void*)data.data(), data.size())
      .set_offsets_buffer("a", offsets.data(), offsets.size());
  REQUIRE(query.submit() == Query::Status::COMPLETE);
}

void read_array(Context& ctx, const std::string& array_uri) {
  Array array(ctx, array_uri, TILEDB_READ);
  auto subarray = Subarray(ctx, array);
  uint8_t start = 97;
  uint8_t end = 97;
  subarray.add_range(0, start, end);

  std::vector<uint16_t> a(1000000);
  std::vector<uint64_t> offsets(256);
  Query query(ctx, array, TILEDB_READ);
  query.set_layout(TILEDB_ROW_MAJOR)
      .set_subarray(subarray)
      .set_data_buffer("a", (void*)a.data(), a.size())
      .set_offsets_buffer("a", offsets);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
}
