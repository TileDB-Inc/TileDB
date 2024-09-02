#include <chrono>
#include <climits>
#include <thread>

#include <tiledb/tiledb>
#include <tiledb/tiledb_experimental>

#include <test/support/tdb_catch.h>

using namespace tiledb;

static void create_array(const std::string& array_uri);
static void dump_schema(const std::string& array_uri);

TEST_CASE(
    "Don't segfault in ArraySchema::dump with unloaded enumerations",
    "[array-schema][enumeration][bug][sc38300]") {
  std::string array_uri = "test_array_schema_dump";

  // Test setup
  create_array(array_uri);
  dump_schema(array_uri);
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

  std::vector<std::string> values = {"fred", "wilma", "barney", "pebbles"};
  auto enmr = Enumeration::create(ctx, "flintstones", values);

  auto attr = Attribute::create<int32_t>(ctx, "a");
  AttributeExperimental::set_enumeration_name(ctx, attr, "flintstones");

  ArraySchema schema(ctx, TILEDB_SPARSE);
  ArraySchemaExperimental::add_enumeration(ctx, schema, enmr);
  schema.set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
      .set_domain(dom)
      .add_attribute(attr);

  Array::create(array_uri, schema);
}

void dump_schema(const std::string& array_uri) {
  Context ctx;
  Array array(ctx, array_uri, TILEDB_READ);
  std::cout << array.schema();
  ArrayExperimental::load_all_enumerations(ctx, array);
  std::cout << array.schema();
}
