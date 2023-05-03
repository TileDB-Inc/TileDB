#include <tiledb/tiledb>
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb;

int main() {
  const std::string array_name = <name>;

  Config config;
  config["rest.server_address"] = "http://localhost:8181";
  // config["rest.use_refactored_array_open"] = "true";
  config["rest.token"] = <token> Context ctx(config);

  // Create the array
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<uint8_t>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<uint8_t>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  Attribute attr = Attribute::create<uint8_t>(ctx, "a");
  // attr.set_nullable(true);
  schema.add_attribute(attr);
  Array::create(array_name, schema);
}