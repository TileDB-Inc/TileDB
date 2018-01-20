//
// Created by gaddra on 1/20/18.
//

#include <tiledb>

int main() {
  tiledb::Context ctx;
  tiledb::MapSchema schema(ctx);

  auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
  auto a2 = tiledb::Attribute::create<double>(ctx, "a2");
  auto a3 = tiledb::Attribute::create<char>(ctx, "a3");
  a3.set_cell_val_num(TILEDB_VAR_NUM);

  schema << a1 << a2 << a3;
  //tiledb::create_map("my_map", schema);

  tiledb::Map map(ctx, "my_map");
  auto item1 = map.create_item(1);
  auto item2 = map.create_item(2.0);
  auto item3 = map.create_item(std::string("key3"));

  item1["a1"] = 3;
  item1["a2"] = 123.1;
  item1["a3"] = std::string("Asca");

  map << item1;
  map.flush();

  auto get = map.get_item(1);

  int v = item1["a1"].get<int>();
  std::string s = item1["a3"];

  std::cout << v << s;

  std::cout << get.get<char, std::string>("a3") << get.get_single<int>("a1");

}