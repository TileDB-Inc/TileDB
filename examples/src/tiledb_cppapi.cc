//
// Created by gaddra on 12/18/17.
//


#include <tdbpp>

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cout << "Usage: ./tiledb_cppapi.cc <dir_with_some_array>\n";
    return 1;
  }
  auto ctx = tdb::Context(argv[1]);
  auto arrays = ctx.arrays();
  std::cout << "Found " << arrays.size() << " array(s).\n";
  if (arrays.size()) {
    std::cout << "Using array: " << arrays[0].uri() << "\n";
    auto &array = arrays[0];
    std::cout << array << '\n';

    std::vector<int32_t> buff(64);
    auto q = tdb::Query(array, TILEDB_READ);
    auto sizes = q.attributes({"a1"}).set_buffer<tdb::type::INT32>("a1", buff).layout(TILEDB_GLOBAL_ORDER).submit();
  }
  return 0;
}