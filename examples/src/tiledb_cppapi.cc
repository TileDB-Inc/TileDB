//
// Created by gaddra on 12/18/17.
//


#include <tdbpp>
#include <numeric>

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cout << "Usage: ./tiledb_cppapi.cc <dir_with_some_array_in_it>\n";
    return 1;
  }
  auto ctx = tdb::Context(argv[1]);
  auto arrays = ctx.arrays();
  std::cout << "Found " << arrays.size() << " array(s).\n";
  if (arrays.size()) {
    std::cout << "Using array: " << arrays[0].uri() << "\n";
    auto &array = arrays[0];
    std::cout << array << '\n';

    std::vector<char> buff;
    std::vector<uint64_t> off;
    auto q = tdb::Query(array, TILEDB_READ);
    auto status = q.attributes({"a2"}).resize_var_buffer<tdb::type::CHAR>("a2", off, buff, 3).layout(TILEDB_GLOBAL_ORDER).submit();
    auto sizes = q.buff_sizes();
    std::cout << status << "," << sizes[0] << "," << sizes[1] << "\n";
    auto  el = q.group_by_cell(off, buff, sizes[0], sizes[1]);
    for (auto e : el) std::cout << std::accumulate(e.begin(), e.end(), std::string("")) << " ";
    std::cout << "\n";
    for (unsigned i = 0; i < 4; ++i) {
      for (unsigned j = 0; j < 4; ++j) {
        std::cout << buff[(i*4) + j] << " ";
      }
      std::cout << "\n";
    }
  }
  return 0;
}