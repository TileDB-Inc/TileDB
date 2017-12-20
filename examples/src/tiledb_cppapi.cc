//
// Created by gaddra on 12/18/17.
//


#include <tdbpp>
#include <numeric>

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cout << "Usage: ./tiledb_cppapi.cc <dir_with_my_dense_array>\n";
    return 1;
  }
  auto ctx = tdb::Context(argv[1]);
  auto arrays = ctx.arrays();
  std::cout << "Found " << arrays.size() << " array(s). ";
  if (arrays.size()) {
    std::cout << "Using array: " << arrays[0].uri() << "\n";
    auto &array = arrays[0];
    std::cout << array << "\n\n";

    std::vector<char> buff;
    std::vector<uint64_t> off;
    auto q = tdb::Query(array, TILEDB_READ);

    std::string attr = "a2";
    tiledb_layout_t layout = TILEDB_GLOBAL_ORDER;

    std::cout << "Attribute: " << attr << ", Layout: " << tdb::from_tiledb(layout) << "\n\n";
    q.attributes({attr}).resize_var_buffer<tdb::type::CHAR>(attr, off, buff, 1).layout(layout);

    auto status = q.submit();
    auto sizes = q.buff_sizes();
    std::cout << "Submit 1: " << status << "," << sizes[0] << "," << sizes[1] << "\n";
    auto  cells = tdb::group_by_cell(off, buff, sizes[0], sizes[1]);
    for (auto &e : cells) std::cout << std::accumulate(e.begin(), e.end(), std::string()) << " ";

    std::cout << "\n\nattr " << attr << " status: " << q.attribute_status(attr) <<  "\n\n";

    status = q.submit();
    sizes = q.buff_sizes();
    std::cout << "Submit 2: " << status << "," << sizes[0] << "," << sizes[1] << "\n";
    cells = tdb::group_by_cell(off, buff, sizes[0], sizes[1]);
    for (auto &e : cells) std::cout << std::accumulate(e.begin(), e.end(), std::string()) << " ";
  }
  return 0;
}