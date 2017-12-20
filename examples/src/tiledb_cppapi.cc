#include <tdbpp>

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cout << "Usage:\n"
              << "\t./examples/tiledb_dense_create && ./examples/tiledb_dense_write_global_1 \n"
              << "\t./examples/tiledb_cppapi.cc .\n";
    return 1;
  }
  tdb::Context ctx = tdb::Context(argv[1]);
  std::vector<tdb::Array> arrays = ctx.arrays();

  std::cout << "Found " << arrays.size() << " array(s). ";

  if (arrays.size()) {
    std::cout << "Using array: " << arrays[0].uri() << "\n";
    tdb::Array &array = arrays[0];
    std::cout << array << "\n\n";

    std::vector<char> buffer;
    std::vector<uint64_t> offsets;
    auto query = tdb::Query(array, TILEDB_READ);

    std::string attr = "a2";
    tiledb_layout_t layout = TILEDB_ROW_MAJOR;

    std::cout << "Attribute: " << attr << ", Layout: " << tdb::from_tiledb(layout) << "\n\n";


    query.attributes({attr});
    query.layout(layout);
    query.subarray<tdb::type::UINT64>({1,3,1,3});
    // Current limit to 10 el, change to 9 and doesn't fit cell (2,3)
    query.resize_var_buffer<tdb::type::CHAR>(attr, offsets, buffer, 3, 0, 10);

    std::cout << "Allocated buffer sizes: offset=" << offsets.size() << ", data=" << buffer.size() << "\n\n";

    tdb::Query::Status status = query.submit();
    auto sizes = query.buff_sizes();
    std::cout << "Submit 1\nStatus: " << status << ", offset-buff size: " << sizes[0] << ", data-buff size: " << sizes[1] << "\n";
    auto  cells = tdb::group_by_cell(offsets, buffer, sizes[0], sizes[1]);
    for (auto &e : cells) std::cout << std::string(e.data(), e.size()) << " ";

    std::cout << "\n\nAttribute \"" << attr << "\" status: " << query.attribute_status(attr) <<  "\n\n";

    status = query.submit();
    sizes = query.buff_sizes();
    std::cout << "Submit 2\nStatus:" << status << ", offset-buff size: " << sizes[0] << ", data-buff size: " << sizes[1] << "\n";
    cells = tdb::group_by_cell(offsets, buffer, sizes[0], sizes[1]);
    for (auto &e : cells) std::cout << std::string(e.data(), e.size()) << " ";
    std::cout << std::endl;
  }
  return 0;
}