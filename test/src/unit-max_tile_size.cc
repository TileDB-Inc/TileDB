/**
 * @file   unit-cppapi-max-fragment-size.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests the C API for maximum tile size across all fragments of an array.
 */

// -d=yes --break "C++ API: Max tile size\, dense\, fixed\, with consolidation",

// -d=yes --break "C++ API: Max fragment size\, sparse\, variable string dimension\, fix attr",

// -d=yes --break "C++ API: Max fragment size\, dense create/evolve, fixed dim/attr",

//  -d=yes --break "C++ API: Max fragment size\, sparse\, variable string dimension\, variable string attribute"

//  -d=yes --break "C++ API: Max fragment size\, sparse\, fix dimension\, nullable variable string attribute",

//  -d=yes --break "C++ API: Max fragment size\, sparse\, fix dimension\, variable string attribute", 

#if TILEDB_SERIALIZATION
//  -d=yes --break "C++ API: Max tile size\, serialization"
#endif


#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/serialization/array.h"

#include <numeric>

struct CAPI_MaxTileSizeFx {
  //const char* array_name = "cpp_max_tile_size";

  bool showing_data = false;

  CAPI_MaxTileSizeFx()
    : vfs_(ctx_) {
  }

  void remove_temp_dir(const std::string& path) {
    if (vfs_.is_dir(path.c_str())) {
      vfs_.remove_dir(path.c_str());
    }
  };

  template <const int nchars = 1>
  void create_array_with_attr(const std::string& array_name, const char* attr_name){
    // Create a TileDB context.
    // Context ctx;
    tiledb::Config cfg;
    tiledb::Context ctx(cfg);
    tiledb::VFS vfs(ctx);

    remove_temp_dir(array_name.c_str());

    // The array will be 1x4 with dimension "rows", with domain
    // [1,4].
    tiledb::Domain domain(ctx);
    domain.add_dimension(
        tiledb::Dimension::create<int>(ctx, "rows", {{1, 4}}, 1));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    // Add one attribute "a1", so each (i) cell can store
    // a nchars characters on "attr_name".
    schema.add_attribute(
        tiledb::Attribute::create<std::array<char, nchars>>(ctx, attr_name));
    // schema.add_attribute(Attribute::create<char>(ctx, "a1"));
    //   schema.add_attribute(Attribute::create<float[2]>(ctx, "a2"));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  };

  template <const int nchars = 1>
  void write_array_attr(const std::string& array_name, const char* attr_name) {
    tiledb::Context ctx;

    // Prepare some data for the array
    std::vector<std::array<char, nchars>> a;
    for (auto i = 0; i < 4; ++i) {
      std::array<char, nchars> buf;
      memset(&buf[0], 'a' + i, nchars);
      a.emplace_back(buf);
    }
    #if 0
    std::vector<float> a2 = {1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,  4.1f,
                             4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
                             8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f,
                             11.2f, 12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f,
                             15.1f, 15.2f, 16.1f, 16.2f};
    #endif

    // Open the array for writing and create the query.
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer(attr_name, a);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  #if 0
  template <const int nchars = 1>
  void read_array_attr(const char* array_name, const char* attr_name) {
    tiledb::Context ctx;

    // Prepare the array for reading
    tiledb::Array array(ctx, array_name, TILEDB_READ);

    // Slice only rows 1, 2 and cols 2, 3, 4
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, 1, 4);
    //.add_range(1, 2, 4);

    // Prepare the vector that will hold the result
    // (of size 6 elements for "a1" and 12 elements for "a2" since
    // it stores two floats per cell)
    //  std::vector<char> data_a1(6);
    //  std::vector<std::array<char,2>> data_a1(6);
    std::vector<std::array<char, nchars>> data_a(6);
    std::vector<float> data_a2(12);

    // Prepare the query
    tiledb::Query query(ctx, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer(attr_name, data_a);
    //  .set_data_buffer("a1", data_a1);
    //.set_data_buffer("a2", data_a2);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Print out the results.
    std::cout << "Reading both attributes a1 and a2:\n";
    for (int i = 0; i < 6; ++i) {
      //    std::cout << "a1: " << data_a1[i] << ", a2: (" << data_a2[2 * i] <<
      //    ","
      //              << data_a2[2 * i + 1] << ")\n";
      // std::cout << "a1: " << data_a[i][0] << "\n";
      std::cout << attr_name << ": ";
      for (auto i2 = 0; i2 < nchars; ++i2) {
        std::cout << data_a[i][i2];
      }

      // std::cout << "a1: " << data_a1[i][0] << data_a1[i][1] << "\n";
      std::cout << "\n";
    }
  };
  #endif

  uint64_t get_fragments_extreme(const std::string& array_uri) {
    tiledb::Context ctx;
    //tiledb_fragment_tile_size_extremes_t tiledb_fragment_tile_size_extremes;
    uint64_t max_in_memory_tile_size = 0;
    CHECK(
        tiledb_array_maximum_tile_size(
            &*ctx.ptr(),
            array_uri.c_str(),
            &max_in_memory_tile_size, 
            nullptr) == TILEDB_OK);

    if (showing_data) {
      std::cout << "maximum in memory tile size "
                << max_in_memory_tile_size
                << std::endl;
    }
    return max_in_memory_tile_size;
  }

  tiledb::Context ctx_;
  tiledb::VFS vfs_;
};

TEST_CASE_METHOD(
    CAPI_MaxTileSizeFx,
    "C++ API: Max tile size, dense, fixed, with consolidation",
    "[capi][max-tile-size][dense][consolidate]") {
  // Name of array.
  std::string array_name("fragments_consolidation_array");

  auto create_array = [&]() -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    remove_temp_dir(array_name.c_str());

    // The array will be 4x4 with dimensions "rows" and "cols", with domain
    // [1,4] and space tiles 2x2
    tiledb::Domain domain(ctx);
    domain
        .add_dimension(tiledb::Dimension::create<int>(ctx, "rows", {{1, 4}}, 2))
        .add_dimension(
            tiledb::Dimension::create<int>(ctx, "cols", {{1, 4}}, 2));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    // Add a single attribute "a" so each (i,j) cell can store an integer.
    schema.add_attribute(tiledb::Attribute::create<int32_t>(ctx, "a"));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  };

  auto write_array_1 = [&]() -> void {
    tiledb::Context ctx;

    // Prepare some data for the array
    std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8};

    // Open the array for writing and create the query.
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, 1, 2).add_range(1, 1, 4);
    tiledb::Query query(ctx, array);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  auto write_array_2 = [&]() -> void {
    tiledb::Context ctx;

    // Prepare some data for the array
    std::vector<int> data = {101, 102, 103, 104};

    // Open the array for writing and create the query.
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, 2, 3).add_range(1, 2, 3);
    tiledb::Query query(ctx, array);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  auto write_array_3 = [&]() -> void {
    tiledb::Context ctx;

    // Prepare some data for the array
    std::vector<int> data = {201};

    // Open the array for writing and create the query.
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, 1, 1).add_range(1, 1, 1);
    tiledb::Query query(ctx, array);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  auto write_array_4 = [&]() -> void {
    tiledb::Context ctx;

    // Prepare some data for the array
    std::vector<int> data = {202};

    // Open the array for writing and create the query.
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, 3, 3).add_range(1, 4, 4);
    tiledb::Query query(ctx, array);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  #if 0
  auto read_array = [&]() -> void {
    tiledb::Context ctx;

    // Prepare the array for reading
    tiledb::Array array(ctx, array_name, TILEDB_READ);

    // Read the entire array
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, 1, 4).add_range(1, 1, 4);

    // Prepare the vector that will hold the result
    std::vector<int> data(16);
    std::vector<int> coords_rows(16);
    std::vector<int> coords_cols(16);

    // Prepare the query
    tiledb::Query query(ctx, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data)
        .set_data_buffer("rows", coords_rows)
        .set_data_buffer("cols", coords_cols);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Print out the results.
    for (int r = 0; r < 16; r++) {
      int i = coords_rows[r];
      int j = coords_cols[r];
      int a = data[r];
      std::cout << "Cell (" << i << ", " << j << ") has data " << a << "\n";
    }
  };
  #endif

  tiledb::Context ctx;

  // Create and write array only if it does not exist
  create_array();
  CHECK(get_fragments_extreme(array_name) == 0);
  write_array_1();
  CHECK(get_fragments_extreme(array_name) == 4 * sizeof(int32_t));
  write_array_2();
  CHECK(get_fragments_extreme(array_name) == 4 * sizeof(int32_t));
  write_array_3();
  CHECK(get_fragments_extreme(array_name) == 4 * sizeof(int32_t));
  write_array_4();
  CHECK(get_fragments_extreme(array_name) == 4 * sizeof(int32_t));

  // consolidate
  tiledb::Array::consolidate(ctx, array_name);

  CHECK(get_fragments_extreme(array_name) == 4 * sizeof(int32_t));
}

TEST_CASE_METHOD(
    CAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, variable string dimension, fix attr",
    "[capi][max-tile-size][sparse][var-dimension]") {
std::string array_name("sparse_string_dimension");

auto  create_array = [&]()-> void {
  // Create a TileDB context.
  tiledb::Context ctx;

  remove_temp_dir(array_name);

  // The array will be 2d array with dimensions "rows" and "cols"
  // "rows" is a string dimension type, so the domain and extent is null
  tiledb::Domain domain(ctx);
  domain.add_dimension(tiledb::Dimension::create(
      ctx, "rows", TILEDB_STRING_ASCII, nullptr, nullptr));

  // The array will be sparse.
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);

  // Add a single attribute "a" so each (i,j) cell can store an integer.
  schema.add_attribute(tiledb::Attribute::create<int32_t>(ctx, "a"));

  // Create the (empty) array on disk.
  tiledb::Array::create(array_name, schema);
};

auto  write_array = [&](uint64_t num_rows)->void {
  tiledb::Context ctx;

  std::vector<int> a_buff(num_rows);
  auto start_val = 0;
  std::iota(a_buff.begin(), a_buff.end(), start_val);

  // For the string dimension, convert the increasing value from int to
  // string.

  std::vector<int> d1_buff(num_rows);
  std::iota(d1_buff.begin(), d1_buff.end(), start_val + 1);

  std::vector<uint64_t> d1_offsets;
  d1_offsets.reserve(num_rows);
  std::string d1_var;
  uint64_t offset = 0;
  for (uint64_t i = start_val; i < start_val + num_rows; i++) {
    auto val = std::to_string(i);
    d1_offsets.emplace_back(offset);
    offset += val.size();
    d1_var += val;
  }

  // Open the array for writing and create the query.
  tiledb::Array array(ctx, array_name, TILEDB_WRITE);
  tiledb::Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED) 
      .set_data_buffer("a", a_buff)
      .set_data_buffer("rows", d1_var)
      .set_offsets_buffer("rows", d1_offsets)
      ;

  // Perform the write and close the array.
  query.submit();
  array.close();
};

#if 0
auto  read_array = [&](int num_rows) -> void {
  tiledb::Context ctx;

  // Prepare the array for reading
  tiledb::Array array(ctx, array_name, TILEDB_READ);

  // Slice only rows "bb", "c" and cols 3, 4
//  Subarray subarray(ctx, array);
  //subarray.add_range(0, std::string("a"), std::string("c"));
   //.add_range<int32_t>(1, 2, 4);
//  subarray.add_range(0, std::string("0"), std::to_string(num_rows));

  // Prepare the query
  tiledb::Query query(ctx, array, TILEDB_READ);
  //  query.set_subarray(subarray);

  // Prepare the vector that will hold the result.
  // We take an upper bound on the result size, as we do not
  // know a priori how big it is (since the array is sparse)
  // std::vector<int32_t> data(3);
  // std::vector<char> rows(4);
  // std::vector<uint64_t> rows_offsets(3);
  // std::vector<int32_t> cols(3);

  //std::vector<int> d1_buff(num_rows);
  //std::vector<int> d2_buff(num_vals);
  std::vector<int> a1_buff(num_rows);
  std::vector<uint64_t> d1_offsets(num_rows);
  //std::vector<uint8_t> a2_val(num_vals);
  std::string d1_var;
  d1_var.resize(num_rows * std::to_string(num_rows).size());

  //query.set_layout(TILEDB_ROW_MAJOR)
  //query.set_layout(TILEDB_GLOBAL_ORDER) 
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", a1_buff)  // data)
      //.set_data_buffer("rows", d1_buff)        // rows)
      .set_data_buffer("rows", d1_var)        // rows)
      .set_offsets_buffer("rows", d1_offsets)  // rows_offsets)
      ;
  //.set_data_buffer("cols", cols);

  // Submit the query and close the array.
  query.submit();
  array.close();

  // Print out the results.
  auto result_num = query.result_buffer_elements()["rows"];
  //auto result_num = query.result_buffer_elements()["a"];
  uint64_t rownumcnt = 0;
  for (uint64_t r = 0; r < result_num.first; r++) {
    // For strings we must compute the length based on the offsets
    uint64_t row_start = d1_offsets[r];
    uint64_t row_end =
        //r == result_num.first - 1 ? result_num.first : d1_offsets[r + 1] - 1;
        //r == result_num.first - 1 ? &d1_var.back()+1 : d1_offsets[r + 1] - 1;
        r == result_num.first - 1 ? d1_var.end() - d1_var.begin() - 1 :
                                    d1_offsets[r + 1] - 1;
    // std::string i(rows.data() + row_start, row_end - row_start + 1);
    std::string i(d1_var.data() + row_start, row_end - row_start + 1);
    // int32_t j = cols[r];
    //int32_t a = data[r];
    int32_t a = a1_buff[r];
    std::cout << rownumcnt++ << ": "
              << "Cell (" << i  //<< ", " << j 
      << ") has data " << a << "\n";
  }
};
#endif

    showing_data = true; //TBD: disable/remove me production
    create_array();
    // write_array(9);
    write_array(1);
    // TBD: get correct values for the max size!!!
    CHECK(get_fragments_extreme(array_name) == 4);
    // write_array(99);
    write_array(2);
    // TBD: get correct values for the max size!!!
    CHECK(get_fragments_extreme(array_name) == 8);
    // write_array(999);
    write_array(3);
    // TBD: get correct values for the max size!!!
    CHECK(get_fragments_extreme(array_name) == 12);

}

TEST_CASE_METHOD(
    CAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, dense create/evolve, fixed dim/attr",
    "[capi][max-tile-size][dense][evolve]") {
  std::string array_name("tile_size_array");


  auto create_array = [&](const std::string& array_name) -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    remove_temp_dir(array_name.c_str());

    // The array will be 1x4 with dimensions "rows"  with domain
    // [1,4].
    tiledb::Domain domain(ctx);
    domain.add_dimension(
        tiledb::Dimension::create<int>(ctx, "rows", {{1, 4}}, 1));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    // Add one attribute "a1", so each (i) cell can store
    // a character on "a1".
    schema.add_attribute(
        tiledb::Attribute::create<std::array<char, 2>>(ctx, "a2"));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  };

#if 0
  auto write_array_a = [&]() -> void {
    tiledb::Context ctx;

    // Prepare some data for the array
    std::vector<char> a = {'a',
                          'b',
                          'c',  
                          'd' ,
                          'e',
                          'f',
                          'g',
                          'h'};
    #if 0
    std::vector<float> a2 = {1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,  4.1f,
                             4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
                             8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f,
                             11.2f, 12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f,
                             15.1f, 15.2f, 16.1f, 16.2f};
    #endif

    // Open the array for writing and create the query.
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a2", a);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };
#endif

  #if 0
  auto read_array_a1 = [&]() -> void {
    tiledb::Context ctx;

    // Prepare the array for reading
    tiledb::Array array(ctx, array_name, TILEDB_READ);

    // Slice only rows 1, 2 and cols 2, 3, 4
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, 1, 4);
    //.add_range(1, 2, 4);

    // Prepare the vector that will hold the result
    // (of size 6 elements for "a1" and 12 elements for "a2" since
    // it stores two floats per cell)
    //  std::vector<char> data_a1(6);
    //  std::vector<std::array<char,2>> data_a1(6);
    std::vector<std::array<char, 1>> data_a1(6);
    std::vector<float> data_a2(12);

    // Prepare the query
    tiledb::Query query(ctx, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", data_a1);
    //.set_data_buffer("a2", data_a2);

    // Submit the query and close the array.
    query.submit();
    array.close();

    if (showing_data) {
      // Print out the results.
      std::cout << "Reading attribute a1:\n";
      for (int i = 0; i < 6; ++i)
        //    std::cout << "a1: " << data_a1[i] << ", a2: (" << data_a2[2 * i] <<
        //    ","
        //              << data_a2[2 * i + 1] << ")\n";
        std::cout << "a1: " << data_a1[i][0] << "\n";
      // std::cout << "a1: " << data_a1[i][0] << data_a1[i][1] << "\n";
      std::cout << "\n";
    }
  };
  #endif

  #if 0
  auto read_array_subselect_a2 = [&](const char* array_name) -> void {
    tiledb::Context ctx;

    // Prepare the array for reading
    tiledb::Array array(ctx, array_name, TILEDB_READ);

    // Slice only rows 1, 2
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, 1, 2);

    // Prepare the vector that will hold the result
    // (of size 6 elements for "a1")
    std::vector<char> data_a1(6);

    // Prepare the query - subselect over "a1" only
    tiledb::Query query(ctx, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a2", data_a1);

    // Submit the query and close the array.
    query.submit();
    array.close();

    // Print out the results.
    std::cout << "Subselecting on attribute a2:\n";
    for (int i = 0; i < 6; ++i)
      std::cout << "a2: " << data_a1[i] << "\n";
  };
  #endif

  auto array_schema_evolve_A = [&](/* const Context& ctx*/) -> void {
    tiledb::Context ctx;
    tiledb::ArraySchemaEvolution schemaEvolution =
        tiledb::ArraySchemaEvolution(ctx);

    // Add attribute b
    tiledb::Attribute b =
        tiledb::Attribute::create<std::array<char, 257>>(ctx, "b257");
    schemaEvolution.add_attribute(b);

    schemaEvolution.drop_attribute(std::string("a2"));

    // evolve array
    schemaEvolution.array_evolve(array_name);
  };

  auto array_schema_evolve_B = [&](/* const Context& ctx*/) -> void {
    tiledb::Context ctx;
    tiledb::ArraySchemaEvolution schemaEvolution =
        tiledb::ArraySchemaEvolution(ctx);

    // Add attribute c
    tiledb::Attribute c =
        tiledb::Attribute::create<std::array<char, 42>>(ctx, "c42");
    schemaEvolution.add_attribute(c);

    schemaEvolution.drop_attribute(std::string("b257"));

    // evolve array
    schemaEvolution.array_evolve(array_name);
  };

  auto check_with_evolve = [&]() -> int {
    tiledb::Config cfg;
    tiledb::Context ctx(cfg);
    tiledb::VFS vfs(ctx);

    //const char* array_name = "max_tile_size_evolve";
    //const char* glob_array_name = array_name.c_str();
    //const char* array_name = glob_array_name;

    #if 0
    auto remove_temp_dir = [&vfs](const std::string& path) {
      if (vfs.is_dir(path.c_str())) {
        vfs.remove_dir(path.c_str());
      }
    };
    #endif
    remove_temp_dir(array_name);

    // CHECK(get_fragments_extreme(array_name) == 0);
    create_array(array_name);
    CHECK(get_fragments_extreme(array_name) == 0);
    write_array_attr<2>(array_name, "a2");
    CHECK(get_fragments_extreme(array_name) == 2);

    array_schema_evolve_A();
    CHECK(get_fragments_extreme(array_name) == 2);
    write_array_attr<257>(array_name, "b257");
    CHECK(get_fragments_extreme(array_name) == 257);

    array_schema_evolve_B();
    CHECK(get_fragments_extreme(array_name) == 257);
    write_array_attr<42>(array_name, "c42");
    //earlier fragment should still have value of 257
    CHECK(get_fragments_extreme(array_name) == 257);

    // now want to consolidate, but not vacuum, max
    // should still be 257
    tiledb::Array::consolidate(ctx, array_name);
    // After consolidation, old fragment should still be there with 257 
    CHECK(get_fragments_extreme(array_name) == 257);

    // then vacuum, now max should become 42 ...
    tiledb::Array::vacuum(ctx, array_name);
    CHECK(get_fragments_extreme(array_name) == 42);

    return 0;
  };

  auto check_with_create = [&]() -> int {
    tiledb::Config cfg;
    tiledb::Context ctx(cfg);
    tiledb::VFS vfs(ctx);

    const char* array_name = "max_tile_size";

    #if 0
    auto remove_temp_dir = [&vfs](const std::string& path) {
      if (vfs.is_dir(path.c_str())) {
        vfs.remove_dir(path.c_str());
      }
    };
#endif
    remove_temp_dir(array_name);

    // TBD: non-existent array may -not- return zero, hmm...
    // CHECK(get_fragments_extreme(array_name) == 0);
    create_array_with_attr<2>(array_name, "a2");
    CHECK(get_fragments_extreme(array_name) == 0);
    write_array_attr<2>(array_name, "a2");
    CHECK(get_fragments_extreme(array_name) == 2);

    remove_temp_dir(array_name);
    // CHECK(get_fragments_extreme(array_name) == 0);
    create_array_with_attr<257>(array_name, "b257");
    CHECK(get_fragments_extreme(array_name) == 0);
    write_array_attr<257>(array_name, "b257");
    CHECK(get_fragments_extreme(array_name) == 257);

    remove_temp_dir(array_name);
    // CHECK(get_fragments_extreme(array_name) == 0);
    create_array_with_attr<42>(array_name, "c42");
    CHECK(get_fragments_extreme(array_name) == 0);
    write_array_attr<42>(array_name, "c42");
    CHECK(get_fragments_extreme(array_name) == 42);

    return 0;
  };
  SECTION(" - Create") {
    check_with_create();
  }
  SECTION(" - Evolve") {
    check_with_evolve();
  }
}

TEST_CASE_METHOD(
    CAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, variable string dimension, variable string attribute",
    "[capi][max-tile-size][sparse][evolve]") {
  std::string array_name("tile_size_array");

  remove_temp_dir(array_name);

  auto create_array = [&]() -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    remove_temp_dir(array_name);

    // The array will be 1x4 with dimensions "rows"  with domain
    // [1,4].
    tiledb::Domain domain(ctx);
    domain.add_dimension(
        tiledb::Dimension::create(ctx, "rows", TILEDB_STRING_ASCII, nullptr, nullptr));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
    schema.set_domain(domain);

    // Add one attribute "a1", so each (i) cell can store
    // a character on "a1".
    schema.add_attribute(tiledb::Attribute::create<std::string>(ctx, "a1")
                             .set_nullable(true));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  };
  auto write_array = [&](
    std::string &dim_data, // expecting single-char values, one for each row
    std::vector<uint64_t> &&dim_offsets,
    std::string &&attr_data, // expecting single-char values, one for each row, 
                            // may be empty.. dim_data.size()-1
                         
    // std::vector<uint64_t> &attr_offsets
    std::vector<uint8_t> &genned_validity
    ) -> void {

    //assert(attr_data.size() <= dim_data().size());

    // Create a TileDB context.
    tiledb::Context ctx;

    // need buffer to be non-null (x.data()) even if there is not data
    // to avoid internal API failure even tho there will be no actual data,
    // when all validity values are zero.
    // expecting dim_data to be non-empty!
    attr_data.reserve(dim_data.size());  

    // std::vector<uint64_t> attr_offsets(attr_data.size());
    std::vector<uint64_t> attr_offsets(dim_data.size(), 0);
    for (decltype(attr_data.size()) i = 0; i < attr_data.size(); i++) {
      attr_offsets[i] = i;
    }
    for (decltype(attr_data.size()) i = attr_data.size(); i < dim_data.size();
         ++i) {
      attr_offsets[i] = attr_data.size();
    }
    std::vector<uint8_t> attr_val(dim_data.size());
    // same need, non-null buffer address even tho no data
    attr_val.reserve(dim_data.size());
    for (decltype(attr_data.size()) i = 0; i < attr_data.size(); ++i) {
      attr_val[i] = 1;
    }
    for (decltype(attr_data.size()) i = attr_data.size(); i < dim_data.size();
         ++i) {
      attr_val[i] = 0;
    }

    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a1", attr_data)
        .set_offsets_buffer("a1", attr_offsets)
        .set_validity_buffer("a1", attr_val)
        .set_data_buffer("rows", dim_data)
        .set_offsets_buffer("rows", dim_offsets);

    // Perform the write and close the array.
    query.submit();
    array.close();
    genned_validity = attr_val;
  };

  // TBD: disable/remove when its served its purpose...
  showing_data = true;

  create_array();
  //write_array("a", {0}, "", {});
  std::string basic_key_data{"abcdefghijklmnopqrstuvwxyz"};
  std::vector<uint8_t> genned_validity;
  write_array(basic_key_data, {0}, "", genned_validity);

  CHECK(get_fragments_extreme(array_name) == 8);

  // writing 26 empty items
  write_array(
      basic_key_data,
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      "",
      genned_validity
  );
  // 208 / 16 == 8 (bytes per offset, offsets dominate over data size)
  CHECK(get_fragments_extreme(array_name) == 208);

  // writing 24 empty items, 2 occupied items
  write_array(
      basic_key_data,
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      "AB",
      genned_validity
  );

  CHECK(get_fragments_extreme(array_name) == 208);

}

TEST_CASE_METHOD(
    CAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, fix dimension, nullable variable "
    "string attribute",
    "[capi][max-tile-size][dense][evolve]") {
  std::string array_name("tile_size_array");

  remove_temp_dir(array_name);

  auto create_array = [&]() -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    remove_temp_dir(array_name);

    // The array will be 1x4 with dimensions "rows"  with domain
    // [1,4].
    tiledb::Domain domain(ctx);
    domain.add_dimension(
        tiledb::Dimension::create<int>(ctx, "rows", {{1, 24}}, 1));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    // Add one attribute "a1", so each (i) cell can store
    // a string on "a1".
    schema.add_attribute(
        tiledb::Attribute::create<std::string>(ctx, "a1").set_nullable(true));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  };
  auto write_array =
      [&](std::string&&
              attr_data,  // expecting single-char values, one for each row
          std::vector<uint64_t>&& attr_offsets,
          std::vector<uint8_t>&& attr_val,
          std::vector<int>&& subrange) -> void {

    // Create a TileDB context.
    tiledb::Context ctx;

    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    tiledb::Subarray subarray(ctx, array);
    //subarray.add_range(0, 1, 2);
    subarray.add_range(0, subrange[0], subrange[1]);
    
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", attr_data)
        .set_offsets_buffer("a1", attr_offsets)
        .set_validity_buffer("a1", attr_val)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  // TBD: disable/remove when its served its purpose...
  showing_data = true;

  create_array();
  // write_array("a", {0}, "", {});
  //std::string basic_key_data{"abcdefghijklmnopqrstuvwxyz"};

  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 8);
#if 0
  write_array(
      "",
      {0, 0, 0, 0},
      {0},
      {1, 1});  // will core choke on empty string?
  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 8);

  write_array("z", {0, 0, 0, 0}, {0}, {1, 1});

  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 8);

  write_array(
      "a",
      {0, 0, 0, 0},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      {1, 0, 0, 0},
      {1, 4});
  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 208);

  write_array(
      "AB",
      {0, 1, 1, 1},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      {1, 1, 0, 0},
      {1, 4});

  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 208);

  write_array(
      "AbcDefgHijklMnopqr",
      {0, 3, 7, 13},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      {1, 1, 1, 1},
      {3,6});

  get_fragments_extreme(array_name);
#endif

  write_array(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      {0, 26, 52, 62, 88},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      //{1, 1, 1, 1, 1, 1},
      {1, 1, 1, 1, 1},
      {3, 7});

  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 208);
  // CHECK(get_fragments_extreme(array_name) == 208);

  write_array(
      "AbcDefgHijklMnopqr",
      {0, 3, 7, 13},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      {1, 1, 1, 1},
      {3, 6});

  get_fragments_extreme(array_name);

  // consolidate
  tiledb::Array::consolidate(ctx_, array_name);

  get_fragments_extreme(array_name);
}

TEST_CASE_METHOD(
    CAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, fix dimension, variable "
    "string attribute",
    "[capi][max-tile-size][dense][evolve]") {
  std::string array_name("tile_size_array");

  remove_temp_dir(array_name);

  auto create_array = [&]() -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    remove_temp_dir(array_name);

    // The array will be 1x4 with dimensions "rows"  with domain
    // [1,4].
    tiledb::Domain domain(ctx);
    domain.add_dimension(
        //tiledb::Dimension::create<int>(ctx, "rows", {{1, 24}}, 1));
        tiledb::Dimension::create<int>(ctx, "rows", {{1, 26}}, 26));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    // Add one attribute "a1", so each (i) cell can store
    // a string on "a1".
    schema.add_attribute(tiledb::Attribute::create<std::string>(ctx, "a1"));
                             //.set_nullable(true));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  };
  auto write_array = [&](std::string&& attr_data,  // expecting single-char
                                                   // values, one for each row
                         std::vector<uint64_t>&& attr_offsets,
                         //std::vector<uint8_t>&& attr_val,
                         std::vector<int>&& subrange) -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    tiledb::Subarray subarray(ctx, array);
    // subarray.add_range(0, 1, 2);
    subarray.add_range(0, subrange[0], subrange[1]);

    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", attr_data)
        .set_offsets_buffer("a1", attr_offsets)
        //.set_validity_buffer("a1", attr_val)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  // TBD: disable/remove when its served its purpose...
  showing_data = true;

  create_array();
  // write_array("a", {0}, "", {});
  // std::string basic_key_data{"abcdefghijklmnopqrstuvwxyz"};

  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 8);

#if 0
  write_array(
      "",
      //{0, 0, 0, 0},
      {0},
      //{0},
      {1, 1});  // will core choke on empty string?
  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 8);
#endif

  //write_array("lmnopqrstuvwxy", {0}, {1, 1});
  write_array("lmnopqrstuvwxy", {0,1,2,3,4,5,6,7,8,9,10,11,12,13}, {1, 14});
  get_fragments_extreme(array_name);

  write_array("l", {0}, {1, 1});
  get_fragments_extreme(array_name);

  write_array("lm", {0}, {1, 1});
  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 8);
  write_array("lmn", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmno", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmnop", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmnopq", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmnopqr", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmnopqrs", {0}, {1, 1});
  get_fragments_extreme(array_name);

  write_array("lmnopqrst", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmnopqrstu", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmnopqrstuv", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmnopqrstuvw", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmnopqrstuvwx", {0}, {1, 1});
  get_fragments_extreme(array_name);
  write_array("lmnopqrstuvwxy", {0}, {1, 1});
  get_fragments_extreme(array_name);

#if 0
  write_array(
      "a",
      {0, 0, 0, 0},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      {1, 0, 0, 0},
      {1, 4});
  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 208);

  write_array(
      "AB",
      {0, 1, 1, 1},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      //{1, 1, 0, 0},
      {1, 4});

  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 208);

  write_array(
      "AbcDefgHijklMnopqr",
      {0, 3, 7, 13},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      //{1, 1, 1, 1},
      {3,6});

  get_fragments_extreme(array_name);
#endif

  write_array(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      //{0, 26, 52, 62, 88},
      {0},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      //{1, 1, 1, 1, 1, 1},
      //{1, 1, 1, 1, 1},
      {1,1});
  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 208);

  write_array(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      {0, 26, 52, 62, 88},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      //{1, 1, 1, 1, 1, 1},
      //{1, 1, 1, 1, 1},
      {3, 7});

  get_fragments_extreme(array_name);
  // CHECK(get_fragments_extreme(array_name) == 208);

  write_array(
      "AbcDefgHijklMnopqr",
      {0, 3, 7, 13},
      //{0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
      // 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      //{1, 1, 1, 1},
      {3, 6});

  get_fragments_extreme(array_name);

  // consolidate
  tiledb::Array::consolidate(ctx_, array_name);
  //tiledb::Array::vacuum(ctx_, array_name);

  get_fragments_extreme(array_name);
}

#if TILEDB_SERIALIZATION
TEST_CASE_METHOD(
    CAPI_MaxTileSizeFx,
    "C++ API: Max tile size, serialization",
    "[capi][max-tile-size][serialization]") {
  std::string array_name("tile_size_array");
  tiledb::sm::Buffer buff;
  std::vector<uint64_t> values_to_try{
      0, 4, 208, 1024, 64 * 1024 * 1024, 64 * 1024 * 1024 + 1};
  for (auto v : values_to_try) {
    CHECK(tiledb::sm::serialization::maximum_tile_size_serialize(
        &v,
        tiledb::sm::SerializationType::CAPNP,
        &buff).ok());
    uint64_t retrieved_val = std::numeric_limits<uint64_t>::max();
    CHECK(tiledb::sm::serialization::maximum_tile_size_deserialize(
        &retrieved_val,
        tiledb::sm::SerializationType::CAPNP,
        buff).ok());
    CHECK(v == retrieved_val);
  }
}
#endif