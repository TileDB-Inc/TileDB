/**
 * @file   unit-max-tile-size.cc
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
 * Tests the C/CPP API for maximum tile size across all fragments of an array.
 */

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

struct CandCXXAPI_MaxTileSizeFx {
  const std::string main_array_name = "max_tile_size";

  bool showing_data = false;

  CandCXXAPI_MaxTileSizeFx()
    : vfs_(ctx_) {
  }

  void remove_temp_dir(const std::string& path) {
    if (vfs_.is_dir(path.c_str())) {
      vfs_.remove_dir(path.c_str());
    }
  };

  template <const int nchars = 1>
  void create_array_with_attr(const std::string& array_name, const std::string&& attr_name){
    // Create a TileDB context.
    // Context ctx;
    tiledb::Config cfg;
    tiledb::Context ctx(cfg);
    tiledb::VFS vfs(ctx);

    remove_temp_dir(array_name.c_str());

    // The array will be 1d with dimension "rows", with domain
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

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  };

  template <const int nchars = 1>
  void write_array_attr(const std::string& array_name, const std::string&& attr_name) {
    tiledb::Context ctx;

    // Prepare some data for the array
    std::vector<std::array<char, nchars>> a;
    for (auto i = 0; i < 4; ++i) {
      std::array<char, nchars> buf;
      memset(&buf[0], 'a' + i, nchars);
      a.emplace_back(buf);
    }

    // Open the array for writing and create the query.
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer(attr_name, a);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  uint64_t c_get_fragments_max_in_memory_tile_size(const std::string& array_uri) {
    tiledb::Context ctx;
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

  uint64_t cpp_get_fragments_max_in_memory_tile_size(const std::string& array_uri) {
    tiledb::Context ctx;
    tiledb::Array array(ctx, array_uri, TILEDB_READ);
    uint64_t max_in_memory_tile_size = 0;
    array.get_max_in_memory_tile_size(&max_in_memory_tile_size);

    if (showing_data) {
      std::cout << "maximum in memory tile size " << max_in_memory_tile_size
                << std::endl;
    }
    return max_in_memory_tile_size;
  }

  tiledb::Context ctx_;
  tiledb::VFS vfs_;
};

TEST_CASE_METHOD(
    CandCXXAPI_MaxTileSizeFx,
    "C++ API: Max tile size, dense, fixed, with consolidation",
    "[capi][cppapi][max-tile-size][dense][consolidate]") {
  // Name of array.
  auto& array_name = main_array_name;

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

  tiledb::Context ctx;

  uint64_t capi_max = 0; // init to avoid msvc warning as error miss inside CHECK()
  uint64_t cppapi_max;
  // Create and write array
  create_array();
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 0 * sizeof(int32_t));
  cppapi_max=cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);
  write_array_1();
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 4 * sizeof(int32_t));
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);
  write_array_2();
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 4 * sizeof(int32_t));
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);
  write_array_3();
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 4 * sizeof(int32_t));
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);
  write_array_4();
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 4 * sizeof(int32_t));
  cppapi_max=cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);

  // consolidate
  tiledb::Array::consolidate(ctx, array_name);

  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 4 * sizeof(int32_t));
  cppapi_max=cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);

  // consolidate
  tiledb::Array::vacuum(ctx, array_name);

  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 4 * sizeof(int32_t));
}

TEST_CASE_METHOD(
    CandCXXAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, variable string dimension, fix attr",
    "[capi][max-tile-size][sparse][var-dimension]") {
tiledb::Context ctx;
  auto& array_name = main_array_name;

  auto  create_array = [&]()-> void {

  remove_temp_dir(main_array_name);

  // The array will be 1d array with dimension "rows"
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
      auto val = std::to_string(d1_buff[i]);
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

  // init to avoid msvc warning as error miss inside CHECK()
  uint64_t capi_max = 0;  
  uint64_t cppapi_max;
  create_array();
  write_array(1);
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 8);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);
  write_array(2);
  //size of data and the single offset to start of extent same
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 16);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);

  // consolidate
  tiledb::Array::consolidate(ctx, array_name);
  // now '12' after consolidate(), data tiles pick up extra
  // overhead to support time traveling.
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 24);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);

  write_array(3);
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 24);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);
  write_array(1);
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 24);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);

  // consolidate
  tiledb::Array::consolidate(ctx, array_name);
  // now '28' after consolidate(), data tiles pick up extra
  // overhead to support time traveling.
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 56);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);

  // vacuum
  tiledb::Array::vacuum(ctx, array_name);
  // still '28', vacuuming does not remove the earlier data.
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 56);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);

  // secondary attempt still retains the overhead.
  tiledb::Array::consolidate(ctx, array_name);
  // now '28' after consolidate(), data tiles pick up extra
  // overhead to support time traveling, preserved across multiple attempts.
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 56);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);

  // vacuum
  tiledb::Array::vacuum(ctx, array_name);
  // still '28', vacuuming does not remove the earlier data, even after a
  // secondary consolidate against previously vacuumed data.
  CHECK((capi_max= c_get_fragments_max_in_memory_tile_size(array_name)) == 56);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(cppapi_max == capi_max);
}

TEST_CASE_METHOD(
    CandCXXAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, dense create/evolve, fixed dim/attr",
    "[capi][max-tile-size][dense][evolve]") {
  auto& array_name = main_array_name;

  auto create_array = [&](const std::string& array_name) -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    remove_temp_dir(array_name.c_str());

    // The array will be 1x4 with dimension "rows"  with domain
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

  tiledb::Config cfg;
  tiledb::Context ctx(cfg);
  tiledb::VFS vfs(ctx);

  remove_temp_dir(array_name);

  SECTION(" - Create") {
    create_array_with_attr<2>(array_name, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);
    write_array_attr<2>(array_name, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 4);

    remove_temp_dir(array_name);
    create_array_with_attr<257>(array_name, "b257");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);
    write_array_attr<257>(array_name, "b257");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);

    remove_temp_dir(array_name);
    create_array_with_attr<42>(array_name, "c42");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);
    write_array_attr<42>(array_name, "c42");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 42);
  }
  SECTION(" - Evolve") {
    create_array(array_name);
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);
    write_array_attr<2>(array_name, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 4);

    array_schema_evolve_A();
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 4);
    write_array_attr<257>(array_name, "b257");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);

    array_schema_evolve_B();
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);
    write_array_attr<42>(array_name, "c42");
    // earlier fragment should still have value of 257
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);

    // now want to consolidate, but not vacuum, max
    // should still be 257
    tiledb::Array::consolidate(ctx, array_name);
    // After consolidation, old fragment should still be there with 257
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);

    // then vacuum, now max should become 42 ...
    tiledb::Array::vacuum(ctx, array_name);
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 42);
  }
}

TEST_CASE_METHOD(
    CandCXXAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, variable string dimension, variable string attribute",
    "[capi][max-tile-size][sparse][evolve]") {
  std::string array_name("tile_size_array");

  remove_temp_dir(array_name);

  auto create_array = [&]() -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    remove_temp_dir(array_name);

    // The array will be 1d array with dimension "rows"
    // "rows" is a string dimension type, so the domain and extent is null
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

  create_array();
  std::string basic_key_data{"abcdefghijklmnopqrstuvwxyz"};
  std::vector<uint8_t> genned_validity;
  write_array(basic_key_data, {0}, "", genned_validity);

  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // writing 26 empty items
  write_array(
      basic_key_data,
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      "",
      genned_validity
  );
  // 208 / 16 == 8 (bytes per offset, offsets dominate over data size)
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 208);

  // writing 24 empty items, 2 occupied items
  write_array(
      basic_key_data,
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      "AB",
      genned_validity
  );

  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 208);

}

TEST_CASE_METHOD(
    CandCXXAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, fix dimension, variable "
    "string attribute",
    "[capi][max-tile-size][dense][evolve]") {
  std::string array_name("tile_size_array");

  remove_temp_dir(array_name);

  auto create_array = [&](uint64_t extents = 1) -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    remove_temp_dir(array_name);

    // The array will be 1x27 with dimension "rows"  with domain
    // [1,27] and extent specified by parameter.
    tiledb::Domain domain(ctx);
    domain.add_dimension(
        tiledb::Dimension::create<int>(ctx, "rows", {{1, 27}}, extents));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    // Add one attribute "a1", so each (i) cell can store
    // a string on "a1".
    schema.add_attribute(tiledb::Attribute::create<std::string>(ctx, "a1"));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  };
  auto write_array = [&](std::string&& attr_data,  // expecting single-char
                                                   // values, one for each row
                         std::vector<uint64_t>&& attr_offsets,
                         std::vector<int>&& subrange) -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    tiledb::Subarray subarray(ctx, array);
    subarray.add_range(0, subrange[0], subrange[1]);

    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", attr_data)
        .set_offsets_buffer("a1", attr_offsets)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  create_array(1);
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 0);

  // zero-len data written
  // at index 1
  write_array(
      "",
      {0},
      {1, 1});
  // size of single offset (sizeof(uint64_t)) dominates
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // attribute is variable non-nullable, 
  // So the single offset (uint64_t) of 8 bytes will be max with strings <= 8 bytes
  // writing len 1 data at idx 1
  write_array("l", {0}, {1, 1});
  // size of single offset dominates
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // writing len 2 data at idx1
  write_array("lm", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 3 data at idx 1
  write_array("lmn", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 4 data at idx 1
  write_array("lmno", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 5 data at idx 1
  write_array("lmnop", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 6 data at idx 1
  write_array("lmnopq", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 7 data at idx 1
  write_array("lmnopqr", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 8 data at idx 1
  write_array("lmnopqrs", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // still writing 1 item, but now length of variable 
  // data greater than the 1 offset
  // writing len 9 data at idx 1
  write_array("lmnopqrst", {0}, {1, 1});
  //length of data dominates
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 9);
  // writing len 10 data at idx 1
  write_array("lmnopqrstu", {0}, {1, 1});
  // length of data dominates
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 10);
  // writing len 11 data at idx 1
  write_array("lmnopqrstuv", {0}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 11);
  // writing len 12 data at idx 1
  write_array("lmnopqrstuvw", {0}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 12);
  // writing len 13 data at idx 1
  write_array("lmnopqrstuvwx", {0}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 13);
  // writing len 14 data at idx 1
  write_array("lmnopqrstuvwxy", {0}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items, 
  // idx 1 "a", idx 2 "", idx 3 "", idx 4 ""
  write_array(
      "a",
      {0, 0, 0, 0},
      {1, 4});
  // earlier item len 14 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items, 
  // idx 1 "A", idx 2 "B", idx 3 "", idx 4 ""
  write_array(
      "AB",
      {0, 1, 1, 1},
      {1, 4});

  // earlier item len 14 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  //4 items, 
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijkl, idx 6 "nopqr"
  write_array(
      "AbcDefgHijklmNopqr",
      {0, 3, 7, 13},
      {3,6});

  // earlier item len 14 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 5 items, max of any individual item is 26 bytes
  // 26 @ idx 3, 26 @idx 4, 10 @ idx 5, 26 @ idx 6, 26 @ id x7
  write_array(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      {0, 26, 52, 62, 88},
      {3, 7});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  write_array(
    //1 item, 114 bytes @ idx 1
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      //{0, 26, 52, 62, 88},
      {0}, //offsets
      {1,1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 5 items, max of any individual item is 26 bytes
  // 26 @ idx 3, 26 @idx 4, 10 @ idx 5, 26 @ idx 6, 26 @ id x7
  write_array(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      {0, 26, 52, 62, 88},
      {3, 7});
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 4 items, various lengths
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "Nopqr"
  write_array(
      "AbcDefgHijklmNopqr",
      {0, 3, 7, 13},
      {3, 6});
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // consolidate
  tiledb::Array::consolidate(ctx_, array_name);
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  tiledb::Array::vacuum(ctx_, array_name);
  // earlier item len 114 idx 1 still dominates (even after vacuum, still present)
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 4 items, various lengths
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "Nopqr"
  write_array(
      "AbcDefgHijklmNopqr",
      {0, 3, 7, 13},
      {1, 4});
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // consolidate
  tiledb::Array::consolidate(ctx_, array_name);
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  tiledb::Array::vacuum(ctx_, array_name);
  // some earlier item(s) len 26 from idx 3..7 now dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // 4 items, various lengths
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "N", idx 7 "Opqr"
  write_array(
      "AbcDefgHijklmNOpqr",
      {0, 3, 7, 13, 14},
      {3, 7});
  // earlier item(s) len 26 idx 3..7 dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // consolidate
  tiledb::Array::consolidate(ctx_, array_name);
  // earlier item(s) len 26 idx 3..7 dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  tiledb::Array::vacuum(ctx_, array_name);
  // after vacuum, now down to lesser items, offset sizeof(uint64_t) again dominant
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
}

TEST_CASE_METHOD(
    CandCXXAPI_MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, fix dimension, nullable variable "
    "string attribute",
    "[capi][max-tile-size][dense][evolve]") {
  std::string array_name("tile_size_array");

  remove_temp_dir(array_name);

  auto create_array = [&](int extents = 1) -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    remove_temp_dir(array_name);

    // The array will be 1d with dimensions "rows"  with domain
    // [1,27].
    tiledb::Domain domain(ctx);
    domain.add_dimension(
        tiledb::Dimension::create<int>(ctx, "rows", {{1, 27}}, extents));

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
  auto write_array = [&](std::string&& attr_data,  // expecting single-char
                                                   // values, one for each row
                         std::vector<uint64_t>&& attr_offsets,
                         std::vector<uint8_t>&& attr_val,
                         std::vector<int>&& subrange) -> void {
    // Create a TileDB context.
    tiledb::Context ctx;

    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    tiledb::Subarray subarray(ctx, array);
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

  create_array(1);
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);

  // zero-len data written
  // at index 1
  write_array("", {0}, {1}, {1, 1});  // will core choke on empty string?
  // size of single offset (sizeof(uint64_t)) dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // attribute is variable non-nullable,
  // So the single offset (uint64_t) of 8 bytes will be max with strings <= 8
  // bytes writing len 1 data at idx 1
  write_array("l", {0}, {1}, {1, 1});
  // size of single offset dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // writing len 2 data at idx1
  write_array("lm", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 3 data at idx 1
  write_array("lmn", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 4 data at idx 1
  write_array("lmno", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 5 data at idx 1
  write_array("lmnop", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 6 data at idx 1
  write_array("lmnopq", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 7 data at idx 1
  write_array("lmnopqr", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // writing len 8 data at idx 1
  write_array("lmnopqrs", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // still writing 1 item, but now length of variable
  // data greater than the 1 offset
  // writing len 9 data at idx 1
  write_array("lmnopqrst", {0}, {1}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 9);
  // writing len 10 data at idx 1
  write_array("lmnopqrstu", {0}, {1}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 10);
  // writing len 11 data at idx 1
  write_array("lmnopqrstuv", {0}, {1}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 11);
  // writing len 12 data at idx 1
  write_array("lmnopqrstuvw", {0}, {1}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 12);
  // writing len 13 data at idx 1
  write_array("lmnopqrstuvwx", {0}, {1}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 13);
  // writing len 14 data at idx 1
  write_array("lmnopqrstuvwxy", {0}, {1}, {1, 1});
  // length of data dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items,
  // idx 1 "a", idx 2 "", idx 3 "", idx 4 ""
  write_array("a", {0, 0, 0, 0}, {1,1,1,1}, {1, 4});
  // earlier item len 14 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items,
  // idx 1 "A", idx 2 "B", idx 3 "", idx 4 ""
  write_array("AB", {0, 1, 1, 1}, {1, 1, 1, 1}, {1, 4});

  // earlier item len 14 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items,
  //  idx 3 "Abc, idx 4 "Defg", idx 5 "Hijkl, idx 6 "nopqr"
  write_array("AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 1, 1, 1}, {3, 6});

  // earlier item len 14 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 5 items, max of any individual item is 26 bytes
  // 26 @ idx 3, 26 @idx 4, 10 @ idx 5, 26 @ idx 6, 26 @ id x7
  write_array(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      {0, 26, 52, 62, 88},
      {1, 1, 1, 1, 1},
      {3, 7});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  write_array(
      // 1 item, 114 bytes @ idx 1
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      //{0, 26, 52, 62, 88},
      {0},  // offsets
      {1},
      {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 5 items, max of any individual item is 26 bytes
  // 26 @ idx 3, 26 @idx 4, 10 @ idx 5, 26 @ idx 6, 26 @ id x7
  write_array(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      {0, 26, 52, 62, 88},
      {1, 1, 1, 1, 1},
      {3, 7});
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 4 items, various lengths
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "Nopqr"
  write_array("AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 1, 1, 1}, {3, 6});
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // consolidate
  tiledb::Array::consolidate(ctx_, array_name);
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  tiledb::Array::vacuum(ctx_, array_name);
  // earlier item len 114 idx 1 still dominates (even after vacuum, still
  // present)
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 4 items, various lengths
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "Nopqr"
  write_array("AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 1, 1, 1}, {1, 4});
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // consolidate
  tiledb::Array::consolidate(ctx_, array_name);
  // earlier item len 114 idx 1 still dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  tiledb::Array::vacuum(ctx_, array_name);
  // some earlier item(s) len 26 from idx 3..7 now dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // 4 items, various lengths
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "N", idx 7 "Opqr"
  write_array("AbcDefgHijklmNOpqr", {0, 3, 7, 13, 14}, {1, 1, 1, 1, 1}, {3, 7});
  // earlier item(s) len 26 idx 3..7 dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // consolidate
  tiledb::Array::consolidate(ctx_, array_name);
  // earlier item(s) len 26 idx 3..7 dominates
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  tiledb::Array::vacuum(ctx_, array_name);
  // after vacuum, now down to lesser items, offset sizeof(uint64_t) again
  // dominant
  CHECK(cpp_get_fragments_max_in_memory_tile_size(array_name) == 8);
}

#ifdef TILEDB_SERIALIZATION
TEST_CASE_METHOD(
    CandCXXAPI_MaxTileSizeFx,
    "C++ API: Max tile size, serialization",
    "[max-tile-size][serialization]") {
  std::string array_name("tile_size_array");
  tiledb::sm::Buffer buff;
  std::vector<uint64_t> values_to_try{
      0,
      4,
      208,
      1024,
      64 * 1024 * 1024,
      64 * 1024 * 1024 + 1,
      std::numeric_limits<uint64_t>::max()};
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