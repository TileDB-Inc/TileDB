/**
 * @file   unit-max-tile-size.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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
 * Tests the C/CPP API for obtaining maximum tile size across all fragments of
 * an array.
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

struct MaxTileSizeFx {
  const std::string main_array_name_ = "max_tile_size_array";

  MaxTileSizeFx()
      : vfs_(ctx_) {
  }

  ~MaxTileSizeFx() {
    remove_temp_dir(main_array_name_);
  }

  void remove_temp_dir(const std::string& path) {
    if (vfs_.is_dir(path.c_str())) {
      vfs_.remove_dir(path.c_str());
    }
  };

  void create_sparse_array_string_dim_int32_attr() {
    remove_temp_dir(main_array_name_.c_str());

    // Set up the Domain/Dimension items for the array being created.
    tiledb::Domain domain(ctx_);
    domain.add_dimension(tiledb::Dimension::create(
        ctx_, "d1", TILEDB_STRING_ASCII, nullptr, nullptr));

    // The array will be sparse.
    tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(domain);

    // Add a single attribute "a1" so each cell can store an integer.
    schema.add_attribute(tiledb::Attribute::create<int32_t>(ctx_, "a1"));

    // Create the (empty) array on disk.
    tiledb::Array::create(main_array_name_, schema);
  }

  void write_sparse_array_string_dim_int32_attr(uint64_t num_rows) {
    // Prepare the data to be written.
    // Writes num_rows rows containing int32 attr with corresponding string
    // coordinate that is value-of-attr + 1, attr values start at 0, coords
    // start at 1.
    // coords['1'] ==> val 0
    // ...
    // coords[to_string(num_rows)] ==> num_rows
    std::vector<int32_t> a_buff(num_rows);
    auto start_val = 0;
    std::iota(a_buff.begin(), a_buff.end(), start_val);

    // For the string dimension, convert the increasing value from int to
    // string, and prepare the cumulative value buffer and corresponding
    // offsets buffer.
    std::vector<uint64_t> d1_offsets;
    d1_offsets.reserve(num_rows);
    std::string d1_var;
    uint64_t offset = 0;
    for (uint64_t i = start_val; i < start_val + num_rows; i++) {
      auto val = std::to_string(i + 1);
      d1_offsets.emplace_back(offset);
      offset += val.size();
      d1_var += val;
    }

    // Open the array for writing and create the query.
    tiledb::Array array(ctx_, main_array_name_, TILEDB_WRITE);
    tiledb::Query query(ctx_, array, TILEDB_WRITE);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a1", a_buff)
        .set_data_buffer("d1", d1_var)
        .set_offsets_buffer("d1", d1_offsets);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  void create_dense_array_int_dim_string_attr(
      std::array<int, 2> d1_domain,
      int d1_extents,
      bool a1_is_nullable,
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order) {
    remove_temp_dir(main_array_name_.c_str());

    // Set up the Domain/Dimension items for the array being created.
    tiledb::Domain domain(ctx_);
    domain.add_dimension(
        tiledb::Dimension::create<int>(ctx_, "d1", d1_domain, d1_extents));

    // The array will be dense with the indicated order(s).
    tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(domain)
        .set_tile_order(tile_order)
        .set_cell_order(cell_order);

    // Specify attribute, nullable or not as indicated.
    auto a1_attr = tiledb::Attribute::create<std::string>(ctx_, "a1");
    if (a1_is_nullable) {
      a1_attr.set_nullable(true);
    }
    schema.add_attribute(a1_attr);

    // Create the array.
    tiledb::Array::create(main_array_name_, schema);
  }

  void write_dense_array_int_dim_string_attr_null(
      std::string a1_data,
      std::vector<uint64_t> a1_offsets,
      std::vector<uint8_t> a1_validity,
      std::vector<int> subrange) {
    // Define needed objects.
    tiledb::Array array(ctx_, main_array_name_, TILEDB_WRITE);
    tiledb::Query query(ctx_, array);
    tiledb::Subarray subarray(ctx_, array);

    // Initialize the objects, preparing the query.
    subarray.add_range(0, subrange[0], subrange[1]);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", a1_data)
        .set_offsets_buffer("a1", a1_offsets)
        .set_validity_buffer("a1", a1_validity)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  void write_dense_array_int_dim_string_attr(
      std::string a1_data,
      std::vector<uint64_t> a1_offsets,
      std::vector<int> subrange) {
    // Define needed objects.
    tiledb::Array array(ctx_, main_array_name_, TILEDB_WRITE);
    tiledb::Query query(ctx_, array);
    tiledb::Subarray subarray(ctx_, array);

    // Initialize the objects, preparing the query.
    subarray.add_range(0, subrange[0], subrange[1]);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", a1_data)
        .set_offsets_buffer("a1", a1_offsets)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  void create_dense_array_int_dim_int_dim_int32_attr(
      std::array<int, 2> d1_domain,
      int d1_extents,
      std::array<int, 2> d2_domain,
      int d2_extents,
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order) {
    remove_temp_dir(main_array_name_.c_str());

    // Set up the Domain/Dimension items for the array being created.
    tiledb::Domain domain(ctx_);
    domain
        .add_dimension(
            tiledb::Dimension::create<int>(ctx_, "d1", d1_domain, d1_extents))
        .add_dimension(
            tiledb::Dimension::create<int>(ctx_, "d2", d2_domain, d2_extents));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(domain)
        .set_tile_order(tile_order)
        .set_cell_order(cell_order);

    // Add a single attribute "a1" so each (i,j) cell can store an integer.
    schema.add_attribute(tiledb::Attribute::create<int32_t>(ctx_, "a1"));

    // Create the (empty) array on disk.
    tiledb::Array::create(main_array_name_, schema);
  }

  void write_dense_int_dim_int_dim_int32_attr(
      std::vector<int32_t> a1_data,
      std::array<int, 2> dim1_range,
      std::array<int, 2> dim2_range) {
    // Open the array for writing and create the query.
    tiledb::Array array(ctx_, main_array_name_, TILEDB_WRITE);
    tiledb::Subarray subarray(ctx_, array);
    subarray.add_range(0, dim1_range[0], dim1_range[1])
        .add_range(1, dim2_range[0], dim2_range[1]);
    tiledb::Query query(ctx_, array);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", a1_data)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  template <int nchar>
  void create_dense_array_int_dim_nchar_attr(
      std::array<int, 2> d1_domain,
      int d1_extents,
      const std::string attr_name) {
    remove_temp_dir(main_array_name_.c_str());

    // The array will be 1d with dimension "d1", with domain, extents
    // as passed.
    tiledb::Domain domain(ctx_);
    domain.add_dimension(
        tiledb::Dimension::create<int>(ctx_, "d1", d1_domain, d1_extents));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    // Add one attribute attr_name, for storing in each (ith) cell
    // nchar characters of 'a'+i.
    schema.add_attribute(
        tiledb::Attribute::create<std::array<char, nchar>>(ctx_, attr_name));

    // Create the (empty) array on disk.
    tiledb::Array::create(main_array_name_, schema);
  }

  template <const int nchars = 1>
  void write_dense_array_attr_nchar_ntimes(
      const std::string& array_name,
      const std::string attr_name,
      int ntimes = 4) {
    // Prepare some data to write to the array.
    std::vector<std::array<char, nchars>> a;
    for (auto i = 0; i < ntimes; ++i) {
      std::array<char, nchars> buf;
      memset(&buf[0], 'a' + i, nchars);
      a.emplace_back(buf);
    }

    // Open the array for writing and create the query.
    tiledb::Array array(ctx_, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx_, array);
    query.set_layout(TILEDB_ROW_MAJOR).set_data_buffer(attr_name, a);

    // Perform the write and close the array.
    query.submit();
    array.close();
  };

  void array_schema_evolve_char_attr_257() {
    // Targeted against schema created in create_dense_array_int_dim_nchar_attr.
    tiledb::ArraySchemaEvolution schemaEvolution =
        tiledb::ArraySchemaEvolution(ctx_);

    // Add attribute b.
    tiledb::Attribute b =
        tiledb::Attribute::create<std::array<char, 257>>(ctx_, "b257");
    schemaEvolution.add_attribute(b);

    // Drop attribute a2.
    schemaEvolution.drop_attribute(std::string("a2"));

    // Evolve array.
    schemaEvolution.array_evolve(main_array_name_);
  };

  void array_schema_evolve_char_attr_42() {
    // Targeted against schema created in create_dense_array_int_dim_nchar_attr.
    tiledb::ArraySchemaEvolution schemaEvolution =
        tiledb::ArraySchemaEvolution(ctx_);

    // Add attribute c.
    tiledb::Attribute c =
        tiledb::Attribute::create<std::array<char, 42>>(ctx_, "c42");
    schemaEvolution.add_attribute(c);

    // Drop attribute b257.
    schemaEvolution.drop_attribute(std::string("b257"));

    // Evolve array.
    schemaEvolution.array_evolve(main_array_name_);
  }

  void create_sparse_array_string_dim_string_attr(bool is_nullable) {
    remove_temp_dir(main_array_name_.c_str());

    // Set up the Domain/Dimension items for the array being created.
    tiledb::Domain domain(ctx_);
    domain.add_dimension(tiledb::Dimension::create(
        ctx_, "d1", TILEDB_STRING_ASCII, nullptr, nullptr));

    // The array will be sparse.
    tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(domain);

    // Add attribute "a1".
    auto attr = tiledb::Attribute::create<std::string>(ctx_, "a1");
    if (is_nullable) {
      attr.set_nullable(true);
    }
    schema.add_attribute(attr);

    // Create the (empty) array on disk.
    tiledb::Array::create(main_array_name_, schema);
  }

  void write_sparse_array_string_dim_string_attr(
      std::string& dim_data,  // Expecting single-char values, one for each row.
      std::vector<uint64_t> dim_offsets,
      std::string attr_data  // Expecting single-char values, one for each
                             // row, may be empty... dim_data.size()-1.
  ) {
    // attr_data needs its buffer retrieved with .data() to be non-null to
    // avoid internal API failure, even if there is no data, which can occur
    // when all validity values are zero. dim_data should always contain data.
    attr_data.reserve(dim_data.size());

    // Prepare offsets to attr items...
    std::vector<uint64_t> attr_offsets(dim_data.size(), 0);
    // ...offsets for those for which actually have data;
    for (decltype(attr_data.size()) i = 0; i < attr_data.size(); i++) {
      attr_offsets[i] = i;
    }
    // ...pad out offsets for those which do not have data.
    for (decltype(attr_data.size()) i = attr_data.size(); i < dim_data.size();
         ++i) {
      attr_offsets[i] = attr_data.size();
    }

    // Prepare the validity values for the data
    std::vector<uint8_t> attr_val(dim_data.size());
    // attr_val also needs a non empty buffer, and ...
    attr_val.reserve(dim_data.size());
    // ... init those for which there is data
    for (decltype(attr_data.size()) i = 0; i < attr_data.size(); ++i) {
      attr_val[i] = 1;
    }
    // ... init those trailing which are empty and have no data
    for (decltype(attr_data.size()) i = attr_data.size(); i < dim_data.size();
         ++i) {
      attr_val[i] = 0;
    }

    // Open the array for writing and create the query.
    tiledb::Array array(ctx_, main_array_name_, TILEDB_WRITE);
    tiledb::Query query(ctx_, array);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a1", attr_data)
        .set_offsets_buffer("a1", attr_offsets)
        .set_validity_buffer("a1", attr_val)
        .set_data_buffer("d1", dim_data)
        .set_offsets_buffer("d1", dim_offsets);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  uint64_t c_get_fragments_max_in_memory_tile_size(
      const std::string& array_uri) {
    tiledb::Array array(ctx_, array_uri, TILEDB_READ);
    uint64_t max_in_memory_tile_size = 0;
    CHECK(
        tiledb_array_maximum_tile_size(
            &*array.ptr(), &max_in_memory_tile_size) == TILEDB_OK);

    return max_in_memory_tile_size;
  }

  uint64_t cpp_get_fragments_max_in_memory_tile_size(
      const std::string& array_uri) {
    tiledb::Array array(ctx_, array_uri, TILEDB_READ);
    uint64_t max_in_memory_tile_size = 0;
    max_in_memory_tile_size = array.get_max_in_memory_tile_size();

    return max_in_memory_tile_size;
  }

  tiledb::Context ctx_;
  tiledb::VFS vfs_;
};  // struct MaxTileSizeFx

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max tile size, dense, fixed, with consolidation",
    "[capi][cppapi][max-tile-size][dense][consolidate][vacuum]") {
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t cell_order = TILEDB_COL_MAJOR;
  SECTION(" - tile/cell row major") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
  }
  SECTION(" - tile/cell col major") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
  }
  uint64_t capi_max;
  uint64_t cppapi_max;

  // Create array and validate 0 max tile size.
  create_dense_array_int_dim_int_dim_int32_attr(
      {1, 4}, 2, {1, 4}, 2, tile_order, cell_order);
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  CHECK(capi_max == 0 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Write first fragment and validate sizes.
  write_dense_int_dim_int_dim_int32_attr(
      {1, 2, 3, 4, 5, 6, 6, 8}, {1, 2}, {1, 4});
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Write second fragment and validate sizes.
  write_dense_int_dim_int_dim_int32_attr({101, 102, 103, 104}, {2, 3}, {2, 3});
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Write third fragment and validate sizes.
  write_dense_int_dim_int_dim_int32_attr({201}, {1, 1}, {1, 1});
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Write fourth fragment and validate sizes.
  write_dense_int_dim_int_dim_int32_attr({202}, {3, 3}, {4, 4});
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Consolidate and validate sizes.
  tiledb::Config config;
  config.set("sm.consolidation.buffer_size", "10000");
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);

  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Vacuum.
  tiledb::Array::vacuum(ctx_, main_array_name_);

  CHECK(
      c_get_fragments_max_in_memory_tile_size(main_array_name_) ==
      4 * sizeof(int32_t));
}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, variable string dimension, fix attr, "
    "with consolidation",
    "[capi][max-tile-size][sparse][var-dimension][consolidate][vacuum]") {
  // In addition to validation of particular schema in use,
  // will be checking that C and CPP apis return same values.
  uint64_t capi_max;
  uint64_t cppapi_max;

  // Create the array.
  create_sparse_array_string_dim_int32_attr();
  // Write 1 row (1 string coord, 1 int32)
  write_sparse_array_string_dim_int32_attr(1);
  // Size of (64bit) attr offset dominant size.
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  CHECK(capi_max == 8);
  CHECK(cppapi_max == capi_max);

  // Write 2 rows (2 string coords, 2 int32s)
  write_sparse_array_string_dim_int32_attr(2);
  // Offsets (64bit) to the two coords are dominant in size.
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  CHECK(capi_max == 16);
  CHECK(cppapi_max == capi_max);

  // Consolidate.
  tiledb::Config config;
  config.set("sm.consolidation.buffer_size", "10000");
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);

  // Now '24' after consolidate(), data tiles pick up extra overhead to support
  // time traveling.
  CHECK(capi_max == 24);
  CHECK(cppapi_max == capi_max);

  // Write 3 rows (3 string coords, 3 int32s)
  write_sparse_array_string_dim_int32_attr(3);
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  // 3 (64bit/8byte) coord offsets do not exceed the overhead previously
  // acquired.
  CHECK(capi_max == 24);
  CHECK(cppapi_max == capi_max);
  // Write 1 rows (1 string coords, 1 int32)
  write_sparse_array_string_dim_int32_attr(1);
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);
  CHECK(capi_max == 24);
  CHECK(cppapi_max == capi_max);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);

  // Changed, data tiles pick up extra overhead to support time traveling.
  CHECK(capi_max == 56);
  CHECK(cppapi_max == capi_max);

  // Sparse/dense vacuum/consolidate semantics differ,
  // sparse retains any old data and time travel overhead.
  // Vacuum.
  tiledb::Array::vacuum(ctx_, main_array_name_);
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);

  // No change, vacuuming sparse does not remove the earlier data.
  CHECK(capi_max == 56);
  CHECK(cppapi_max == capi_max);

  // Secondary attempt still retains the overhead.
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);

  // No change after consolidate(), extra
  // overhead retained across multiple consolidates.
  CHECK(capi_max == 56);
  CHECK(cppapi_max == capi_max);

  // Vacuum.
  tiledb::Array::vacuum(ctx_, main_array_name_);
  capi_max = c_get_fragments_max_in_memory_tile_size(main_array_name_);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(main_array_name_);

  // No change, vacuuming sparse does not remove the earlier data, even after a
  // secondary consolidate against previously vacuumed data.
  CHECK(capi_max == 56);
  CHECK(cppapi_max == capi_max);
}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, dense create/evolve, fixed dim/attr",
    "[capi][max-tile-size][dense][evolve][consolidate]") {
  SECTION(" - Create") {
    // Create empty array with attr "a2" of char a2[1]
    create_dense_array_int_dim_nchar_attr<2>({1, 4}, 1, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 0);
    // Write attr a2 data len 2
    write_dense_array_attr_nchar_ntimes<2>(main_array_name_, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 4);

    // Create empty array with attr "b257" of char b257[257].
    create_dense_array_int_dim_nchar_attr<257>({1, 2}, 1, "b257");
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 0);
    // Write attr b257 data len 257
    write_dense_array_attr_nchar_ntimes<257>(main_array_name_, "b257", 2);
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 257);

    // Create empty array with attr "a2" of char c42[42].
    create_dense_array_int_dim_nchar_attr<42>({1, 20}, 1, "c42");
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 0);
    // Write c42 data len 42
    write_dense_array_attr_nchar_ntimes<42>(main_array_name_, "c42", 20);
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 42);
  }
  SECTION(" - Evolve") {
    // Create empty array with attr "a2" of char a2[1].
    create_dense_array_int_dim_nchar_attr<2>({1, 4}, 1, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 0);
    // Write attr a2 data len 2
    write_dense_array_attr_nchar_ntimes<2>(main_array_name_, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 4);

    // Evolve the attr replacing a2 with char b257[257].
    array_schema_evolve_char_attr_257();
    // Observe the the evolving change did not affect max size of any data.
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 4);
    // Write data to the (now only) "b257" attribute.
    write_dense_array_attr_nchar_ntimes<257>(main_array_name_, "b257");
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 257);

    // Evolve the attr replacing b257 with char c42[42].
    array_schema_evolve_char_attr_42();
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 257);
    write_dense_array_attr_nchar_ntimes<42>(main_array_name_, "c42");
    // Earlier fragment should still have dominant value of 257.
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 257);

    // Now want to consolidate, but not vacuum, max should still be 257.
    tiledb::Config config;
    config.set("sm.consolidation.buffer_size", "10000");
    tiledb::Array::consolidate(ctx_, main_array_name_, &config);
    // After consolidation, old fragment should still be there with 257.
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 257);

    // Sparse/dense vacuum/consolidate semantics differ, vacuum dense eliminates
    // old data.  After Vacuum, max should now be 42.
    tiledb::Array::vacuum(ctx_, main_array_name_);
    CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 42);
  }
}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, variable string dimension, variable "
    "string attribute",
    "[capi][max-tile-size][sparse][evolve]") {
  // Create empty array.
  create_sparse_array_string_dim_string_attr(true);

  std::string basic_key_data{"abcdefghijklmnopqrstuvwxyz"};
  // Write 1 empty data item, indexed by basic_key_data.
  write_sparse_array_string_dim_string_attr(basic_key_data, {0}, "");
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 26);

  // Writing 26 empty items, individual letter indices.
  write_sparse_array_string_dim_string_attr(
      basic_key_data,
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      "");
  // 208 / 26 == 8 (bytes per offset, offsets dominate over data size)
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 208);

  // Writing 24 empty items, 2 occupied items.
  write_sparse_array_string_dim_string_attr(
      basic_key_data,
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      "AB");
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 208);
}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, fix dimension, variable "
    "string attribute",
    "[capi][max-tile-size][dense][evolve][consolidate][vacuum]") {
  // The init's are to quiet MSVC complaining about possibile use of uninit'd
  // variables.
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_COL_MAJOR;
  // In addition to the paraticular array variant exercised here,
  // also trying multiple orderings "just-in-case" some difference might exist.
  SECTION(" - tile/cell row_major") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
  }
  SECTION(" - tile/cell col_major") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
  }
  SECTION(" - tile/cell row/col") {
    tile_order = TILEDB_ROW_MAJOR;
    cell_order = TILEDB_COL_MAJOR;
  }
  SECTION(" - tile/cell col/row") {
    tile_order = TILEDB_COL_MAJOR;
    cell_order = TILEDB_ROW_MAJOR;
  }
  // Create empty array.
  create_dense_array_int_dim_string_attr(
      {1, 27}, 1, false, tile_order, cell_order);
  // No data tiles written, should be zero.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 0);

  // Zero-len data written at index 1.
  write_dense_array_int_dim_string_attr("", {0}, {1, 1});

  // Size of single offset (sizeof(uint64_t)) dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Attribute is variable non-nullable, so the single offset (uint64_t) of
  // 8 bytes will be max with strings <= 8 bytes writing len 1 data at idx 1.
  write_dense_array_int_dim_string_attr("l", {0}, {1, 1});

  // Size of single offset dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 2 data at idx 1.
  write_dense_array_int_dim_string_attr("lm", {0}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 3 data at idx 1.
  write_dense_array_int_dim_string_attr("lmn", {0}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 4 data at idx 1.
  write_dense_array_int_dim_string_attr("lmno", {0}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 5 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnop", {0}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 6 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnopq", {0}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 7 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnopqr", {0}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 8 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnopqrs", {0}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Still writing 1 item, but now length of variable data greater than the 1
  // offset, writing len 9 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnopqrst", {0}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 9);

  // Writing len 10 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnopqrstu", {0}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 10);

  // Writing len 11 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnopqrstuv", {0}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 11);

  // Writing len 12 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnopqrstuvw", {0}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 12);

  // Writing len 13 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnopqrstuvwx", {0}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 13);

  // Writing len 14 data at idx 1.
  write_dense_array_int_dim_string_attr("lmnopqrstuvwxy", {0}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 14);

  // 4 items starting @ 1: { "a", "", "", ""}.
  write_dense_array_int_dim_string_attr("a", {0, 0, 0, 0}, {1, 4});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 14);

  // 4 items starting @ 1: {"A", "B", "", ""}.
  write_dense_array_int_dim_string_attr("AB", {0, 1, 1, 1}, {1, 4});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 14);

  // 4 items starting @ 3: {"Abc", "Defg", "Hijkl", "nopqr"}.
  write_dense_array_int_dim_string_attr(
      "AbcDefgHijklmNopqr", {0, 3, 7, 13}, {3, 6});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 14);

  // 5 items, max of any individual item is 26 bytes starting @ 1
  // {"ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz", "0123456789",
  //  "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"}.
  write_dense_array_int_dim_string_attr(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0, 26, 52, 62, 88},
      {3, 7});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 26);

  // 1 item, 114 bytes @ idx 1.
  write_dense_array_int_dim_string_attr(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0},  // offsets
      {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // 5 items, max of any individual item is 26 bytes starting @ 3
  // {"ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz", "0123456789",
  //  "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"}.
  write_dense_array_int_dim_string_attr(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0, 26, 52, 62, 88},
      {3, 7});
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // 4 items, various lengths starting @ 3: {"Abc, "Defg", "Hijklm, "Nopqr"}.
  write_dense_array_int_dim_string_attr(
      "AbcDefgHijklmNopqr", {0, 3, 7, 13}, {3, 6});
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // Consolidate.
  tiledb::Config config;
  config.set("sm.consolidation.buffer_size", "10000");
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);

  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // Vacuum and validate size.
  tiledb::Array::vacuum(ctx_, main_array_name_);

  // Earlier item len 114 idx 1 still dominates (even after vacuum, still
  // present).
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // 4 items, various lengths starting @ 3: {"Abc, "Defg", "Hijklm, "Nopqr"}.
  write_dense_array_int_dim_string_attr(
      "AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 4});

  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // Consolidate and validate size.
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);

  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // Vacuum and validate size.
  tiledb::Array::vacuum(ctx_, main_array_name_);

  // Some earlier item(s) len 26 from idx 3..7 now dominate.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 26);

  // 5 items, various lengths starting @ 3: {"Abc, "Defg", "Hijklm, "N",
  // "Opqr"}.
  write_dense_array_int_dim_string_attr(
      "AbcDefgHijklmNOpqr", {0, 3, 7, 13, 14}, {3, 7});

  // Earlier item(s) len 26 idx 3..7 dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 26);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);

  // Earlier item(s) len 26 idx 3..7 dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 26);

  tiledb::Array::vacuum(ctx_, main_array_name_);

  // After vacuum, now down to lesser items, offset sizeof(uint64_t) again
  // dominant.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);
}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, fix dimension, nullable variable "
    "string attribute",
    "[capi][max-tile-size][dense][evolve][consolidate][vacuum]") {
  // Create empty array.
  create_dense_array_int_dim_string_attr(
      {1, 27}, 1, true, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  // No data tiles written, should be zero.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 0);

  // Zero-len data written at index 1.
  write_dense_array_int_dim_string_attr_null("", {0}, {1}, {1, 1});

  // Size of single offset (sizeof(uint64_t)) dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Attribute is variable non-nullable,
  // So the single offset (uint64_t) of 8 bytes will be max with strings <= 8
  // bytes, writing len 1 data at idx 1.
  write_dense_array_int_dim_string_attr_null("l", {0}, {1}, {1, 1});

  // Size of single offset dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 2 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lm", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 3 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmn", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 4 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmno", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 5 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmnop", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 6 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmnopq", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 7 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmnopqr", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Writing len 8 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmnopqrs", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);

  // Still writing 1 item, but now length of variable data greater than the 1
  // offset, writing len 9 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmnopqrst", {0}, {1}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 9);

  // Writing len 10 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmnopqrstu", {0}, {1}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 10);

  // Writing len 11 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmnopqrstuv", {0}, {1}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 11);

  // Writing len 12 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmnopqrstuvw", {0}, {1}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 12);

  // Writing len 13 data at idx 1.
  write_dense_array_int_dim_string_attr_null("lmnopqrstuvwx", {0}, {1}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 13);

  // Writing len 14 data at idx 1.
  write_dense_array_int_dim_string_attr_null(
      "lmnopqrstuvwxy", {0}, {1}, {1, 1});

  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 14);

  // 4 items, starting @ idx 1 {"a", "", "", ""}.
  write_dense_array_int_dim_string_attr_null(
      "a", {0, 0, 0, 0}, {1, 1, 1, 1}, {1, 4});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 14);

  // 4 items,
  // idx 1 "A", idx 2 "B", idx 3 "", idx 4 "".
  write_dense_array_int_dim_string_attr_null(
      "AB", {0, 1, 1, 1}, {1, 1, 1, 1}, {1, 4});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 14);

  // 4 items,
  //  idx 3 "Abc, idx 4 "Defg", idx 5 "Hijkl, idx 6 "nopqr".
  write_dense_array_int_dim_string_attr_null(
      "AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 1, 1, 1}, {3, 6});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 14);

  // 5 items, max of any individual item is 26 bytes starting @ 3:
  // {"ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz", "0123456789",
  //  "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"}.
  write_dense_array_int_dim_string_attr_null(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0, 26, 52, 62, 88},
      {1, 1, 1, 1, 1},
      {3, 7});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 26);

  // 1 item, 114 bytes @ idx 1.
  write_dense_array_int_dim_string_attr_null(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0},
      {1},
      {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // 5 items, max of any individual item is 26 bytes, starting @ 3:
  // {"ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz", "0123456789",
  //  "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"}.
  write_dense_array_int_dim_string_attr_null(
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0, 26, 52, 62, 88},
      {1, 1, 1, 1, 1},
      {3, 7});
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // 4 items, various lengths @ 3: {"Abc", "Defg", "Hijklm", "Nopqr"}.
  write_dense_array_int_dim_string_attr_null(
      "AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 1, 1, 1}, {3, 6});

  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // Consolidat and validate size.
  tiledb::Config config;
  config.set("sm.consolidation.buffer_size", "10000");
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);

  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // Vacuum and validate size.
  tiledb::Array::vacuum(ctx_, main_array_name_);

  // Earlier item len 114 idx 1 still dominates (even after vacuum, still
  // present).
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // 4 items, various lengths, starting @ 3: {"Abc", "Defg", "Hijklm", "Nopqr"}.
  write_dense_array_int_dim_string_attr_null(
      "AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 1, 1, 1}, {1, 4});

  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // Consolidate and validate size.
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);

  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 114);

  // Vacuum and validate size.
  tiledb::Array::vacuum(ctx_, main_array_name_);

  // Some earlier item(s) len 26 from idx 3..7 now dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 26);

  // 5 items, various lengths, starting @ 3: {"Abc", "Defg", "Hijklm", "N",
  // "Opqr"}.
  write_dense_array_int_dim_string_attr_null(
      "AbcDefgHijklmNOpqr", {0, 3, 7, 13, 14}, {1, 1, 1, 1, 1}, {3, 7});

  // Earlier item(s) len 26 idx 3..7 dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 26);

  // Consolidate and validate size.
  tiledb::Array::consolidate(ctx_, main_array_name_, &config);

  // Earlier item(s) len 26 idx 3..7 dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(main_array_name_) == 26);

  // Vacuum and validate size.
  tiledb::Array::vacuum(ctx_, main_array_name_);

  // After vacuum, now down to lesser items, offset sizeof(uint64_t) again
  // dominant.
  CHECK(cpp_get_fragments_max_in_memory_tile_size(main_array_name_) == 8);
}

#ifdef TILEDB_SERIALIZATION
TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max tile size, serialization",
    "[max-tile-size][serialization]") {
  tiledb::sm::Buffer buff;
  std::vector<uint64_t> values_to_try{
      0,
      4,
      208,
      1024,
      64 * 1024 * 1024,
      64 * 1024 * 1024 + 1,
      std::numeric_limits<uint64_t>::max()};

  // Loop through values_to_try serializing/deserializing and comparing
  // to original value.
  for (auto v : values_to_try) {
    CHECK(tiledb::sm::serialization::maximum_tile_size_serialize(
              &v, tiledb::sm::SerializationType::CAPNP, &buff)
              .ok());
    uint64_t retrieved_val = std::numeric_limits<uint64_t>::max();
    CHECK(tiledb::sm::serialization::maximum_tile_size_deserialize(
              &retrieved_val, tiledb::sm::SerializationType::CAPNP, buff)
              .ok());
    CHECK(v == retrieved_val);
  }
}
#endif
