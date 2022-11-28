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
 * Tests the C/CPP API for obtaining maximum tile size across all fragments of an array.
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
  const std::string main_array_name_ = "max_tile_size";

  bool showing_data_ = false;

  MaxTileSizeFx()
    : vfs_(ctx_) {
  }

  void remove_temp_dir(const std::string& path) {
    if (vfs_.is_dir(path.c_str())) {
      vfs_.remove_dir(path.c_str());
    }
  };

  /*
   * Array creation, Write, routine naming scheme as follows:
   * 
   * [(as useful) template <dim/attr types for most expected reuseability flexibility>]
   * create_[sparse|dense]_[dimensionality]_[series-of-dim-descriptions]_[series-of-attribute-descriptions]
   * 
   * The word 'array' was specifically left out as it is reasonably implied from dense/sparse, and 
   * would seem to simply add additional verbiage to be parsed without providing any additional
   * enlightenment.
   * 
   * [dimesionality] could be omitted, but did seem to provide likely value that otherwise would
   * require the (mental) parsing of the series-of-dim-descriptions to determine the dimensionality.
   * 
   * The series-of-dim-descriptions and series-of-attr-descriptions currently use dN and aN to
   * (i) identifiy which dimension/attribute is being described and its order of creation
   * as well as (ii) identifying the specific names of the dimension/attribute being created.  
   * Those names could be substituted with something else ("row"/"col", "volume") but would 
   * lose some of the at-a-glance numbered ordering of the dimension/attribute creation.
   * 
   * The write routines will be similarly named with at least one 'write' match for each of 
   * the 'create' routines.
   */

  template <typename a1_type>
  void create_sparse_1d_d1_var_a1_fix(
      std::string array_name
      ) {
    // Set up the Domain/Dimension items for the array being created.
    tiledb::Domain domain(ctx_);
    domain.add_dimension(tiledb::Dimension::create(
        ctx_, "d1", TILEDB_STRING_ASCII, nullptr, nullptr));

    // The array will be sparse.
    tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(domain);

    // Add a single attribute "a1" so each (i,j) cell can store an integer.
    schema.add_attribute(tiledb::Attribute::create<a1_type>(ctx_, "a1"));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  }

  template <typename a1_type>
  void write_sparse_d1_var_a1_fix_fabricate(std::string array_name, uint64_t num_rows) {
    // Prepare the data to be written.
    std::vector<a1_type> a_buff(num_rows);
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
    tiledb::Array array(ctx_, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx_, array, TILEDB_WRITE);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a1", a_buff)
        .set_data_buffer("d1", d1_var)
        .set_offsets_buffer("d1", d1_offsets);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  template <typename dim1_type, typename a1_type>
  void create_dense_1d_d1_fix_a1_var(
      std::string array_name,
      std::array<dim1_type, 2> d1_domain,
      int d1_extents,
      bool a1_is_nullable,
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order
  ) {
    // Set up the Domain/Dimension items for the array being created.
    tiledb::Domain domain(ctx_);
    domain.add_dimension(tiledb::Dimension::create<dim1_type>(
        ctx_, "d1", d1_domain, d1_extents));

    // The array will be dense with the indicated order(s).
    tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(domain).set_tile_order(tile_order);
    schema.set_domain(domain).set_cell_order(cell_order);
    // Specify attribute, nullable or not as indicated.
    auto a1_attr = tiledb::Attribute::create<a1_type>(ctx_, "a1");
    if (a1_is_nullable) {
      a1_attr.set_nullable(true);
    }
    schema.add_attribute(a1_attr);

    // Create the array.
    tiledb::Array::create(array_name, schema);
  }

  void write_dense_1d_fix_a1_var_null(
      std::string array_name,
      std::string&& attr_data,
      std::vector<uint64_t> && attr_offsets,
      std::vector<uint8_t> && attr_validity,
      std::vector<int> && subrange) {
    // Define needed objects.
    tiledb::Array array(ctx_, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx_, array);
    tiledb::Subarray subarray(ctx_, array);

    // Initialize the objects, preparing the query.
    subarray.add_range(0, subrange[0], subrange[1]);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", attr_data)
        .set_offsets_buffer("a1", attr_offsets)
        .set_validity_buffer("a1", attr_validity)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  void write_dense_array_1d_fix_a1_var(
      std::string array_name,
      std::string&& attr_data,
      std::vector<uint64_t>&& attr_offsets,
      std::vector<int>&& subrange) {
    // Define needed objects.
    tiledb::Array array(ctx_, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx_, array);
    tiledb::Subarray subarray(ctx_, array);
 
    // Initialize the objects, preparing the query.
    subarray.add_range(0, subrange[0], subrange[1]);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", attr_data)
        .set_offsets_buffer("a1", attr_offsets)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  template <typename dim1_type, typename dim2_type, typename a1_type>
  void create_dense_2d_d1_fix_d2_fix_a1_fix(
      std::string array_name,
      std::array<dim1_type, 2> d1_domain,
      int d1_extents,
      std::array<dim2_type, 2> d2_domain,
      int d2_extents,
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order) {
    // Set up the Domain/Dimension items for the array being created.
    tiledb::Domain domain(ctx_);
    domain
        .add_dimension(tiledb::Dimension::create<dim1_type>(
            ctx_, "d1", d1_domain, d1_extents))
        .add_dimension(tiledb::Dimension::create<dim2_type>(
            ctx_, "d2", d2_domain, d2_extents));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(domain)
        .set_tile_order(tile_order)
        .set_cell_order(cell_order);

    // Add a single attribute "a1" so each (i,j) cell can store an integer.
    schema.add_attribute(tiledb::Attribute::create<a1_type>(ctx_, "a1"));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  }

  template <typename d1_type, typename a1_type>
  void create_dense_1d_d1_var_a1_fix(
    const std::string& array_name,
      std::array<d1_type, 2> d1_domain,
      int d1_extents,
      const std::string&& attr_name) {
    remove_temp_dir(array_name.c_str());

    // The array will be 1d with dimension "d1", with domain, extents
    // as passed.
    tiledb::Domain domain(ctx_);
    domain.add_dimension(
        tiledb::Dimension::create<d1_type>(ctx_, "d1", d1_domain, d1_extents));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx_, TILEDB_DENSE);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    // Add one attribute "a1", so each (i) cell can store
    // a nchars characters on "attr_name" (std::array<char,nchars> now via a1_type).
    schema.add_attribute(
        tiledb::Attribute::create<a1_type>(ctx_, attr_name));

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  };

  void array_schema_evolve_A(const std::string& array_name) {
    // Targeted against schema created in
    // create_dense_1d_d1_var_a1_fix.
    tiledb::ArraySchemaEvolution schemaEvolution =
        tiledb::ArraySchemaEvolution(ctx_);

    // Add attribute b.
    tiledb::Attribute b =
        tiledb::Attribute::create<std::array<char, 257>>(ctx_, "b257");
    schemaEvolution.add_attribute(b);

    schemaEvolution.drop_attribute(std::string("a2"));

    // Evolve array.
    schemaEvolution.array_evolve(array_name);
  };

  void array_schema_evolve_B(const std::string& array_name) {
    // Targeted against schema created in
    // create_dense_1d_d1_var_a1_fix.

    tiledb::ArraySchemaEvolution schemaEvolution =
        tiledb::ArraySchemaEvolution(ctx_);

    // Add attribute c.
    tiledb::Attribute c =
        tiledb::Attribute::create<std::array<char, 42>>(ctx_, "c42");
    schemaEvolution.add_attribute(c);

    schemaEvolution.drop_attribute(std::string("b257"));

    // Evolve array.
    schemaEvolution.array_evolve(array_name);
  }

  template <typename a1_type>
  void create_sparse_1d_d1_var_a1_var(
      std::string array_name,
      bool is_nullable) {
    // Set up the Domain/Dimension items for the array being created.
    tiledb::Domain domain(ctx_);
    domain.add_dimension(tiledb::Dimension::create(
        ctx_, "d1", TILEDB_STRING_ASCII, nullptr, nullptr));

    // The array will be dense.
    tiledb::ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(domain);

    // Add one attribute "a1", so each (i) cell can store
    // a character on "a1".
    auto attr = tiledb::Attribute::create<a1_type>(ctx_, "a1");
    if (is_nullable) {
      attr.set_nullable(true);
    }
    schema.add_attribute(attr);

    // Create the (empty) array on disk.
    tiledb::Array::create(array_name, schema);
  }

  template <const int nchars = 1>
  void write_array_attr(const std::string& array_name, const std::string&& attr_name) {
    // Prepare some data for the array.
    std::vector<std::array<char, nchars>> a;
    for (auto i = 0; i < 4; ++i) {
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

  void write_sparse_1d_d1_var_a1_var(
      std::string array_name,
      std::string& dim_data,  // Expecting single-char values, one for each row.
      std::vector<uint64_t>&& dim_offsets,
      std::string&& attr_data // Expecting single-char values, one for each
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
    // attr_val also needs a non empty buffer and ...
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
    tiledb::Array array(ctx_, array_name, TILEDB_WRITE);
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

  template <typename dimtype, typename a1_type>
  void write_array_2_dims(
      const std::string array_name,
      std::vector<a1_type> data,
      std::array<dimtype, 2> dim1_range,
      std::array<dimtype, 2> dim2_range) {
    // Open the array for writing and create the query.
    tiledb::Array array(ctx_, array_name, TILEDB_WRITE);
    tiledb::Subarray subarray(ctx_, array);
    subarray.add_range(0, dim1_range[0], dim1_range[1])
        .add_range(1, dim2_range[0], dim2_range[1]);
    tiledb::Query query(ctx_, array);
    query.set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", data)
        .set_subarray(subarray);

    // Perform the write and close the array.
    query.submit();
    array.close();
  }

  uint64_t c_get_fragments_max_in_memory_tile_size(
      const std::string& array_uri) {
    uint64_t max_in_memory_tile_size = 0;
    CHECK(
        tiledb_array_maximum_tile_size(
            &*ctx_.ptr(),
            array_uri.c_str(),
            &max_in_memory_tile_size, 
            nullptr) == TILEDB_OK);

    if (showing_data_) {
      std::cout << "maximum in memory tile size "
                << max_in_memory_tile_size
                << std::endl;
    }
    return max_in_memory_tile_size;
  }

  uint64_t cpp_get_fragments_max_in_memory_tile_size(const std::string& array_uri) {
    tiledb::Array array(ctx_, array_uri, TILEDB_READ);
    uint64_t max_in_memory_tile_size = 0;
    array.get_max_in_memory_tile_size(&max_in_memory_tile_size);

    if (showing_data_) {
      std::cout << "maximum in memory tile size " << max_in_memory_tile_size
                << std::endl;
    }
    return max_in_memory_tile_size;
  }

  tiledb::Context ctx_;
  tiledb::VFS vfs_;
};  // struct MaxTileSizeFx

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max tile size, dense, fixed, with consolidation",
    "[capi][cppapi][max-tile-size][dense][consolidate]") {
  // Name of array.
  auto& array_name = main_array_name_;
  remove_temp_dir(array_name.c_str());

  uint64_t capi_max = 0; // init to avoid msvc warning as error miss inside CHECK()
  uint64_t cppapi_max;

  // Create array
  create_dense_2d_d1_fix_d2_fix_a1_fix<int, int, int32_t>(
      array_name, {1, 4}, 2, {1, 4}, 2, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
      capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max=cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 0 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Write first fragment and validate sizes.
  write_array_2_dims<int, int32_t>(array_name, {1, 2, 3, 4, 5, 6, 6, 8}, {1, 2}, {1, 4});
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Write second fragment and validate sizes.
  write_array_2_dims<int, int32_t>(
      array_name, {101, 102, 103, 104}, {2, 3}, {2, 3});
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Write third fragment and validate sizes.
  write_array_2_dims<int, int32_t>(array_name, {201}, {1, 1}, {1, 1});
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Write fourth fragment and validate sizes.
  write_array_2_dims<int, int32_t>(array_name, {202}, {3, 3}, {4, 4});
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max=cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Consolidate and validate sizes.
  tiledb::Array::consolidate(ctx_, array_name);

  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max=cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 4 * sizeof(int32_t));
  CHECK(cppapi_max == capi_max);

  // Vacuum.
  tiledb::Array::vacuum(ctx_, array_name);

  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 4 * sizeof(int32_t));
}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, variable string dimension, fix attr",
    "[capi][max-tile-size][sparse][var-dimension]") {
  auto& array_name = main_array_name_;

  remove_temp_dir(main_array_name_);

  // Init to avoid msvc warning as error miss inside CHECK().
  uint64_t capi_max = 0;  
  uint64_t cppapi_max;
  create_sparse_1d_d1_var_a1_fix<int32_t>(array_name);
  write_sparse_d1_var_a1_fix_fabricate<int32_t>(array_name, 1);

  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 8);
  CHECK(cppapi_max == capi_max);
  write_sparse_d1_var_a1_fix_fabricate<int32_t>(array_name, 2);

  // Size of data and the single offset to start of extent same.
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 16);
  CHECK(cppapi_max == capi_max);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, array_name);
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);

  // Now '24' after consolidate(), data tiles pick up extra
  // overhead to support time traveling.
  CHECK(capi_max == 24);
  CHECK(cppapi_max == capi_max);

  write_sparse_d1_var_a1_fix_fabricate<int32_t>(array_name, 3);
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 24);
  CHECK(cppapi_max == capi_max);
  write_sparse_d1_var_a1_fix_fabricate<int32_t>(array_name, 1);
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);
  CHECK(capi_max == 24);
  CHECK(cppapi_max == capi_max);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, array_name);
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);

  // Changed, data tiles pick up extra overhead to support time traveling.
  CHECK(capi_max == 56);
  CHECK(cppapi_max == capi_max);

  // Sparse/dense vacuum/consolidate semantics differ, sparse retains 
  //  any old data and time travel overhead.
  // Vacuum.
  tiledb::Array::vacuum(ctx_, array_name);
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);

  // No change, vacuuming sparse does not remove the earlier data.
  CHECK(capi_max == 56);
  CHECK(cppapi_max == capi_max);

  // Secondary attempt still retains the overhead.
  tiledb::Array::consolidate(ctx_, array_name);
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);

  // No change after consolidate(), extra
  // overhead retained across multiple consolidate()s.
  CHECK(capi_max == 56);
  CHECK(cppapi_max == capi_max);

  // Vacuum.
  tiledb::Array::vacuum(ctx_, array_name);
  capi_max = c_get_fragments_max_in_memory_tile_size(array_name);
  cppapi_max = cpp_get_fragments_max_in_memory_tile_size(array_name);

  // No change, vacuuming sparse does not remove the earlier data, even after a
  // secondary consolidate against previously vacuumed data.
  CHECK(capi_max == 56);
  CHECK(cppapi_max == capi_max);
}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, dense create/evolve, fixed dim/attr",
    "[capi][max-tile-size][dense][evolve]") {
  auto& array_name = main_array_name_;

  remove_temp_dir(array_name);

  SECTION(" - Create") {
    create_dense_1d_d1_var_a1_fix<int, std::array<char, 2>>(
        array_name, { 1, 4 }, 1, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);
    write_array_attr<2>(array_name, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 4);

    remove_temp_dir(array_name);
    create_dense_1d_d1_var_a1_fix<int, std::array<char, 257>>(
        array_name, {1, 4}, 1, "b257");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);
    write_array_attr<257>(array_name, "b257");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);

    remove_temp_dir(array_name);
    create_dense_1d_d1_var_a1_fix<int, std::array<char, 42>>(
        array_name, {1, 4}, 1, "c42");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);
    write_array_attr<42>(array_name, "c42");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 42);
  }
  SECTION(" - Evolve") {
    create_dense_1d_d1_var_a1_fix<int, std::array<char, 2>>(
        array_name, {1, 4}, 1, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);
    write_array_attr<2>(array_name, "a2");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 4);

    array_schema_evolve_A(array_name);
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 4);
    write_array_attr<257>(array_name, "b257");
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);

    array_schema_evolve_B(array_name);
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);
    write_array_attr<42>(array_name, "c42");
    // Earlier fragment should still have value of 257.
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);

    // Now want to consolidate, but not vacuum, max
    // should still be 257.
    tiledb::Array::consolidate(ctx_, array_name);
    // After consolidation, old fragment should still be there with 257.
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 257);

    // Sparse/dense vacuum/consolidate semantics differ, 
    //  vacuum dense eliminates old data
    // Vacuum, now max should become 42.
    tiledb::Array::vacuum(ctx_, array_name);
    CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 42);
  }
}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, variable string dimension, variable string attribute",
    "[capi][max-tile-size][sparse][evolve]") {
  std::string array_name("tile_size_array");

  remove_temp_dir(array_name);

  create_sparse_1d_d1_var_a1_var<std::string>(
      array_name,
      true     // bool is_nullable
  );

  std::string basic_key_data{"abcdefghijklmnopqrstuvwxyz"};

  write_sparse_1d_d1_var_a1_var(
      array_name,
      basic_key_data,  // dim_data - expecting single-char values, one for each
                       // row.
      {0},             // std::vector<uint64_t> && dim_offsets,
      // attr_data - expecting single-char values, one for each
      // d1 element, may be empty.. size to be dim_data.size()-1.
      ""               // std::string && attr_data,
  );


  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // Writing 26 empty items.
  write_sparse_1d_d1_var_a1_var(
      array_name,
      basic_key_data,
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      "");
  // 208 / 16 == 8 (bytes per offset, offsets dominate over data size)
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 208);

  // Writing 24 empty items, 2 occupied items.
  write_sparse_1d_d1_var_a1_var(
      array_name,
      basic_key_data,
      {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
       13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
      "AB");

  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 208);

}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, fix dimension, variable "
    "string attribute",
    "[capi][max-tile-size][dense][evolve]") {
  std::string array_name("tile_size_array");

  remove_temp_dir(array_name);

  create_dense_1d_d1_fix_a1_var<int, std::string>(
      array_name,        // std::string array_name,
      {1, 27},           // std::array<dim1_type,2> d1_domain,
      1,                 // int d1_extents,
      false,             // bool a1_is_nullable,
      TILEDB_ROW_MAJOR,  // tiledb_layout_t tile_order,
      TILEDB_ROW_MAJOR   // tiledb_layout_t cell_order
  );
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);

  // Zero-len data written
  // at index 1.
  write_dense_array_1d_fix_a1_var(
      array_name,
      "",     // std::string && attr_data,
      {0},    // std::vector<uint64_t> && attr_offsets,
      {1, 1}  // std::vector<int> && subrange
  );
  // Size of single offset (sizeof(uint64_t)) dominates.
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // Attribute is variable non-nullable, 
  // So the single offset (uint64_t) of 8 bytes will be max with strings <= 8 bytes
  // writing len 1 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "l", {0}, {1, 1});
  // Size of single offset dominates.
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // Writing len 2 data at idx1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lm", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 3 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmn", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 4 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmno", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 5 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnop", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 6 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnopq", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 7 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnopqr", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 8 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnopqrs", {0}, {1, 1});
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // Still writing 1 item, but now length of variable 
  // data greater than the 1 offset,
  // writing len 9 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnopqrst", {0}, {1, 1});
  // >ength of data dominates.
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 9);
  // Writing len 10 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnopqrstu", {0}, {1, 1});
  // Length of data dominates.
  CHECK( c_get_fragments_max_in_memory_tile_size(array_name) == 10);
  // Writing len 11 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnopqrstuv", {0}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 11);
  // Writing len 12 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnopqrstuvw", {0}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 12);
  // Writing len 13 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnopqrstuvwx", {0}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 13);
  // Writing len 14 data at idx 1.
  write_dense_array_1d_fix_a1_var(
      array_name, "lmnopqrstuvwxy", {0}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items, 
  // idx 1 "a", idx 2 "", idx 3 "", idx 4 "".
  write_dense_array_1d_fix_a1_var(
      array_name,
      "a",
      {0, 0, 0, 0},
      {1, 4});
  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items, 
  // idx 1 "A", idx 2 "B", idx 3 "", idx 4 "".
  write_dense_array_1d_fix_a1_var(
      array_name,
      "AB",
      {0, 1, 1, 1},
      {1, 4});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  //4 items, 
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijkl, idx 6 "nopqr".
  write_dense_array_1d_fix_a1_var(
      array_name,
      "AbcDefgHijklmNopqr",
      {0, 3, 7, 13},
      {3,6});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 5 items, max of any individual item is 26 bytes
  // 26 @ idx 3, 26 @idx 4, 10 @ idx 5, 26 @ idx 6, 26 @ id x7.
  write_dense_array_1d_fix_a1_var(
      array_name,
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0, 26, 52, 62, 88},
      {3, 7});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  write_dense_array_1d_fix_a1_var(
      array_name,
      // 1 item, 114 bytes @ idx 1.
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0}, // offsets
      {1,1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 5 items, max of any individual item is 26 bytes
  // 26 @ idx 3, 26 @idx 4, 10 @ idx 5, 26 @ idx 6, 26 @ id x7.
  write_dense_array_1d_fix_a1_var(
      array_name,
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0, 26, 52, 62, 88},
      {3, 7});
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 4 items, various lengths
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "Nopqr".
  write_dense_array_1d_fix_a1_var(
      array_name,
      "AbcDefgHijklmNopqr",
      {0, 3, 7, 13},
      {3, 6});
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, array_name);
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  tiledb::Array::vacuum(ctx_, array_name);
  // Earlier item len 114 idx 1 still dominates (even after vacuum, still present).
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 4 items, various lengths
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "Nopqr".
  write_dense_array_1d_fix_a1_var(
      array_name,
      "AbcDefgHijklmNopqr",
      {0, 3, 7, 13},
      {1, 4});
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, array_name);
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  tiledb::Array::vacuum(ctx_, array_name);
  // Some earlier item(s) len 26 from idx 3..7 now dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // 4 items, various lengths
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "N", idx 7 "Opqr".
  write_dense_array_1d_fix_a1_var(
      array_name,
      "AbcDefgHijklmNOpqr",
      {0, 3, 7, 13, 14},
      {3, 7});
  // Earlier item(s) len 26 idx 3..7 dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, array_name);
  // Earlier item(s) len 26 idx 3..7 dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  tiledb::Array::vacuum(ctx_, array_name);
  // After vacuum, now down to lesser items, offset sizeof(uint64_t) again dominant.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
}

TEST_CASE_METHOD(
    MaxTileSizeFx,
    "C++ API: Max fragment size, sparse, fix dimension, nullable variable "
    "string attribute",
    "[capi][max-tile-size][dense][evolve]") {
  std::string array_name("tile_size_array");

  remove_temp_dir(array_name);

  create_dense_1d_d1_fix_a1_var<int, std::string>(
      array_name,        // std::string array_name,
      {1, 27},           // std::array<dim1_type,2> d1_domain,
      1,                 // int d1_extents,
      true,              // bool a1_is_nullable,
      TILEDB_ROW_MAJOR,  // tiledb_layout_t tile_order,
      TILEDB_ROW_MAJOR   // tiledb_layout_t cell_order
  );
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 0);

  // Zero-len data written at index 1.
  write_dense_1d_fix_a1_var_null(
      array_name,
      "",     // std::string && attr_data,
      {0},    // std::vector<uint64_t> && attr_offsets,
      {1},    // std::vector<uint8_t> && attr_val,
      {1, 1}  // std::vector<int> && subrange
  );
  // Size of single offset (sizeof(uint64_t)) dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // Attribute is variable non-nullable,
  // So the single offset (uint64_t) of 8 bytes will be max with strings <= 8
  // bytes writing len 1 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name,
      "l",    // std::string && attr_data,
      {0},    // std::vector<uint64_t> && attr_offsets,
      {1},    // std::vector<uint8_t> && attr_val,
      {1, 1}  // std::vector<int> && subrange
  );
  // Size of single offset dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // Writing len 2 data at idx1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lm", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 3 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmn", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 4 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmno", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 5 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnop", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 6 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnopq", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 7 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnopqr", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);
  // Writing len 8 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnopqrs", {0}, {1}, {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 8);

  // Still writing 1 item, but now length of variable
  // data greater than the 1 offset
  // writing len 9 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnopqrst", {0}, {1}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 9);
  // Writing len 10 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnopqrstu", {0}, {1}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 10);
  // Writing len 11 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnopqrstuv", {0}, {1}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 11);
  // Writing len 12 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnopqrstuvw", {0}, {1}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 12);
  // Writing len 13 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnopqrstuvwx", {0}, {1}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 13);
  // Writing len 14 data at idx 1.
  write_dense_1d_fix_a1_var_null(
      array_name, "lmnopqrstuvwxy", {0}, {1}, {1, 1});
  // Length of data dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items,
  // idx 1 "a", idx 2 "", idx 3 "", idx 4 "".
  write_dense_1d_fix_a1_var_null(
      array_name, "a", {0, 0, 0, 0}, {1, 1, 1, 1}, {1, 4});
  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items,
  // idx 1 "A", idx 2 "B", idx 3 "", idx 4 "".
  write_dense_1d_fix_a1_var_null(
      array_name, "AB", {0, 1, 1, 1}, {1, 1, 1, 1}, {1, 4});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 4 items,
  //  idx 3 "Abc, idx 4 "Defg", idx 5 "Hijkl, idx 6 "nopqr".
  write_dense_1d_fix_a1_var_null(
      array_name, "AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 1, 1, 1}, {3, 6});

  // Earlier item len 14 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 14);

  // 5 items, max of any individual item is 26 bytes
  // 26 @ idx 3, 26 @idx 4, 10 @ idx 5, 26 @ idx 6, 26 @ id x7.
  write_dense_1d_fix_a1_var_null(
      array_name,
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0, 26, 52, 62, 88},
      {1, 1, 1, 1, 1},
      {3, 7});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  write_dense_1d_fix_a1_var_null(
      array_name,
      // 1 item, 114 bytes @ idx 1.
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      {0},  // offsets
      {1},
      {1, 1});
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 5 items, max of any individual item is 26 bytes,
  // 26 @ idx 3, 26 @idx 4, 10 @ idx 5, 26 @ idx 6, 26 @ id x7.
  write_dense_1d_fix_a1_var_null(
      array_name,
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ"
      "KLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
      //{0, 26, 52, 62, 88, 114},
      {0, 26, 52, 62, 88},
      {1, 1, 1, 1, 1},
      {3, 7});
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 4 items, various lengths,
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "Nopqr".
  write_dense_1d_fix_a1_var_null(
      array_name, "AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 1, 1, 1}, {3, 6});
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, array_name);
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  tiledb::Array::vacuum(ctx_, array_name);
  // Earlier item len 114 idx 1 still dominates (even after vacuum, still
  // present).
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // 4 items, various lengths,
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "Nopqr".
  write_dense_1d_fix_a1_var_null(
      array_name, "AbcDefgHijklmNopqr", {0, 3, 7, 13}, {1, 1, 1, 1}, {1, 4});
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, array_name);
  // Earlier item len 114 idx 1 still dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 114);

  tiledb::Array::vacuum(ctx_, array_name);
  // Some earlier item(s) len 26 from idx 3..7 now dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // 4 items, various lengths,
  // idx 3 "Abc, idx 4 "Defg", idx 5 "Hijklm, idx 6 "N", idx 7 "Opqr".
  write_dense_1d_fix_a1_var_null(
      array_name,
      "AbcDefgHijklmNOpqr",
      {0, 3, 7, 13, 14},
      {1, 1, 1, 1, 1},
      {3, 7});
  // Earlier item(s) len 26 idx 3..7 dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  // Consolidate.
  tiledb::Array::consolidate(ctx_, array_name);
  // Earlier item(s) len 26 idx 3..7 dominates.
  CHECK(c_get_fragments_max_in_memory_tile_size(array_name) == 26);

  tiledb::Array::vacuum(ctx_, array_name);
  // After vacuum, now down to lesser items, offset sizeof(uint64_t) again
  // dominant.
  CHECK(cpp_get_fragments_max_in_memory_tile_size(array_name) == 8);
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