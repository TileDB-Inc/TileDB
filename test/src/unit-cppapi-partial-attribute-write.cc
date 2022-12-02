/**
 * @file   unit-cppapi-partial-attribute-write.cc
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
 * Tests the CPP API for partial attribute write.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;
using namespace tiledb::test;

/** Tests for CPP API partial attribute write. */
struct CppPartialAttrWriteFx {
  // Constants.
  const char* ARRAY_NAME = "test_partial_attr_write_array";

  // TileDB context.
  Context ctx_;
  VFS vfs_;

  // Constructors/destructors.
  CppPartialAttrWriteFx();
  ~CppPartialAttrWriteFx();

  // Functions.
  void create_sparse_array(bool allows_dups = false);
  void create_dense_array();
  void write_sparse(
      tiledb_layout_t layout,
      std::vector<int> a1,
      std::vector<uint64_t> a2,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp);
  void write_dense(
      tiledb_layout_t layout,
      std::vector<int> a1,
      std::vector<uint64_t> a2,
      uint64_t timestamp);
  void read_sparse(
      std::vector<int>& a1,
      std::vector<uint64_t>& a2,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2);

  void remove_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

CppPartialAttrWriteFx::CppPartialAttrWriteFx()
    : vfs_(ctx_) {
  Config config;
  ctx_ = Context(config);
  vfs_ = VFS(ctx_);
}

CppPartialAttrWriteFx::~CppPartialAttrWriteFx() {
}

void CppPartialAttrWriteFx::create_sparse_array(bool allows_dups) {
  // Create dimensions.
  auto d1 = Dimension::create<uint64_t>(ctx_, "d1", {{1, 4}}, 2);
  auto d2 = Dimension::create<uint64_t>(ctx_, "d2", {{1, 4}}, 2);

  // Create domain.
  Domain domain(ctx_);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create attributes.
  auto a1 = Attribute::create<int32_t>(ctx_, "a1");
  auto a2 = Attribute::create<int64_t>(ctx_, "a2");

  // Create array schmea.
  ArraySchema schema(ctx_, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.set_capacity(2);
  schema.add_attributes(a1);
  schema.add_attributes(a2);

  if (allows_dups) {
    schema.set_allows_dups(true);
  }

  // Set up filters.
  Filter filter(ctx_, TILEDB_FILTER_NONE);
  FilterList filter_list(ctx_);
  filter_list.add_filter(filter);
  schema.set_coords_filter_list(filter_list);

  Array::create(ARRAY_NAME, schema);
}

void CppPartialAttrWriteFx::create_dense_array() {
  // Create dimensions.
  auto d1 = Dimension::create<uint64_t>(ctx_, "d1", {{1, 4}}, 2);
  auto d2 = Dimension::create<uint64_t>(ctx_, "d2", {{1, 4}}, 2);

  // Create domain.
  Domain domain(ctx_);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create attributes.
  auto a1 = Attribute::create<int32_t>(ctx_, "a1");
  auto a2 = Attribute::create<int64_t>(ctx_, "a2");

  // Create array schmea.
  ArraySchema schema(ctx_, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.set_capacity(20);
  schema.add_attributes(a1);
  schema.add_attributes(a2);

  // Set up filters.
  Filter filter(ctx_, TILEDB_FILTER_NONE);
  FilterList filter_list(ctx_);
  filter_list.add_filter(filter);
  schema.set_coords_filter_list(filter_list);

  Array::create(ARRAY_NAME, schema);
}

void CppPartialAttrWriteFx::write_sparse(
    tiledb_layout_t layout,
    std::vector<int> a1,
    std::vector<uint64_t> a2,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp) {
  // Open array.
  Array array(ctx_, ARRAY_NAME, TILEDB_WRITE, timestamp);

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  QueryExperimental::allow_partial_attribute_write(ctx_, query);
  query.set_layout(layout);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);
  query.submit();

  dim1.clear();
  dim2.clear();

  query.set_data_buffer("a1", a1);
  query.submit();

  a1.clear();

  query.set_data_buffer("a2", a2);
  query.submit();
  query.finalize();

  // Close array.
  array.close();
}

void CppPartialAttrWriteFx::write_dense(
    tiledb_layout_t layout,
    std::vector<int> a1,
    std::vector<uint64_t> a2,
    uint64_t timestamp) {
  // Open array.
  Array array(ctx_, ARRAY_NAME, TILEDB_WRITE, timestamp);

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  QueryExperimental::allow_partial_attribute_write(ctx_, query);
  query.set_layout(layout);
  query.set_data_buffer("a1", a1);
  query.submit();

  a1.clear();

  query.set_data_buffer("a2", a2);
  query.submit();
  query.finalize();

  // Close array.
  array.close();
}

void CppPartialAttrWriteFx::read_sparse(
    std::vector<int>& a1,
    std::vector<uint64_t>& a2,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2) {
  // Open array.
  Array array(ctx_, ARRAY_NAME, TILEDB_READ);

  // Create query.
  Query query(ctx_, array, TILEDB_READ);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_data_buffer("a1", a1);
  query.set_data_buffer("a2", a2);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);

  // Submit the query.
  query.submit();
  CHECK(query.query_status() == Query::Status::COMPLETE);

  // Close array.
  array.close();
}

void CppPartialAttrWriteFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name)) {
    return;
  }

  vfs_.remove_dir(array_name);
}

void CppPartialAttrWriteFx::remove_array() {
  remove_array(ARRAY_NAME);
}

bool CppPartialAttrWriteFx::is_array(const std::string& array_name) {
  return vfs_.is_dir(array_name);
}

TEST_CASE_METHOD(
    CppPartialAttrWriteFx,
    "CPP API: Test partial attribute write, bad layout",
    "[cppapi][partial-attribute-write][bad-layout]") {
  remove_array();
  create_sparse_array();

  // Write fragment.
  CHECK_THROWS_WITH(
      write_sparse(
          TILEDB_GLOBAL_ORDER,
          {0, 1, 2, 3, 4, 5, 6, 7},
          {8, 9, 10, 11, 12, 13, 14, 15},
          {1, 1, 1, 2, 3, 4, 3, 3},
          {1, 2, 4, 3, 1, 2, 3, 4},
          1),
      "Query: Partial attribute write is only supported for unordered writes.");

  remove_array();
}

TEST_CASE_METHOD(
    CppPartialAttrWriteFx,
    "CPP API: Test partial attribute write, bad dense layout",
    "[cppapi][partial-attribute-write][bad-dense-layout]") {
  remove_array();
  create_dense_array();

  // Write fragment.
  CHECK_THROWS_WITH(
      write_dense(
          TILEDB_ROW_MAJOR,
          {0, 1, 2, 3, 4, 5, 6, 7},
          {8, 9, 10, 11, 12, 13, 14, 15},
          1),
      "Query: Partial attribute write is only supported for unordered writes.");

  // Write fragment.
  CHECK_THROWS_WITH(
      write_dense(
          TILEDB_COL_MAJOR,
          {0, 1, 2, 3, 4, 5, 6, 7},
          {8, 9, 10, 11, 12, 13, 14, 15},
          1),
      "Query: Partial attribute write is only supported for unordered writes.");

  remove_array();
}

TEST_CASE_METHOD(
    CppPartialAttrWriteFx,
    "CPP API: Test partial attribute write, not all dimensions set on first "
    "write",
    "[cppapi][partial-attribute-write][not-all-dims-set]") {
  remove_array();
  create_sparse_array();

  // Open array.
  Array array(ctx_, ARRAY_NAME, TILEDB_WRITE);

  // Create query.
  std::vector<uint64_t> dim1(10);
  Query query(ctx_, array, TILEDB_WRITE);
  QueryExperimental::allow_partial_attribute_write(ctx_, query);
  query.set_layout(TILEDB_UNORDERED);
  query.set_data_buffer("d1", dim1);
  CHECK_THROWS_WITH(query.submit(), "Query: Dimension buffer d2 is not set");

  array.close();

  remove_array();
}

TEST_CASE_METHOD(
    CppPartialAttrWriteFx,
    "CPP API: Test partial attribute write",
    "[cppapi][partial-attribute-write]") {
  remove_array();
  create_sparse_array();

  // Write fragment.
  write_sparse(
      TILEDB_UNORDERED,
      {0, 1, 2, 3, 4, 5, 6, 7},
      {8, 9, 10, 11, 12, 13, 14, 15},
      {1, 1, 1, 2, 3, 4, 3, 3},
      {1, 2, 4, 3, 1, 2, 3, 4},
      1);

  size_t buffer_size = 8;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> a2(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, a2, dim1, dim2);

  CHECK(a1 == std::vector<int>({0, 1, 2, 3, 4, 5, 6, 7}));
  CHECK(a2 == std::vector<uint64_t>({8, 9, 10, 11, 12, 13, 14, 15}));
  CHECK(dim1 == std::vector<uint64_t>({1, 1, 1, 2, 3, 4, 3, 3}));
  CHECK(dim2 == std::vector<uint64_t>({1, 2, 4, 3, 1, 2, 3, 4}));

  remove_array();
}