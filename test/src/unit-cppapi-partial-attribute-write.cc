/**
 * @file   unit-cppapi-partial-attribute-write.cc
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
 * Tests the CPP API for partial attribute write.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

using namespace tiledb;
using namespace tiledb::test;

/** Tests for CPP API partial attribute write. */
struct CppPartialAttrWriteFx {
  VFSTestSetup vfs_test_setup_;

  // TileDB context.
  Context ctx_;
  const std::string array_name_;

  // Constructors/destructors.
  CppPartialAttrWriteFx();

  // Functions.
  void create_sparse_array(bool allows_dups = false);
  void write_sparse_dims(
      tiledb_layout_t layout,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp,
      std::unique_ptr<Array>& array,
      std::unique_ptr<Query>& query);
  void write_sparse_dims_and_a1(
      tiledb_layout_t layout,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      std::vector<int> a1,
      uint64_t timestamp,
      std::unique_ptr<Array>& array,
      std::unique_ptr<Query>& query);
  void write_sparse_a1(Query& query, std::vector<int> a1);
  void write_sparse_a2(Query& query, std::vector<int64_t> a2);
  void read_sparse(
      std::vector<int>& a1,
      std::vector<int64_t>& a2,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2);
};

CppPartialAttrWriteFx::CppPartialAttrWriteFx()
    : array_name_(vfs_test_setup_.array_uri("test_partial_attr_write_array")) {
  Config config;
  config["sm.allow_separate_attribute_writes"] = "true";
  vfs_test_setup_.update_config(config.ptr().get());
  ctx_ = vfs_test_setup_.ctx();
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

  // Create array schema.
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

  Array::create(array_name_, schema);
}

void CppPartialAttrWriteFx::write_sparse_dims(
    tiledb_layout_t layout,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp,
    std::unique_ptr<Array>& array,
    std::unique_ptr<Query>& query) {
  // Open array.
  array = std::make_unique<Array>(
      ctx_,
      array_name_,
      TILEDB_WRITE,
      tiledb::TemporalPolicy(tiledb::TimeTravel, timestamp));

  // Create query.
  query = std::make_unique<Query>(ctx_, *array, TILEDB_WRITE);
  query->set_layout(layout);
  query->set_data_buffer("d1", dim1);
  query->set_data_buffer("d2", dim2);

  query->submit();
}

void CppPartialAttrWriteFx::write_sparse_dims_and_a1(
    tiledb_layout_t layout,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    std::vector<int> a1,
    uint64_t timestamp,
    std::unique_ptr<Array>& array,
    std::unique_ptr<Query>& query) {
  // Open array.
  array = std::make_unique<Array>(
      ctx_,
      array_name_,
      TILEDB_WRITE,
      tiledb::TemporalPolicy(tiledb::TimeTravel, timestamp));

  // Create query.
  query = std::make_unique<Query>(ctx_, *array, TILEDB_WRITE);
  query->set_layout(layout);
  query->set_data_buffer("d1", dim1);
  query->set_data_buffer("d2", dim2);
  query->set_data_buffer("a1", a1);

  query->submit();
}

void CppPartialAttrWriteFx::write_sparse_a1(Query& query, std::vector<int> a1) {
  query.set_data_buffer("a1", a1);

  query.submit();
}

void CppPartialAttrWriteFx::write_sparse_a2(
    Query& query, std::vector<int64_t> a2) {
  query.set_data_buffer("a2", a2);
  query.submit();
}

void CppPartialAttrWriteFx::read_sparse(
    std::vector<int>& a1,
    std::vector<int64_t>& a2,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2) {
  // Open array.
  Array array(ctx_, array_name_, TILEDB_READ);

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

TEST_CASE_METHOD(
    CppPartialAttrWriteFx,
    "CPP API: Test partial attribute write, not all dimensions set on first "
    "write",
    "[cppapi][partial-attribute-write][not-all-dims-set]") {
  create_sparse_array();

  // Open array.
  Array array(ctx_, array_name_, TILEDB_WRITE);

  // Create query.
  std::vector<uint64_t> dim1(10);
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_UNORDERED);
  query.set_data_buffer("d1", dim1);
  CHECK_THROWS_WITH(
      query.submit(),
      "Query: [check_buffer_names] Dimension buffer d2 is not set");

  array.close();
}

TEST_CASE_METHOD(
    CppPartialAttrWriteFx,
    "CPP API: Test partial attribute write, basic test",
    "[cppapi][partial-attribute-write][basic][rest]") {
  create_sparse_array();

  // Write fragment, seperating dimensions and attributes.
  std::unique_ptr<Array> array = nullptr;
  std::unique_ptr<Query> query = nullptr;
  write_sparse_dims(
      TILEDB_UNORDERED,
      {1, 1, 1, 2, 3, 4, 3, 3},
      {1, 2, 4, 3, 1, 2, 3, 4},
      1,
      array,
      query);
  write_sparse_a1(*query, {0, 1, 2, 3, 4, 5, 6, 7});
  write_sparse_a2(*query, {8, 9, 10, 11, 12, 13, 14, 15});
  query->finalize();
  array->close();

  size_t buffer_size = 8;
  std::vector<int> a1(buffer_size);
  std::vector<int64_t> a2(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, a2, dim1, dim2);

  CHECK(a1 == std::vector<int>({0, 1, 2, 3, 4, 5, 6, 7}));
  CHECK(a2 == std::vector<int64_t>({8, 9, 10, 11, 12, 13, 14, 15}));
  CHECK(dim1 == std::vector<uint64_t>({1, 1, 1, 2, 3, 4, 3, 3}));
  CHECK(dim2 == std::vector<uint64_t>({1, 2, 4, 3, 1, 2, 3, 4}));
}

TEST_CASE_METHOD(
    CppPartialAttrWriteFx,
    "CPP API: Test partial attribute write, basic test 2",
    "[cppapi][partial-attribute-write][basic2][rest]") {
  create_sparse_array();

  // Write fragment, seperating dimensions and attributes.
  std::unique_ptr<Array> array = nullptr;
  std::unique_ptr<Query> query = nullptr;
  write_sparse_dims_and_a1(
      TILEDB_UNORDERED,
      {1, 1, 1, 2, 3, 4, 3, 3},
      {1, 2, 4, 3, 1, 2, 3, 4},
      {0, 1, 2, 3, 4, 5, 6, 7},
      1,
      array,
      query);
  write_sparse_a2(*query, {8, 9, 10, 11, 12, 13, 14, 15});
  query->finalize();
  array->close();

  size_t buffer_size = 8;
  std::vector<int> a1(buffer_size);
  std::vector<int64_t> a2(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, a2, dim1, dim2);

  CHECK(a1 == std::vector<int>({0, 1, 2, 3, 4, 5, 6, 7}));
  CHECK(a2 == std::vector<int64_t>({8, 9, 10, 11, 12, 13, 14, 15}));
  CHECK(dim1 == std::vector<uint64_t>({1, 1, 1, 2, 3, 4, 3, 3}));
  CHECK(dim2 == std::vector<uint64_t>({1, 2, 4, 3, 1, 2, 3, 4}));
}

TEST_CASE_METHOD(
    CppPartialAttrWriteFx,
    "CPP API: Test partial attribute write, rewrite",
    "[cppapi][partial-attribute-write][rewrite][rest]") {
  create_sparse_array();

  // Write fragment.
  std::unique_ptr<Array> array = nullptr;
  std::unique_ptr<Query> query = nullptr;
  write_sparse_dims(
      TILEDB_UNORDERED,
      {1, 1, 1, 2, 3, 4, 3, 3},
      {1, 2, 4, 3, 1, 2, 3, 4},
      1,
      array,
      query);
  write_sparse_a1(*query, {0, 1, 2, 3, 4, 5, 6, 7});

  // Try to rewrite an attribute, will throw an exception.
  CHECK_THROWS_WITH(
      write_sparse_a1(*query, {8, 9, 10, 11, 12, 13, 14, 15}),
      "[TileDB::Query] Error: Buffer a1 was already written");

  write_sparse_a2(*query, {8, 9, 10, 11, 12, 13, 14, 15});

  query->finalize();
  array->close();

  size_t buffer_size = 8;
  std::vector<int> a1(buffer_size);
  std::vector<int64_t> a2(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, a2, dim1, dim2);

  CHECK(a1 == std::vector<int>({0, 1, 2, 3, 4, 5, 6, 7}));
  CHECK(a2 == std::vector<int64_t>({8, 9, 10, 11, 12, 13, 14, 15}));
  CHECK(dim1 == std::vector<uint64_t>({1, 1, 1, 2, 3, 4, 3, 3}));
  CHECK(dim2 == std::vector<uint64_t>({1, 2, 4, 3, 1, 2, 3, 4}));
}

TEST_CASE_METHOD(
    CppPartialAttrWriteFx,
    "CPP API: Test partial attribute write, missing attributes",
    "[cppapi][partial-attribute-write][missing-attributes][rest]") {
  create_sparse_array();

  // Write fragment, seperating dimensions and attributes.
  std::unique_ptr<Array> array = nullptr;
  std::unique_ptr<Query> query = nullptr;
  write_sparse_dims(
      TILEDB_UNORDERED,
      {1, 1, 1, 2, 3, 4, 3, 3},
      {1, 2, 4, 3, 1, 2, 3, 4},
      1,
      array,
      query);
  write_sparse_a1(*query, {0, 1, 2, 3, 4, 5, 6, 7});
  if (vfs_test_setup_.is_rest()) {
    CHECK_THROWS(query->finalize());
  } else {
    CHECK_THROWS_WITH(
        query->finalize(), "UnorderWriter: Not all buffers already written");
  }
  array->close();

  size_t buffer_size = 8;
  std::vector<int> a1(buffer_size, 0);
  std::vector<int64_t> a2(buffer_size, 0);
  std::vector<uint64_t> dim1(buffer_size, 0);
  std::vector<uint64_t> dim2(buffer_size, 0);
  read_sparse(a1, a2, dim1, dim2);

  CHECK(a1 == std::vector<int>({0, 0, 0, 0, 0, 0, 0, 0}));
  CHECK(a2 == std::vector<int64_t>({0, 0, 0, 0, 0, 0, 0, 0}));
  CHECK(dim1 == std::vector<uint64_t>({0, 0, 0, 0, 0, 0, 0, 0}));
  CHECK(dim2 == std::vector<uint64_t>({0, 0, 0, 0, 0, 0, 0, 0}));
}
