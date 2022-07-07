/**
 * @file   unit-cppapi-deletes.cc
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
 * Tests the CPP API for deletes.
 */

#include "catch.hpp"
#include "test/src/ast_helpers.h"
#include "test/src/helpers.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

using namespace tiledb;
using namespace tiledb::test;

/** Tests for C API deletes. */
struct DeletesFx {
  // Constants.
  const char* SPARSE_ARRAY_NAME = "test_deletes_array";

  // TileDB context.
  Context ctx_;
  VFS vfs_;
  sm::StorageManager* sm_;

  std::string key_ = "0123456789abcdeF0123456789abcdeF";
  const tiledb_encryption_type_t enc_type_ = TILEDB_AES_256_GCM;

  // Constructors/destructors.
  DeletesFx();
  ~DeletesFx();

  // Functions.
  void set_legacy();
  void create_sparse_array(bool allows_dups = false, bool encrypt = false);
  void write_sparse(
      std::vector<int> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp,
      bool encrypt = false);
  void read_sparse(
      std::vector<int>& a1,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2,
      std::string& stats,
      tiledb_layout_t layout,
      uint64_t timestamp,
      bool encrypt = false);
  void write_delete_condition(
      QueryCondition& qc,
      uint64_t timestamp,
      bool encrypt = false,
      bool error_expected = false);
  void check_delete_conditions(
      std::vector<QueryCondition> qcs,
      uint64_t timestamp,
      bool encrypt = false);
  void remove_sparse_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

DeletesFx::DeletesFx()
    : vfs_(ctx_) {
  Config config;
  config.set("sm.consolidation.buffer_size", "1000");
  ctx_ = Context(config);
  sm_ = ctx_.ptr().get()->ctx_->storage_manager();
  vfs_ = VFS(ctx_);
}

DeletesFx::~DeletesFx() {
}

void DeletesFx::set_legacy() {
  Config config;
  config.set("sm.consolidation.buffer_size", "1000");
  config.set("sm.query.sparse_global_order.reader", "legacy");
  config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");

  ctx_ = Context(config);
  sm_ = ctx_.ptr().get()->ctx_->storage_manager();
  vfs_ = VFS(ctx_);
}

void DeletesFx::create_sparse_array(bool allows_dups, bool encrypt) {
  // Create dimensions.
  auto d1 = Dimension::create<uint64_t>(ctx_, "d1", {{1, 4}}, 2);
  auto d2 = Dimension::create<uint64_t>(ctx_, "d2", {{1, 4}}, 2);

  // Create domain.
  Domain domain(ctx_);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create attributes.
  auto a1 = Attribute::create<int32_t>(ctx_, "a1");

  // Create array schmea.
  ArraySchema schema(ctx_, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.set_capacity(20);
  schema.add_attributes(a1);

  if (allows_dups) {
    schema.set_allows_dups(true);
  }

  // Set up filters.
  Filter filter(ctx_, TILEDB_FILTER_NONE);
  FilterList filter_list(ctx_);
  filter_list.add_filter(filter);
  schema.set_coords_filter_list(filter_list);

  if (encrypt) {
    Array::create(SPARSE_ARRAY_NAME, schema, enc_type_, key_);
  } else {
    Array::create(SPARSE_ARRAY_NAME, schema);
  }
}

void DeletesFx::write_sparse(
    std::vector<int> a1,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp,
    bool encrypt) {
  // Open array.
  std::unique_ptr<Array> array;
  if (encrypt) {
    array.reset(new Array(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_WRITE,
        enc_type_,
        std::string(key_),
        timestamp));
  } else {
    array.reset(new Array(ctx_, SPARSE_ARRAY_NAME, TILEDB_WRITE, timestamp));
  }

  // Create query.
  Query query(ctx_, *array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_data_buffer("a1", a1);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);

  // Submit/finalize the query.
  query.submit();
  query.finalize();

  // Close array.
  array->close();
}

void DeletesFx::read_sparse(
    std::vector<int>& a1,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2,
    std::string& stats,
    tiledb_layout_t layout,
    uint64_t timestamp,
    bool encrypt) {
  // Open array.
  std::unique_ptr<Array> array;
  if (encrypt) {
    array.reset(new Array(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_READ,
        enc_type_,
        std::string(key_),
        timestamp));
  } else {
    array.reset(new Array(ctx_, SPARSE_ARRAY_NAME, TILEDB_READ, timestamp));
  }

  // Create query.
  Query query(ctx_, *array, TILEDB_READ);
  query.set_layout(layout);
  query.set_data_buffer("a1", a1);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);

  // Submit the query.
  query.submit();
  CHECK(query.query_status() == Query::Status::COMPLETE);

  // Get the query stats.
  stats = query.stats();

  // Close array.
  array->close();
}

void DeletesFx::write_delete_condition(
    QueryCondition& qc, uint64_t timestamp, bool encrypt, bool error_expected) {
  // Open array.
  std::unique_ptr<Array> array;
  if (encrypt) {
    array.reset(new Array(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_DELETE,
        enc_type_,
        std::string(key_),
        timestamp));
  } else {
    array.reset(new Array(ctx_, SPARSE_ARRAY_NAME, TILEDB_DELETE, timestamp));
  }

  // Create query.
  Query query(ctx_, *array, TILEDB_DELETE);

  query.set_condition(qc);

  try {
    query.submit();
  } catch (std::exception&) {
    CHECK(error_expected);
  }

  CHECK(
      query.query_status() ==
      (error_expected ? Query::Status::FAILED : Query::Status::COMPLETE));

  // Close array.
  array->close();
}

void DeletesFx::check_delete_conditions(
    std::vector<QueryCondition> qcs, uint64_t timestamp, bool encrypt) {
  // Open array.
  std::unique_ptr<Array> array;
  if (encrypt) {
    array.reset(new Array(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_READ,
        enc_type_,
        std::string(key_),
        timestamp));
  } else {
    array.reset(new Array(ctx_, SPARSE_ARRAY_NAME, TILEDB_READ, timestamp));
  }
  auto array_ptr = array->ptr()->array_;
  auto& array_dir = array_ptr->array_directory();
  auto& enc_key = *array_ptr->encryption_key();

  // Load delete conditions.
  auto&& [st, delete_conditions] =
      sm_->load_delete_conditions(array_dir, enc_key);
  REQUIRE(st.ok());
  REQUIRE(delete_conditions->size() == qcs.size());

  for (uint64_t i = 0; i < qcs.size(); i++) {
    // Compare to negated condition.
    auto cmp = qcs[i].ptr()->query_condition_->negated_condition();
    CHECK(tiledb::test::ast_equal(delete_conditions->at(i).ast(), cmp.ast()));
  }

  array->close();
}

void DeletesFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  vfs_.remove_dir(array_name);
}

void DeletesFx::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

bool DeletesFx::is_array(const std::string& array_name) {
  return vfs_.is_dir(array_name);
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test writting delete condition",
    "[cppapi][deletes][write-check]") {
  remove_sparse_array();

  bool encrypt = GENERATE(true, false);
  create_sparse_array(false, encrypt);

  // Define query condition (a1 < 4.0).
  QueryCondition qc(ctx_);
  int32_t val = 4;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_LT);

  // Define query condition (a1 > 8.0).
  QueryCondition qc2(ctx_);
  int32_t val2 = 8;
  qc2.init("a1", &val2, sizeof(int32_t), TILEDB_GT);

  write_delete_condition(qc, 1, encrypt);
  check_delete_conditions({qc}, 2, encrypt);

  write_delete_condition(qc2, 3, encrypt);
  check_delete_conditions({qc}, 2, encrypt);
  check_delete_conditions({qc, qc2}, 4, encrypt);

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test writting invalid delete condition",
    "[cppapi][deletes][write-check][invalid]") {
  remove_sparse_array();
  create_sparse_array();

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx_);
  int32_t val = 4;
  qc.init("b", &val, sizeof(int32_t), TILEDB_LT);

  write_delete_condition(qc, 1, false, true);

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test open for delete invalid version",
    "[cppapi][deletes][][invalid-version]") {
  std::string v11_arrays_dir =
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays/sparse_array_v11";
  std::string exception;
  try {
    Array(ctx_, v11_arrays_dir, TILEDB_DELETE);
  } catch (std::exception& e) {
    exception = e.what();
  }

  CHECK(
      exception ==
      "[TileDB::Array] Error: Cannot open array for deletes; Array format "
      "version (11) is smaller than the minimum supported version (16).");
}