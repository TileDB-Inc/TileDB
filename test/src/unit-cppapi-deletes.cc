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

#include <test/support/tdb_catch.h>
#include "test/src/ast_helpers.h"
#include "test/src/helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
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
  void set_purge_deleted_cells();
  void create_sparse_array(bool allows_dups = false, bool encrypt = false);
  void create_sparse_array_v11();
  void write_sparse(
      std::vector<int> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp,
      bool encrypt = false);
  void write_sparse_v11(uint64_t timestamp);
  void read_sparse(
      std::vector<int>& a1,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2,
      std::string& stats,
      tiledb_layout_t layout,
      uint64_t timestamp,
      bool encrypt = false);
  void consolidate_sparse(bool vacuum = false);
  void consolidate_commits_sparse(bool vacuum);
  void write_delete_condition(
      QueryCondition& qc, uint64_t timestamp, bool encrypt = false);
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
  sm_ = ctx_.ptr().get()->storage_manager();
  vfs_ = VFS(ctx_);
}

DeletesFx::~DeletesFx() {
}

void DeletesFx::set_purge_deleted_cells() {
  Config config;
  config.set("sm.consolidation.buffer_size", "1000");
  config.set("sm.consolidation.purge_deleted_cells", "true");

  ctx_ = Context(config);
  sm_ = ctx_.ptr().get()->storage_manager();
  vfs_ = VFS(ctx_);
}

void DeletesFx::set_legacy() {
  Config config;
  config.set("sm.consolidation.buffer_size", "1000");
  config.set("sm.query.sparse_global_order.reader", "legacy");
  config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");

  ctx_ = Context(config);
  sm_ = ctx_.ptr().get()->storage_manager();
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

void DeletesFx::create_sparse_array_v11() {
  // Get the v11 sparse array.
  std::string v11_arrays_dir =
      std::string(TILEDB_TEST_INPUTS_DIR) + "/arrays/sparse_array_v11";
  REQUIRE(
      tiledb_vfs_copy_dir(
          ctx_.ptr().get(),
          vfs_.ptr().get(),
          v11_arrays_dir.c_str(),
          SPARSE_ARRAY_NAME) == TILEDB_OK);
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
    array = std::make_unique<Array>(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_WRITE,
        enc_type_,
        std::string(key_),
        timestamp);
  } else {
    array = std::make_unique<Array>(
        ctx_, SPARSE_ARRAY_NAME, TILEDB_WRITE, timestamp);
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
void DeletesFx::write_sparse_v11(uint64_t timestamp) {
  // Prepare cell buffers.
  std::vector<int> buffer_a1{0, 1, 2, 3};
  std::vector<uint64_t> buffer_a2{0, 1, 3, 6};
  std::string buffer_var_a2("abbcccdddd");
  std::vector<float> buffer_a3{0.1f, 0.2f, 1.1f, 1.2f, 2.1f, 2.2f, 3.1f, 3.2f};
  std::vector<uint64_t> buffer_coords_dim1{1, 1, 1, 2};
  std::vector<uint64_t> buffer_coords_dim2{1, 2, 4, 3};

  // Open array.
  Array array(ctx_, SPARSE_ARRAY_NAME, TILEDB_WRITE, timestamp);

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_data_buffer("a1", buffer_a1);
  query.set_data_buffer(
      "a2", (void*)buffer_var_a2.c_str(), buffer_var_a2.size());
  query.set_offsets_buffer("a2", buffer_a2);
  query.set_data_buffer("a3", buffer_a3);
  query.set_data_buffer("d1", buffer_coords_dim1);
  query.set_data_buffer("d2", buffer_coords_dim2);

  // Submit/finalize the query.
  query.submit();
  query.finalize();

  // Close array.
  array.close();
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
    array = std::make_unique<Array>(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_READ,
        enc_type_,
        std::string(key_),
        timestamp);
  } else {
    array = std::make_unique<Array>(
        ctx_, SPARSE_ARRAY_NAME, TILEDB_READ, timestamp);
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

void DeletesFx::consolidate_sparse(bool vacuum) {
  auto config = ctx_.config();
  Array::consolidate(ctx_, SPARSE_ARRAY_NAME, &config);

  if (vacuum) {
    REQUIRE_NOTHROW(Array::vacuum(ctx_, SPARSE_ARRAY_NAME, &config));
  }
}

void DeletesFx::consolidate_commits_sparse(bool vacuum) {
  auto config = ctx_.config();
  config["sm.consolidation.mode"] = "commits";
  Array::consolidate(ctx_, SPARSE_ARRAY_NAME, &config);

  if (vacuum) {
    config["sm.vacuum.mode"] = "commits";
    REQUIRE_NOTHROW(Array::vacuum(ctx_, SPARSE_ARRAY_NAME, &config));
  }
}

void DeletesFx::write_delete_condition(
    QueryCondition& qc, uint64_t timestamp, bool encrypt) {
  // Open array.
  std::unique_ptr<Array> array;
  if (encrypt) {
    array = std::make_unique<Array>(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_DELETE,
        enc_type_,
        std::string(key_),
        timestamp);
  } else {
    array = std::make_unique<Array>(
        ctx_, SPARSE_ARRAY_NAME, TILEDB_DELETE, timestamp);
  }

  // Create query.
  Query query(ctx_, *array, TILEDB_DELETE);

  query.set_condition(qc);
  query.submit();
  CHECK(query.query_status() == Query::Status::COMPLETE);

  // Close array.
  array->close();
}

void DeletesFx::check_delete_conditions(
    std::vector<QueryCondition> qcs, uint64_t timestamp, bool encrypt) {
  // Open array.
  std::unique_ptr<Array> array;
  if (encrypt) {
    array = std::make_unique<Array>(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_READ,
        enc_type_,
        std::string(key_),
        timestamp);
  } else {
    array = std::make_unique<Array>(
        ctx_, SPARSE_ARRAY_NAME, TILEDB_READ, timestamp);
  }
  auto array_ptr = array->ptr()->array_;

  // Load delete conditions.
  auto&& [st, delete_conditions, update_values] =
      sm_->load_delete_and_update_conditions(*array_ptr);
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
    "CPP API: Test writing delete condition",
    "[cppapi][deletes][write-check]") {
  remove_sparse_array();

  bool encrypt = GENERATE(true, false);
  create_sparse_array(false, encrypt);

  // Define query condition (a1 < 4).
  QueryCondition qc(ctx_);
  int32_t val = 4;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_LT);

  // Define query condition (a1 > 8).
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
    "CPP API: Test deletes, writing invalid delete condition",
    "[cppapi][deletes][write-check][invalid]") {
  remove_sparse_array();
  create_sparse_array();

  // Define query condition (b < 4.0).
  QueryCondition qc(ctx_);
  int32_t val = 4;
  qc.init("b", &val, sizeof(int32_t), TILEDB_LT);

  REQUIRE_THROWS_AS(write_delete_condition(qc, 1, false), tiledb::TileDBError);

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test deletes, open for delete invalid version",
    "[cppapi][deletes][invalid-version]") {
  if constexpr (is_experimental_build) {
    return;
  }

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

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test deletes, reading with delete condition",
    "[cppapi][deletes][read]") {
  remove_sparse_array();

  bool consolidate = GENERATE(true, false);
  bool purge_deleted_cells = GENERATE(true, false);
  bool vacuum = GENERATE(true, false);
  bool allows_dups = GENERATE(true, false);
  bool legacy = GENERATE(true, false);
  tiledb_layout_t read_layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);
  if (!consolidate && (vacuum || purge_deleted_cells)) {
    return;
  }

  create_sparse_array(allows_dups);

  // Write fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  // Define query condition (a1 < 2).
  QueryCondition qc(ctx_);
  int32_t val = 2;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_LT);

  // Write condition.
  write_delete_condition(qc, 3);
  // Write another fragment that will not be affected by the condition.
  write_sparse({1}, {4}, {4}, 5);

  // Set purge consolidation config, if needed.
  if (purge_deleted_cells) {
    set_purge_deleted_cells();
  }

  // Consolidate with delete.
  if (consolidate) {
    consolidate_sparse(vacuum);
  }

  // Test read for both refactored and legacy.
  if (legacy) {
    set_legacy();
  }

  // Reading before the delete condition timestamp.
  uint64_t buffer_size = legacy ? 100 : purge_deleted_cells ? 2 : 4;
  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, read_layout, 2);

  std::vector<int> c_a1 = {0, 1, 2, 3};
  std::vector<uint64_t> c_dim1 = {1, 1, 1, 2};
  std::vector<uint64_t> c_dim2 = {1, 2, 4, 3};
  if (purge_deleted_cells) {
    c_a1 = {2, 3};
    c_dim1 = {1, 2};
    c_dim2 = {4, 3};
  }
  CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

  // Reading after delete condition timestamp.
  buffer_size = legacy ? 100 : 2;
  std::vector<int> a1_2(buffer_size);
  std::vector<uint64_t> dim1_2(buffer_size);
  std::vector<uint64_t> dim2_2(buffer_size);
  read_sparse(a1_2, dim1_2, dim2_2, stats, read_layout, 4);

  std::vector<int> c_a1_2 = {2, 3};
  std::vector<uint64_t> c_dim1_2 = {1, 2};
  std::vector<uint64_t> c_dim2_2 = {4, 3};
  CHECK(!memcmp(c_a1_2.data(), a1_2.data(), c_a1_2.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_2.data(), dim1_2.data(), c_dim1_2.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_2.data(), dim2_2.data(), c_dim2_2.size() * sizeof(uint64_t)));

  // Reading after new fragment.
  buffer_size = legacy ? 100 : 3;
  std::vector<int> a1_3(buffer_size);
  std::vector<uint64_t> dim1_3(buffer_size);
  std::vector<uint64_t> dim2_3(buffer_size);
  read_sparse(a1_3, dim1_3, dim2_3, stats, read_layout, 6);

  std::vector<int> c_a1_3 = {2, 3, 1};
  std::vector<uint64_t> c_dim1_3 = {1, 2, 4};
  std::vector<uint64_t> c_dim2_3 = {4, 3, 4};
  CHECK(!memcmp(c_a1_3.data(), a1_3.data(), c_a1_3.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_3.data(), dim1_3.data(), c_dim1_3.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_3.data(), dim2_3.data(), c_dim2_3.size() * sizeof(uint64_t)));

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test deletes, reading with delete condition, consolidated "
    "fragment",
    "[cppapi][deletes][read][consolidated]") {
  remove_sparse_array();

  bool consolidate = GENERATE(true, false);
  bool purge_deleted_cells = GENERATE(true, false);
  bool vacuum = GENERATE(true, false);
  bool allows_dups = GENERATE(true, false);
  bool legacy = GENERATE(true, false);
  tiledb_layout_t read_layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);
  if (!consolidate && (vacuum || purge_deleted_cells)) {
    return;
  }

  create_sparse_array(allows_dups);

  // Write fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  // Write another fragment that will not be affected by the condition.
  write_sparse({1}, {4}, {4}, 5);

  // Consolidate.
  consolidate_sparse(true);

  // Define query condition (a1 < 2).
  QueryCondition qc(ctx_);
  int32_t val = 2;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_LT);

  // Write condition.
  write_delete_condition(qc, 3);
  // Write another fragment that will rewrite deleted cells.
  write_sparse({0, 1}, {1, 1}, {1, 2}, 7);

  // Set purge consolidation config, if needed.
  if (purge_deleted_cells) {
    set_purge_deleted_cells();
  }

  // Consolidate with delete.
  if (consolidate) {
    consolidate_sparse(vacuum);
  }

  // Test read for both refactored and legacy.
  if (legacy) {
    set_legacy();
  }

  // Reading before the delete condition timestamp.
  uint64_t buffer_size = legacy ? 100 : purge_deleted_cells ? 2 : 4;
  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, read_layout, 2);

  std::vector<int> c_a1 = {0, 1, 2, 3};
  std::vector<uint64_t> c_dim1 = {1, 1, 1, 2};
  std::vector<uint64_t> c_dim2 = {1, 2, 4, 3};
  if (purge_deleted_cells) {
    c_a1 = {2, 3};
    c_dim1 = {1, 2};
    c_dim2 = {4, 3};
  }
  CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

  // Reading at delete condition timestamp.
  buffer_size = legacy ? 100 : 2;
  std::vector<int> a1_2(buffer_size);
  std::vector<uint64_t> dim1_2(buffer_size);
  std::vector<uint64_t> dim2_2(buffer_size);
  read_sparse(a1_2, dim1_2, dim2_2, stats, read_layout, 3);

  std::vector<int> c_a1_2 = {2, 3};
  std::vector<uint64_t> c_dim1_2 = {1, 2};
  std::vector<uint64_t> c_dim2_2 = {4, 3};
  CHECK(!memcmp(c_a1_2.data(), a1_2.data(), c_a1_2.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_2.data(), dim1_2.data(), c_dim1_2.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_2.data(), dim2_2.data(), c_dim2_2.size() * sizeof(uint64_t)));

  // Reading after delete condition timestamp.
  buffer_size = legacy ? 100 : 2;
  std::vector<int> a1_3(buffer_size);
  std::vector<uint64_t> dim1_3(buffer_size);
  std::vector<uint64_t> dim2_3(buffer_size);
  read_sparse(a1_3, dim1_3, dim2_3, stats, read_layout, 4);

  std::vector<int> c_a1_3 = {2, 3};
  std::vector<uint64_t> c_dim1_3 = {1, 2};
  std::vector<uint64_t> c_dim2_3 = {4, 3};
  CHECK(!memcmp(c_a1_3.data(), a1_3.data(), c_a1_3.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_3.data(), dim1_3.data(), c_dim1_3.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_3.data(), dim2_3.data(), c_dim2_3.size() * sizeof(uint64_t)));

  // Reading after new fragment.
  buffer_size = legacy ? 100 : 3;
  std::vector<int> a1_4(buffer_size);
  std::vector<uint64_t> dim1_4(buffer_size);
  std::vector<uint64_t> dim2_4(buffer_size);
  read_sparse(a1_4, dim1_4, dim2_4, stats, read_layout, 6);

  std::vector<int> c_a1_4 = {2, 3, 1};
  std::vector<uint64_t> c_dim1_4 = {1, 2, 4};
  std::vector<uint64_t> c_dim2_4 = {4, 3, 4};
  CHECK(!memcmp(c_a1_4.data(), a1_4.data(), c_a1_4.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_4.data(), dim1_4.data(), c_dim1_4.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_4.data(), dim2_4.data(), c_dim2_4.size() * sizeof(uint64_t)));

  // Reading after adding deleted cells.
  buffer_size = legacy ? 100 : 5;
  std::vector<int> a1_5(buffer_size);
  std::vector<uint64_t> dim1_5(buffer_size);
  std::vector<uint64_t> dim2_5(buffer_size);
  read_sparse(a1_5, dim1_5, dim2_5, stats, read_layout, 8);

  std::vector<int> c_a1_5_ordered = {0, 1, 2, 3, 1};
  std::vector<uint64_t> c_dim1_5_ordered = {1, 1, 1, 2, 4};
  std::vector<uint64_t> c_dim2_5_ordered = {1, 2, 4, 3, 4};
  std::vector<int> c_a1_5_unordered = {2, 3, 1, 0, 1};
  std::vector<uint64_t> c_dim1_5_unordered = {1, 2, 4, 1, 1};
  std::vector<uint64_t> c_dim2_5_unordered = {4, 3, 4, 1, 2};
  if (read_layout == TILEDB_GLOBAL_ORDER) {
    CHECK(!memcmp(
        c_a1_5_ordered.data(),
        a1_5.data(),
        c_a1_5_ordered.size() * sizeof(int)));
    CHECK(!memcmp(
        c_dim1_5_ordered.data(),
        dim1_5.data(),
        c_dim1_5_ordered.size() * sizeof(uint64_t)));
    CHECK(!memcmp(
        c_dim2_5_ordered.data(),
        dim2_5.data(),
        c_dim2_5_ordered.size() * sizeof(uint64_t)));
  } else {
    CHECK(
        (!memcmp(
             c_a1_5_ordered.data(),
             a1_5.data(),
             c_a1_5_ordered.size() * sizeof(int)) ||
         !memcmp(
             c_a1_5_unordered.data(),
             a1_5.data(),
             c_a1_5_unordered.size() * sizeof(int))));
    CHECK(
        (!memcmp(
             c_dim1_5_ordered.data(),
             dim1_5.data(),
             c_dim1_5_ordered.size() * sizeof(uint64_t)) ||
         !memcmp(
             c_dim1_5_unordered.data(),
             dim1_5.data(),
             c_dim1_5_unordered.size() * sizeof(uint64_t))));
    CHECK(
        (!memcmp(
             c_dim2_5_ordered.data(),
             dim2_5.data(),
             c_dim2_5_ordered.size() * sizeof(uint64_t)) ||
         !memcmp(
             c_dim2_5_unordered.data(),
             dim2_5.data(),
             c_dim2_5_unordered.size() * sizeof(uint64_t))));
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test deletes, reading with delete condition, delete duplicates "
    "from later fragments",
    "[cppapi][deletes][duplicates]") {
  remove_sparse_array();

  bool purge_deleted_cells = GENERATE(true, false);
  bool consolidate = GENERATE(true, false);
  bool vacuum = GENERATE(true, false);
  bool allows_dups = GENERATE(true, false);
  bool legacy = GENERATE(true, false);
  tiledb_layout_t read_layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);
  if (!consolidate && (vacuum || purge_deleted_cells)) {
    return;
  }

  create_sparse_array(allows_dups);

  // Write fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  // Write another fragment. Cell (1, 1) will be replaced.
  write_sparse({4}, {1}, {1}, 3);

  // Define query condition (a1 == 4).
  QueryCondition qc(ctx_);
  int32_t val = 4;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_EQ);

  // Write condition.
  write_delete_condition(qc, 5);

  // Set purge consolidation config, if needed.
  if (purge_deleted_cells) {
    set_purge_deleted_cells();
  }

  // Consolidate with delete.
  if (consolidate) {
    consolidate_sparse(vacuum);
  }

  // Test read for both refactored and legacy.
  if (legacy) {
    set_legacy();
  }

  // Reading.
  uint64_t buffer_size = legacy ? 100 : (allows_dups ? 4 : 3);
  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, read_layout, 7);

  std::vector<int> c_a1 = {0, 1, 2, 3};
  std::vector<uint64_t> c_dim1 = {1, 1, 1, 2};
  std::vector<uint64_t> c_dim2 = {1, 2, 4, 3};
  if (!allows_dups) {
    c_a1 = {1, 2, 3};
    c_dim1 = {1, 1, 2};
    c_dim2 = {2, 4, 3};
  }
  CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test deletes, commits consolidation",
    "[cppapi][deletes][commits][consolidation]") {
  remove_sparse_array();

  create_sparse_array();

  bool vacuum = GENERATE(false, true);

  // Write fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  // Define query condition (a1 < 2).
  QueryCondition qc(ctx_);
  int32_t val = 2;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_LT);

  // Write condition.
  write_delete_condition(qc, 3);

  // Write fragment.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 5);

  // Define query condition (a1 > 4).
  QueryCondition qc2(ctx_);
  int32_t val2 = 4;
  qc2.init("a1", &val2, sizeof(int32_t), TILEDB_GT);

  // Write condition.
  write_delete_condition(qc2, 7);

  consolidate_commits_sparse(vacuum);

  check_delete_conditions({qc}, 4);

  check_delete_conditions({qc, qc2}, 8);

  // Define query condition (a1 == 9).
  QueryCondition qc3(ctx_);
  int32_t val3 = 9;
  qc3.init("a1", &val3, sizeof(int32_t), TILEDB_EQ);

  // Write one more condition, in between the existing conditions, this will
  // ensure the conditions get sorted.
  write_delete_condition(qc3, 4);

  check_delete_conditions({qc, qc3, qc2}, 8);

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test deletes, consolidation, delete same cell earlier",
    "[cppapi][deletes][consolidation][same-cell]") {
  remove_sparse_array();

  bool allows_dups = GENERATE(true, false);
  bool legacy = GENERATE(true, false);
  bool vacuum = GENERATE(true, false);
  tiledb_layout_t read_layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);

  create_sparse_array(allows_dups);

  // Write fragment with one cell.
  write_sparse({3}, {1}, {1}, 1);

  // Write another fragment with one cell.
  write_sparse({3}, {2}, {2}, 3);

  // Define query condition (a1 < 4).
  QueryCondition qc(ctx_);
  int32_t val = 4;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_LT);

  // Write condition.
  write_delete_condition(qc, 7);

  // Consolidate.
  consolidate_sparse(vacuum);

  // Reading before the delete condition timestamp.
  uint64_t buffer_size = 2;
  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, read_layout, 6);

  std::vector<int> c_a1 = {3, 3};
  std::vector<uint64_t> c_dim1 = {1, 2};
  std::vector<uint64_t> c_dim2 = {1, 2};
  CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

  // Reading after delete condition timestamp.
  buffer_size = 2;
  std::vector<int> a1_empty(buffer_size);
  std::vector<uint64_t> dim1_empty(buffer_size);
  std::vector<uint64_t> dim2_empty(buffer_size);
  read_sparse(a1_empty, dim1_empty, dim2_empty, stats, read_layout, 8);

  std::vector<int> c_a1_empty = {0};
  std::vector<uint64_t> c_dim1_empty = {0};
  std::vector<uint64_t> c_dim2_empty = {0};
  CHECK(!memcmp(
      c_a1_empty.data(), a1_empty.data(), c_a1_empty.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_empty.data(),
      dim1_empty.data(),
      c_dim1_empty.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_empty.data(),
      dim2_empty.data(),
      c_dim2_empty.size() * sizeof(uint64_t)));

  // Define query condition (a1 < 5).
  QueryCondition qc2(ctx_);
  int32_t val2 = 5;
  qc2.init("a1", &val2, sizeof(int32_t), TILEDB_LT);

  // Write condition, but earlier.
  write_delete_condition(qc2, 5);

  // Write another fragment with one cell.
  write_sparse({3}, {3}, {3}, 9);

  // Consolidate.
  consolidate_sparse(vacuum);

  // Test read for both refactored and legacy.
  if (legacy) {
    set_legacy();
  }

  // Reading before new delete condition timestamp.
  buffer_size = legacy ? 100 : 2;
  std::vector<int> a1_2(buffer_size);
  std::vector<uint64_t> dim1_2(buffer_size);
  std::vector<uint64_t> dim2_2(buffer_size);
  read_sparse(a1_2, dim1_2, dim2_2, stats, read_layout, 4);

  std::vector<int> c_a1_2 = {3, 3};
  std::vector<uint64_t> c_dim1_2 = {1, 2};
  std::vector<uint64_t> c_dim2_2 = {1, 2};
  CHECK(!memcmp(c_a1_2.data(), a1_2.data(), c_a1_2.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_2.data(), dim1_2.data(), c_dim1_2.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_2.data(), dim2_2.data(), c_dim2_2.size() * sizeof(uint64_t)));

  // Reading after new delete condition timestamp.
  buffer_size = legacy ? 100 : 1;
  std::vector<int> a1_3(buffer_size);
  std::vector<uint64_t> dim1_3(buffer_size);
  std::vector<uint64_t> dim2_3(buffer_size);
  read_sparse(a1_3, dim1_3, dim2_3, stats, read_layout, 6);

  std::vector<int> c_a1_3 = {0};
  std::vector<uint64_t> c_dim1_3 = {0};
  std::vector<uint64_t> c_dim2_3 = {0};
  CHECK(!memcmp(c_a1_3.data(), a1_3.data(), c_a1_3.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_3.data(), dim1_3.data(), c_dim1_3.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_3.data(), dim2_3.data(), c_dim2_3.size() * sizeof(uint64_t)));

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test deletes, multiple consolidation with deletes",
    "[cppapi][deletes][consolidation][multiple]") {
  remove_sparse_array();

  bool purge_deleted_cells = GENERATE(true, false);
  bool allows_dups = GENERATE(true, false);
  bool legacy = GENERATE(true, false);
  bool vacuum = GENERATE(true, false);
  tiledb_layout_t read_layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);

  create_sparse_array(allows_dups);

  // Write fragment.
  write_sparse({0, 1}, {1, 1}, {1, 2}, 1);

  write_sparse({2, 3}, {1, 2}, {4, 3}, 3);

  // Define query condition (a1 < 2).
  QueryCondition qc(ctx_);
  int32_t val = 2;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_LT);

  // Write condition.
  write_delete_condition(qc, 5);

  // Set purge consolidation config, if needed.
  if (purge_deleted_cells) {
    set_purge_deleted_cells();
  }

  // Consolidate.
  consolidate_sparse(vacuum);

  // Write fragment.
  write_sparse({4, 5, 6, 7}, {3, 3, 4, 4}, {3, 4, 3, 4}, 7);

  // Define query condition (d2 == 3).
  QueryCondition qc2(ctx_);
  uint64_t val2 = 3;
  qc2.init("d2", &val2, sizeof(uint64_t), TILEDB_EQ);

  // Write condition.
  write_delete_condition(qc2, 9);

  // Consolidate.
  consolidate_sparse(vacuum);

  // Test read for both refactored and legacy.
  if (legacy) {
    set_legacy();
  }

  // Read at time 6.
  uint64_t buffer_size = legacy ? 100 : 2;
  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, read_layout, 6);

  std::vector<int> c_a1 = {2, 3};
  std::vector<uint64_t> c_dim1 = {1, 2};
  std::vector<uint64_t> c_dim2 = {4, 3};
  if (purge_deleted_cells) {
    c_a1 = {2};
    c_dim1 = {1};
    c_dim2 = {4};
  }
  CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

  // Read at time 8.
  buffer_size = legacy ? 100 : 6;
  std::vector<int> a1_2(buffer_size);
  std::vector<uint64_t> dim1_2(buffer_size);
  std::vector<uint64_t> dim2_2(buffer_size);
  read_sparse(a1_2, dim1_2, dim2_2, stats, read_layout, 8);

  std::vector<int> c_a1_2 = {2, 3, 4, 5, 6, 7};
  std::vector<uint64_t> c_dim1_2 = {1, 2, 3, 3, 4, 4};
  std::vector<uint64_t> c_dim2_2 = {4, 3, 3, 4, 3, 4};
  if (purge_deleted_cells) {
    c_a1_2 = {2, 5, 7};
    c_dim1_2 = {1, 3, 4};
    c_dim2_2 = {4, 4, 4};
  }
  CHECK(!memcmp(c_a1_2.data(), a1_2.data(), c_a1_2.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_2.data(), dim1_2.data(), c_dim1_2.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_2.data(), dim2_2.data(), c_dim2_2.size() * sizeof(uint64_t)));

  // Reading everything.
  buffer_size = legacy ? 100 : 3;
  std::vector<int> a1_3(buffer_size);
  std::vector<uint64_t> dim1_3(buffer_size);
  std::vector<uint64_t> dim2_3(buffer_size);
  read_sparse(a1_3, dim1_3, dim2_3, stats, read_layout, 10);

  std::vector<int> c_a1_3 = {2, 5, 7};
  std::vector<uint64_t> c_dim1_3 = {1, 3, 4};
  std::vector<uint64_t> c_dim2_3 = {4, 4, 4};
  CHECK(!memcmp(c_a1_3.data(), a1_3.data(), c_a1_3.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_3.data(), dim1_3.data(), c_dim1_3.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_3.data(), dim2_3.data(), c_dim2_3.size() * sizeof(uint64_t)));

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test deletes, multiple cells with same coords in same fragment",
    "[cppapi][deletes][consolidation][multiple-cells-same-coords]") {
  remove_sparse_array();

  bool purge_deleted_cells = GENERATE(true, false);
  bool allows_dups = GENERATE(true, false);
  bool legacy = GENERATE(true, false);
  bool vacuum = GENERATE(true, false);
  tiledb_layout_t read_layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);

  create_sparse_array(allows_dups);

  // Write fragment.
  write_sparse({1, 2}, {1, 1}, {1, 2}, 1);

  // Write fragment with same coords.
  write_sparse({3, 4}, {1, 1}, {1, 2}, 3);

  // Consolidate.
  consolidate_sparse(vacuum);

  // Write fragment, again with same coords.
  write_sparse({5, 6}, {1, 1}, {1, 2}, 5);

  // Define query condition (a1 == 3).
  QueryCondition qc(ctx_);
  int val = 3;
  qc.init("a1", &val, sizeof(int), TILEDB_EQ);

  // Write condition.
  write_delete_condition(qc, 7);

  // Set purge consolidation config, if needed.
  if (purge_deleted_cells) {
    set_purge_deleted_cells();
  }

  // Consolidate.
  consolidate_sparse(vacuum);

  // Test read for both refactored and legacy.
  if (legacy) {
    set_legacy();
  }

  // Read at time 6.
  uint64_t buffer_size = 100;
  if (!legacy) {
    if (purge_deleted_cells) {
      if (allows_dups) {
        buffer_size = 5;
      } else {
        buffer_size = 2;
      }
    } else {
      if (allows_dups) {
        buffer_size = 6;
      } else {
        buffer_size = 2;
      }
    }
  }
  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, read_layout, 6);

  std::vector<int> c_a1;
  std::vector<uint64_t> c_dim1;
  std::vector<uint64_t> c_dim2;
  if (purge_deleted_cells) {
    if (allows_dups) {
      c_a1 = {};

      // First two numbers for a1 should be 1 and 5.
      CHECK((a1[0] == 1 || a1[1] == 1));
      CHECK((a1[0] == 5 || a1[1] == 5));
      CHECK(a1[0] != a1[1]);

      // Last three numbers for a1 should be 2, 4, 6.
      CHECK((a1[2] == 2 || a1[2] == 4 || a1[2] == 6));
      CHECK((a1[3] == 2 || a1[3] == 4 || a1[3] == 6));
      CHECK((a1[4] == 2 || a1[4] == 4 || a1[4] == 6));
      CHECK(a1[2] != a1[3]);
      CHECK(a1[3] != a1[4]);
      CHECK(a1[4] != a1[2]);
      c_dim1 = {1, 1, 1, 1, 1};
      c_dim2 = {1, 1, 2, 2, 2};
    } else {
      c_a1 = {5, 6};
      c_dim1 = {1, 1};
      c_dim2 = {1, 2};
    }
  } else {
    if (allows_dups) {
      c_a1 = {};

      // First three numbers for a1 should be 1, 3, 5.
      CHECK((a1[0] == 1 || a1[0] == 3 || a1[0] == 5));
      CHECK((a1[1] == 1 || a1[1] == 3 || a1[1] == 5));
      CHECK((a1[2] == 1 || a1[2] == 3 || a1[2] == 5));
      CHECK(a1[0] != a1[1]);
      CHECK(a1[1] != a1[2]);
      CHECK(a1[2] != a1[0]);

      // Last three numbers for a1 should be 2, 4, 6.
      CHECK((a1[3] == 2 || a1[3] == 4 || a1[3] == 6));
      CHECK((a1[4] == 2 || a1[4] == 4 || a1[4] == 6));
      CHECK((a1[5] == 2 || a1[5] == 4 || a1[5] == 6));
      CHECK(a1[3] != a1[4]);
      CHECK(a1[4] != a1[5]);
      CHECK(a1[5] != a1[3]);

      c_dim1 = {1, 1, 1, 1, 1, 1};
      c_dim2 = {1, 1, 1, 2, 2, 2};
    } else {
      c_a1 = {5, 6};
      c_dim1 = {1, 1};
      c_dim2 = {1, 2};
    }
  }

  CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

  // Read at time 8.
  buffer_size = 100;
  if (!legacy) {
    if (allows_dups) {
      buffer_size = 5;
    } else {
      buffer_size = 2;
    }
  }
  std::vector<int> a1_2(buffer_size);
  std::vector<uint64_t> dim1_2(buffer_size);
  std::vector<uint64_t> dim2_2(buffer_size);
  read_sparse(a1_2, dim1_2, dim2_2, stats, read_layout, 8);

  std::vector<int> c_a1_2;
  std::vector<uint64_t> c_dim1_2;
  std::vector<uint64_t> c_dim2_2;
  if (allows_dups) {
    c_a1_2 = {};

    // First two numbers for a1 should be 1 and 5.
    CHECK((a1_2[0] == 1 || a1_2[1] == 1));
    CHECK((a1_2[0] == 5 || a1_2[1] == 5));
    CHECK(a1_2[0] != a1_2[1]);

    // Last three numbers for a1 should be 2, 4, 6.
    CHECK((a1_2[2] == 2 || a1_2[2] == 4 || a1_2[2] == 6));
    CHECK((a1_2[3] == 2 || a1_2[3] == 4 || a1_2[3] == 6));
    CHECK((a1_2[4] == 2 || a1_2[4] == 4 || a1_2[4] == 6));
    CHECK(a1_2[2] != a1_2[3]);
    CHECK(a1_2[3] != a1_2[4]);
    CHECK(a1_2[4] != a1_2[2]);
    c_dim1_2 = {1, 1, 1, 1, 1};
    c_dim2_2 = {1, 1, 2, 2, 2};
  } else {
    c_a1_2 = {5, 6};
    c_dim1_2 = {1, 1};
    c_dim2_2 = {1, 2};
  }

  CHECK(!memcmp(c_a1_2.data(), a1_2.data(), c_a1_2.size() * sizeof(int)));
  CHECK(!memcmp(
      c_dim1_2.data(), dim1_2.data(), c_dim1_2.size() * sizeof(uint64_t)));
  CHECK(!memcmp(
      c_dim2_2.data(), dim2_2.data(), c_dim2_2.size() * sizeof(uint64_t)));

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test deletes, multiple cells with same coords in same fragment, "
    "across tiles",
    "[cppapi][deletes][consolidation][multiple-cells-same-coords][across-"
    "tiles]") {
  remove_sparse_array();

  bool purge_deleted_cells = GENERATE(true, false);
  bool allows_dups = GENERATE(true, false);
  bool legacy = GENERATE(true, false);
  bool vacuum = GENERATE(true, false);
  tiledb_layout_t read_layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);

  create_sparse_array(allows_dups);

  // Write fragments.
  // We write 8 cells per fragments for 6 fragments. Then it gets consolidated
  // into one. So we'll get in order 6xcell1, 6xcell2... total 48 cells. Tile
  // capacity is 20 so we'll end up with 3 tiles. First break in the tiles will
  // be in the middle of cell3, second will be in the middle of the cells7.
  for (uint64_t i = 0; i < 5; i++) {
    write_sparse(
        {1, 2, 3, 4, 5, 6, 7, 8},
        {1, 1, 2, 2, 1, 1, 2, 2},
        {1, 2, 1, 2, 3, 4, 3, 4},
        i + 1);
  }

  // Consolidate.
  consolidate_sparse(vacuum);

  // Write one more fragment.
  write_sparse(
      {1, 2, 3, 4, 5, 6, 7, 8},
      {1, 1, 2, 2, 1, 1, 2, 2},
      {1, 2, 1, 2, 3, 4, 3, 4},
      6);

  // Define query condition (a1 == 3).
  QueryCondition qc(ctx_);
  int val = 3;
  qc.init("a1", &val, sizeof(int), TILEDB_EQ);

  // Write condition.
  write_delete_condition(qc, 2);

  // Set purge consolidation config, if needed.
  if (purge_deleted_cells) {
    set_purge_deleted_cells();
  }

  // Consolidate.
  consolidate_sparse(vacuum);

  // Test read for both refactored and legacy.
  if (legacy) {
    set_legacy();
  }

  // Read at time 1.
  uint64_t buffer_size = 100;
  uint64_t expected_elements = 0;
  if (purge_deleted_cells) {
    expected_elements = 7;
  } else {
    expected_elements = 8;
  }
  if (!legacy) {
    buffer_size = expected_elements;
  }
  std::string stats;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, read_layout, 1);

  if (read_layout == TILEDB_UNORDERED) {
    // For unordered, count the number of elements.
    std::vector<int> count(8);
    std::vector<int> expected;
    for (uint64_t i = 0; i < expected_elements; i++) {
      if ((a1[i] - 1) < 8) {
        count[a1[i] - 1]++;
      }
    }

    if (purge_deleted_cells) {
      expected = {1, 1, 0, 1, 1, 1, 1, 1};
    } else {
      expected = {1, 1, 1, 1, 1, 1, 1, 1};
    }

    CHECK(!memcmp(count.data(), expected.data(), 8 * sizeof(int)));
  } else {
    // For ordered, check the exact results.
    std::vector<int> c_a1;
    std::vector<uint64_t> c_dim1;
    std::vector<uint64_t> c_dim2;
    if (purge_deleted_cells) {
      c_a1 = {1, 2, 4, 5, 6, 7, 8};
      c_dim1 = {1, 1, 2, 1, 1, 2, 2};
      c_dim2 = {1, 2, 2, 3, 4, 3, 4};
    } else {
      c_a1 = {1, 2, 3, 4, 5, 6, 7, 8};
      c_dim1 = {1, 1, 2, 2, 1, 1, 2, 2};
      c_dim2 = {1, 2, 1, 2, 3, 4, 3, 4};
    }

    CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
    CHECK(
        !memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
    CHECK(
        !memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));
  }

  // Read at time 10.
  buffer_size = 100;
  expected_elements = 0;
  if (allows_dups) {
    expected_elements = 46;
  } else {
    expected_elements = 8;
  }
  if (!legacy) {
    buffer_size = expected_elements;
  }
  std::vector<int> a1_2(buffer_size);
  std::vector<uint64_t> dim1_2(buffer_size);
  std::vector<uint64_t> dim2_2(buffer_size);
  read_sparse(a1_2, dim1_2, dim2_2, stats, read_layout, 10);

  if (read_layout == TILEDB_UNORDERED) {
    // For unordered, count the number of elements.
    std::vector<int> count(8);
    std::vector<int> expected;
    for (uint64_t i = 0; i < expected_elements; i++) {
      if ((a1_2[i] - 1) < 8) {
        count[a1_2[i] - 1]++;
      }
    }

    if (allows_dups) {
      expected = {6, 6, 4, 6, 6, 6, 6, 6};
    } else {
      expected = {1, 1, 1, 1, 1, 1, 1, 1};
    }

    CHECK(!memcmp(count.data(), expected.data(), 8 * sizeof(int)));
  } else {
    // For ordered, check the exact results.
    std::vector<int> c_a1_2;
    std::vector<uint64_t> c_dim1_2;
    std::vector<uint64_t> c_dim2_2;
    if (allows_dups) {
      c_a1_2 = {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3,
                4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6,
                6, 6, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8};
      c_dim1_2 = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2,
                  2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                  1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2};
      c_dim2_2 = {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1,
                  2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4,
                  4, 4, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4};
    } else {
      c_a1_2 = {1, 2, 3, 4, 5, 6, 7, 8};
      c_dim1_2 = {1, 1, 2, 2, 1, 1, 2, 2};
      c_dim2_2 = {1, 2, 1, 2, 3, 4, 3, 4};
    }

    CHECK(!memcmp(c_a1_2.data(), a1_2.data(), c_a1_2.size() * sizeof(int)));
    CHECK(!memcmp(
        c_dim1_2.data(), dim1_2.data(), c_dim1_2.size() * sizeof(uint64_t)));
    CHECK(!memcmp(
        c_dim2_2.data(), dim2_2.data(), c_dim2_2.size() * sizeof(uint64_t)));
  }

  remove_sparse_array();
}

TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test consolidating fragment with delete timestamp, with purge "
    "option",
    "[cppapi][deletes][consolidation][with-delete-meta][purge]") {
  remove_sparse_array();

  create_sparse_array(false);

  write_sparse({0}, {1}, {1}, 1);
  write_sparse({1}, {4}, {4}, 2);

  // Define query condition (a1 == 1).
  QueryCondition qc(ctx_);
  int32_t val = 1;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_EQ);

  // Write condition.
  write_delete_condition(qc, 3);

  consolidate_sparse(true);

  write_sparse({1}, {4}, {4}, 3);
  set_purge_deleted_cells();
  consolidate_sparse(true);

  // Reading after new fragment.
  std::string stats;
  uint64_t buffer_size = 1;
  std::vector<int> a1(buffer_size);
  std::vector<uint64_t> dim1(buffer_size);
  std::vector<uint64_t> dim2(buffer_size);
  read_sparse(a1, dim1, dim2, stats, TILEDB_UNORDERED, 1);

  std::vector<int> c_a1 = {0};
  std::vector<uint64_t> c_dim1 = {1};
  std::vector<uint64_t> c_dim2 = {1};

  CHECK(!memcmp(c_a1.data(), a1.data(), c_a1.size() * sizeof(int)));
  CHECK(!memcmp(c_dim1.data(), dim1.data(), c_dim1.size() * sizeof(uint64_t)));
  CHECK(!memcmp(c_dim2.data(), dim2.data(), c_dim2.size() * sizeof(uint64_t)));

  remove_sparse_array();
}

// TODO: remove once tiledb_vfs_copy_dir is implemented for windows.
#ifndef _WIN32
TEST_CASE_METHOD(
    DeletesFx,
    "CPP API: Test writing delete in middle of fragment consolidated without "
    "timestamps",
    "[cppapi][deletes][write][old-consolidated-fragment]") {
  if constexpr (is_experimental_build) {
    return;
  }

  remove_sparse_array();
  create_sparse_array_v11();
  // Write first fragment.
  write_sparse_v11(1);

  // Write second fragment.
  write_sparse_v11(3);

  // Consolidate.
  consolidate_sparse();

  // Upgrade to latest version.
  Array::upgrade_version(ctx_, SPARSE_ARRAY_NAME);

  // Define query condition (d2 == 3).
  QueryCondition qc2(ctx_);
  uint64_t val2 = 3;
  qc2.init("d2", &val2, sizeof(uint64_t), TILEDB_EQ);

  // Try to write condition.
  REQUIRE_THROWS_AS(write_delete_condition(qc2, 2, false), tiledb::TileDBError);

  remove_sparse_array();
}
#endif
