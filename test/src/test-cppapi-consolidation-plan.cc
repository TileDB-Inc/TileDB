/**
 * @file test-cppapi-consolidation-plan.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Tests the ConsolidationPlan API
 */

#include "test/support/src/helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"

#include <test/support/tdb_catch.h>

using namespace tiledb;
using namespace tiledb::test;

struct CppConsolidationPlanFx {
  // Constants.
  const char* SPARSE_ARRAY_NAME = "test_deletes_array";

  // TileDB context.
  Context ctx_;
  VFS vfs_;

  std::string key_ = "0123456789abcdeF0123456789abcdeF";
  const tiledb_encryption_type_t enc_type_ = TILEDB_AES_256_GCM;

  // Constructors/destructors.
  CppConsolidationPlanFx();
  ~CppConsolidationPlanFx();

  // Functions.
  void create_sparse_array(bool allows_dups = false, bool encrypt = false);
  void write_sparse(
      std::vector<int> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp,
      bool encrypt = false);
  void remove_sparse_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
  void check_last_error(std::string expected);
};

CppConsolidationPlanFx::CppConsolidationPlanFx()
    : vfs_(ctx_) {
  Config config;
  config.set("sm.consolidation.buffer_size", "1000");
  ctx_ = Context(config);
  vfs_ = VFS(ctx_);

  remove_sparse_array();
}

CppConsolidationPlanFx::~CppConsolidationPlanFx() {
  remove_sparse_array();
}

void CppConsolidationPlanFx::create_sparse_array(
    bool allows_dups, bool encrypt) {
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

void CppConsolidationPlanFx::write_sparse(
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

void CppConsolidationPlanFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  vfs_.remove_dir(array_name);
}

void CppConsolidationPlanFx::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

bool CppConsolidationPlanFx::is_array(const std::string& array_name) {
  return vfs_.is_dir(array_name);
}

void CppConsolidationPlanFx::check_last_error(std::string expected) {
  const char* msg = "unset";
  tiledb_error_t* err{nullptr};
  tiledb_ctx_get_last_error(ctx_.ptr().get(), &err);
  if (err != nullptr) {
    tiledb_error_message(err, &msg);
  }

  CHECK(msg == expected);
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan",
    "[cppapi][consolidation_plan]") {
  create_sparse_array();
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 1024 * 1024);

  uint64_t num_nodes = consolidation_plan.num_nodes();
  CHECK(num_nodes == 0);

  CHECK_THROWS_WITH(
      consolidation_plan.num_fragments(0),
      "Error: ConsolidationPlan: Trying to access a node that doesn't exists");

  CHECK_THROWS_WITH(
      consolidation_plan.fragment_uri(0, 0),
      "Error: ConsolidationPlan: Trying to access a node that doesn't exists");
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan dump",
    "[cppapi][consolidation_plan][dump]") {
  create_sparse_array();
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 1024 * 1024);

  // Check dump
  std::string dump_str = "Not implemented\n";
  FILE* gold_fout = fopen("gold_fout.txt", "w");
  const char* dump = dump_str.c_str();
  fwrite(dump, sizeof(char), strlen(dump), gold_fout);
  fclose(gold_fout);
  FILE* fout = fopen("fout.txt", "w");
  consolidation_plan.dump(fout);
  fclose(fout);
#ifdef _WIN32
  CHECK(!system("FC gold_fout.txt fout.txt > nul"));
#else
  CHECK(!system("diff gold_fout.txt fout.txt"));
#endif
  CHECK_NOTHROW(vfs_.remove_file("gold_fout.txt"));
  CHECK_NOTHROW(vfs_.remove_file("fout.txt"));
}
