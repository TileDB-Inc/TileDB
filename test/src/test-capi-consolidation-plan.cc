/**
 * @file test-capi-consolidation-plan.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <test/support/tdb_catch.h>

using namespace tiledb;
using namespace tiledb::test;

struct ConsolidationPlanFx {
  // Constants.
  const char* SPARSE_ARRAY_NAME = "test_deletes_array";

  // TileDB context.
  Context ctx_;
  VFS vfs_;

  std::string key_ = "0123456789abcdeF0123456789abcdeF";
  const tiledb_encryption_type_t enc_type_ = TILEDB_AES_256_GCM;

  // Constructors/destructors.
  ConsolidationPlanFx();
  ~ConsolidationPlanFx();

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

ConsolidationPlanFx::ConsolidationPlanFx()
    : vfs_(ctx_) {
  Config config;
  config.set("sm.consolidation.buffer_size", "1000");
  ctx_ = Context(config);
  vfs_ = VFS(ctx_);

  remove_sparse_array();
}

ConsolidationPlanFx::~ConsolidationPlanFx() {
  remove_sparse_array();
}

void ConsolidationPlanFx::create_sparse_array(bool allows_dups, bool encrypt) {
  // Create dimensions.
  auto d1 = Dimension::create<uint64_t>(ctx_, "d1", {{1, 4}}, 2);
  auto d2 = Dimension::create<uint64_t>(ctx_, "d2", {{1, 4}}, 2);

  // Create domain.
  Domain domain(ctx_);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create attributes.
  auto a1 = Attribute::create<int32_t>(ctx_, "a1");

  // Create array schema.
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

void ConsolidationPlanFx::write_sparse(
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
        TemporalPolicy(TimeTravel, timestamp),
        EncryptionAlgorithm(AESGCM, key_.c_str()));
  } else {
    array = std::make_unique<Array>(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_WRITE,
        TemporalPolicy(TimeTravel, timestamp));
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

void ConsolidationPlanFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  vfs_.remove_dir(array_name);
}

void ConsolidationPlanFx::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

bool ConsolidationPlanFx::is_array(const std::string& array_name) {
  return vfs_.is_dir(array_name);
}

void ConsolidationPlanFx::check_last_error(std::string expected) {
  const char* msg = "unset";
  tiledb_error_t* err{nullptr};
  tiledb_ctx_get_last_error(ctx_.ptr().get(), &err);
  if (err != nullptr) {
    tiledb_error_message(err, &msg);
  }

  CHECK(msg == expected);
}

TEST_CASE_METHOD(
    ConsolidationPlanFx,
    "CAPI: Consolidation plan",
    "[capi][consolidation-plan]") {
  create_sparse_array();
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};

  tiledb_consolidation_plan_t* consolidation_plan{};
  CHECK(
      TILEDB_OK == tiledb_consolidation_plan_create_with_mbr(
                       ctx_.ptr().get(),
                       array.ptr().get(),
                       1024 * 1024,
                       &consolidation_plan));

  uint64_t num_nodes = 11;
  CHECK(
      TILEDB_OK == tiledb_consolidation_plan_get_num_nodes(
                       ctx_.ptr().get(), consolidation_plan, &num_nodes));
  CHECK(num_nodes == 0);

  uint64_t num_fragments = 11;
  CHECK(
      TILEDB_ERR ==
      tiledb_consolidation_plan_get_num_fragments(
          ctx_.ptr().get(), consolidation_plan, 0, &num_fragments));
  CHECK(num_fragments == 11);
  check_last_error(
      "Error: ConsolidationPlan: Trying to access a node that doesn't exist.");

  const char* frag_uri = nullptr;
  CHECK(
      TILEDB_ERR == tiledb_consolidation_plan_get_fragment_uri(
                        ctx_.ptr().get(), consolidation_plan, 0, 0, &frag_uri));
  CHECK(frag_uri == nullptr);
  check_last_error(
      "Error: ConsolidationPlan: Trying to access a node that doesn't exist.");

  tiledb_consolidation_plan_free(&consolidation_plan);
}

TEST_CASE_METHOD(
    ConsolidationPlanFx,
    "CAPI: Consolidation plan dump",
    "[capi][consolidation-plan][dump]") {
  create_sparse_array();
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};

  tiledb_consolidation_plan_t* consolidation_plan{};
  CHECK(
      TILEDB_OK == tiledb_consolidation_plan_create_with_mbr(
                       ctx_.ptr().get(),
                       array.ptr().get(),
                       1024 * 1024,
                       &consolidation_plan));

  // Check dump
  char* str = nullptr;
  tiledb_consolidation_plan_dump_json_str(
      ctx_.ptr().get(), consolidation_plan, &str);

  std::string plan(str);
  CHECK(plan == "{\n  \"nodes\": [\n  ]\n}\n");

  tiledb_consolidation_plan_free_json_str(&str);
  tiledb_consolidation_plan_free(&consolidation_plan);
}