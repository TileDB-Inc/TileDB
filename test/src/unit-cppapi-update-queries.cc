/**
 * @file   unit-cppapi-updates.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests the C++ API for update value related functions.
 */

#include <test/support/tdb_catch.h>
#include "test/src/ast_helpers.h"
#include "test/src/helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/update_value.h"

using namespace tiledb;

class UpdateValue {
 public:
  UpdateValue(const std::string& field_name, uint64_t val)
      : field_name_(field_name)
      , val_(sizeof(uint64_t)) {
    memcpy(val_.data(), &val, sizeof(uint64_t));
  }

  std::string field_name() {
    return field_name_;
  }

  void* val() {
    return val_.data();
  }

  size_t val_size() {
    return val_.size();
  }

 private:
  std::string field_name_;
  std::vector<uint8_t> val_;
};

struct UpdatesFx {
  // Constants.
  const char* SPARSE_ARRAY_NAME = "test_updates_array";

  // TileDB context.
  Context ctx_;
  VFS vfs_;
  sm::StorageManager* sm_;

  std::string key_ = "0123456789abcdeF0123456789abcdeF";
  const tiledb_encryption_type_t enc_type_ = TILEDB_AES_256_GCM;

  // Constructors/destructors.
  UpdatesFx();
  ~UpdatesFx();

  // Functions.
  void create_sparse_array(bool allows_dups = false, bool encrypt = false);
  void write_update_condition(
      QueryCondition& qc,
      std::vector<UpdateValue>& uvs,
      uint64_t timestamp,
      bool encrypt = false,
      bool error_expected = false);
  void check_update_conditions(
      std::vector<QueryCondition> qcs,
      std::vector<std::vector<UpdateValue>> uvs,
      uint64_t timestamp,
      bool encrypt = false);
  void remove_sparse_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

UpdatesFx::UpdatesFx()
    : vfs_(ctx_) {
  Config config;
  config.set("sm.consolidation.buffer_size", "1000");
  config["sm.allow_updates_experimental"] = "true";
  ctx_ = Context(config);
  sm_ = ctx_.ptr().get()->storage_manager();
  vfs_ = VFS(ctx_);
}

UpdatesFx::~UpdatesFx() {
}

void UpdatesFx::create_sparse_array(bool allows_dups, bool encrypt) {
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

void UpdatesFx::write_update_condition(
    QueryCondition& qc,
    std::vector<UpdateValue>& uvs,
    uint64_t timestamp,
    bool encrypt,
    bool error_expected) {
  // Open array.
  std::unique_ptr<Array> array;
  if (encrypt) {
    array = std::make_unique<Array>(
        ctx_,
        SPARSE_ARRAY_NAME,
        TILEDB_UPDATE,
        enc_type_,
        std::string(key_),
        timestamp);
  } else {
    array = std::make_unique<Array>(
        ctx_, SPARSE_ARRAY_NAME, TILEDB_UPDATE, timestamp);
  }

  // Create query.
  Query query(ctx_, *array, TILEDB_UPDATE);

  query.set_condition(qc);
  for (auto& uv : uvs) {
    QueryExperimental::add_update_value_to_query(
        ctx_, query, uv.field_name().c_str(), uv.val(), uv.val_size());
  }

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

void UpdatesFx::check_update_conditions(
    std::vector<QueryCondition> qcs,
    std::vector<std::vector<UpdateValue>> uvs,
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
  auto array_ptr = array->ptr()->array_;

  // Load delete conditions.
  auto&& [st, conditions, update_values] =
      sm_->load_delete_and_update_conditions(*array_ptr);
  REQUIRE(st.ok());
  REQUIRE(conditions->size() == qcs.size());

  for (uint64_t i = 0; i < qcs.size(); i++) {
    // Compare to negated condition.
    auto cmp = qcs[i].ptr()->query_condition_->negated_condition();
    CHECK(tiledb::test::ast_equal(conditions->at(i).ast(), cmp.ast()));

    auto& loaded_update_values = update_values->at(i);
    for (uint64_t j = 0; j < uvs[i].size(); j++) {
      CHECK(uvs[i][j].field_name() == loaded_update_values[j].field_name());
    }
  }

  array->close();
}

void UpdatesFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  vfs_.remove_dir(array_name);
}

void UpdatesFx::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

bool UpdatesFx::is_array(const std::string& array_name) {
  return vfs_.is_dir(array_name);
}

TEST_CASE("C++ API: Test setting an update value", "[cppapi][updates]") {
  const std::string array_name = "cpp_unit_update_values";
  Config config;
  config["sm.allow_updates_experimental"] = "true";
  Context ctx(config);
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array and query.
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);
  Array array(ctx, array_name, TILEDB_UPDATE);
  Query query(ctx, array);

  // Set update value.
  int val = 1;
  QueryExperimental::add_update_value_to_query(
      ctx, query, "a", &val, sizeof(val));

  query.ptr()->query_->update_values()[0].check(
      array.ptr()->array_->array_schema_latest());

  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE_METHOD(
    UpdatesFx,
    "CPP API: Test writing update condition",
    "[cppapi][updates][write-check]") {
  remove_sparse_array();

  bool encrypt = GENERATE(true, false);
  create_sparse_array(false, encrypt);

  // Define query condition (a1 < 4).
  QueryCondition qc(ctx_);
  int32_t val = 4;
  qc.init("a1", &val, sizeof(int32_t), TILEDB_LT);

  // Define update values for first condition.
  std::vector<UpdateValue> uvs;
  uvs.emplace_back("a1", 1);

  // Define query condition (a1 > 8).
  QueryCondition qc2(ctx_);
  int32_t val2 = 8;
  qc2.init("a1", &val2, sizeof(int32_t), TILEDB_GT);

  // Define update values for second condition.
  std::vector<UpdateValue> uvs2;
  uvs2.emplace_back("a1", 2);
  uvs2.emplace_back("d1", 1);

  write_update_condition(qc, uvs, 1, encrypt);
  check_update_conditions({qc}, {uvs}, 2, encrypt);

  write_update_condition(qc2, uvs2, 3, encrypt);
  check_update_conditions({qc}, {uvs}, 2, encrypt);
  check_update_conditions({qc, qc2}, {uvs, uvs2}, 4, encrypt);

  remove_sparse_array();
}