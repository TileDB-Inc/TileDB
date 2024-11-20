/**
 * @file test-cppapi-consolidation-plan.cc
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
#include "tiledb/api/c_api/buffer/buffer_api_internal.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/serialization/consolidation.h"

#include <test/support/tdb_catch.h>

using namespace tiledb;
using namespace tiledb::api;
using namespace tiledb::test;

struct CppConsolidationPlanFx {
  // Constants.
  const char* SPARSE_ARRAY_NAME = "test_consolidation_plan_array";

  // TileDB context.
  Context ctx_;
  VFS vfs_;
  Config cfg_;

  // Constructors/destructors.
  CppConsolidationPlanFx();
  ~CppConsolidationPlanFx();

  // Functions.
  void create_sparse_array(bool allows_dups = false);
  std::string write_sparse(
      std::vector<int> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp);
  void remove_sparse_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
  void check_last_error(std::string expected);
  void validate_plan(
      uint64_t fragment_size,
      const Array& array,
      ConsolidationPlan& plan,
      std::vector<std::vector<std::string>> expected_plan);
  tiledb::sm::ConsolidationPlan call_handler(
      uint64_t fragment_size,
      const Array& array,
      tiledb::sm::SerializationType stype);
};

CppConsolidationPlanFx::CppConsolidationPlanFx()
    : vfs_(ctx_) {
  cfg_.set("sm.consolidation.buffer_size", "1000");
  ctx_ = Context(cfg_);
  vfs_ = VFS(ctx_);

  remove_sparse_array();
}

CppConsolidationPlanFx::~CppConsolidationPlanFx() {
  remove_sparse_array();
}

void CppConsolidationPlanFx::create_sparse_array(bool allows_dups) {
  // Create dimensions.
  auto d1 = Dimension::create<uint64_t>(ctx_, "d1", {{1, 999}}, 2);
  auto d2 = Dimension::create<uint64_t>(ctx_, "d2", {{1, 999}}, 2);

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

  Array::create(SPARSE_ARRAY_NAME, schema);
}

std::string CppConsolidationPlanFx::write_sparse(
    std::vector<int> a1,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp) {
  // Open array.
  std::unique_ptr<Array> array;
  array = std::make_unique<Array>(
      ctx_,
      SPARSE_ARRAY_NAME,
      TILEDB_WRITE,
      TemporalPolicy(TimestampStartEnd, 0, timestamp));

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

  return tiledb::sm::URI(query.fragment_uri(0)).last_path_part();
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

tiledb::sm::ConsolidationPlan CppConsolidationPlanFx::call_handler(
    uint64_t fragment_size,
    const Array& array,
    tiledb::sm::SerializationType stype) {
  auto req_buf = make_handle<tiledb_buffer_handle_t>(
      ctx_.ptr().get()->resources().serialization_memory_tracker());
  auto resp_buf = make_handle<tiledb_buffer_handle_t>(
      ctx_.ptr().get()->resources().serialization_memory_tracker());

  tiledb::sm::serialization::serialize_consolidation_plan_request(
      fragment_size, cfg_.ptr()->config(), stype, req_buf->buffer());
  auto rval = tiledb_handle_consolidation_plan_request(
      ctx_.ptr().get(),
      array.ptr().get(),
      static_cast<tiledb_serialization_type_t>(stype),
      req_buf,
      resp_buf);
  REQUIRE(rval == TILEDB_OK);

  auto fragments_per_node =
      tiledb::sm::serialization::deserialize_consolidation_plan_response(
          stype, resp_buf->buffer());
  // construct consolidation plan from the members we got from serialization
  return tiledb::sm::ConsolidationPlan(fragment_size, fragments_per_node);
}

void CppConsolidationPlanFx::validate_plan(
    [[maybe_unused]] uint64_t fragment_size,
    [[maybe_unused]] const Array& array,
    ConsolidationPlan& plan,
    std::vector<std::vector<std::string>> expected_plan) {
#ifdef TILEDB_SERIALIZATION
  auto stype = GENERATE(
      tiledb::sm::SerializationType::JSON,
      tiledb::sm::SerializationType::CAPNP);
  auto deserialized_plan = call_handler(fragment_size, array, stype);
  // Now the two plans should be exactly the same.
  REQUIRE(plan.dump() == deserialized_plan.dump());
#endif

  // Take all the nodes in the plan, make a string out of them, the string will
  // be the sorted fragment URIs.
  std::vector<std::string> string_plan(plan.num_nodes());
  for (size_t n = 0; n < plan.num_nodes(); n++) {
    std::vector<std::string> node_uris;
    node_uris.reserve(plan.num_fragments(n));
    for (size_t f = 0; f < plan.num_fragments(n); f++) {
      node_uris.emplace_back(plan.fragment_uri(n, f));
    }

    std::sort(node_uris.begin(), node_uris.end());
    for (size_t f = 0; f < plan.num_fragments(n); f++) {
      string_plan[n] += node_uris[f];
    }
  }

  // Sort the node strings.
  std::sort(string_plan.begin(), string_plan.end());

  // Now generate the same for the expected plan.
  std::vector<std::string> expected_string_plan(expected_plan.size());
  for (size_t n = 0; n < expected_plan.size(); n++) {
    std::vector<std::string> node_uris;
    node_uris.reserve(expected_plan[n].size());
    for (size_t f = 0; f < expected_plan[n].size(); f++) {
      node_uris.emplace_back(expected_plan[n][f]);
    }

    std::sort(node_uris.begin(), node_uris.end());
    for (size_t f = 0; f < plan.num_fragments(n); f++) {
      expected_string_plan[n] += node_uris[f];
    }
  }

  // Sort the node strings.
  std::sort(expected_string_plan.begin(), expected_string_plan.end());

  // Now the two plans should be exactly the same.
  CHECK(string_plan == expected_string_plan);
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan",
    "[cppapi][consolidation-plan]") {
  create_sparse_array();
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 1024 * 1024);

  uint64_t num_nodes = consolidation_plan.num_nodes();
  CHECK(num_nodes == 0);

  CHECK_THROWS_WITH(
      consolidation_plan.num_fragments(0),
      "Error: ConsolidationPlan: Trying to access a node that doesn't exist.");

  CHECK_THROWS_WITH(
      consolidation_plan.fragment_uri(0, 0),
      "Error: ConsolidationPlan: Trying to access a node that doesn't exist.");
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan dump",
    "[cppapi][consolidation-plan][dump]") {
  create_sparse_array();
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);

  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 1024 * 1024);

  // Check dump.
  CHECK(consolidation_plan.dump() == "{\n  \"nodes\": [\n  ]\n}\n");
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan, de-interleave 1",
    "[cppapi][consolidation-plan][de-interleave-1]") {
  create_sparse_array();

  // Write one fragment with NED [1, 4][1, 4] and one with NED [2, 6][2, 6].
  // Since they intersect, they should be batched together.
  auto uri1 = write_sparse({0, 1}, {1, 4}, {1, 4}, 1);
  auto uri2 = write_sparse({2, 3}, {2, 6}, {2, 6}, 2);

  // Create a consolidation plan with max fragment size of 1. That way, the
  // two fragments above don't risk getting batched together when combining
  // small fragments.
  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 1);

  // Validate the plan.
  validate_plan(1, array, consolidation_plan, {{uri1, uri2}});
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan, de-interleave 2",
    "[cppapi][consolidation-plan][de-interleave-2]") {
  create_sparse_array();

  // Write one fragment with NED [1, 4][1, 4] and one with NED [2, 6][2, 6].
  // Since they intersect, they should be batched together.
  auto uri1 = write_sparse({0, 1}, {1, 4}, {1, 4}, 1);
  auto uri2 = write_sparse({2, 3}, {2, 6}, {2, 6}, 2);

  // Write one fragment with NED [10, 14][10, 14] and one with NED [12, 16][12,
  // 16]. Since they intersect, they should be batched together.
  auto uri3 = write_sparse({4, 5}, {10, 14}, {10, 14}, 3);
  auto uri4 = write_sparse({6, 7}, {12, 16}, {12, 16}, 4);

  // Create a consolidation plan with max fragment size of 1. That way, the
  // two fragments above don't risk getting batched together when combining
  // small fragments.
  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 1);

  // Validate the plan.
  validate_plan(1, array, consolidation_plan, {{uri1, uri2}, {uri3, uri4}});
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan, de-interleave 3",
    "[cppapi][consolidation-plan][de-interleave-3]") {
  create_sparse_array();

  // Write one fragment with NED [1, 4][1, 4] and one with NED [4, 6][4, 6].
  // Since they intersect, they should be batched together.
  auto uri1 = write_sparse({0, 1}, {1, 4}, {1, 4}, 1);
  auto uri2 = write_sparse({2, 3}, {4, 6}, {4, 6}, 2);

  // Write one fragment with NED [1, 1][6, 6]. It doesn't intersect any of the
  // original fragments but intersects the combination, so they should all get
  // batched together.
  auto uri3 = write_sparse({4, 5}, {4, 6}, {4, 6}, 3);

  // Create a consolidation plan with max fragment size of 1. That way, the
  // two fragments above don't risk getting batched together when combining
  // small fragments.
  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 1);

  // Validate the plan.
  validate_plan(1, array, consolidation_plan, {{uri1, uri2, uri3}});
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan, split 1",
    "[cppapi][consolidation-plan][split-1]") {
  create_sparse_array(true);

  // Write one large frarment of ~200k and one small of roughly 4k.
  std::vector<int> a1(10 * 1024);
  std::vector<uint64_t> d(10 * 1024);
  std::fill(a1.begin(), a1.end(), 1);
  std::fill(d.begin(), d.end(), 1);
  auto uri1 = write_sparse(a1, d, d, 1);
  auto uri2 = write_sparse({0, 1}, {2, 4}, {2, 4}, 2);

  // Create a consolidation plan with max fragment size of 10k. That way, only
  // the first fragment needs to be split.
  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 10 * 1024);

  // Validate the plan.
  validate_plan(10 * 1024, array, consolidation_plan, {{uri1}});
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan, combine small 1",
    "[cppapi][consolidation-plan][combine-small-1]") {
  create_sparse_array(true);

  // Write one fragment with NED [1, 2][1, 2] and one with NED [3, 4][3, 4].
  auto uri1 = write_sparse({0, 1}, {1, 2}, {1, 2}, 1);
  auto uri2 = write_sparse({2, 3}, {3, 4}, {3, 4}, 2);

  // Create a consolidation plan with max fragment size of 100k. That way, the
  // smaller fragments are considered for combining.
  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 100 * 1024);

  // Validate the plan.
  validate_plan(100 * 1024, array, consolidation_plan, {{uri1, uri2}});
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan, combine small 2",
    "[cppapi][consolidation-plan][combine-small-2]") {
  create_sparse_array(true);

  // Write one fragment with NED [1, 2][1, 2] and one with NED [5, 6][5, 6]. In
  // the middle at [3, 3], we add a large fragment which will prevent
  // consolidation.
  auto uri1 = write_sparse({0, 1}, {1, 2}, {1, 2}, 1);

  std::vector<int> a1(10 * 1024);
  std::vector<uint64_t> d(10 * 1024);
  std::fill(a1.begin(), a1.end(), 2);
  std::fill(d.begin(), d.end(), 3);
  auto uri2 = write_sparse(a1, d, d, 2);

  auto uri3 = write_sparse({3, 4}, {5, 6}, {5, 6}, 3);

  // Create a consolidation plan with max fragment size of 100k. That way, the
  // smaller fragments are considered for combining.
  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 100 * 1024);

  // Validate the plan, we should only have a node for the large fragment to be
  // split.
  validate_plan(100 * 1024, array, consolidation_plan, {{uri2}});
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan, combine small 3",
    "[cppapi][consolidation-plan][combine-small-3]") {
  create_sparse_array(true);

  // Write one fragment with NED [1, 2][1, 2] and one with NED [5, 6][5, 6]. In
  // the middle at [8, 8], we add a large fragment. This time it will not
  // prevent consolidation.
  auto uri1 = write_sparse({0, 1}, {1, 2}, {1, 2}, 1);

  std::vector<int> a1(10 * 1024);
  std::vector<uint64_t> d(10 * 1024);
  std::fill(a1.begin(), a1.end(), 2);
  std::fill(d.begin(), d.end(), 8);
  auto uri2 = write_sparse(a1, d, d, 2);

  auto uri3 = write_sparse({3, 4}, {5, 6}, {5, 6}, 3);

  // Create a consolidation plan with max fragment size of 100k. That way, the
  // smaller fragments are considered for combining.
  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 100 * 1024);

  // Validate the plan, we should only have a node for the large fragment to be
  // split.
  validate_plan(100 * 1024, array, consolidation_plan, {{uri1, uri3}, {uri2}});
}

TEST_CASE_METHOD(
    CppConsolidationPlanFx,
    "C++ API: Consolidation plan, complex 1",
    "[cppapi][consolidation-plan][complex 1]") {
  create_sparse_array(true);

  // Write one fragment with NED [11, 14][11, 14] and one with NED [14, 16][14,
  // 16]. Since they intersect, they should be batched together.
  auto uri1 = write_sparse({0, 1}, {11, 14}, {11, 14}, 1);
  auto uri2 = write_sparse({2, 3}, {14, 16}, {14, 16}, 2);

  // Write one fragment with NED [11, 11][16, 16]. It doesn't intersect any of
  // the original fragments but intersects the combination, so they should all
  // get batched together. Make this fragment big so the first three fragments
  // together won't be considered a small fragment later.
  std::vector<int> a1(10 * 1024);
  std::vector<uint64_t> d1(10 * 1024);
  std::vector<uint64_t> d2(10 * 1024);
  std::fill(a1.begin(), a1.end(), 4);
  std::fill(d1.begin(), d1.end(), 11);
  std::fill(d2.begin(), d2.end(), 16);
  auto uri3 = write_sparse(a1, d1, d2, 3);

  // Write one small fragment with NED [1, 1][1, 1] It will not be batched with
  // any others as it would intersect others.
  auto uri4 = write_sparse({5, 6}, {1, 1}, {1, 1}, 4);

  // Write one large fragment with NED [2, 2][2, 2]. It should get split by
  // itself.
  std::fill(a1.begin(), a1.end(), 7);
  std::fill(d1.begin(), d1.end(), 2);
  std::fill(d2.begin(), d2.end(), 2);
  auto uri5 = write_sparse(a1, d1, d2, 5);

  // Write two small fragment with NED [20, 20][20, 20] and [30, 30][30, 30].
  // They should get batched together.
  auto uri6 = write_sparse({8, 9}, {20, 20}, {20, 20}, 6);
  auto uri7 = write_sparse({10, 11}, {30, 30}, {30, 30}, 7);

  // Create a consolidation plan with max fragment size of 100k. That way, the
  // smaller fragments are considered for combining.
  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  ConsolidationPlan consolidation_plan(ctx_, array, 100 * 1024);

  // Validate the plan.
  validate_plan(
      100 * 1024,
      array,
      consolidation_plan,
      {{uri1, uri2, uri3}, {uri6, uri7}, {uri5}});
}
