/**
 * @file   unit-capi-serialized_queries.cc
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
 * Tests for query serialization/deserialization.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/query/legacy/reader.h"
#include "tiledb/sm/query/writers/writer_base.h"
#include "tiledb/sm/serialization/query.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <any>
#include <cassert>
#include <map>

using namespace tiledb;
using ResultSetType = std::map<std::string, std::any>;

namespace {

template <class T>
bool check_result(const T a, const T b, size_t start, size_t end) {
  auto a_exp = T(a.begin() + start, a.begin() + end);
  auto b_exp = T(b.begin() + start, b.begin() + end);
  return a_exp == b_exp;
}

template <class TResult, class TExpected>
bool check_result(
    const TResult a,
    const TExpected b,
    optional<size_t> start = nullopt,
    optional<size_t> end = nullopt) {
  TResult b_typed;
  if constexpr (std::is_same<TExpected, std::any>::value) {
    b_typed = std::any_cast<TResult>(b);
  } else {
    b_typed = b;
  }
  if (start.has_value()) {
    assert(end.has_value());
    return check_result(a, b_typed, *start, *end);
  } else {
    return check_result(a, b_typed, 0, b_typed.size());
  }
}

struct SerializationFx {
  test::VFSTestSetup vfs_test_setup_;
  tiledb_ctx_t* ctx_;
  Context ctx;
  const std::string array_uri;

  SerializationFx()
      : ctx_{vfs_test_setup_.ctx_c}
      , ctx{vfs_test_setup_.ctx()}
      , array_uri{vfs_test_setup_.array_uri("testarray")} {
  }

  static void check_read_stats(const Query& query) {
    auto stats = ((sm::WriterBase*)query.ptr()->query_->strategy())->stats();
    REQUIRE(stats != nullptr);
    auto counters = stats->counters();
    REQUIRE(counters != nullptr);
    auto loop_num =
        counters->find("Context.Query.Reader.loop_num");
    REQUIRE((loop_num != counters->end()));
    REQUIRE(loop_num->second > 0);
  }

  static void check_write_stats(const Query& query) {
    auto stats = ((sm::Reader*)query.ptr()->query_->strategy())->stats();
    REQUIRE(stats != nullptr);
    auto counters = stats->counters();
    REQUIRE(counters != nullptr);
    auto loop_num =
        counters->find("Context.Query.Writer.attr_num");
    REQUIRE((loop_num != counters->end()));
    REQUIRE(loop_num->second > 0);
  }

  static void check_subarray_stats(int dim0_expected, int dim1_expected) {
    Stats::enable();
    std::string stats;
    Stats::dump(&stats);
    // Note: if these checks fail, use Stats::dump(stdout) to validate counters
    CHECK(
        stats.find(
            "\"Context.subSubarray.add_range_dim_0\": " +
            std::to_string(dim0_expected)) != std::string::npos);
    CHECK(
        stats.find(
            "\"Context.subSubarray.add_range_dim_1\": " +
            std::to_string(dim1_expected)) != std::string::npos);
    Stats::disable();
  }

  static void check_delete_stats(const Query& query) {
    auto stats = ((sm::Reader*)query.ptr()->query_->strategy())->stats();
    REQUIRE(stats != nullptr);
    auto counters = stats->counters();
    REQUIRE(counters != nullptr);
    auto dowork_num = counters->find(
        "Context.Query.Deletes.dowork.timer_count");
    REQUIRE((dowork_num != counters->end()));
    REQUIRE(dowork_num->second > 0);
  }

  void create_array(tiledb_array_type_t type) {
    ArraySchema schema(ctx, type);
    Domain domain(ctx);
    domain.add_dimension(Dimension::create<int32_t>(ctx, "d1", {0, 100}, 2))
        .add_dimension(Dimension::create<int32_t>(ctx, "d2", {0, 10}, 2));
    schema.set_domain(domain);

    schema.add_attribute(Attribute::create<uint32_t>(ctx, "a1"));
    schema.add_attribute(
        Attribute::create<std::array<uint32_t, 2>>(ctx, "a2").set_nullable(
            true));
    schema.add_attribute(Attribute::create<std::vector<char>>(ctx, "a3"));

    Array::create(array_uri, schema);
  }

  ResultSetType write_dense_array() {
    std::vector<int32_t> subarray = {1, 10, 1, 10};
    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<uint8_t> a2_nullable;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells =
        (subarray[1] - subarray[0] + 1) * (subarray[3] - subarray[2] + 1);
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);
      a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    ResultSetType results;
    results["a1"] = a1;
    results["a2"] = a2;
    results["a2_nullable"] = a2_nullable;
    results["a3_data"] = a3_data;
    results["a3_offsets"] = a3_offsets;

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.set_subarray(subarray);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();

    // The deserialized query should also include the write stats
    check_write_stats(query);

    return results;
  }

  void write_dense_array_ranges() {
    std::vector<int32_t> subarray = {1, 10, 1, 10};
    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<uint8_t> a2_nullable;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells =
        (subarray[1] - subarray[0] + 1) * (subarray[3] - subarray[2] + 1);
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);
      a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.add_range(0, subarray[0], subarray[1]);
    cppapi_subarray.add_range(1, subarray[2], subarray[3]);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();

    // The deserialized query should also include the write stats
    check_write_stats(query);
  }

  void write_sparse_array() {
    std::vector<int32_t> d1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<int32_t> d2 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<uint8_t> a2_nullable;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells = 10;
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);
      a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_layout(TILEDB_UNORDERED);
    query.set_data_buffer("d1", d1);
    query.set_data_buffer("d2", d2);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();

    // The deserialized query should also include the write stats
    check_write_stats(query);
  }

  void write_sparse_delete() {
    Array array(ctx, array_uri, TILEDB_DELETE);
    Query query(ctx, array);

    // Define query condition (a1 < 5).
    QueryCondition qc(ctx);
    int32_t val = 5;
    qc.init("a1", &val, sizeof(int32_t), TILEDB_LT);
    query.set_condition(qc);

    // Submit query
    query.submit();

    // The deserialized query should also include the delete stats
    check_delete_stats(query);
  }

  void write_sparse_array_split_coords() {
    std::vector<int32_t> d1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    std::vector<int32_t> d2 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<uint8_t> a2_nullable;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells = 10;
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);
      a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_layout(TILEDB_UNORDERED);
    query.set_data_buffer("d1", d1);
    query.set_data_buffer("d2", d2);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();

    // The deserialized query should also include the write stats
    check_write_stats(query);
  }
};

}  // namespace

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, dense",
    "[query][dense][serialization][rest]") {
  create_array(TILEDB_DENSE);
  auto expected_results = write_dense_array();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(500);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.set_subarray(subarray);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    // The deserialized query should also include the read stats
    check_read_stats(query);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 100);
    REQUIRE(std::get<1>(result_el["a2"]) == 200);
    REQUIRE(std::get<2>(result_el["a2"]) == 100);
    REQUIRE(std::get<0>(result_el["a3"]) == 100);
    REQUIRE(std::get<1>(result_el["a3"]) == 5050);

    REQUIRE(check_result(a1, expected_results["a1"]));
    REQUIRE(check_result(a2, expected_results["a2"]));
    REQUIRE(check_result(a2_nullable, expected_results["a2_nullable"]));
    REQUIRE(check_result(a3_data, expected_results["a3_data"]));
    REQUIRE(check_result(a3_offsets, expected_results["a3_offsets"]));
  }

  SECTION("- Read all, with condition") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(500);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.set_subarray(subarray);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    uint32_t cmp_value = 5;
    QueryCondition condition(ctx);
    condition.init("a1", &cmp_value, sizeof(uint32_t), TILEDB_LT);
    query.set_condition(condition);

    // Submit query
    query.submit();
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    // The deserialized query should also include the read stats
    check_read_stats(query);

    // We expect all cells where `a1` >= `cmp_value` to be filtered
    // out. For the refactored reader, filtered out means the value is
    // replaced with the fill value.
    auto result_el = query.result_buffer_elements_nullable();
    if (test::use_refactored_dense_reader()) {
      REQUIRE(std::get<1>(result_el["a1"]) == 100);
      REQUIRE(std::get<1>(result_el["a2"]) == 200);
      REQUIRE(std::get<2>(result_el["a2"]) == 100);
      REQUIRE(std::get<0>(result_el["a3"]) == 100);
      REQUIRE(std::get<1>(result_el["a3"]) == 110);

      auto null_val = std::numeric_limits<uint32_t>::max();
      for (uint64_t i = 5; i < 100; i++) {
        REQUIRE(a1[i] == null_val);
        REQUIRE(a2[i * 2] == null_val);
        REQUIRE(a2[i * 2 + 1] == null_val);
        REQUIRE(a2_nullable[i] == 0);
        REQUIRE(a3_offsets[i] == 10 + i);
        REQUIRE(a3_data[10 + i] == 0);
      }
    } else {
      REQUIRE(std::get<1>(result_el["a1"]) == 5);
      REQUIRE(std::get<1>(result_el["a2"]) == 10);
      REQUIRE(std::get<2>(result_el["a2"]) == 5);
      REQUIRE(std::get<0>(result_el["a3"]) == 5);
      REQUIRE(std::get<1>(result_el["a3"]) == 15);
    }

    REQUIRE(check_result(a1, expected_results["a1"], 0, 5));
    REQUIRE(check_result(a2, expected_results["a2"], 0, 10));
    REQUIRE(check_result(a2_nullable, expected_results["a2_nullable"], 0, 5));
    REQUIRE(check_result(a3_data, expected_results["a3_data"], 0, 15));
    REQUIRE(check_result(a3_offsets, expected_results["a3_offsets"], 0, 5));
  }

  SECTION("- Read subarray") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(500);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {3, 4, 3, 4};

    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.set_subarray(subarray);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    // The deserialized query should also include the read stats
    check_read_stats(query);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 4);
    REQUIRE(std::get<1>(result_el["a2"]) == 8);
    REQUIRE(std::get<2>(result_el["a2"]) == 4);
    REQUIRE(std::get<0>(result_el["a3"]) == 4);
    REQUIRE(std::get<1>(result_el["a3"]) == 114);

    a1.resize(4);
    a2.resize(8);
    a2_nullable.resize(4);
    a3_offsets.resize(4);
    a3_data.resize(114);
    auto a3_exp = std::any_cast<decltype(a3_data)>(expected_results["a3_data"]);
    a3_exp.resize(114);
    REQUIRE(a1 == decltype(a1)({22, 23, 32, 33}));
    REQUIRE(a2 == decltype(a2)({22, 44, 23, 46, 32, 64, 33, 66}));
    REQUIRE(
        a2_nullable == decltype(a2_nullable)({'\x01', '\x01', '\x01', '\x01'}));
    REQUIRE(a3_data == a3_exp);
    REQUIRE(a3_offsets == decltype(a3_offsets)({0, 23, 47, 80}));
  }

  SECTION("- Incomplete read") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(4);
    std::vector<uint32_t> a2(4);
    std::vector<uint8_t> a2_nullable(4);
    std::vector<char> a3_data(60);
    std::vector<uint64_t> a3_offsets(4);
    std::vector<int32_t> subarray = {3, 4, 3, 4};
    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.set_subarray(subarray);
    query.set_subarray(cppapi_subarray);

    auto set_buffers = [&](Query& q) {
      q.set_data_buffer("a1", a1);
      q.set_data_buffer("a2", a2);
      q.set_validity_buffer("a2", a2_nullable);
      q.set_data_buffer("a3", a3_data);
      q.set_offsets_buffer("a3", a3_offsets);
    };

    // Submit initial query.
    set_buffers(query);
    query.submit();
    // The deserialized query should also include the read stats
    check_read_stats(query);
    REQUIRE(query.query_status() == Query::Status::INCOMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 2);
    REQUIRE(std::get<1>(result_el["a2"]) == 4);
    REQUIRE(std::get<2>(result_el["a2"]) == 2);
    REQUIRE(std::get<0>(result_el["a3"]) == 2);
    REQUIRE(std::get<1>(result_el["a3"]) == 47);

    // Reset buffers, serialize and resubmit
    set_buffers(query);
    query.submit();
    // The deserialized query should also include the read stats
    check_read_stats(query);

    REQUIRE(query.query_status() == Query::Status::INCOMPLETE);
    result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 1);
    REQUIRE(std::get<1>(result_el["a2"]) == 2);
    REQUIRE(std::get<2>(result_el["a2"]) == 1);
    REQUIRE(std::get<0>(result_el["a3"]) == 1);
    REQUIRE(std::get<1>(result_el["a3"]) == 33);

    // TODO: check results

    // Reset buffers, serialize and resubmit
    set_buffers(query);
    query.submit();
    // The deserialized query should also include the read stats
    check_read_stats(query);

    REQUIRE(query.query_status() == Query::Status::COMPLETE);
    result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 1);
    REQUIRE(std::get<1>(result_el["a2"]) == 2);
    REQUIRE(std::get<2>(result_el["a2"]) == 1);
    REQUIRE(std::get<0>(result_el["a3"]) == 1);
    REQUIRE(std::get<1>(result_el["a3"]) == 34);

    // TODO: check results
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, sparse",
    "[query][sparse][serialization][rest]") {
  create_array(TILEDB_SPARSE);
  write_sparse_array();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.set_subarray(subarray);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();

    // The deserialized query should also include the read stats
    check_read_stats(query);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 10);
    REQUIRE(std::get<1>(result_el["a2"]) == 20);
    REQUIRE(std::get<2>(result_el["a2"]) == 10);
    REQUIRE(std::get<0>(result_el["a3"]) == 10);
    REQUIRE(std::get<1>(result_el["a3"]) == 55);

    // TODO: check results
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, sparse, old client",
    "[query][sparse][serialization][old-client][rest]") {
  create_array(TILEDB_SPARSE);
  write_sparse_array();

  Config config;
  config.set("sm.query.sparse_global_order.reader", "legacy");
  config.set("sm.query.sparse_unordered_with_dups.reader", "legacy");

  vfs_test_setup_.update_config(config.ptr().get());
  ctx_ = vfs_test_setup_.ctx_c;
  auto ctx_client = vfs_test_setup_.ctx();

  SECTION("- Read all") {
    Array array(ctx_client, array_uri, TILEDB_READ);
    Query query(ctx_client, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    query.set_layout(TILEDB_GLOBAL_ORDER);
    Subarray cppapi_subarray(ctx_client, array);
    cppapi_subarray.set_subarray(subarray);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    // The deserialized query should also include the read stats
    check_read_stats(query);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 10);
    REQUIRE(std::get<1>(result_el["a2"]) == 20);
    REQUIRE(std::get<2>(result_el["a2"]) == 10);
    REQUIRE(std::get<0>(result_el["a3"]) == 10);
    REQUIRE(std::get<1>(result_el["a3"]) == 55);

    // TODO: check results
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, split coords, sparse",
    "[query][sparse][serialization][split-coords]") {
  create_array(TILEDB_SPARSE);
  write_sparse_array_split_coords();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<int32_t> coords(1000);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.set_subarray(subarray);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("__coords", coords);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    // The deserialized query should also include the read stats
    check_read_stats(query);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el[tiledb::test::TILEDB_COORDS]) == 20);
    REQUIRE(std::get<1>(result_el["a1"]) == 10);
    REQUIRE(std::get<1>(result_el["a2"]) == 20);
    REQUIRE(std::get<2>(result_el["a2"]) == 10);
    REQUIRE(std::get<0>(result_el["a3"]) == 10);
    REQUIRE(std::get<1>(result_el["a3"]) == 55);

    // TODO: check results
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, dense ranges",
    "[query][dense][serialization][rest]") {
  create_array(TILEDB_DENSE);
  write_dense_array_ranges();
  if (!vfs_test_setup_.is_rest()) {
    check_subarray_stats(1, 1);
  }

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.add_range(0, subarray[0], subarray[1]);
    cppapi_subarray.add_range(1, subarray[2], subarray[3]);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    // The deserialized query should also include the read stats
    check_read_stats(query);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 100);
    REQUIRE(std::get<1>(result_el["a2"]) == 200);
    REQUIRE(std::get<2>(result_el["a2"]) == 100);
    REQUIRE(std::get<0>(result_el["a3"]) == 100);
    REQUIRE(std::get<1>(result_el["a3"]) == 5050);

    // TODO: check results
  }

  SECTION("- Read subarray") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {3, 4, 3, 4};

    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.add_range(0, subarray[0], subarray[1]);
    cppapi_subarray.add_range(1, subarray[2], subarray[3]);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    // The deserialized query should also include the read stats
    check_read_stats(query);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 4);
    REQUIRE(std::get<1>(result_el["a2"]) == 8);
    REQUIRE(std::get<2>(result_el["a2"]) == 4);
    REQUIRE(std::get<0>(result_el["a3"]) == 4);
    REQUIRE(std::get<1>(result_el["a3"]) == 114);

    // TODO: check results
  }

  SECTION("- Incomplete read") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(4);
    std::vector<uint32_t> a2(4);
    std::vector<uint8_t> a2_nullable(4);
    std::vector<char> a3_data(60);
    std::vector<uint64_t> a3_offsets(4);
    std::vector<int32_t> subarray = {3, 4, 3, 4};
    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.add_range(0, subarray[0], subarray[1]);
    cppapi_subarray.add_range(1, subarray[2], subarray[3]);
    query.set_subarray(cppapi_subarray);

    auto set_buffers = [&](Query& q) {
      q.set_data_buffer("a1", a1);
      q.set_data_buffer("a2", a2);
      q.set_validity_buffer("a2", a2_nullable);
      q.set_data_buffer("a3", a3_data);
      q.set_offsets_buffer("a3", a3_offsets);
    };

    // Submit initial query.
    set_buffers(query);
    query.submit();
    // The deserialized query should also include the read stats
    check_read_stats(query);
    REQUIRE(query.query_status() == Query::Status::INCOMPLETE);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 2);
    REQUIRE(std::get<1>(result_el["a2"]) == 4);
    REQUIRE(std::get<2>(result_el["a2"]) == 2);
    REQUIRE(std::get<0>(result_el["a3"]) == 2);
    REQUIRE(std::get<1>(result_el["a3"]) == 47);

    // Reset buffers, serialize and resubmit
    set_buffers(query);
    // Submit query
    query.submit();
    // The deserialized query should also include the read stats
    check_read_stats(query);

    REQUIRE(query.query_status() == Query::Status::INCOMPLETE);
    result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 1);
    REQUIRE(std::get<1>(result_el["a2"]) == 2);
    REQUIRE(std::get<2>(result_el["a2"]) == 1);
    REQUIRE(std::get<0>(result_el["a3"]) == 1);
    REQUIRE(std::get<1>(result_el["a3"]) == 33);

    // Reset buffers, serialize and resubmit
    set_buffers(query);
    // Submit query
    query.submit();
    // The deserialized query should also include the read stats
    check_read_stats(query);

    REQUIRE(query.query_status() == Query::Status::COMPLETE);
    result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 1);
    REQUIRE(std::get<1>(result_el["a2"]) == 2);
    REQUIRE(std::get<2>(result_el["a2"]) == 1);
    REQUIRE(std::get<0>(result_el["a3"]) == 1);
    REQUIRE(std::get<1>(result_el["a3"]) == 34);

    // TODO: check results
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, sparse delete",
    "[query][sparse][delete][serialization][rest]") {
  create_array(TILEDB_SPARSE);
  write_sparse_array();
  write_sparse_delete();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<uint8_t> a2_nullable(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    Subarray cppapi_subarray(ctx, array);
    cppapi_subarray.set_subarray(subarray);
    query.set_subarray(cppapi_subarray);
    query.set_data_buffer("a1", a1);
    query.set_data_buffer("a2", a2);
    query.set_validity_buffer("a2", a2_nullable);
    query.set_data_buffer("a3", a3_data);
    query.set_offsets_buffer("a3", a3_offsets);

    // Submit query
    query.submit();

    // The deserialized query should also include the read stats
    check_read_stats(query);

    auto result_el = query.result_buffer_elements_nullable();
    REQUIRE(std::get<1>(result_el["a1"]) == 5);
    REQUIRE(std::get<1>(result_el["a2"]) == 10);
    REQUIRE(std::get<2>(result_el["a2"]) == 5);
    REQUIRE(std::get<0>(result_el["a3"]) == 5);
    REQUIRE(std::get<1>(result_el["a3"]) == 40);

    // TODO: check results
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Global order writes serialization",
    "[global-order-write][serialization][dense][rest]") {
  uint64_t tile_extent = 2;
  ArraySchema schema(ctx, TILEDB_DENSE);
  Domain domain(ctx);
  domain.add_dimension(
      Dimension::create<uint64_t>(ctx, "d1", {0, 200}, tile_extent));
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create<uint32_t>(ctx, "a1"));
  schema.add_attribute(
      Attribute::create<std::array<uint32_t, 2>>(ctx, "a2").set_nullable(true));
  schema.add_attribute(Attribute::create<std::vector<char>>(ctx, "a3"));
  Array::create(array_uri, schema);

  // Build input data
  uint64_t ncells = 100;
  // This needs to be tile-aligned
  uint64_t chunk_size = 4;

  std::vector<uint32_t> a1;
  std::vector<uint32_t> a2;
  std::vector<uint8_t> a2_nullable;
  std::vector<char> a3_data;
  std::vector<uint64_t> a3_offsets;
  for (uint64_t i = 0; i < ncells; i++) {
    a1.push_back(i);
    a2.push_back(i);
    a2.push_back(2 * i);
    a2_nullable.push_back(a2.back() % 5 == 0 ? 0 : 1);
    std::string a3 = "abcd";
    a3_offsets.push_back(i % chunk_size * a3.size());
    a3_data.insert(a3_data.end(), a3.begin(), a3.end());
  }

  Array array(ctx, array_uri, TILEDB_WRITE);
  Query query(ctx, array);
  Subarray subarray(ctx, array);
  query.set_layout(TILEDB_GLOBAL_ORDER);

  uint64_t last_space_tile =
      (ncells / tile_extent +
       static_cast<uint64_t>(ncells % tile_extent != 0)) *
          tile_extent -
      1;
  subarray.add_range(0, static_cast<uint64_t>(0), last_space_tile);
  query.set_subarray(subarray);

  uint64_t begin = 0;
  uint64_t end = chunk_size - 1;
  while (begin < end) {
    query.set_data_buffer("a1", a1.data() + begin, end - begin + 1);
    query.set_data_buffer("a2", a2.data() + begin * 2, (end - begin + 1) * 2);
    query.set_validity_buffer(
        "a2", a2_nullable.data() + begin, end - begin + 1);
    query.set_data_buffer(
        "a3", a3_data.data() + begin * 4, (end - begin + 1) * 4);
    query.set_offsets_buffer("a3", a3_offsets.data() + begin, end - begin + 1);
    begin += chunk_size;
    end = std::min(last_space_tile, end + chunk_size);

    if (begin < end) {
      query.submit();
    }
  }

  // Submit query
  query.submit_and_finalize();
  REQUIRE(query.query_status() == Query::Status::COMPLETE);

  // Read and validate results
  {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1_result(ncells);
    std::vector<uint32_t> a2_result(ncells * 2);
    std::vector<uint8_t> a2_result_nullable(ncells);
    std::vector<char> a3_result_data(4 * ncells);
    std::vector<uint64_t> a3_result_offsets(ncells);
    Subarray subarray(ctx, array);
    subarray.add_range(0, static_cast<uint64_t>(0), ncells - 1);
    query.set_subarray(subarray);
    query.set_data_buffer("a1", a1_result.data(), a1_result.size());
    query.set_data_buffer("a2", a2_result.data(), a2_result.size());
    query.set_validity_buffer(
        "a2", a2_result_nullable.data(), a2_result_nullable.size());
    query.set_data_buffer("a3", a3_result_data.data(), a3_result_data.size());
    query.set_offsets_buffer(
        "a3", a3_result_offsets.data(), a3_result_offsets.size());

    query.submit();
    REQUIRE(query.query_status() == Query::Status::COMPLETE);

    for (uint64_t i = 0; i < ncells; ++i) {
      CHECK(a1[i] == a1_result[i]);
    }
    for (uint64_t i = 0; i < ncells * 2; ++i) {
      CHECK(a2[i] == a2_result[i]);
    }
    for (uint64_t i = 0; i < ncells; ++i) {
      CHECK(a2_nullable[i] == a2_result_nullable[i]);
    }
    for (uint64_t i = 0; i < ncells * 4; ++i) {
      CHECK(a3_data[i] == a3_result_data[i]);
    }
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Derialization of a var size read query correctly resets original buffer "
    "sizes",
    "[capi][query][serialization][reset-buffers][rest]") {
  create_array(TILEDB_SPARSE);
  write_sparse_array();

  Array array(ctx, array_uri, TILEDB_READ);
  Query query(ctx, array);
  query.set_layout(TILEDB_UNORDERED);

  uint64_t up_limit = 16;
  std::vector<char> a3_data(up_limit);
  uint64_t offsets_size = up_limit / 8;
  std::vector<uint64_t> a3_offsets(offsets_size);

  auto set_buffers = [&]() {
    query.set_data_buffer("a3", a3_data.data(), a3_data.size());
    query.set_offsets_buffer("a3", a3_offsets.data(), a3_offsets.size());
  };

  set_buffers();
  Query::Status status;
  do {
    query.submit();
    status = query.query_status();
  } while (status == Query::Status::INCOMPLETE);

  REQUIRE(status == Query::Status::COMPLETE);
}
