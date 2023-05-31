/**
 * @file test-cppapi-aggregates.cc
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
 * Tests the Aggregates API.
 */

#include "test/support/src/helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/query/readers/aggregators/count_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/sum_aggregator.h"

#include <test/support/tdb_catch.h>

using namespace tiledb;
using namespace tiledb::test;

template <class T>
struct CppAggregatesFx {
  // Constants.
  const char* SPARSE_ARRAY_NAME = "test_aggregates_array";

  // TileDB context.
  Context ctx_;
  VFS vfs_;

  std::string key_ = "0123456789abcdeF0123456789abcdeF";
  const tiledb_encryption_type_t enc_type_ = TILEDB_AES_256_GCM;

  // Constructors/destructors.
  CppAggregatesFx();
  ~CppAggregatesFx();

  // Functions.
  void create_sparse_array(
      bool allows_dups = false, bool nullable = false, bool encrypt = false);
  std::string write_sparse(
      std::vector<T> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp,
      optional<std::vector<uint8_t>> a1_validity = nullopt,
      bool encrypt = false);
  void remove_sparse_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

template <class T>
CppAggregatesFx<T>::CppAggregatesFx()
    : vfs_(ctx_) {
  ctx_ = Context();
  vfs_ = VFS(ctx_);

  remove_sparse_array();
}

template <class T>
CppAggregatesFx<T>::~CppAggregatesFx() {
  remove_sparse_array();
}

template <class T>
void CppAggregatesFx<T>::create_sparse_array(
    bool allows_dups, bool nullable, bool encrypt) {
  // Create dimensions.
  auto d1 = Dimension::create<uint64_t>(ctx_, "d1", {{1, 999}}, 2);
  auto d2 = Dimension::create<uint64_t>(ctx_, "d2", {{1, 999}}, 2);

  // Create domain.
  Domain domain(ctx_);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create attributes.
  auto a1 = Attribute::create<T>(ctx_, "a1");
  a1.set_nullable(nullable);

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

template <class T>
std::string CppAggregatesFx<T>::write_sparse(
    std::vector<T> a1,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp,
    optional<std::vector<uint8_t>> a1_validity,
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
        TemporalPolicy(TimestampStartEnd, 0, timestamp));
  }

  // Create query.
  Query query(ctx_, *array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_data_buffer("a1", a1);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);
  if (a1_validity.has_value()) {
    query.set_validity_buffer("a1", a1_validity.value());
  }

  // Submit/finalize the query.
  query.submit();
  query.finalize();

  // Close array.
  array->close();

  return query.fragment_uri(0);
}

template <class T>
void CppAggregatesFx<T>::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  vfs_.remove_dir(array_name);
}

template <class T>
void CppAggregatesFx<T>::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

template <class T>
bool CppAggregatesFx<T>::is_array(const std::string& array_name) {
  return vfs_.is_dir(array_name);
}

TEST_CASE_METHOD(
    CppAggregatesFx<int32_t>,
    "C++ API: Aggregates basic count",
    "[cppapi][aggregates][basic][count]") {
  bool request_data = GENERATE(true, false);
  bool allow_dups = GENERATE(true, false);
  bool set_ranges = GENERATE(true, false);

  create_sparse_array(allow_dups);

  // Write fragments, only cell 3,3 is duplicated.
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1);
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3);
  write_sparse({8, 9, 10, 11}, {2, 1, 3, 4}, {1, 3, 1, 1}, 4);
  write_sparse({12, 13, 14, 15}, {4, 3, 3, 4}, {2, 3, 4, 4}, 6);

  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  Query query(ctx_, array, TILEDB_READ);

  // Add a count aggregator to the query.
  query.ptr()->query_->add_aggregator_to_default_channel(
      "Count",
      std::make_shared<tiledb::sm::CountAggregator>(
          tiledb::sm::constants::all_attributes));

  if (set_ranges) {
    // Slice only rows 2, 3
    Subarray subarray(ctx_, array);
    subarray.add_range<uint64_t>(0, 3, 4);
    query.set_subarray(subarray);
  }

  // Set the data buffer for the aggregator.
  std::vector<uint64_t> count(1);
  std::vector<uint64_t> dim1(100);
  std::vector<uint64_t> dim2(100);
  std::vector<int> a1(100);
  query.set_layout(TILEDB_UNORDERED);
  query.set_data_buffer("Count", count);

  if (request_data) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer("a1", a1);
  }

  // Submit the query.
  query.submit();

  // Check the results.
  auto result_el = query.result_buffer_elements_nullable();
  uint64_t expected_count;
  std::vector<uint64_t> expected_dim1;
  std::vector<uint64_t> expected_dim2;
  std::vector<int> expected_a1;
  if (set_ranges) {
    if (allow_dups) {
      expected_dim1 = {3, 3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {2, 3, 1, 1, 2, 3, 4, 4};
      expected_a1 = {6, 7, 10, 11, 12, 13, 14, 15};
    } else {
      expected_dim1 = {3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 1, 2, 3, 4, 4};
      expected_a1 = {10, 6, 11, 12, 13, 14, 15};
    }
    expected_count = allow_dups ? 8 : 7;
  } else {
    if (allow_dups) {
      expected_dim1 = {1, 1, 1, 2, 2, 2, 3, 3, 2, 1, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 4, 3, 2, 4, 2, 3, 1, 3, 1, 1, 2, 3, 4, 4};
      expected_a1 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    } else {
      expected_dim1 = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 4};
      expected_a1 = {0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 12, 13, 14, 15};
    }
    expected_count = allow_dups ? 16 : 15;
  }
  CHECK(std::get<1>(result_el["Count"]) == 1);
  CHECK(count[0] == expected_count);

  if (request_data) {
    CHECK(std::get<1>(result_el["d1"]) == expected_count);
    CHECK(std::get<1>(result_el["d2"]) == expected_count);
    CHECK(std::get<1>(result_el["a1"]) == expected_count);
    dim1.resize(expected_dim1.size());
    dim2.resize(expected_dim2.size());
    a1.resize(expected_a1.size());
    CHECK(dim1 == expected_dim1);
    CHECK(dim2 == expected_dim2);
    CHECK(a1 == expected_a1);
  }

  // Close array.
  array.close();
}

typedef tuple<
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    float,
    double>
    FixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE_METHOD(
    CppAggregatesFx,
    "C++ API: Aggregates basic sum",
    "[cppapi][aggregates][basic][sum]",
    FixedTypesUnderTest) {
  typedef TestType T;
  bool request_data = GENERATE(true, false);
  bool allow_dups = GENERATE(true, false);
  bool set_ranges = GENERATE(true, false);
  bool nullable = GENERATE(true, false);

  CppAggregatesFx<T>::create_sparse_array(allow_dups, nullable);

  // Write fragments, only cell 3,3 is duplicated.
  optional<std::vector<uint8_t>> validity_values = nullopt;
  if (nullable) {
    validity_values = {1, 0, 1, 0};
  }
  CppAggregatesFx<T>::write_sparse(
      {0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1, validity_values);
  CppAggregatesFx<T>::write_sparse(
      {4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3, validity_values);
  CppAggregatesFx<T>::write_sparse(
      {8, 9, 10, 11}, {2, 1, 3, 4}, {1, 3, 1, 1}, 4, validity_values);
  CppAggregatesFx<T>::write_sparse(
      {12, 13, 14, 15}, {4, 3, 3, 4}, {2, 3, 4, 4}, 6, validity_values);

  Array array{
      CppAggregatesFx<T>::ctx_,
      CppAggregatesFx<T>::SPARSE_ARRAY_NAME,
      TILEDB_READ};
  Query query(CppAggregatesFx<T>::ctx_, array, TILEDB_READ);

  // Add a count aggregator to the query.
  query.ptr()->query_->add_aggregator_to_default_channel(
      "Sum",
      std::make_shared<tiledb::sm::SumAggregator<T>>(
          "a1", array.ptr()->array_->array_schema_latest()));

  if (set_ranges) {
    // Slice only rows 2, 3
    Subarray subarray(CppAggregatesFx<T>::ctx_, array);
    subarray.add_range<uint64_t>(0, 3, 4);
    query.set_subarray(subarray);
  }

  // Set the data buffer for the aggregator.
  std::vector<typename tiledb::sm::sum_type_data<T>::sum_type> sum(1);
  std::vector<uint8_t> validity(1);
  std::vector<uint64_t> dim1(100);
  std::vector<uint64_t> dim2(100);
  std::vector<T> a1(100);
  std::vector<uint8_t> a1_validity(100);
  query.set_layout(TILEDB_UNORDERED);
  query.set_data_buffer("Sum", sum);
  if (nullable) {
    query.set_validity_buffer("Sum", validity);
  }

  if (request_data) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer("a1", a1);

    if (nullable) {
      query.set_validity_buffer("a1", a1_validity);
    }
  }

  // Submit the query.
  query.submit();

  // Check the results.
  typename tiledb::sm::sum_type_data<T>::sum_type expected_sum;
  if (nullable) {
    if (set_ranges) {
      expected_sum = 42;
    } else {
      expected_sum = 56;
    }
  } else {
    if (set_ranges) {
      expected_sum = allow_dups ? 88 : 81;
    } else {
      expected_sum = allow_dups ? 120 : 113;
    }
  }

  auto result_el = query.result_buffer_elements_nullable();
  uint64_t expected_count;
  std::vector<uint64_t> expected_dim1;
  std::vector<uint64_t> expected_dim2;
  std::vector<T> expected_a1;
  std::vector<uint8_t> expected_a1_validity;
  if (set_ranges) {
    if (allow_dups) {
      expected_dim1 = {3, 3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {2, 3, 1, 1, 2, 3, 4, 4};
      expected_a1 = {6, 7, 10, 11, 12, 13, 14, 15};
      expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0};
    } else {
      expected_dim1 = {3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 1, 2, 3, 4, 4};
      expected_a1 = {10, 6, 11, 12, 13, 14, 15};
      expected_a1_validity = {1, 1, 0, 1, 0, 1, 0};
    }
    expected_count = allow_dups ? 8 : 7;
  } else {
    if (allow_dups) {
      expected_dim1 = {1, 1, 1, 2, 2, 2, 3, 3, 2, 1, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 4, 3, 2, 4, 2, 3, 1, 3, 1, 1, 2, 3, 4, 4};
      expected_a1 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
      expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    } else {
      expected_dim1 = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 4};
      expected_a1 = {0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 12, 13, 14, 15};
      expected_a1_validity = {1, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0};
    }
    expected_count = allow_dups ? 16 : 15;
  }
  CHECK(std::get<1>(result_el["Sum"]) == 1);
  CHECK(sum[0] == expected_sum);

  if (request_data) {
    CHECK(std::get<1>(result_el["d1"]) == expected_count);
    CHECK(std::get<1>(result_el["d2"]) == expected_count);
    CHECK(std::get<1>(result_el["a1"]) == expected_count);

    if (nullable) {
      CHECK(std::get<2>(result_el["a1"]) == expected_count);
    }

    dim1.resize(expected_dim1.size());
    dim2.resize(expected_dim2.size());
    a1.resize(expected_a1.size());
    CHECK(dim1 == expected_dim1);
    CHECK(dim2 == expected_dim2);
    CHECK(a1 == expected_a1);

    if (nullable) {
      a1_validity.resize(expected_a1_validity.size());
      CHECK(a1_validity == expected_a1_validity);
    }
  }

  // Close array.
  array.close();
}