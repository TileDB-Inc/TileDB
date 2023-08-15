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
#include "tiledb/sm/query/readers/aggregators/min_max_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/sum_aggregator.h"

#include <test/support/tdb_catch.h>

using namespace tiledb;
using namespace tiledb::test;

template <class T>
struct CppAggregatesFx {
  // Constants.
  const char* SPARSE_ARRAY_NAME = "test_aggregates_array";
  unsigned STRING_CELL_VAL_NUM = 2;

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
      bool allows_dups = false,
      bool nullable = false,
      bool var = false,
      bool encrypt = false);
  void write_sparse(
      std::vector<int> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp,
      optional<std::vector<uint8_t>> a1_validity = nullopt,
      bool encrypt = false);
  void write_sparse(
      std::vector<std::string> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp,
      optional<std::vector<uint8_t>> a1_validity = nullopt,
      bool encrypt = false);
  std::vector<uint8_t> make_data_buff(std::vector<int> values);
  void create_sparse_array_and_write_fragments(
      const bool allow_dups, const bool nullable);
  void validate_sparse_data(
      Query& query,
      const bool set_ranges,
      const bool allow_dups,
      const bool nullable,
      const uint64_t expected_count,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2,
      std::vector<uint8_t>& a1,
      std::vector<uint8_t>& a1_validity);
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
    bool allows_dups, bool nullable, bool var, bool encrypt) {
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
  if (std::is_same<T, std::string>::value) {
    a1.set_cell_val_num(
        var ? TILEDB_VAR_NUM : CppAggregatesFx<T>::STRING_CELL_VAL_NUM);
  }

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
void CppAggregatesFx<T>::write_sparse(
    std::vector<int> a1,
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

  std::vector<uint8_t> a1_buff = make_data_buff(a1);
  uint64_t cell_val_num = std::is_same<T, std::string>::value ?
                              CppAggregatesFx<T>::STRING_CELL_VAL_NUM :
                              1;

  // Create query.
  Query query(ctx_, *array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_data_buffer(
      "a1", static_cast<void*>(a1_buff.data()), a1.size() * cell_val_num);
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
}

template <class T>
void CppAggregatesFx<T>::write_sparse(
    std::vector<std::string> a1,
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

  std::string a1_data;
  std::vector<uint64_t> a1_offsets;
  uint64_t offset = 0;
  for (auto& v : a1) {
    a1_data += v;
    a1_offsets.emplace_back(offset);
    offset += v.length();
  }

  // Create query.
  Query query(ctx_, *array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_offsets_buffer("a1", a1_offsets);
  query.set_data_buffer("a1", a1_data);
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
}

template <class T>
std::vector<uint8_t> CppAggregatesFx<T>::make_data_buff(
    std::vector<int> values) {
  std::vector<T> data;
  for (auto& v : values) {
    data.emplace_back(static_cast<T>(v));
  }

  std::vector<uint8_t> ret(data.size() * sizeof(T));
  memcpy(ret.data(), data.data(), ret.size());

  return ret;
}

template <>
std::vector<uint8_t> CppAggregatesFx<std::string>::make_data_buff(
    std::vector<int> values) {
  std::vector<char> data;
  for (auto& v : values) {
    for (uint64_t i = 0; i < STRING_CELL_VAL_NUM; i++) {
      data.emplace_back(static_cast<char>(v + '0'));
    }
  }

  std::vector<uint8_t> ret(data.size());
  memcpy(ret.data(), data.data(), ret.size());

  return ret;
}

template <class T>
void CppAggregatesFx<T>::create_sparse_array_and_write_fragments(
    const bool allow_dups, const bool nullable) {
  create_sparse_array(allow_dups, nullable);

  // Write fragments, only cell 3,3 is duplicated.
  optional<std::vector<uint8_t>> validity_values = nullopt;
  if (nullable) {
    validity_values = {1, 0, 1, 0};
  }
  write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1, validity_values);
  write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3, validity_values);
  write_sparse({8, 9, 10, 11}, {2, 1, 3, 4}, {1, 3, 1, 1}, 4, validity_values);
  write_sparse(
      {12, 13, 14, 15}, {4, 3, 3, 4}, {2, 3, 4, 4}, 6, validity_values);
}

template <class T>
void CppAggregatesFx<T>::validate_sparse_data(
    Query& query,
    const bool set_ranges,
    const bool allow_dups,
    const bool nullable,
    const uint64_t expected_count,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2,
    std::vector<uint8_t>& a1,
    std::vector<uint8_t>& a1_validity) {
  std::vector<uint64_t> expected_dim1;
  std::vector<uint64_t> expected_dim2;
  std::vector<uint8_t> expected_a1;
  std::vector<uint8_t> expected_a1_validity;
  if (set_ranges) {
    if (allow_dups) {
      expected_dim1 = {3, 3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {2, 3, 1, 1, 2, 3, 4, 4};
      expected_a1 = make_data_buff({6, 7, 10, 11, 12, 13, 14, 15});
      expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0};
    } else {
      expected_dim1 = {3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 1, 2, 3, 4, 4};
      expected_a1 = make_data_buff({10, 6, 11, 12, 13, 14, 15});
      expected_a1_validity = {1, 1, 0, 1, 0, 1, 0};
    }
  } else {
    if (allow_dups) {
      expected_dim1 = {1, 1, 1, 2, 2, 2, 3, 3, 2, 1, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 4, 3, 2, 4, 2, 3, 1, 3, 1, 1, 2, 3, 4, 4};
      expected_a1 = make_data_buff(
          {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});
      expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    } else {
      expected_dim1 = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 4};
      expected_a1 =
          make_data_buff({0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 12, 13, 14, 15});
      expected_a1_validity = {1, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0};
    }
  }

  uint64_t cell_val_num = std::is_same<T, std::string>::value ?
                              CppAggregatesFx<T>::STRING_CELL_VAL_NUM :
                              1;

  auto result_el = query.result_buffer_elements_nullable();
  CHECK(std::get<1>(result_el["d1"]) == expected_count);
  CHECK(std::get<1>(result_el["d2"]) == expected_count);
  CHECK(std::get<1>(result_el["a1"]) == expected_count * cell_val_num);
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

  create_sparse_array_and_write_fragments(allow_dups, false);

  Array array{ctx_, SPARSE_ARRAY_NAME, TILEDB_READ};
  Query query(ctx_, array, TILEDB_READ);

  // TODO: Change to real CPPAPI. Add a count aggregator to the query.
  query.ptr()->query_->add_aggregator_to_default_channel(
      "Count", std::make_shared<tiledb::sm::CountAggregator>());

  if (set_ranges) {
    // Slice only rows 2, 3
    Subarray subarray(ctx_, array);
    subarray.add_range<uint64_t>(0, 3, 4);
    query.set_subarray(subarray);
  }

  // Set the data buffer for the aggregator.
  uint64_t cell_size = sizeof(int);
  std::vector<uint64_t> count(1);
  std::vector<uint64_t> dim1(100);
  std::vector<uint64_t> dim2(100);
  std::vector<uint8_t> a1(100 * cell_size);
  query.set_layout(TILEDB_UNORDERED);
  uint64_t count_data_size = sizeof(uint64_t);
  // TODO: Change to real CPPAPI.
  CHECK(query.ptr()
            ->query_->set_data_buffer("Count", count.data(), &count_data_size)
            .ok());

  if (request_data) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer(
        "a1", static_cast<void*>(a1.data()), a1.size() / cell_size);
  }

  // Submit the query.
  query.submit();

  // Check the results.
  auto result_el = query.result_buffer_elements_nullable();
  uint64_t expected_count;
  if (set_ranges) {
    expected_count = allow_dups ? 8 : 7;
  } else {
    expected_count = allow_dups ? 16 : 15;
  }

  // TODO: use 'std::get<1>(result_el["Count"]) == 1' once we use the
  // set_data_buffer api.
  // CHECK(std::get<1>(result_el["Count"]) == 1);
  CHECK(count[0] == expected_count);

  if (request_data) {
    auto validity = std::vector<uint8_t>();
    validate_sparse_data(
        query,
        set_ranges,
        allow_dups,
        false,
        expected_count,
        dim1,
        dim2,
        a1,
        validity);
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
    SumFixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE_METHOD(
    CppAggregatesFx,
    "C++ API: Aggregates basic sum",
    "[cppapi][aggregates][basic][sum]",
    SumFixedTypesUnderTest) {
  typedef TestType T;
  bool request_data = GENERATE(true, false);
  bool allow_dups = GENERATE(true, false);
  bool set_ranges = GENERATE(true, false);
  bool nullable = GENERATE(true, false);

  CppAggregatesFx<T>::create_sparse_array_and_write_fragments(
      allow_dups, nullable);

  Array array{
      CppAggregatesFx<T>::ctx_,
      CppAggregatesFx<T>::SPARSE_ARRAY_NAME,
      TILEDB_READ};
  Query query(CppAggregatesFx<T>::ctx_, array, TILEDB_READ);

  // TODO: Change to real CPPAPI. Add a count aggregator to the query.
  query.ptr()->query_->add_aggregator_to_default_channel(
      "Sum",
      std::make_shared<tiledb::sm::SumAggregator<T>>(
          tiledb::sm::FieldInfo("a1", false, nullable, 1)));

  if (set_ranges) {
    // Slice only rows 2, 3
    Subarray subarray(CppAggregatesFx<T>::ctx_, array);
    subarray.add_range<uint64_t>(0, 3, 4);
    query.set_subarray(subarray);
  }

  // Set the data buffer for the aggregator.
  uint64_t cell_size = sizeof(T);
  std::vector<typename tiledb::sm::sum_type_data<T>::sum_type> sum(1);
  std::vector<uint8_t> sum_validity(1);
  std::vector<uint64_t> dim1(100);
  std::vector<uint64_t> dim2(100);
  std::vector<uint8_t> a1(100 * cell_size);
  std::vector<uint8_t> a1_validity(100);
  query.set_layout(TILEDB_UNORDERED);
  uint64_t sum_data_size =
      sizeof(typename tiledb::sm::sum_type_data<T>::sum_type);
  // TODO: Change to real CPPAPI.
  CHECK(query.ptr()
            ->query_->set_data_buffer("Sum", sum.data(), &sum_data_size)
            .ok());
  uint64_t returned_validity_size = 1;
  if (nullable) {
    // TODO: Change to real CPPAPI. Use set_validity_buffer from the internal
    // query directly because the CPPAPI doesn't know what is an aggregate and
    // what the size of an aggregate should be.
    CHECK(query.ptr()
              ->query_
              ->set_validity_buffer(
                  "Sum", sum_validity.data(), &returned_validity_size)
              .ok());
  }

  if (request_data) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer(
        "a1", static_cast<void*>(a1.data()), a1.size() / cell_size);

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
  if (set_ranges) {
    expected_count = allow_dups ? 8 : 7;
  } else {
    expected_count = allow_dups ? 16 : 15;
  }

  // TODO: use 'std::get<1>(result_el["Sum"]) == 1' once we use the
  // set_data_buffer api.
  // CHECK(std::get<1>(result_el["Sum"]) == 1);
  CHECK(sum[0] == expected_sum);

  if (request_data) {
    CppAggregatesFx<T>::validate_sparse_data(
        query,
        set_ranges,
        allow_dups,
        nullable,
        expected_count,
        dim1,
        dim2,
        a1,
        a1_validity);
  }

  // Close array.
  array.close();
}

typedef tuple<
    std::pair<uint8_t, tiledb::sm::MinAggregator<uint8_t>>,
    std::pair<uint16_t, tiledb::sm::MinAggregator<uint16_t>>,
    std::pair<uint32_t, tiledb::sm::MinAggregator<uint32_t>>,
    std::pair<uint64_t, tiledb::sm::MinAggregator<uint64_t>>,
    std::pair<int8_t, tiledb::sm::MinAggregator<int8_t>>,
    std::pair<int16_t, tiledb::sm::MinAggregator<int16_t>>,
    std::pair<int32_t, tiledb::sm::MinAggregator<int32_t>>,
    std::pair<int64_t, tiledb::sm::MinAggregator<int64_t>>,
    std::pair<float, tiledb::sm::MinAggregator<float>>,
    std::pair<double, tiledb::sm::MinAggregator<double>>,
    std::pair<std::string, tiledb::sm::MinAggregator<std::string>>,
    std::pair<uint8_t, tiledb::sm::MaxAggregator<uint8_t>>,
    std::pair<uint16_t, tiledb::sm::MaxAggregator<uint16_t>>,
    std::pair<uint32_t, tiledb::sm::MaxAggregator<uint32_t>>,
    std::pair<uint64_t, tiledb::sm::MaxAggregator<uint64_t>>,
    std::pair<int8_t, tiledb::sm::MaxAggregator<int8_t>>,
    std::pair<int16_t, tiledb::sm::MaxAggregator<int16_t>>,
    std::pair<int32_t, tiledb::sm::MaxAggregator<int32_t>>,
    std::pair<int64_t, tiledb::sm::MaxAggregator<int64_t>>,
    std::pair<float, tiledb::sm::MaxAggregator<float>>,
    std::pair<double, tiledb::sm::MaxAggregator<double>>,
    std::pair<std::string, tiledb::sm::MaxAggregator<std::string>>>
    MinMaxFixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "C++ API: Aggregates basic min/max",
    "[cppapi][aggregates][basic][min-max]",
    MinMaxFixedTypesUnderTest) {
  typedef decltype(TestType::first) T;
  typedef decltype(TestType::second) AGG;
  CppAggregatesFx<T> fx;
  bool request_data = GENERATE(true, false);
  bool allow_dups = GENERATE(true, false);
  bool set_ranges = GENERATE(true, false);
  bool nullable = GENERATE(true, false);
  bool min = std::is_same<AGG, tiledb::sm::MinAggregator<T>>::value;

  fx.create_sparse_array_and_write_fragments(allow_dups, nullable);

  Array array{fx.ctx_, fx.SPARSE_ARRAY_NAME, TILEDB_READ};
  Query query(fx.ctx_, array, TILEDB_READ);

  // TODO: Change to real CPPAPI. Add a count aggregator to the query.
  uint64_t cell_val_num =
      std::is_same<T, std::string>::value ? fx.STRING_CELL_VAL_NUM : 1;
  query.ptr()->query_->add_aggregator_to_default_channel(
      "MinMax",
      std::make_shared<AGG>(
          tiledb::sm::FieldInfo("a1", false, nullable, cell_val_num)));

  if (set_ranges) {
    // Slice only rows 2, 3
    Subarray subarray(fx.ctx_, array);
    subarray.add_range<uint64_t>(0, 3, 4);
    query.set_subarray(subarray);
  }

  // Set the data buffer for the aggregator.
  uint64_t cell_size =
      std::is_same<T, std::string>::value ? fx.STRING_CELL_VAL_NUM : sizeof(T);
  std::vector<uint8_t> min_max(cell_size);
  std::vector<uint8_t> min_max_validity(1);
  std::vector<uint64_t> dim1(100);
  std::vector<uint64_t> dim2(100);
  std::vector<uint8_t> a1(100 * cell_size);
  std::vector<uint8_t> a1_validity(100);
  query.set_layout(TILEDB_UNORDERED);

  // TODO: Change to real CPPAPI. Use set_data_buffer and set_validity_buffer
  // from the internal query directly because the CPPAPI doesn't know what is
  // an aggregate and what the size of an aggregate should be.
  uint64_t returned_min_max_size = cell_size;
  uint64_t returned_validity_size = 1;
  CHECK(query.ptr()
            ->query_
            ->set_data_buffer("MinMax", min_max.data(), &returned_min_max_size)
            .ok());
  if (nullable) {
    CHECK(query.ptr()
              ->query_
              ->set_validity_buffer(
                  "MinMax", min_max_validity.data(), &returned_validity_size)
              .ok());
  }

  if (request_data) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer(
        "a1", static_cast<void*>(a1.data()), a1.size() / cell_size);

    if (nullable) {
      query.set_validity_buffer("a1", a1_validity);
    }
  }

  // Submit the query.
  query.submit();

  // Check the results.
  std::vector<uint8_t> expected_min_max;
  if (nullable) {
    if (set_ranges) {
      expected_min_max = fx.make_data_buff({min ? 6 : 14});
    } else {
      expected_min_max = fx.make_data_buff({min ? 0 : 14});
    }
  } else {
    if (set_ranges) {
      expected_min_max = fx.make_data_buff({min ? 6 : 15});
    } else {
      expected_min_max = fx.make_data_buff({min ? 0 : 15});
    }
  }

  auto result_el = query.result_buffer_elements_nullable();
  uint64_t expected_count;
  std::vector<uint64_t> expected_dim1;
  std::vector<uint64_t> expected_dim2;
  std::vector<uint8_t> expected_a1;
  std::vector<uint8_t> expected_a1_validity;
  if (set_ranges) {
    expected_count = allow_dups ? 8 : 7;
  } else {
    expected_count = allow_dups ? 16 : 15;
  }

  // TODO: use 'std::get<1>(result_el["MinMax"]) == 1' once we use the
  // set_data_buffer api.
  CHECK(returned_min_max_size == cell_size);
  CHECK(min_max == expected_min_max);

  if (request_data) {
    fx.validate_sparse_data(
        query,
        set_ranges,
        allow_dups,
        nullable,
        expected_count,
        dim1,
        dim2,
        a1,
        a1_validity);
  }

  // Close array.
  array.close();
}

typedef tuple<
    tiledb::sm::MinAggregator<std::string>,
    tiledb::sm::MaxAggregator<std::string>>
    AggUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "C++ API: Aggregates basic min/max var",
    "[cppapi][aggregates][basic][min-max][var]",
    AggUnderTest) {
  CppAggregatesFx<std::string> fx;
  typedef TestType AGG;
  bool min = std::is_same<AGG, tiledb::sm::MinAggregator<std::string>>::value;
  bool request_data = GENERATE(true, false);
  bool allow_dups = GENERATE(true, false);
  bool set_ranges = GENERATE(true, false);
  bool nullable = GENERATE(true, false);

  fx.create_sparse_array(allow_dups, nullable, true);

  // Write fragments, only cell 3,3 is duplicated.
  optional<std::vector<uint8_t>> validity_values = nullopt;
  if (nullable) {
    validity_values = {1, 0, 1, 0};
  }
  fx.write_sparse(
      {"0", "1", "2", "3"}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1, validity_values);
  fx.write_sparse(
      {"4", "5", "6", "7"}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3, validity_values);
  fx.write_sparse(
      {"8", "99", "10", "11"}, {2, 1, 3, 4}, {1, 3, 1, 1}, 4, validity_values);
  fx.write_sparse(
      {"12", "13", "14", "15"}, {4, 3, 3, 4}, {2, 3, 4, 4}, 6, validity_values);

  Array array{fx.ctx_, fx.SPARSE_ARRAY_NAME, TILEDB_READ};
  Query query(fx.ctx_, array, TILEDB_READ);

  // TODO: Change to real CPPAPI. Add a count aggregator to the query.
  query.ptr()->query_->add_aggregator_to_default_channel(
      "MinMax",
      std::make_shared<AGG>(
          tiledb::sm::FieldInfo("a1", true, nullable, TILEDB_VAR_NUM)));

  if (set_ranges) {
    // Slice only rows 2, 3
    Subarray subarray(fx.ctx_, array);
    subarray.add_range<uint64_t>(0, 3, 4);
    query.set_subarray(subarray);
  }

  // Set the data buffer for the aggregator.
  std::vector<uint64_t> min_max_offset(1);
  std::string min_max_data;
  min_max_data.resize(10);
  std::vector<uint8_t> min_max_validity(1);
  std::vector<uint64_t> dim1(100);
  std::vector<uint64_t> dim2(100);
  std::vector<uint64_t> a1_offsets(100);
  std::string a1_data;
  a1_data.resize(100);
  std::vector<uint8_t> a1_validity(100);
  query.set_layout(TILEDB_UNORDERED);

  // TODO: Change to real CPPAPI. Use set_data_buffer and set_validity_buffer
  // from the internal query directly because the CPPAPI doesn't know what is
  // an aggregate and what the size of an aggregate should be.
  uint64_t returned_min_max_data_size = 10;
  uint64_t returned_min_max_offsets_size = 8;
  uint64_t returned_validity_size = 1;
  CHECK(query.ptr()
            ->query_
            ->set_data_buffer(
                "MinMax", min_max_data.data(), &returned_min_max_data_size)
            .ok());
  CHECK(query.ptr()
            ->query_
            ->set_offsets_buffer(
                "MinMax", min_max_offset.data(), &returned_min_max_offsets_size)
            .ok());
  if (nullable) {
    CHECK(query.ptr()
              ->query_
              ->set_validity_buffer(
                  "MinMax", min_max_validity.data(), &returned_validity_size)
              .ok());
  }

  if (request_data) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer("a1", a1_data);
    query.set_offsets_buffer("a1", a1_offsets);

    if (nullable) {
      query.set_validity_buffer("a1", a1_validity);
    }
  }

  // Submit the query.
  query.submit();

  // Check the results.
  std::string expected_min_max;
  if (nullable) {
    if (set_ranges) {
      expected_min_max = min ? "10" : "6";
    } else {
      expected_min_max = min ? "0" : "8";
    }
  } else {
    if (set_ranges) {
      expected_min_max = min ? "10" : (allow_dups ? "7" : "6");
    } else {
      expected_min_max = min ? "0" : "99";
    }
  }

  auto result_el = query.result_buffer_elements_nullable();
  uint64_t expected_count;
  std::vector<uint64_t> expected_dim1;
  std::vector<uint64_t> expected_dim2;
  std::vector<std::string> expected_a1;
  std::vector<uint8_t> expected_a1_validity;
  if (set_ranges) {
    if (allow_dups) {
      expected_dim1 = {3, 3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {2, 3, 1, 1, 2, 3, 4, 4};
      expected_a1 = {"6", "7", "10", "11", "12", "13", "14", "15"};
      expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0};
    } else {
      expected_dim1 = {3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 1, 2, 3, 4, 4};
      expected_a1 = {"10", "6", "11", "12", "13", "14", "15"};
      expected_a1_validity = {1, 1, 0, 1, 0, 1, 0};
    }
    expected_count = allow_dups ? 8 : 7;
  } else {
    if (allow_dups) {
      expected_dim1 = {1, 1, 1, 2, 2, 2, 3, 3, 2, 1, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 4, 3, 2, 4, 2, 3, 1, 3, 1, 1, 2, 3, 4, 4};
      expected_a1 = {
          "0",
          "1",
          "2",
          "3",
          "4",
          "5",
          "6",
          "7",
          "8",
          "99",
          "10",
          "11",
          "12",
          "13",
          "14",
          "15"};
      expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    } else {
      expected_dim1 = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4};
      expected_dim2 = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 4};
      expected_a1 = {
          "0",
          "1",
          "8",
          "4",
          "99",
          "2",
          "3",
          "5",
          "10",
          "6",
          "11",
          "12",
          "13",
          "14",
          "15"};
      expected_a1_validity = {1, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0};
    }
    expected_count = allow_dups ? 16 : 15;
  }

  // Generate a vector from the read data to compare against our expectation.
  uint64_t expected_a1_size = 0;
  std::vector<std::string> a1_data_vec;
  for (uint64_t c = 0; c < expected_count - 1; c++) {
    auto size = a1_offsets[c + 1] - a1_offsets[c];
    expected_a1_size += size;
    a1_data_vec.emplace_back(a1_data.substr(a1_offsets[c], size));
  }
  auto size = std::get<1>(result_el["a1"]) - a1_offsets[expected_count - 1];
  expected_a1_size += size;
  a1_data_vec.emplace_back(
      a1_data.substr(a1_offsets[expected_count - 1], size));

  // TODO: use 'std::get<1>(result_el["MinMax"]) == 1' once we use the
  // set_data_buffer api.
  CHECK(returned_min_max_offsets_size == 8);
  CHECK(returned_min_max_data_size == expected_min_max.size());

  min_max_data.resize(expected_min_max.size());
  CHECK(min_max_data == expected_min_max);
  CHECK(min_max_offset[0] == 0);

  if (request_data) {
    CHECK(std::get<1>(result_el["d1"]) == expected_count);
    CHECK(std::get<1>(result_el["d2"]) == expected_count);
    CHECK(std::get<1>(result_el["a1"]) == expected_a1_size);
    CHECK(std::get<0>(result_el["a1"]) == expected_count);

    if (nullable) {
      CHECK(std::get<2>(result_el["a1"]) == expected_count);
    }

    dim1.resize(expected_dim1.size());
    dim2.resize(expected_dim2.size());
    CHECK(dim1 == expected_dim1);
    CHECK(dim2 == expected_dim2);
    CHECK(a1_data_vec == expected_a1);

    if (nullable) {
      a1_validity.resize(expected_a1_validity.size());
      CHECK(a1_validity == expected_a1_validity);
    }
  }

  // Close array.
  array.close();
}