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
  const char* ARRAY_NAME = "test_aggregates_sparse_array";
  unsigned STRING_CELL_VAL_NUM = 2;

  // TileDB context.
  Context ctx_;
  VFS vfs_;

  // Test parameters.
  bool dense_;
  bool nullable_;
  bool request_data_;
  bool allow_dups_;
  bool set_ranges_;
  bool set_qc_;
  tiledb_layout_t layout_;

  std::string key_ = "0123456789abcdeF0123456789abcdeF";
  const tiledb_encryption_type_t enc_type_ = TILEDB_AES_256_GCM;

  // Constructors/destructors.
  CppAggregatesFx();
  ~CppAggregatesFx();

  // Functions.
  bool generate_test_params();
  void create_dense_array(bool var = false, bool encrypt = false);
  void create_sparse_array(bool var = false, bool encrypt = false);
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
  void write_dense(
      std::vector<int> a1,
      uint64_t dim1_min,
      uint64_t dim1_max,
      uint64_t dim2_min,
      uint64_t dim2_max,
      uint64_t timestamp,
      optional<std::vector<uint8_t>> a1_validity = nullopt,
      bool encrypt = false);
  void write_dense_string(
      std::vector<std::string> a1,
      uint64_t dim1_min,
      uint64_t dim1_max,
      uint64_t dim2_min,
      uint64_t dim2_max,
      uint64_t timestamp,
      optional<std::vector<uint8_t>> a1_validity = nullopt,
      bool encrypt = false);
  std::vector<uint8_t> make_data_buff(
      std::vector<int> values,
      optional<std::vector<uint8_t>> validity = nullopt);
  void create_array_and_write_fragments();
  void create_var_array_and_write_fragments();
  void set_ranges_and_condition_if_needed(
      Array& array, Query& query, const bool var);
  void validate_data(
      Query& query,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2,
      std::vector<uint8_t>& a1,
      std::vector<uint8_t>& a1_validity);
  void validate_data_var(
      Query& query,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2,
      std::string& a1_data,
      std::vector<uint64_t>& a1_offsets,
      std::vector<uint8_t>& a1_validity);
  void remove_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

template <class T>
CppAggregatesFx<T>::CppAggregatesFx()
    : vfs_(ctx_) {
  ctx_ = Context();
  vfs_ = VFS(ctx_);

  remove_array();
}

template <class T>
CppAggregatesFx<T>::~CppAggregatesFx() {
  remove_array();
}

template <class T>
bool CppAggregatesFx<T>::generate_test_params() {
  dense_ = GENERATE(true, false);
  request_data_ = GENERATE(true, false);
  nullable_ = GENERATE(true, false);
  allow_dups_ = GENERATE(true, false);
  set_ranges_ = GENERATE(true, false);
  set_qc_ = GENERATE(true, false);
  layout_ = GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR, TILEDB_GLOBAL_ORDER);

  if (dense_ && allow_dups_) {
    return false;
  }

  if (!dense_ and set_qc_) {
    return false;
  }

  if (!dense_ && layout_ != TILEDB_GLOBAL_ORDER) {
    return false;
  }

  if (!dense_) {
    layout_ = TILEDB_UNORDERED;
  }

  return true;
}

template <class T>
void CppAggregatesFx<T>::create_dense_array(bool var, bool encrypt) {
  // Create dimensions.
  auto d1 = Dimension::create<uint64_t>(ctx_, "d1", {{1, 12}}, 3);
  auto d2 = Dimension::create<uint64_t>(ctx_, "d2", {{1, 12}}, 3);

  // Create domain.
  Domain domain(ctx_);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create attributes.
  auto a1 = Attribute::create<T>(ctx_, "a1");
  a1.set_nullable(nullable_);
  if (std::is_same<T, std::string>::value) {
    a1.set_cell_val_num(
        var ? TILEDB_VAR_NUM : CppAggregatesFx<T>::STRING_CELL_VAL_NUM);
  }

  // Create array schema.
  ArraySchema schema(ctx_, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a1);

  // Set up filters.
  Filter filter(ctx_, TILEDB_FILTER_NONE);
  FilterList filter_list(ctx_);
  filter_list.add_filter(filter);
  schema.set_coords_filter_list(filter_list);

  if (encrypt) {
    Array::create(ARRAY_NAME, schema, enc_type_, key_);
  } else {
    Array::create(ARRAY_NAME, schema);
  }
}

template <class T>
void CppAggregatesFx<T>::create_sparse_array(bool var, bool encrypt) {
  // Create dimensions.
  auto d1 = Dimension::create<uint64_t>(ctx_, "d1", {{1, 999}}, 2);
  auto d2 = Dimension::create<uint64_t>(ctx_, "d2", {{1, 999}}, 2);

  // Create domain.
  Domain domain(ctx_);
  domain.add_dimension(d1);
  domain.add_dimension(d2);

  // Create attributes.
  auto a1 = Attribute::create<T>(ctx_, "a1");
  a1.set_nullable(nullable_);
  if (std::is_same<T, std::string>::value) {
    a1.set_cell_val_num(
        var ? TILEDB_VAR_NUM : CppAggregatesFx<T>::STRING_CELL_VAL_NUM);
  }

  // Create array schema.
  ArraySchema schema(ctx_, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.set_capacity(20);
  schema.add_attributes(a1);

  if (allow_dups_) {
    schema.set_allows_dups(true);
  }

  // Set up filters.
  Filter filter(ctx_, TILEDB_FILTER_NONE);
  FilterList filter_list(ctx_);
  filter_list.add_filter(filter);
  schema.set_coords_filter_list(filter_list);

  if (encrypt) {
    Array::create(ARRAY_NAME, schema, enc_type_, key_);
  } else {
    Array::create(ARRAY_NAME, schema);
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
        ARRAY_NAME,
        TILEDB_WRITE,
        TemporalPolicy(TimeTravel, timestamp),
        EncryptionAlgorithm(AESGCM, key_.c_str()));
  } else {
    array = std::make_unique<Array>(
        ctx_,
        ARRAY_NAME,
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
        ARRAY_NAME,
        TILEDB_WRITE,
        TemporalPolicy(TimeTravel, timestamp),
        EncryptionAlgorithm(AESGCM, key_.c_str()));
  } else {
    array = std::make_unique<Array>(
        ctx_,
        ARRAY_NAME,
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
void CppAggregatesFx<T>::write_dense(
    std::vector<int> a1,
    uint64_t dim1_min,
    uint64_t dim1_max,
    uint64_t dim2_min,
    uint64_t dim2_max,
    uint64_t timestamp,
    optional<std::vector<uint8_t>> a1_validity,
    bool encrypt) {
  // Open array.
  std::unique_ptr<Array> array;
  if (encrypt) {
    array = std::make_unique<Array>(
        ctx_,
        ARRAY_NAME,
        TILEDB_WRITE,
        TemporalPolicy(TimeTravel, timestamp),
        EncryptionAlgorithm(AESGCM, key_.c_str()));
  } else {
    array = std::make_unique<Array>(
        ctx_,
        ARRAY_NAME,
        TILEDB_WRITE,
        TemporalPolicy(TimestampStartEnd, 0, timestamp));
  }

  std::vector<uint8_t> a1_buff = make_data_buff(a1);
  uint64_t cell_val_num = std::is_same<T, std::string>::value ?
                              CppAggregatesFx<T>::STRING_CELL_VAL_NUM :
                              1;

  // Create the subarray.
  Subarray subarray(ctx_, *array);
  subarray.add_range(0, dim1_min, dim1_max).add_range(1, dim2_min, dim2_max);

  // Create query.
  Query query(ctx_, *array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  query.set_data_buffer(
      "a1", static_cast<void*>(a1_buff.data()), a1.size() * cell_val_num);
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
void CppAggregatesFx<T>::write_dense_string(
    std::vector<std::string> a1,
    uint64_t dim1_min,
    uint64_t dim1_max,
    uint64_t dim2_min,
    uint64_t dim2_max,
    uint64_t timestamp,
    optional<std::vector<uint8_t>> a1_validity,
    bool encrypt) {
  // Open array.
  std::unique_ptr<Array> array;
  if (encrypt) {
    array = std::make_unique<Array>(
        ctx_,
        ARRAY_NAME,
        TILEDB_WRITE,
        TemporalPolicy(TimeTravel, timestamp),
        EncryptionAlgorithm(AESGCM, key_.c_str()));
  } else {
    array = std::make_unique<Array>(
        ctx_,
        ARRAY_NAME,
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

  // Create the subarray.
  Subarray subarray(ctx_, *array);
  subarray.add_range(0, dim1_min, dim1_max).add_range(1, dim2_min, dim2_max);

  // Create query.
  Query query(ctx_, *array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  query.set_offsets_buffer("a1", a1_offsets);
  query.set_data_buffer("a1", a1_data);
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
    std::vector<int> values, optional<std::vector<uint8_t>> validity) {
  std::vector<T> data;
  if (validity.has_value()) {
    auto a = tiledb::Attribute::create<T>(ctx_, "unused");
    const void* fill_value = nullptr;
    uint64_t size = 0;
    a.get_fill_value(&fill_value, &size);

    for (uint64_t i = 0; i < values.size(); i++) {
      auto v = values[i];
      if (set_qc_ &&
          (v == 4 || v == 35 || (nullable_ && validity.value()[i] == 0))) {
        data.emplace_back(*(const T*)fill_value);
      } else {
        data.emplace_back(static_cast<T>(v));
      }
    }
  } else {
    for (auto& v : values) {
      data.emplace_back(v);
    }
  }

  std::vector<uint8_t> ret(data.size() * sizeof(T));
  memcpy(ret.data(), data.data(), ret.size());

  return ret;
}

template <>
std::vector<uint8_t> CppAggregatesFx<std::string>::make_data_buff(
    std::vector<int> values, optional<std::vector<uint8_t>> validity) {
  std::vector<char> data;
  if (validity.has_value()) {
    for (uint64_t c = 0; c < values.size(); c++) {
      auto v = values[c];
      if (set_qc_ &&
          (v == 4 || v == 35 || (nullable_ && validity.value()[c] == 0))) {
        for (uint64_t i = 0; i < STRING_CELL_VAL_NUM; i++) {
          data.emplace_back(0);
        }
      } else {
        for (uint64_t i = 0; i < STRING_CELL_VAL_NUM; i++) {
          data.emplace_back(static_cast<char>(v + '0'));
        }
      }
    }
  } else {
    for (auto& v : values) {
      for (uint64_t i = 0; i < STRING_CELL_VAL_NUM; i++) {
        data.emplace_back(static_cast<char>(v + '0'));
      }
    }
  }

  std::vector<uint8_t> ret(data.size());
  memcpy(ret.data(), data.data(), ret.size());

  return ret;
}

template <class T>
void CppAggregatesFx<T>::create_array_and_write_fragments() {
  if (dense_) {
    create_dense_array();

    optional<std::vector<uint8_t>> validity_full = nullopt;
    optional<std::vector<uint8_t>> validity_single = nullopt;
    optional<std::vector<uint8_t>> validity_two_full = nullopt;
    if (nullable_) {
      validity_full = {1, 0, 1, 0, 1, 0, 1, 0, 1};
      validity_single = {1};
      validity_two_full = {
          1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    }

    // Write first tile.
    write_dense({0, 1, 2, 3, 4, 5, 6, 7, 8}, 1, 3, 1, 3, 1, validity_full);

    // Write second tile.
    write_dense(
        {9, 10, 11, 12, 255, 14, 15, 16, 17}, 1, 3, 4, 6, 3, validity_full);

    // Overwrite single value in second tile. This will create overlapping
    // fragment domains.
    write_dense({13}, 2, 2, 5, 5, 5, validity_single);

    // Write third and fourth tile.
    write_dense(
        {18,
         19,
         20,
         21,
         22,
         23,
         24,
         25,
         26,
         27,
         28,
         29,
         30,
         31,
         32,
         33,
         34,
         35},
        4,
        6,
        1,
        6,
        7,
        validity_two_full);
  } else {
    create_sparse_array();

    // Write fragments, only cell 3,3 is duplicated.
    optional<std::vector<uint8_t>> validity_values = nullopt;
    if (nullable_) {
      validity_values = {1, 0, 1, 0};
    }
    write_sparse({0, 1, 2, 3}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1, validity_values);
    write_sparse({4, 5, 6, 7}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3, validity_values);
    write_sparse(
        {8, 9, 10, 11}, {2, 1, 3, 4}, {1, 3, 1, 1}, 4, validity_values);
    write_sparse(
        {12, 13, 14, 15}, {4, 3, 3, 4}, {2, 3, 4, 4}, 6, validity_values);
  }
}

template <class T>
void CppAggregatesFx<T>::create_var_array_and_write_fragments() {
  if (dense_) {
    create_dense_array(true);

    optional<std::vector<uint8_t>> validity_full = nullopt;
    optional<std::vector<uint8_t>> validity_single = nullopt;
    optional<std::vector<uint8_t>> validity_two_full = nullopt;
    if (nullable_) {
      validity_full = {1, 0, 1, 0, 1, 0, 1, 0, 1};
      validity_single = {1};
      validity_two_full = {
          1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    }

    // Write first tile.
    write_dense_string(
        {"0", "1", "2", "3", "4", "5", "6", "7", "8"},
        1,
        3,
        1,
        3,
        1,
        validity_full);

    // Write second tile.
    write_dense_string(
        {"999", "10", "11", "12", "255", "14", "15", "16", "17"},
        1,
        3,
        4,
        6,
        3,
        validity_full);

    // Overwrite single value in second tile. This will create overlapping
    // fragment domains.
    write_dense_string({"13"}, 2, 2, 5, 5, 5, validity_single);

    // Write third and fourth tile.
    write_dense_string(
        {"18",
         "19",
         "20",
         "21",
         "22",
         "23",
         "24",
         "25",
         "26",
         "27",
         "28",
         "29",
         "30",
         "31",
         "32",
         "33",
         "34",
         "35"},
        4,
        6,
        1,
        6,
        7,
        validity_two_full);
  } else {
    create_sparse_array(true);

    // Write fragments, only cell 3,3 is duplicated.
    optional<std::vector<uint8_t>> validity_values = nullopt;
    if (nullable_) {
      validity_values = {1, 0, 1, 0};
    }

    write_sparse(
        {"0", "1", "2", "3"}, {1, 1, 1, 2}, {1, 2, 4, 3}, 1, validity_values);
    write_sparse(
        {"4", "5", "6", "7"}, {2, 2, 3, 3}, {2, 4, 2, 3}, 3, validity_values);
    write_sparse(
        {"8", "99", "10", "11"},
        {2, 1, 3, 4},
        {1, 3, 1, 1},
        4,
        validity_values);
    write_sparse(
        {"12", "13", "14", "15"},
        {4, 3, 3, 4},
        {2, 3, 4, 4},
        6,
        validity_values);
  }
}

template <class T>
void CppAggregatesFx<T>::set_ranges_and_condition_if_needed(
    Array& array, Query& query, const bool var) {
  if (set_ranges_) {
    // Slice only rows 2 to 5 for dense, 2, 3 for sparse.
    Subarray subarray(CppAggregatesFx<T>::ctx_, array);

    if (dense_) {
      subarray.add_range<uint64_t>(0, 2, 5).add_range<uint64_t>(1, 1, 6);
    } else {
      subarray.add_range<uint64_t>(0, 3, 4);
    }
    query.set_subarray(subarray);
  } else if (dense_) {
    Subarray subarray(CppAggregatesFx<T>::ctx_, array);
    subarray.add_range<uint64_t>(0, 1, 6).add_range<uint64_t>(1, 1, 6);
    query.set_subarray(subarray);
  }

  if (set_qc_) {
    QueryCondition qc1(ctx_);
    QueryCondition qc2(ctx_);

    if (var) {
      std::string val1 = "8";
      qc1.init("a1", val1, TILEDB_NE);

      std::string val2 = "999";
      qc2.init("a1", val2, TILEDB_NE);
    } else {
      auto val1 = make_data_buff({4});
      qc1.init("a1", val1.data(), val1.size(), TILEDB_NE);

      auto val2 = make_data_buff({35});
      qc2.init("a1", val2.data(), val2.size(), TILEDB_NE);
    }

    QueryCondition qc = qc1.combine(qc2, TILEDB_AND);
    query.set_condition(qc);
  }
}

template <class T>
void CppAggregatesFx<T>::validate_data(
    Query& query,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2,
    std::vector<uint8_t>& a1,
    std::vector<uint8_t>& a1_validity) {
  uint64_t expected_count = 0;
  std::vector<uint64_t> expected_dim1;
  std::vector<uint64_t> expected_dim2;
  std::vector<int> expected_a1_int;
  std::vector<uint8_t> expected_a1_validity;
  if (dense_) {
    if (layout_ == TILEDB_ROW_MAJOR) {
      if (set_ranges_) {
        expected_count = 24;
        expected_dim1 = {2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3,
                         4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5};
        expected_dim2 = {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6,
                         1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6};
        expected_a1_int = {3,  4,  5,  12, 13, 14, 6,  7,  8,  15, 16, 17,
                           18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29};
        expected_a1_validity = {0, 1, 0, 0, 1, 0, 1, 0, 1, 1, 0, 1,
                                1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};

      } else {
        expected_count = 36;
        expected_dim1 = {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3,
                         4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6};
        expected_dim2 = {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6,
                         1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6};
        expected_a1_int = {0,  1,  2,  9,  10, 11, 3,  4,  5,  12, 13, 14,
                           6,  7,  8,  15, 16, 17, 18, 19, 20, 21, 22, 23,
                           24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35};
        expected_a1_validity = {1, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 0,
                                1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 0,
                                1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
      }
    } else if (layout_ == TILEDB_COL_MAJOR) {
      if (set_ranges_) {
        expected_count = 24;
        expected_dim1 = {2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5,
                         2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5};
        expected_dim2 = {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3,
                         4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6};
        expected_a1_int = {3,  6,  18, 24, 4,  7,  19, 25, 5,  8,  20, 26,
                           12, 15, 21, 27, 13, 16, 22, 28, 14, 17, 23, 29};
        expected_a1_validity = {0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1,
                                0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0};
      } else {
        expected_count = 36;
        expected_dim1 = {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6,
                         1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6};
        expected_dim2 = {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3,
                         4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6};
        expected_a1_int = {0,  3,  6,  18, 24, 30, 1,  4,  7,  19, 25, 31,
                           2,  5,  8,  20, 26, 32, 9,  12, 15, 21, 27, 33,
                           10, 13, 16, 22, 28, 34, 11, 14, 17, 23, 29, 35};
        expected_a1_validity = {1, 0, 1, 1, 1, 1, 0, 1, 0, 0, 0, 0,
                                1, 0, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0,
                                0, 1, 0, 1, 1, 1, 1, 0, 1, 0, 0, 0};
      }
    } else if (layout_ == TILEDB_GLOBAL_ORDER) {
      if (set_ranges_) {
        expected_count = 24;
        expected_dim1 = {2, 2, 2, 3, 3, 3, 2, 2, 2, 3, 3, 3,
                         4, 4, 4, 5, 5, 5, 4, 4, 4, 5, 5, 5};
        expected_dim2 = {1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6,
                         1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6};
        expected_a1_int = {3,  4,  5,  6,  7,  8,  12, 13, 14, 15, 16, 17,
                           18, 19, 20, 24, 25, 26, 21, 22, 23, 27, 28, 29};
        expected_a1_validity = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                1, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 0};
      } else {
        expected_count = 36;
        expected_dim1 = {1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1, 2, 2, 2, 3, 3, 3,
                         4, 4, 4, 5, 5, 5, 6, 6, 6, 4, 4, 4, 5, 5, 5, 6, 6, 6};
        expected_dim2 = {1, 2, 3, 1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6, 4, 5, 6,
                         1, 2, 3, 1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6, 4, 5, 6};
        expected_a1_int = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                           12, 13, 14, 15, 16, 17, 18, 19, 20, 24, 25, 26,
                           30, 31, 32, 21, 22, 23, 27, 28, 29, 33, 34, 35};
        expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1,
                                0, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1,
                                1, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0};
      }
    }
  } else {
    if (set_ranges_) {
      if (allow_dups_) {
        expected_count = 8;
        expected_dim1 = {3, 3, 3, 4, 4, 3, 3, 4};
        expected_dim2 = {2, 3, 1, 1, 2, 3, 4, 4};
        expected_a1_int = {6, 7, 10, 11, 12, 13, 14, 15};
        expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0};
      } else {
        expected_count = 7;
        expected_dim1 = {3, 3, 4, 4, 3, 3, 4};
        expected_dim2 = {1, 2, 1, 2, 3, 4, 4};
        expected_a1_int = {10, 6, 11, 12, 13, 14, 15};
        expected_a1_validity = {1, 1, 0, 1, 0, 1, 0};
      }
    } else {
      if (allow_dups_) {
        expected_count = 16;
        expected_dim1 = {1, 1, 1, 2, 2, 2, 3, 3, 2, 1, 3, 4, 4, 3, 3, 4};
        expected_dim2 = {1, 2, 4, 3, 2, 4, 2, 3, 1, 3, 1, 1, 2, 3, 4, 4};
        expected_a1_int = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
      } else {
        expected_count = 15;
        expected_dim1 = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4};
        expected_dim2 = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 4};
        expected_a1_int = {0, 1, 8, 4, 9, 2, 3, 5, 10, 6, 11, 12, 13, 14, 15};
        expected_a1_validity = {1, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0};
      }
    }
  }

  std::vector<uint8_t> expected_a1 =
      make_data_buff(expected_a1_int, expected_a1_validity);
  if (set_qc_) {
    for (uint64_t i = 0; i < expected_a1_int.size(); i++) {
      if (expected_a1_int[i] == 4 || expected_a1_int[i] == 35) {
        expected_a1_validity[i] = 0;
      }
    }
  }

  uint64_t cell_val_num = std::is_same<T, std::string>::value ?
                              CppAggregatesFx<T>::STRING_CELL_VAL_NUM :
                              1;

  auto result_el = query.result_buffer_elements_nullable();
  CHECK(std::get<1>(result_el["d1"]) == expected_count);
  CHECK(std::get<1>(result_el["d2"]) == expected_count);
  CHECK(std::get<1>(result_el["a1"]) == expected_count * cell_val_num);
  if (nullable_) {
    CHECK(std::get<2>(result_el["a1"]) == expected_count);
  }
  dim1.resize(expected_dim1.size());
  dim2.resize(expected_dim2.size());
  a1.resize(expected_a1.size());
  CHECK(dim1 == expected_dim1);
  CHECK(dim2 == expected_dim2);
  CHECK(a1 == expected_a1);

  if (nullable_) {
    a1_validity.resize(expected_a1_validity.size());
    CHECK(a1_validity == expected_a1_validity);
  }
}

template <class T>
void CppAggregatesFx<T>::validate_data_var(
    Query& query,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2,
    std::string& a1_data,
    std::vector<uint64_t>& a1_offsets,
    std::vector<uint8_t>& a1_validity) {
  auto result_el = query.result_buffer_elements_nullable();
  uint64_t expected_count = 0;
  std::vector<uint64_t> expected_dim1;
  std::vector<uint64_t> expected_dim2;
  std::vector<std::string> expected_a1;
  std::vector<uint8_t> expected_a1_validity;

  if (dense_) {
    if (layout_ == TILEDB_ROW_MAJOR) {
      if (set_ranges_) {
        expected_count = 24;
        expected_dim1 = {2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3,
                         4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5};
        expected_dim2 = {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6,
                         1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6};
        expected_a1 = {"3",  "4",  "5",  "12", "13", "14", "6",  "7",
                       "8",  "15", "16", "17", "18", "19", "20", "21",
                       "22", "23", "24", "25", "26", "27", "28", "29"};
        expected_a1_validity = {0, 1, 0, 0, 1, 0, 1, 0, 1, 1, 0, 1,
                                1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
      } else {
        expected_count = 36;
        expected_dim1 = {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3,
                         4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6};
        expected_dim2 = {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6,
                         1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6};
        expected_a1 = {"0",  "1",  "2",  "999", "10", "11", "3",  "4",  "5",
                       "12", "13", "14", "6",   "7",  "8",  "15", "16", "17",
                       "18", "19", "20", "21",  "22", "23", "24", "25", "26",
                       "27", "28", "29", "30",  "31", "32", "33", "34", "35"};
        expected_a1_validity = {1, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 0,
                                1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 0,
                                1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
      }
    } else if (layout_ == TILEDB_COL_MAJOR) {
      if (set_ranges_) {
        expected_count = 24;
        expected_dim1 = {2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5,
                         2, 3, 4, 5, 2, 3, 4, 5, 2, 3, 4, 5};
        expected_dim2 = {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3,
                         4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6};
        expected_a1 = {"3",  "6",  "18", "24", "4",  "7",  "19", "25",
                       "5",  "8",  "20", "26", "12", "15", "21", "27",
                       "13", "16", "22", "28", "14", "17", "23", "29"};
        expected_a1_validity = {0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1,
                                0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 0};
      } else {
        expected_count = 36;
        expected_dim1 = {1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6,
                         1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6};
        expected_dim2 = {1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3,
                         4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6};
        expected_a1 = {"0",   "3",  "6",  "18", "24", "30", "1",  "4",  "7",
                       "19",  "25", "31", "2",  "5",  "8",  "20", "26", "32",
                       "999", "12", "15", "21", "27", "33", "10", "13", "16",
                       "22",  "28", "34", "11", "14", "17", "23", "29", "35"};
        expected_a1_validity = {1, 0, 1, 1, 1, 1, 0, 1, 0, 0, 0, 0,
                                1, 0, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0,
                                0, 1, 0, 1, 1, 1, 1, 0, 1, 0, 0, 0};
      }
    } else if (layout_ == TILEDB_GLOBAL_ORDER) {
      if (set_ranges_) {
        expected_count = 24;
        expected_dim1 = {2, 2, 2, 3, 3, 3, 2, 2, 2, 3, 3, 3,
                         4, 4, 4, 5, 5, 5, 4, 4, 4, 5, 5, 5};
        expected_dim2 = {1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6,
                         1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6};
        expected_a1 = {"3",  "4",  "5",  "6",  "7",  "8",  "12", "13",
                       "14", "15", "16", "17", "18", "19", "20", "24",
                       "25", "26", "21", "22", "23", "27", "28", "29"};
        expected_a1_validity = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
                                1, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 0};
      } else {
        expected_count = 36;
        expected_dim1 = {1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1, 2, 2, 2, 3, 3, 3,
                         4, 4, 4, 5, 5, 5, 6, 6, 6, 4, 4, 4, 5, 5, 5, 6, 6, 6};
        expected_dim2 = {1, 2, 3, 1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6, 4, 5, 6,
                         1, 2, 3, 1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6, 4, 5, 6};
        expected_a1 = {"0",   "1",  "2",  "3",  "4",  "5",  "6",  "7",  "8",
                       "999", "10", "11", "12", "13", "14", "15", "16", "17",
                       "18",  "19", "20", "24", "25", "26", "30", "31", "32",
                       "21",  "22", "23", "27", "28", "29", "33", "34", "35"};
        expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1,
                                0, 1, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1,
                                1, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0};
      }
    }
  } else {
    if (set_ranges_) {
      if (allow_dups_) {
        expected_count = 8;
        expected_dim1 = {3, 3, 3, 4, 4, 3, 3, 4};
        expected_dim2 = {2, 3, 1, 1, 2, 3, 4, 4};
        expected_a1 = {"6", "7", "10", "11", "12", "13", "14", "15"};
        expected_a1_validity = {1, 0, 1, 0, 1, 0, 1, 0};
      } else {
        expected_count = 7;
        expected_dim1 = {3, 3, 4, 4, 3, 3, 4};
        expected_dim2 = {1, 2, 1, 2, 3, 4, 4};
        expected_a1 = {"10", "6", "11", "12", "13", "14", "15"};
        expected_a1_validity = {1, 1, 0, 1, 0, 1, 0};
      }
    } else {
      if (allow_dups_) {
        expected_count = 16;
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
        expected_count = 15;
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
    }
  }

  // Generate a vector from the read data to compare against our expectation.
  uint64_t expected_a1_size = 0;
  std::vector<std::string> a1_data_vec;
  for (uint64_t c = 0; c < expected_count - 1; c++) {
    auto size = a1_offsets[c + 1] - a1_offsets[c];
    expected_a1_size += size;

    auto v = a1_data.substr(a1_offsets[c], size);
    a1_data_vec.emplace_back(v);
  }

  auto size = std::get<1>(result_el["a1"]) - a1_offsets[expected_count - 1];
  expected_a1_size += size;
  a1_data_vec.emplace_back(
      a1_data.substr(a1_offsets[expected_count - 1], size));

  // Generate an expected vector taking into consideration the query condition.
  std::vector<std::string> expected_a1_with_qc;
  expected_a1_with_qc.reserve(expected_a1.size());
  for (uint64_t c = 0; c < expected_a1.size(); c++) {
    const auto& v = expected_a1[c];
    if (set_qc_ && (v == "8" || v == "999" ||
                    (nullable_ && expected_a1_validity[c] == 0))) {
      std::string zero;
      zero.resize(1);
      zero[0] = 0;
      expected_a1_with_qc.emplace_back(zero);
    } else {
      expected_a1_with_qc.emplace_back(v);
    }
  }

  // Apply the query condition to the validity values.
  if (set_qc_) {
    for (uint64_t i = 0; i < expected_a1.size(); i++) {
      if (expected_a1[i] == "8" || expected_a1[i] == "999") {
        expected_a1_validity[i] = 0;
      }
    }
  }

  CHECK(std::get<1>(result_el["d1"]) == expected_count);
  CHECK(std::get<1>(result_el["d2"]) == expected_count);
  CHECK(std::get<1>(result_el["a1"]) == expected_a1_size);
  CHECK(std::get<0>(result_el["a1"]) == expected_count);

  if (nullable_) {
    CHECK(std::get<2>(result_el["a1"]) == expected_count);
  }

  dim1.resize(expected_dim1.size());
  dim2.resize(expected_dim2.size());
  CHECK(dim1 == expected_dim1);
  CHECK(dim2 == expected_dim2);
  CHECK(a1_data_vec == expected_a1_with_qc);

  if (nullable_) {
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
void CppAggregatesFx<T>::remove_array() {
  remove_array(ARRAY_NAME);
}

template <class T>
bool CppAggregatesFx<T>::is_array(const std::string& array_name) {
  return vfs_.is_dir(array_name);
}

TEST_CASE_METHOD(
    CppAggregatesFx<int32_t>,
    "C++ API: Aggregates basic count",
    "[cppapi][aggregates][basic][count]") {
  if (!generate_test_params()) {
    return;
  }

  create_array_and_write_fragments();

  Array array{ctx_, ARRAY_NAME, TILEDB_READ};
  Query query(ctx_, array, TILEDB_READ);

  // TODO: Change to real CPPAPI. Add a count aggregator to the query.
  query.ptr()->query_->add_aggregator_to_default_channel(
      "Count", std::make_shared<tiledb::sm::CountAggregator>());

  set_ranges_and_condition_if_needed(array, query, false);

  // Set the data buffer for the aggregator.
  uint64_t cell_size = sizeof(int);
  std::vector<uint64_t> count(1);
  std::vector<uint64_t> dim1(100);
  std::vector<uint64_t> dim2(100);
  std::vector<uint8_t> a1(100 * cell_size);
  std::vector<uint8_t> a1_validity(100);
  query.set_layout(layout_);
  uint64_t count_data_size = sizeof(uint64_t);
  // TODO: Change to real CPPAPI.
  CHECK(query.ptr()
            ->query_->set_data_buffer("Count", count.data(), &count_data_size)
            .ok());

  if (request_data_) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer(
        "a1", static_cast<void*>(a1.data()), a1.size() / cell_size);

    if (nullable_) {
      query.set_validity_buffer("a1", a1_validity);
    }
  }

  // Submit the query.
  query.submit();

  // Check the results.
  auto result_el = query.result_buffer_elements_nullable();
  uint64_t expected_count;
  if (dense_) {
    expected_count = set_ranges_ ? 24 : 36;
  } else {
    if (set_ranges_) {
      expected_count = allow_dups_ ? 8 : 7;
    } else {
      expected_count = allow_dups_ ? 16 : 15;
    }
  }

  // TODO: use 'std::get<1>(result_el["Count"]) == 1' once we use the
  // set_data_buffer api.
  // CHECK(std::get<1>(result_el["Count"]) == 1);
  CHECK(count[0] == expected_count);

  if (request_data_) {
    validate_data(query, dim1, dim2, a1, a1_validity);
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
  if (!CppAggregatesFx<T>::generate_test_params()) {
    return;
  }

  CppAggregatesFx<T>::create_array_and_write_fragments();

  Array array{
      CppAggregatesFx<T>::ctx_, CppAggregatesFx<T>::ARRAY_NAME, TILEDB_READ};
  Query query(CppAggregatesFx<T>::ctx_, array, TILEDB_READ);

  // TODO: Change to real CPPAPI. Add a count aggregator to the query.
  query.ptr()->query_->add_aggregator_to_default_channel(
      "Sum",
      std::make_shared<tiledb::sm::SumAggregator<T>>(tiledb::sm::FieldInfo(
          "a1", false, CppAggregatesFx<T>::nullable_, 1)));

  CppAggregatesFx<T>::set_ranges_and_condition_if_needed(array, query, false);

  // Set the data buffer for the aggregator.
  uint64_t cell_size = sizeof(T);
  std::vector<typename tiledb::sm::sum_type_data<T>::sum_type> sum(1);
  std::vector<uint8_t> sum_validity(1);
  std::vector<uint64_t> dim1(100);
  std::vector<uint64_t> dim2(100);
  std::vector<uint8_t> a1(100 * cell_size);
  std::vector<uint8_t> a1_validity(100);
  query.set_layout(CppAggregatesFx<T>::layout_);
  uint64_t sum_data_size =
      sizeof(typename tiledb::sm::sum_type_data<T>::sum_type);
  // TODO: Change to real CPPAPI.
  CHECK(query.ptr()
            ->query_->set_data_buffer("Sum", sum.data(), &sum_data_size)
            .ok());
  uint64_t returned_validity_size = 1;
  if (CppAggregatesFx<T>::nullable_) {
    // TODO: Change to real CPPAPI. Use set_validity_buffer from the internal
    // query directly because the CPPAPI doesn't know what is an aggregate and
    // what the size of an aggregate should be.
    CHECK(query.ptr()
              ->query_
              ->set_validity_buffer(
                  "Sum", sum_validity.data(), &returned_validity_size)
              .ok());
  }

  if (CppAggregatesFx<T>::request_data_) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer(
        "a1", static_cast<void*>(a1.data()), a1.size() / cell_size);

    if (CppAggregatesFx<T>::nullable_) {
      query.set_validity_buffer("a1", a1_validity);
    }
  }

  // Submit the query.
  query.submit();

  // Check the results.
  typename tiledb::sm::sum_type_data<T>::sum_type expected_sum;
  if (CppAggregatesFx<T>::dense_) {
    if (CppAggregatesFx<T>::nullable_) {
      if (CppAggregatesFx<T>::set_ranges_) {
        expected_sum = CppAggregatesFx<T>::set_qc_ ? 197 : 201;
      } else {
        expected_sum = CppAggregatesFx<T>::set_qc_ ? 315 : 319;
      }
    } else {
      if (CppAggregatesFx<T>::set_ranges_) {
        expected_sum = CppAggregatesFx<T>::set_qc_ ? 398 : 402;
      } else {
        expected_sum = CppAggregatesFx<T>::set_qc_ ? 591 : 630;
      }
    }
  } else {
    if (CppAggregatesFx<T>::nullable_) {
      if (CppAggregatesFx<T>::set_ranges_) {
        expected_sum = 42;
      } else {
        expected_sum = 56;
      }
    } else {
      if (CppAggregatesFx<T>::set_ranges_) {
        expected_sum = CppAggregatesFx<T>::allow_dups_ ? 88 : 81;
      } else {
        expected_sum = CppAggregatesFx<T>::allow_dups_ ? 120 : 113;
      }
    }
  }

  // TODO: use 'std::get<1>(result_el["Sum"]) == 1' once we use the
  // set_data_buffer api.
  // CHECK(std::get<1>(result_el["Sum"]) == 1);
  CHECK(sum[0] == expected_sum);

  if (CppAggregatesFx<T>::request_data_) {
    CppAggregatesFx<T>::validate_data(query, dim1, dim2, a1, a1_validity);
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
    "[cppapi][aggregates][basic][min-max][fixed]",
    MinMaxFixedTypesUnderTest) {
  typedef decltype(TestType::first) T;
  typedef decltype(TestType::second) AGG;
  CppAggregatesFx<T> fx;
  bool min = std::is_same<AGG, tiledb::sm::MinAggregator<T>>::value;
  if (!fx.generate_test_params()) {
    return;
  }

  fx.create_array_and_write_fragments();

  Array array{fx.ctx_, fx.ARRAY_NAME, TILEDB_READ};
  Query query(fx.ctx_, array, TILEDB_READ);

  // TODO: Change to real CPPAPI. Add a count aggregator to the query.
  uint64_t cell_val_num =
      std::is_same<T, std::string>::value ? fx.STRING_CELL_VAL_NUM : 1;
  query.ptr()->query_->add_aggregator_to_default_channel(
      "MinMax",
      std::make_shared<AGG>(
          tiledb::sm::FieldInfo("a1", false, fx.nullable_, cell_val_num)));

  fx.set_ranges_and_condition_if_needed(array, query, false);

  // Set the data buffer for the aggregator.
  uint64_t cell_size =
      std::is_same<T, std::string>::value ? fx.STRING_CELL_VAL_NUM : sizeof(T);
  std::vector<uint8_t> min_max(cell_size);
  std::vector<uint8_t> min_max_validity(1);
  std::vector<uint64_t> dim1(100);
  std::vector<uint64_t> dim2(100);
  std::vector<uint8_t> a1(100 * cell_size);
  std::vector<uint8_t> a1_validity(100);
  query.set_layout(fx.layout_);

  // TODO: Change to real CPPAPI. Use set_data_buffer and set_validity_buffer
  // from the internal query directly because the CPPAPI doesn't know what is
  // an aggregate and what the size of an aggregate should be.
  uint64_t returned_min_max_size = cell_size;
  uint64_t returned_validity_size = 1;
  CHECK(query.ptr()
            ->query_
            ->set_data_buffer("MinMax", min_max.data(), &returned_min_max_size)
            .ok());
  if (fx.nullable_) {
    CHECK(query.ptr()
              ->query_
              ->set_validity_buffer(
                  "MinMax", min_max_validity.data(), &returned_validity_size)
              .ok());
  }

  if (fx.request_data_) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer(
        "a1", static_cast<void*>(a1.data()), a1.size() / cell_size);

    if (fx.nullable_) {
      query.set_validity_buffer("a1", a1_validity);
    }
  }

  // Submit the query.
  query.submit();

  // Check the results.
  std::vector<uint8_t> expected_min_max;
  if (fx.dense_) {
    if (fx.nullable_) {
      if (fx.set_ranges_) {
        expected_min_max = fx.make_data_buff({min ? (fx.set_qc_ ? 6 : 4) : 28});
      } else {
        expected_min_max = fx.make_data_buff({min ? 0 : 34});
      }
    } else {
      if (fx.set_ranges_) {
        expected_min_max = fx.make_data_buff({min ? 3 : 29});
      } else {
        expected_min_max =
            fx.make_data_buff({min ? 0 : (fx.set_qc_ ? 34 : 35)});
      }
    }
  } else {
    if (fx.nullable_) {
      if (fx.set_ranges_) {
        expected_min_max = fx.make_data_buff({min ? 6 : 14});
      } else {
        expected_min_max = fx.make_data_buff({min ? 0 : 14});
      }
    } else {
      if (fx.set_ranges_) {
        expected_min_max = fx.make_data_buff({min ? 6 : 15});
      } else {
        expected_min_max = fx.make_data_buff({min ? 0 : 15});
      }
    }
  }

  // TODO: use 'std::get<1>(result_el["MinMax"]) == 1' once we use the
  // set_data_buffer api.
  CHECK(returned_min_max_size == cell_size);
  CHECK(min_max == expected_min_max);

  if (fx.request_data_) {
    fx.validate_data(query, dim1, dim2, a1, a1_validity);
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
  if (!fx.generate_test_params()) {
    return;
  }

  fx.create_var_array_and_write_fragments();

  Array array{fx.ctx_, fx.ARRAY_NAME, TILEDB_READ};
  Query query(fx.ctx_, array, TILEDB_READ);

  // TODO: Change to real CPPAPI. Add a count aggregator to the query.
  query.ptr()->query_->add_aggregator_to_default_channel(
      "MinMax",
      std::make_shared<AGG>(
          tiledb::sm::FieldInfo("a1", true, fx.nullable_, TILEDB_VAR_NUM)));

  fx.set_ranges_and_condition_if_needed(array, query, true);

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
  query.set_layout(fx.layout_);

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
  if (fx.nullable_) {
    CHECK(query.ptr()
              ->query_
              ->set_validity_buffer(
                  "MinMax", min_max_validity.data(), &returned_validity_size)
              .ok());
  }

  if (fx.request_data_) {
    query.set_data_buffer("d1", dim1);
    query.set_data_buffer("d2", dim2);
    query.set_data_buffer("a1", a1_data);
    query.set_offsets_buffer("a1", a1_offsets);

    if (fx.nullable_) {
      query.set_validity_buffer("a1", a1_validity);
    }
  }

  // Submit the query.
  query.submit();

  // Check the results.
  std::string expected_min_max;
  if (fx.dense_) {
    if (fx.nullable_) {
      if (fx.set_ranges_) {
        expected_min_max = min ? "13" : fx.set_qc_ ? "6" : "8";
      } else {
        expected_min_max = min ? "0" : fx.set_qc_ ? "6" : "999";
      }
    } else {
      if (fx.set_ranges_) {
        expected_min_max = min ? "12" : fx.set_qc_ ? "7" : "8";
      } else {
        expected_min_max = min ? "0" : fx.set_qc_ ? "7" : "999";
      }
    }
  } else {
    if (fx.nullable_) {
      if (fx.set_ranges_) {
        expected_min_max = min ? "10" : "6";
      } else {
        expected_min_max = min ? "0" : "8";
      }
    } else {
      if (fx.set_ranges_) {
        expected_min_max = min ? "10" : (fx.allow_dups_ ? "7" : "6");
      } else {
        expected_min_max = min ? "0" : "99";
      }
    }
  }

  // TODO: use 'std::get<1>(result_el["MinMax"]) == 1' once we use the
  // set_data_buffer api.
  CHECK(returned_min_max_offsets_size == 8);
  CHECK(returned_min_max_data_size == expected_min_max.size());

  min_max_data.resize(expected_min_max.size());
  CHECK(min_max_data == expected_min_max);
  CHECK(min_max_offset[0] == 0);

  if (fx.request_data_) {
    fx.validate_data_var(query, dim1, dim2, a1_data, a1_offsets, a1_validity);
  }

  // Close array.
  array.close();
}