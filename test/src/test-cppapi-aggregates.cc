/**
 * @file test-cppapi-aggregates.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
#include "tiledb/sm/cpp_api/tiledb_experimental"
#include "tiledb/sm/query/readers/aggregators/count_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/min_max_aggregator.h"
#include "tiledb/sm/query/readers/aggregators/sum_aggregator.h"

#include <test/support/src/helper_type.h>
#include <test/support/tdb_catch.h>

using namespace tiledb;
using namespace tiledb::test;

template <class T>
struct CppAggregatesFx {
  // Constants.
  const char* ARRAY_NAME = "test_aggregates_array";
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
  bool use_dim_;
  std::string dim_name_;
  tiledb_layout_t layout_;
  std::vector<bool> set_qc_values_;
  std::vector<bool> use_dim_values_;
  std::vector<std::string> dim_name_values_;
  std::vector<tiledb_layout_t> layout_values_;

  // Constructors/destructors.
  CppAggregatesFx();
  ~CppAggregatesFx();

  // Functions.
  void generate_test_params();
  void run_all_combinations(std::function<void()> fn);
  void create_dense_array(bool var = false);
  void create_sparse_array(bool var = false);
  void write_sparse(
      std::vector<int> a,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp,
      optional<std::vector<uint8_t>> a_validity = nullopt);
  void write_sparse(
      std::vector<std::string> a,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp,
      optional<std::vector<uint8_t>> a_validity = nullopt);
  void write_dense(
      std::vector<int> a,
      uint64_t dim1_min,
      uint64_t dim1_max,
      uint64_t dim2_min,
      uint64_t dim2_max,
      uint64_t timestamp,
      optional<std::vector<uint8_t>> a_validity = nullopt);
  void write_dense_string(
      std::vector<std::string> a,
      uint64_t dim1_min,
      uint64_t dim1_max,
      uint64_t dim2_min,
      uint64_t dim2_max,
      uint64_t timestamp,
      optional<std::vector<uint8_t>> a_validity = nullopt);
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
      std::vector<uint8_t>& a1_validity,
      std::function<void(uint64_t value)> aggregate_fn,
      const bool validate_count = true);
  void validate_data_var(
      Query& query,
      std::vector<uint64_t>& dim1,
      std::vector<uint64_t>& dim2,
      std::string& a1_data,
      std::vector<uint64_t>& a1_offsets,
      std::vector<uint8_t>& a1_validity,
      const bool validate_count = true);
  void validate_tiles_read(Query& query, bool is_count = false);
  void validate_tiles_read_null_count(Query& query);
  void validate_tiles_read_var(Query& query);
  void validate_tiles_read_null_count_var(Query& query);
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
void CppAggregatesFx<T>::generate_test_params() {
  SECTION("dense") {
    dense_ = true;
    nullable_ = GENERATE(true, false);
    allow_dups_ = false;
    set_qc_values_ = {true, false};
    layout_values_ = {TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR, TILEDB_GLOBAL_ORDER};
    use_dim_values_ = {true, false};
    dim_name_values_ = {"d2"};
    if (nullable_ || !std::is_same<T, uint64_t>::value) {
      use_dim_values_ = {false};
      dim_name_values_ = {"d1"};
    }
  }

  SECTION("sparse") {
    dense_ = false;
    nullable_ = GENERATE(true, false);
    allow_dups_ = GENERATE(true, false);
    set_qc_values_ = {false};
    layout_values_ = {TILEDB_ROW_MAJOR, TILEDB_UNORDERED};
    use_dim_values_ = {true, false};
    if (nullable_ || !std::is_same<T, uint64_t>::value) {
      use_dim_values_ = {false};
    }
    dim_name_values_ = {"d1"};
  }
}

template <class T>
void CppAggregatesFx<T>::run_all_combinations(std::function<void()> fn) {
  for (bool use_dim : use_dim_values_) {
    use_dim_ = use_dim;
    for (std::string dim_name : dim_name_values_) {
      dim_name_ = dim_name;
      for (bool set_ranges : {true, false}) {
        set_ranges_ = set_ranges;
        for (bool request_data : {true, false}) {
          request_data_ = request_data;
          for (bool set_qc : set_qc_values_) {
            set_qc_ = set_qc;
            for (tiledb_layout_t layout : layout_values_) {
              // Filter invalid combination. The legacy reader does not support
              // aggregates, and we cannot automatically switch to unordered
              // reads if we are requesting both the aggregates and the data.
              if (request_data && layout != TILEDB_UNORDERED) {
                continue;
              }
              layout_ = layout;
              fn();
            }
          }
        }
      }
    }
  }
}

template <class T>
void CppAggregatesFx<T>::create_dense_array(bool var) {
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

  auto a2 = Attribute::create<T>(ctx_, "a2");
  a2.set_nullable(nullable_);
  if (std::is_same<T, std::string>::value) {
    a2.set_cell_val_num(
        var ? TILEDB_VAR_NUM : CppAggregatesFx<T>::STRING_CELL_VAL_NUM);
  }

  // Create array schema.
  ArraySchema schema(ctx_, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.add_attributes(a1);
  schema.add_attributes(a2);

  // Set up filters.
  Filter filter(ctx_, TILEDB_FILTER_NONE);
  FilterList filter_list(ctx_);
  filter_list.add_filter(filter);
  schema.set_coords_filter_list(filter_list);

  Array::create(ARRAY_NAME, schema);
}

template <class T>
void CppAggregatesFx<T>::create_sparse_array(bool var) {
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

  auto a2 = Attribute::create<T>(ctx_, "a2");
  a2.set_nullable(nullable_);
  if (std::is_same<T, std::string>::value) {
    a2.set_cell_val_num(
        var ? TILEDB_VAR_NUM : CppAggregatesFx<T>::STRING_CELL_VAL_NUM);
  }

  // Create array schema.
  ArraySchema schema(ctx_, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.set_capacity(20);
  schema.add_attributes(a1);
  schema.add_attributes(a2);

  if (allow_dups_) {
    schema.set_allows_dups(true);
  }

  // Set up filters.
  Filter filter(ctx_, TILEDB_FILTER_NONE);
  FilterList filter_list(ctx_);
  filter_list.add_filter(filter);
  schema.set_coords_filter_list(filter_list);

  Array::create(ARRAY_NAME, schema);
}

template <class T>
void CppAggregatesFx<T>::write_sparse(
    std::vector<int> a,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp,
    optional<std::vector<uint8_t>> a_validity) {
  // Open array.
  Array array(
      ctx_,
      ARRAY_NAME,
      TILEDB_WRITE,
      TemporalPolicy(TimestampStartEnd, 0, timestamp));

  std::vector<uint8_t> a_buff = make_data_buff(a);
  uint64_t cell_val_num = std::is_same<T, std::string>::value ?
                              CppAggregatesFx<T>::STRING_CELL_VAL_NUM :
                              1;

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_data_buffer(
      "a1", static_cast<void*>(a_buff.data()), a.size() * cell_val_num);
  query.set_data_buffer(
      "a2", static_cast<void*>(a_buff.data()), a.size() * cell_val_num);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);
  if (a_validity.has_value()) {
    query.set_validity_buffer("a1", a_validity.value());
    query.set_validity_buffer("a2", a_validity.value());
  }

  // Submit/finalize the query.
  query.submit();
  query.finalize();

  // Close array.
  array.close();
}

template <class T>
void CppAggregatesFx<T>::write_sparse(
    std::vector<std::string> a,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp,
    optional<std::vector<uint8_t>> a_validity) {
  // Open array.
  Array array(
      ctx_,
      ARRAY_NAME,
      TILEDB_WRITE,
      TemporalPolicy(TimestampStartEnd, 0, timestamp));

  std::string a_data;
  std::vector<uint64_t> a_offsets;
  uint64_t offset = 0;
  for (auto& v : a) {
    a_data += v;
    a_offsets.emplace_back(offset);
    offset += v.length();
  }

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  query.set_offsets_buffer("a1", a_offsets);
  query.set_data_buffer("a1", a_data);
  query.set_offsets_buffer("a2", a_offsets);
  query.set_data_buffer("a2", a_data);
  query.set_data_buffer("d1", dim1);
  query.set_data_buffer("d2", dim2);
  if (a_validity.has_value()) {
    query.set_validity_buffer("a1", a_validity.value());
    query.set_validity_buffer("a2", a_validity.value());
  }

  // Submit/finalize the query.
  query.submit();
  query.finalize();

  // Close array.
  array.close();
}

template <class T>
void CppAggregatesFx<T>::write_dense(
    std::vector<int> a,
    uint64_t dim1_min,
    uint64_t dim1_max,
    uint64_t dim2_min,
    uint64_t dim2_max,
    uint64_t timestamp,
    optional<std::vector<uint8_t>> a_validity) {
  // Open array.
  Array array(
      ctx_,
      ARRAY_NAME,
      TILEDB_WRITE,
      TemporalPolicy(TimestampStartEnd, 0, timestamp));

  std::vector<uint8_t> a_buff = make_data_buff(a);
  uint64_t cell_val_num = std::is_same<T, std::string>::value ?
                              CppAggregatesFx<T>::STRING_CELL_VAL_NUM :
                              1;

  // Create the subarray.
  Subarray subarray(ctx_, array);
  subarray.add_range(0, dim1_min, dim1_max).add_range(1, dim2_min, dim2_max);

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  query.set_data_buffer(
      "a1", static_cast<void*>(a_buff.data()), a.size() * cell_val_num);
  query.set_data_buffer(
      "a2", static_cast<void*>(a_buff.data()), a.size() * cell_val_num);
  if (a_validity.has_value()) {
    query.set_validity_buffer("a1", a_validity.value());
    query.set_validity_buffer("a2", a_validity.value());
  }

  // Submit/finalize the query.
  query.submit();
  query.finalize();

  // Close array.
  array.close();
}

template <class T>
void CppAggregatesFx<T>::write_dense_string(
    std::vector<std::string> a,
    uint64_t dim1_min,
    uint64_t dim1_max,
    uint64_t dim2_min,
    uint64_t dim2_max,
    uint64_t timestamp,
    optional<std::vector<uint8_t>> a_validity) {
  // Open array.
  Array array(
      ctx_,
      ARRAY_NAME,
      TILEDB_WRITE,
      TemporalPolicy(TimestampStartEnd, 0, timestamp));

  std::string a_data;
  std::vector<uint64_t> a_offsets;
  uint64_t offset = 0;
  for (auto& v : a) {
    a_data += v;
    a_offsets.emplace_back(offset);
    offset += v.length();
  }

  // Create the subarray.
  Subarray subarray(ctx_, array);
  subarray.add_range(0, dim1_min, dim1_max).add_range(1, dim2_min, dim2_max);

  // Create query.
  Query query(ctx_, array, TILEDB_WRITE);
  query.set_layout(TILEDB_ROW_MAJOR);
  query.set_subarray(subarray);
  query.set_offsets_buffer("a1", a_offsets);
  query.set_data_buffer("a1", a_data);
  if (a_validity.has_value()) {
    query.set_validity_buffer("a1", a_validity.value());
  }
  query.set_offsets_buffer("a2", a_offsets);
  query.set_data_buffer("a2", a_data);
  if (a_validity.has_value()) {
    query.set_validity_buffer("a2", a_validity.value());
  }

  // Submit/finalize the query.
  query.submit();
  query.finalize();

  // Close array.
  array.close();
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
      data.emplace_back(static_cast<T>(v));
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
      validity_single =
          std::make_optional<std::vector<uint8_t>>(std::vector<uint8_t>({1}));
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
        {8, 9, 10, 11}, {2, 1, 3, 4}, {1, 3, 1, 1}, 5, validity_values);
    write_sparse({12, 13}, {4, 3}, {2, 3}, 7, validity_values);
    write_sparse({14, 15}, {3, 4}, {4, 4}, 9, validity_values);
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
      validity_single =
          std::make_optional<std::vector<uint8_t>>(std::vector<uint8_t>({1}));
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
    std::vector<uint8_t>& a1_validity,
    std::function<void(uint64_t value)> aggregate_fn,
    const bool validate_count) {
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

  if (request_data_) {
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

    if (validate_count) {
      auto result_el = query.result_buffer_elements_nullable();
      CHECK(std::get<1>(result_el["d1"]) == expected_count);
      CHECK(std::get<1>(result_el["d2"]) == expected_count);
      CHECK(std::get<1>(result_el["a1"]) == expected_count * cell_val_num);
      if (nullable_) {
        CHECK(std::get<2>(result_el["a1"]) == expected_count);
      }
    }
  }

  // Call the aggregate function for all expected values.
  if (use_dim_ && dim_name_ == "d1") {
    for (uint64_t i = 0; i < expected_a1_int.size(); i++) {
      if (!set_qc_ || (expected_a1_int[i] != 4 && expected_a1_int[i] != 35)) {
        aggregate_fn(expected_dim1[i]);
      }
    }
  } else if (use_dim_ && dim_name_ == "d2") {
    for (uint64_t i = 0; i < expected_a1_int.size(); i++) {
      if (!set_qc_ || (expected_a1_int[i] != 4 && expected_a1_int[i] != 35)) {
        aggregate_fn(expected_dim2[i]);
      }
    }
  } else {
    for (uint64_t i = 0; i < expected_a1_int.size(); i++) {
      if (!set_qc_ || (expected_a1_int[i] != 4 && expected_a1_int[i] != 35)) {
        if (!nullable_ || expected_a1_validity[i] == 1) {
          aggregate_fn(expected_a1_int[i]);
        }
      }
    }
  }
}

template <class T>
void CppAggregatesFx<T>::validate_data_var(
    Query& query,
    std::vector<uint64_t>& dim1,
    std::vector<uint64_t>& dim2,
    std::string& a1_data,
    std::vector<uint64_t>& a1_offsets,
    std::vector<uint8_t>& a1_validity,
    const bool validate_count) {
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
  for (uint64_t c = 0; c < expected_count; c++) {
    auto size = a1_offsets[c + 1] - a1_offsets[c];
    expected_a1_size += size;

    auto v = a1_data.substr(a1_offsets[c], size);
    a1_data_vec.emplace_back(v);
  }

  // Generate an expected vector taking into consideration the query
  // condition.
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

  dim1.resize(expected_dim1.size());
  dim2.resize(expected_dim2.size());
  CHECK(dim1 == expected_dim1);
  CHECK(dim2 == expected_dim2);
  CHECK(a1_data_vec == expected_a1_with_qc);

  if (nullable_) {
    a1_validity.resize(expected_a1_validity.size());
    CHECK(a1_validity == expected_a1_validity);
  }

  if (validate_count) {
    auto result_el = query.result_buffer_elements_nullable();
    CHECK(std::get<1>(result_el["d1"]) == expected_count);
    CHECK(std::get<1>(result_el["d2"]) == expected_count);
    CHECK(std::get<1>(result_el["a1"]) == expected_a1_size);
    CHECK(std::get<0>(result_el["a1"]) == expected_count);
    if (nullable_) {
      CHECK(std::get<2>(result_el["a1"]) == expected_count);
    }
  }
}

uint64_t get_stat(std::string name, std::string& stats) {
  // Parse num_tiles_read from the stats.
  std::string to_find = "\"Context.Query.Reader." + name + "\": ";
  auto start_pos = stats.find(to_find);

  if (start_pos != std::string::npos) {
    start_pos += to_find.length();
    auto end_pos = stats.find("\n", start_pos);
    auto str = stats.substr(start_pos, end_pos - start_pos);
    return std::stoull(str);
  }

  return 0;
}

template <class T>
void CppAggregatesFx<T>::validate_tiles_read(Query& query, bool is_count) {
  // Validate the number of tiles read.
  auto stats = query.stats();

  uint64_t num_tiles_read = get_stat("num_tiles_read", stats);
  uint64_t expected_num_tiles_read;
  if (dense_) {
    // Dense has 5 tiles. If we request data or have a query condition, we'll
    // read all of them.
    if (request_data_ || set_qc_) {
      expected_num_tiles_read = 5;
    } else if (set_ranges_) {
      // If we request range, we split all tiles, we'll have to read all instead
      // of using fragment metadata.
      expected_num_tiles_read = is_count || use_dim_ ? 0 : 5;
    } else {
      // One space tile has two result tiles, we'll have to read them instead of
      // using fragment metadata.
      expected_num_tiles_read = is_count || use_dim_ ? 0 : 2;
    }
  } else {
    if (request_data_) {
      // Sparse has 5 tiles * 3 (2 dims/1 attr). One of them will be filtered
      // out when we set ranges.
      if (set_ranges_) {
        // If we set ranges, we filter out one of the 5 tiles.
        expected_num_tiles_read = 12;
      } else {
        // Everything is read.
        expected_num_tiles_read = 15;
      }
    } else {
      if (set_ranges_) {
        // For count, we only read dimension tiles, one of which is filtered so
        // we read 2 dims * 4 tiles. For the attribute, we can process 2 tiles
        // with duplicates and 1 without using the fragment metadata.
        if (allow_dups_) {
          expected_num_tiles_read = use_dim_ || is_count ? 8 : 10;
        } else {
          expected_num_tiles_read = use_dim_ || is_count ? 8 : 11;
        }
      } else {
        if (allow_dups_) {
          // No ranges, array with duplicates can do it all with fragment
          // metadata only.
          expected_num_tiles_read = 0;
        } else {
          // Arrays without duplicates need to run deduplication, so we read the
          // dimension tiles (2 dims * 5 tiles). Only one tile for the attribute
          // can be processed with fragment metadata only.
          expected_num_tiles_read = use_dim_ || is_count ? 10 : 14;
        }
      }
    }
  }

  CHECK(num_tiles_read == expected_num_tiles_read);
}

template <class T>
void CppAggregatesFx<T>::validate_tiles_read_null_count(Query& query) {
  // Validate the number of tiles read.
  auto stats = query.stats();

  uint64_t num_tiles_alloced = get_stat("tiles_allocated", stats);
  uint64_t num_tiles_unfiltered = get_stat("tiles_unfiltered", stats);
  uint64_t expected_num_tiles;
  if (dense_) {
    // Dense has 5 tiles. If we request data or have a query condition, we'll
    // read all of them.
    if (request_data_ || set_qc_) {
      expected_num_tiles = 10;
    } else if (set_ranges_) {
      // If we request range, we split all tiles, we'll have to read all, but
      // only nullable tiles.
      expected_num_tiles = 5;
    } else {
      // One space tile has two result tiles, we'll have to read them.
      expected_num_tiles = 2;
    }
  } else {
    // Sparse has 5 tiles * 4 (2 dims/1 attr (fixed tile + nullable tile)). One
    // of them will be filtered out when we set ranges.
    if (request_data_) {
      if (set_ranges_) {
        // If we set ranges, we filter out one of the 5 tiles.
        expected_num_tiles = 16;
      } else {
        // Everything is read.
        expected_num_tiles = 20;
      }
    } else {
      if (set_ranges_) {
        // We read dimension tiles to filter ranges, one of which is filtered so
        // we read 2 dims * 4 tiles. For the attribute, we can process 2 tiles
        // with duplicates and 1 without using the fragment metadata. Note that
        // we only add one per space tile for the attribute because we only read
        // the validity tile.
        if (allow_dups_) {
          expected_num_tiles = 10;
        } else {
          expected_num_tiles = 11;
        }
      } else {
        if (allow_dups_) {
          // No ranges, array with duplicates can do it all with fragment
          // metadata only.
          expected_num_tiles = 0;
        } else {
          // Arrays without duplicates need to run deduplication, so we read the
          // dimension tiles (2 dims * 5 tiles). Only one tile for the attribute
          // can be processed with fragment metadata only. Note that we only add
          // one per space tile for the attribute because we only read the
          // validity tile.
          expected_num_tiles = 14;
        }
      }
    }
  }

  CHECK(num_tiles_alloced == expected_num_tiles);
  CHECK(num_tiles_unfiltered == expected_num_tiles);
}

template <class T>
void CppAggregatesFx<T>::validate_tiles_read_var(Query& query) {
  // Validate the number of tiles read.
  auto stats = query.stats();

  uint64_t num_tiles_read = get_stat("num_tiles_read", stats);
  uint64_t expected_num_tiles_read;
  if (dense_) {
    // Dense has 5 tiles. If we request data or have a query condition or set
    // ranges, we'll read all of them.
    if (request_data_ || set_qc_ || set_ranges_) {
      expected_num_tiles_read = 5;
    } else {
      // One space tile has two result tiles, we'll have to read them.
      expected_num_tiles_read = 2;
    }
  } else {
    // Sparse has 4 tiles * 3 (2 dims/1 attr). One of them will be filtered
    // out when we set ranges.
    if (request_data_) {
      // We request data so everything is read. With ranges we read 3 tiles,
      // without 4. 2 dims, 1 attr, means we have to multiply by 3.
      if (set_ranges_) {
        expected_num_tiles_read = 9;
      } else {
        expected_num_tiles_read = 12;
      }
    } else {
      if (set_ranges_) {
        // To process ranges, we read 3 tiles * 2 dims at a minimum. For the
        // attribute, the array with duplicates can process one tile with the
        // fragment metadata, which is not the case for the array with no
        // duplicates.
        if (allow_dups_) {
          expected_num_tiles_read = 8;
        } else {
          expected_num_tiles_read = 9;
        }
      } else {
        // No ranges, the array with no duplicates can do it all with only
        // fragment metadata. The array with no duplicates cannot because the
        // cell slab structure never includes a full tile. It needs to read all
        // coordinate tiles and all attribute tiles.
        if (allow_dups_) {
          expected_num_tiles_read = 0;
        } else {
          expected_num_tiles_read = 12;
        }
      }
    }
  }

  CHECK(num_tiles_read == expected_num_tiles_read);
}

template <class T>
void CppAggregatesFx<T>::validate_tiles_read_null_count_var(Query& query) {
  // Validate the number of tiles read.
  auto stats = query.stats();

  uint64_t num_tiles_alloced = get_stat("tiles_allocated", stats);
  uint64_t num_tiles_unfiltered = get_stat("tiles_unfiltered", stats);
  uint64_t expected_num_tiles;
  if (dense_) {
    // Dense has 5 tiles. If we request data or have a query condition we'll
    // read all of them.
    if (request_data_ || set_qc_) {
      expected_num_tiles = 15;
    } else if (set_ranges_) {
      // If we set ranges, we only read the validity tiles.
      expected_num_tiles = 5;
    } else {
      // One space tile has two result tiles, we'll have to read them.
      expected_num_tiles = 2;
    }
  } else {
    // Sparse has 4 tiles * 5 (2 dims/1 attr (offset tile + var tile + nullable
    // tile)). One of them will be filtered out when we set ranges.
    if (request_data_) {
      // We request data so everything is read. With ranges we read 3 tiles,
      // without 4. 2 dims, 1 nullable var attr, means we have to multiply by 5.
      if (set_ranges_) {
        expected_num_tiles = 15;
      } else {
        expected_num_tiles = 20;
      }
    } else {
      if (set_ranges_) {
        // To process ranges, we read 3 tiles * 2 dims at a minimum. For the
        // attribute, the array with duplicates can process one tile with the
        // fragment metadata, which is not the case for the array with no
        // duplicates. Note that we only add one per space tile for the
        // attribute because we only read the validity tile.
        if (allow_dups_) {
          expected_num_tiles = 8;
        } else {
          // 3 * 2 for dims, 3 * 1 for attr.
          expected_num_tiles = 9;
        }
      } else {
        // No ranges, the array with no duplicates can do it all with only
        // fragment metadata. The array with no duplicates cannot because the
        // cell slab structure never includes a full tile. It needs to read all
        // coordinate tiles and all attribute tiles. Note that we only add one
        // per space tile for the attribute because we only read the validity
        // tile.
        if (allow_dups_) {
          expected_num_tiles = 0;
        } else {
          // 4 * 2 for dims, 4 * 1 for attr.
          expected_num_tiles = 12;
        }
      }
    }
  }

  CHECK(num_tiles_alloced == expected_num_tiles);
  CHECK(num_tiles_unfiltered == expected_num_tiles);
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
  generate_test_params();
  create_array_and_write_fragments();

  Array array{ctx_, ARRAY_NAME, TILEDB_READ};

  for (bool set_ranges : {true, false}) {
    set_ranges_ = set_ranges;
    use_dim_ = use_dim_ && !set_ranges;
    for (bool request_data : {true, false}) {
      request_data_ = request_data;
      for (bool set_qc : set_qc_values_) {
        set_qc_ = set_qc;
        for (tiledb_layout_t layout : layout_values_) {
          // Filter invalid combination. The legacy reader does not support
          // aggregates, and we cannot automatically switch to unordered
          // reads if we are requesting both the aggregates and the data.
          if (request_data && layout != TILEDB_UNORDERED) {
            continue;
          }
          layout_ = layout;
          Query query(ctx_, array, TILEDB_READ);

          // Add a count aggregator to the query.
          QueryChannel default_channel =
              QueryExperimental::get_default_channel(query);
          default_channel.apply_aggregate("Count", CountOperation());

          set_ranges_and_condition_if_needed(array, query, false);

          // Set the data buffer for the aggregator.
          uint64_t cell_size = sizeof(int);
          std::vector<uint64_t> count(1);
          std::vector<uint64_t> dim1(100);
          std::vector<uint64_t> dim2(100);
          std::vector<uint8_t> a1(100 * cell_size);
          std::vector<uint8_t> a1_validity(100);
          query.set_layout(layout);
          query.set_data_buffer("Count", count);

          if (request_data) {
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
          uint64_t expected_count;

          if (use_dim_) {
            expected_count = 999;
          } else if (dense_) {
            expected_count = set_ranges ? 24 : 36;
          } else {
            if (set_ranges) {
              expected_count = allow_dups_ ? 8 : 7;
            } else {
              expected_count = allow_dups_ ? 16 : 15;
            }
          }

          auto result_el = query.result_buffer_elements_nullable();
          CHECK(std::get<1>(result_el["Count"]) == 1);
          CHECK(count[0] == expected_count);

          if (request_data) {
            validate_data(
                query, dim1, dim2, a1, a1_validity, [&](uint64_t) -> void {});
          }

          validate_tiles_read(query, true);
        }
      }
    }
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
  CppAggregatesFx<T>::generate_test_params();
  CppAggregatesFx<T>::create_array_and_write_fragments();

  Array array{
      CppAggregatesFx<T>::ctx_, CppAggregatesFx<T>::ARRAY_NAME, TILEDB_READ};

  CppAggregatesFx<T>::run_all_combinations([&]() -> void {
    Query query(CppAggregatesFx<T>::ctx_, array, TILEDB_READ);

    // Add a sum aggregator to the query.
    QueryChannel default_channel =
        QueryExperimental::get_default_channel(query);
    ChannelOperation operation =
        QueryExperimental::create_unary_aggregate<SumOperator>(
            query,
            CppAggregatesFx<T>::use_dim_ ? CppAggregatesFx<T>::dim_name_ :
                                           "a1");
    default_channel.apply_aggregate("Sum", operation);

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
    query.set_data_buffer("Sum", sum);
    if (CppAggregatesFx<T>::nullable_) {
      query.set_validity_buffer("Sum", sum_validity);
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
    uint64_t expected = 0;
    CppAggregatesFx<T>::validate_data(
        query, dim1, dim2, a1, a1_validity, [&](uint64_t v) -> void {
          expected += v;
        });
    typename tiledb::sm::sum_type_data<T>::sum_type expected_sum = expected;

    auto result_el = query.result_buffer_elements_nullable();
    CHECK(std::get<1>(result_el["Sum"]) == 1);
    CHECK(sum[0] == expected_sum);

    CppAggregatesFx<T>::validate_tiles_read(query);
  });

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
    MeanFixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE_METHOD(
    CppAggregatesFx,
    "C++ API: Aggregates basic mean",
    "[cppapi][aggregates][basic][mean]",
    MeanFixedTypesUnderTest) {
  typedef TestType T;
  CppAggregatesFx<T>::generate_test_params();
  CppAggregatesFx<T>::create_array_and_write_fragments();

  Array array{
      CppAggregatesFx<T>::ctx_, CppAggregatesFx<T>::ARRAY_NAME, TILEDB_READ};

  CppAggregatesFx<T>::run_all_combinations([&]() -> void {
    Query query(CppAggregatesFx<T>::ctx_, array, TILEDB_READ);

    QueryChannel default_channel =
        QueryExperimental::get_default_channel(query);
    ChannelOperation operation =
        QueryExperimental::create_unary_aggregate<MeanOperator>(
            query,
            CppAggregatesFx<T>::use_dim_ ? CppAggregatesFx<T>::dim_name_ :
                                           "a1");
    default_channel.apply_aggregate("Mean", operation);

    CppAggregatesFx<T>::set_ranges_and_condition_if_needed(array, query, false);

    // Set the data buffer for the aggregator.
    uint64_t cell_size = sizeof(T);
    std::vector<double> mean(1);
    std::vector<uint8_t> mean_validity(1);
    std::vector<uint64_t> dim1(100);
    std::vector<uint64_t> dim2(100);
    std::vector<uint8_t> a1(100 * cell_size);
    std::vector<uint8_t> a1_validity(100);
    query.set_layout(CppAggregatesFx<T>::layout_);
    query.set_data_buffer("Mean", mean);
    if (CppAggregatesFx<T>::nullable_) {
      query.set_validity_buffer("Mean", mean_validity);
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
    uint64_t expected_sum = 0;
    uint64_t expected_count = 0;
    CppAggregatesFx<T>::validate_data(
        query, dim1, dim2, a1, a1_validity, [&](uint64_t v) -> void {
          expected_sum += v;
          expected_count++;
        });

    double expected_mean =
        static_cast<double>(expected_sum) / static_cast<double>(expected_count);

    auto result_el = query.result_buffer_elements_nullable();
    CHECK(std::get<1>(result_el["Mean"]) == 1);
    CHECK(mean[0] == expected_mean);

    CppAggregatesFx<T>::validate_tiles_read(query);
  });

  // Close array.
  array.close();
}

typedef tuple<
    std::pair<uint8_t, MinOperator>,
    std::pair<uint16_t, MinOperator>,
    std::pair<uint32_t, MinOperator>,
    std::pair<uint64_t, MinOperator>,
    std::pair<int8_t, MinOperator>,
    std::pair<int16_t, MinOperator>,
    std::pair<int32_t, MinOperator>,
    std::pair<int64_t, MinOperator>,
    std::pair<float, MinOperator>,
    std::pair<double, MinOperator>,
    std::pair<std::string, MinOperator>,
    std::pair<uint8_t, MaxOperator>,
    std::pair<uint16_t, MaxOperator>,
    std::pair<uint32_t, MaxOperator>,
    std::pair<uint64_t, MaxOperator>,
    std::pair<int8_t, MaxOperator>,
    std::pair<int16_t, MaxOperator>,
    std::pair<int32_t, MaxOperator>,
    std::pair<int64_t, MaxOperator>,
    std::pair<float, MaxOperator>,
    std::pair<double, MaxOperator>,
    std::pair<std::string, MaxOperator>>
    MinMaxFixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "C++ API: Aggregates basic min/max",
    "[cppapi][aggregates][basic][min-max][fixed]",
    MinMaxFixedTypesUnderTest) {
  typedef decltype(TestType::first) T;
  typedef decltype(TestType::second) AGG;
  CppAggregatesFx<T> fx;
  bool min = std::is_same<AGG, MinOperator>::value;
  fx.generate_test_params();
  fx.create_array_and_write_fragments();

  Array array{fx.ctx_, fx.ARRAY_NAME, TILEDB_READ};

  fx.run_all_combinations([&]() -> void {
    Query query(fx.ctx_, array, TILEDB_READ);

    // Add a min/max aggregator to the query.
    QueryChannel default_channel =
        QueryExperimental::get_default_channel(query);
    ChannelOperation operation = QueryExperimental::create_unary_aggregate<AGG>(
        query, fx.use_dim_ ? fx.dim_name_ : "a1");
    default_channel.apply_aggregate("MinMax", operation);

    fx.set_ranges_and_condition_if_needed(array, query, false);

    // Set the data buffer for the aggregator.
    uint64_t cell_size = std::is_same<T, std::string>::value ?
                             fx.STRING_CELL_VAL_NUM :
                             sizeof(T);
    std::vector<uint8_t> min_max(cell_size);
    std::vector<uint8_t> min_max_validity(1);
    std::vector<uint64_t> dim1(100);
    std::vector<uint64_t> dim2(100);
    std::vector<uint8_t> a1(100 * cell_size);
    std::vector<uint8_t> a1_validity(100);
    query.set_layout(fx.layout_);
    if constexpr (std::is_same<T, std::string>::value) {
      query.set_data_buffer(
          "MinMax",
          static_cast<char*>(static_cast<void*>(min_max.data())),
          min_max.size());
    } else {
      query.set_data_buffer(
          "MinMax",
          static_cast<T*>(static_cast<void*>(min_max.data())),
          min_max.size() / cell_size);
    }
    if (fx.nullable_) {
      query.set_validity_buffer("MinMax", min_max_validity);
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
    uint64_t expected = min ? std::numeric_limits<uint64_t>::max() :
                              std::numeric_limits<uint64_t>::min();
    fx.validate_data(
        query, dim1, dim2, a1, a1_validity, [&](uint64_t v) -> void {
          if (min) {
            expected = std::min(expected, v);
          } else {
            expected = std::max(expected, v);
          }
        });

    std::vector<uint8_t> expected_min_max =
        fx.make_data_buff({static_cast<int>(expected)});

    auto result_el = query.result_buffer_elements_nullable();
    CHECK(
        std::get<1>(result_el["MinMax"]) ==
        (std::is_same<T, std::string>::value ? 2 : 1));
    CHECK(min_max == expected_min_max);

    fx.validate_tiles_read(query);
  });

  // Close array.
  array.close();
}

typedef tuple<MinOperator, MaxOperator> AggUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "C++ API: Aggregates basic min/max var",
    "[cppapi][aggregates][basic][min-max][var]",
    AggUnderTest) {
  CppAggregatesFx<std::string> fx;
  typedef TestType AGG;
  bool min = std::is_same<AGG, MinOperator>::value;
  fx.generate_test_params();
  fx.create_var_array_and_write_fragments();

  Array array{fx.ctx_, fx.ARRAY_NAME, TILEDB_READ};

  for (bool set_ranges : {true, false}) {
    fx.set_ranges_ = set_ranges;
    for (bool request_data : {true, false}) {
      fx.request_data_ = request_data;
      for (bool set_qc : fx.set_qc_values_) {
        fx.set_qc_ = set_qc;
        for (tiledb_layout_t layout : fx.layout_values_) {
          // Filter invalid combination. The legacy reader does not support
          // aggregates, and we cannot automatically switch to unordered
          // reads if we are requesting both the aggregates and the data.
          if (!fx.dense_ && request_data && layout != TILEDB_UNORDERED)
            continue;
          fx.layout_ = layout;
          Query query(fx.ctx_, array, TILEDB_READ);

          // Add a min/max aggregator to the query.
          QueryChannel default_channel =
              QueryExperimental::get_default_channel(query);
          ChannelOperation operation =
              QueryExperimental::create_unary_aggregate<AGG>(query, "a1");
          default_channel.apply_aggregate("MinMax", operation);

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
          query.set_layout(layout);
          query.set_data_buffer("MinMax", min_max_data);
          query.set_offsets_buffer("MinMax", min_max_offset);
          if (fx.nullable_) {
            query.set_validity_buffer("MinMax", min_max_validity);
          }

          if (request_data) {
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
              if (set_ranges) {
                expected_min_max = min ? "13" : set_qc ? "6" : "8";
              } else {
                expected_min_max = min ? "0" : set_qc ? "6" : "999";
              }
            } else {
              if (set_ranges) {
                expected_min_max = min ? "12" : set_qc ? "7" : "8";
              } else {
                expected_min_max = min ? "0" : set_qc ? "7" : "999";
              }
            }
          } else {
            if (fx.nullable_) {
              if (set_ranges) {
                expected_min_max = min ? "10" : "6";
              } else {
                expected_min_max = min ? "0" : "8";
              }
            } else {
              if (set_ranges) {
                expected_min_max = min ? "10" : (fx.allow_dups_ ? "7" : "6");
              } else {
                expected_min_max = min ? "0" : "99";
              }
            }
          }

          auto result_el = query.result_buffer_elements_nullable();
          CHECK(std::get<1>(result_el["MinMax"]) == expected_min_max.size());
          CHECK(std::get<0>(result_el["MinMax"]) == 1);

          min_max_data.resize(expected_min_max.size());
          CHECK(min_max_data == expected_min_max);
          CHECK(min_max_offset[0] == 0);

          if (request_data) {
            a1_offsets[std::get<0>(result_el["a1"])] =
                std::get<1>(result_el["a1"]);
            fx.validate_data_var(
                query, dim1, dim2, a1_data, a1_offsets, a1_validity);
          }

          fx.validate_tiles_read_var(query);
        }
      }
    }
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
    double,
    std::string>
    NullCountFixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE_METHOD(
    CppAggregatesFx,
    "C++ API: Aggregates basic null count",
    "[cppapi][aggregates][basic][null-count][fixed]",
    NullCountFixedTypesUnderTest) {
  typedef TestType T;
  CppAggregatesFx<T>::generate_test_params();
  if (!CppAggregatesFx<T>::nullable_) {
    return;
  }

  CppAggregatesFx<T>::create_array_and_write_fragments();

  Array array{
      CppAggregatesFx<T>::ctx_, CppAggregatesFx<T>::ARRAY_NAME, TILEDB_READ};

  CppAggregatesFx<T>::run_all_combinations([&]() -> void {
    Query query(CppAggregatesFx<T>::ctx_, array, TILEDB_READ);

    QueryChannel default_channel =
        QueryExperimental::get_default_channel(query);
    ChannelOperation operation =
        QueryExperimental::create_unary_aggregate<NullCountOperator>(
            query, "a1");
    default_channel.apply_aggregate("NullCount", operation);

    CppAggregatesFx<T>::set_ranges_and_condition_if_needed(array, query, false);

    // Set the data buffer for the aggregator.
    uint64_t cell_size = std::is_same<T, std::string>::value ?
                             CppAggregatesFx<T>::STRING_CELL_VAL_NUM :
                             sizeof(T);
    std::vector<uint64_t> null_count(1);
    std::vector<uint64_t> dim1(100);
    std::vector<uint64_t> dim2(100);
    std::vector<uint8_t> a1(100 * cell_size);
    std::vector<uint8_t> a1_validity(100);
    query.set_layout(CppAggregatesFx<T>::layout_);
    query.set_data_buffer("NullCount", null_count);

    if (CppAggregatesFx<T>::request_data_) {
      query.set_data_buffer("d1", dim1);
      query.set_data_buffer("d2", dim2);
      query.set_data_buffer(
          "a1", static_cast<void*>(a1.data()), a1.size() / cell_size);
      query.set_validity_buffer("a1", a1_validity);
    }

    // Submit the query.
    query.submit();

    // Check the results.
    uint64_t expected_null_count;
    if (CppAggregatesFx<T>::dense_) {
      if (CppAggregatesFx<T>::set_qc_) {
        expected_null_count = 0;
      } else {
        if (CppAggregatesFx<T>::set_ranges_) {
          expected_null_count = 12;
        } else {
          expected_null_count = 17;
        }
      }
    } else {
      if (CppAggregatesFx<T>::set_ranges_) {
        expected_null_count = CppAggregatesFx<T>::allow_dups_ ? 4 : 3;
      } else {
        expected_null_count = CppAggregatesFx<T>::allow_dups_ ? 8 : 7;
      }
    }

    auto result_el = query.result_buffer_elements_nullable();
    CHECK(std::get<1>(result_el["NullCount"]) == 1);
    CHECK(null_count[0] == expected_null_count);

    if (CppAggregatesFx<T>::request_data_) {
      CppAggregatesFx<T>::validate_data(
          query, dim1, dim2, a1, a1_validity, [&](uint64_t) -> void {
            expected_null_count++;
          });
    }

    CppAggregatesFx<T>::validate_tiles_read(query);
    CppAggregatesFx<T>::validate_tiles_read_null_count(query);
  });

  // Close array.
  array.close();
}

TEST_CASE_METHOD(
    CppAggregatesFx<std::string>,
    "C++ API: Aggregates basic null count var",
    "[cppapi][aggregates][basic][null-count][var]") {
  generate_test_params();
  if (!nullable_) {
    return;
  }

  create_var_array_and_write_fragments();

  Array array{ctx_, ARRAY_NAME, TILEDB_READ};

  for (bool set_ranges : {true, false}) {
    set_ranges_ = set_ranges;
    for (bool request_data : {true, false}) {
      request_data_ = request_data;
      for (bool set_qc : set_qc_values_) {
        set_qc_ = set_qc;
        for (tiledb_layout_t layout : layout_values_) {
          // Filter invalid combination. The legacy reader does not support
          // aggregates, and we cannot automatically switch to unordered
          // reads if we are requesting both the aggregates and the data.
          if (request_data && layout != TILEDB_UNORDERED) {
            continue;
          }
          layout_ = layout;
          Query query(ctx_, array, TILEDB_READ);

          QueryChannel default_channel =
              QueryExperimental::get_default_channel(query);
          ChannelOperation operation =
              QueryExperimental::create_unary_aggregate<NullCountOperator>(
                  query, "a1");
          default_channel.apply_aggregate("NullCount", operation);

          set_ranges_and_condition_if_needed(array, query, true);

          // Set the data buffer for the aggregator.
          std::vector<uint64_t> null_count(1);
          std::vector<uint64_t> dim1(100);
          std::vector<uint64_t> dim2(100);
          std::vector<uint64_t> a1_offsets(100);
          std::string a1_data;
          a1_data.resize(100);
          std::vector<uint8_t> a1_validity(100);
          query.set_layout(layout);
          query.set_data_buffer("NullCount", null_count);

          if (request_data) {
            query.set_data_buffer("d1", dim1);
            query.set_data_buffer("d2", dim2);
            query.set_data_buffer("a1", a1_data);
            query.set_offsets_buffer("a1", a1_offsets);
            query.set_validity_buffer("a1", a1_validity);
          }

          // Submit the query.
          query.submit();

          // Check the results.
          uint64_t expected_null_count;
          if (dense_) {
            if (set_qc) {
              expected_null_count = 0;
            } else {
              if (set_ranges) {
                expected_null_count = 12;
              } else {
                expected_null_count = 17;
              }
            }
          } else {
            if (set_ranges) {
              expected_null_count = allow_dups_ ? 4 : 3;
            } else {
              expected_null_count = allow_dups_ ? 8 : 7;
            }
          }

          auto result_el = query.result_buffer_elements_nullable();
          CHECK(std::get<1>(result_el["NullCount"]) == 1);
          CHECK(null_count[0] == expected_null_count);

          if (request_data) {
            a1_offsets[std::get<0>(result_el["a1"])] =
                std::get<1>(result_el["a1"]);
            validate_data_var(
                query, dim1, dim2, a1_data, a1_offsets, a1_validity);
          }

          validate_tiles_read_var(query);
          validate_tiles_read_null_count_var(query);
        }
      }
    }
  }

  // Close array.
  array.close();
}

TEST_CASE_METHOD(
    CppAggregatesFx<std::string>,
    "C++ API: Aggregates var overflow",
    "[cppapi][aggregates][var][overflow]") {
  generate_test_params();
  if (!nullable_) {
    return;
  }

  create_var_array_and_write_fragments();

  Array array{ctx_, ARRAY_NAME, TILEDB_READ};

  for (bool set_ranges : {true, false}) {
    set_ranges_ = set_ranges;

    // We should always request the data to make sure the test does create an
    // overflow on the var buffer.
    request_data_ = true;
    for (bool set_qc : set_qc_values_) {
      set_qc_ = set_qc;
      for (tiledb_layout_t layout : layout_values_) {
        // Filter invalid combination. The legacy reader does not support
        // aggregates, and we cannot automatically switch to unordered
        // reads if we are requesting both the aggregates and the data.
        if (layout != TILEDB_UNORDERED) {
          continue;
        }
        layout_ = layout;
        Query query(ctx_, array, TILEDB_READ);

        QueryChannel default_channel =
            QueryExperimental::get_default_channel(query);
        ChannelOperation operation =
            QueryExperimental::create_unary_aggregate<NullCountOperator>(
                query, "a1");
        default_channel.apply_aggregate("NullCount", operation);

        // Add another aggregator on the second attribute. We will make the
        // first attribute get a var size overflow, which should not impact
        // the results of the second attribute as we don't request data for
        // the second attribute.
        query.ptr()->query_->add_aggregator_to_default_channel(
            "NullCount2",
            std::make_shared<
                tiledb::sm::NullCountAggregator>(tiledb::sm::FieldInfo(
                "a2", true, nullable_, TILEDB_VAR_NUM, tdb_type<std::string>)));

        set_ranges_and_condition_if_needed(array, query, true);

        // Set the data buffer for the aggregator.
        std::vector<uint64_t> null_count(1);
        std::vector<uint64_t> null_count2(1);
        std::vector<uint64_t> dim1(100);
        std::vector<uint64_t> dim2(100);
        std::vector<uint64_t> a1_offsets(100);
        std::string a1_data;
        a1_data.resize(100);
        std::vector<uint8_t> a1_validity(100);
        query.set_layout(layout);
        query.set_data_buffer("NullCount", null_count);
        query.set_data_buffer("NullCount2", null_count2);

        // Here we run a few iterations until the query completes and update
        // the buffers as we go.
        uint64_t var_buffer_size = dense_ ? 20 : 3;
        uint64_t curr_var_buffer_size = 0;
        uint64_t curr_elem = 0;
        uint64_t iter = 0;
        for (iter = 0; iter < 10; iter++) {
          query.set_data_buffer("d1", dim1.data() + curr_elem, 100 - curr_elem);
          query.set_data_buffer("d2", dim2.data() + curr_elem, 100 - curr_elem);
          query.set_data_buffer(
              "a1", a1_data.data() + curr_var_buffer_size, var_buffer_size);
          query.set_offsets_buffer(
              "a1", a1_offsets.data() + curr_elem, 100 - curr_elem);

          if (nullable_) {
            query.set_validity_buffer(
                "a1", a1_validity.data() + curr_elem, 100 - curr_elem);
          }

          // Submit the query.
          query.submit();

          // Adjust offsets.
          auto result_el = query.result_buffer_elements_nullable();
          for (uint64_t i = curr_elem;
               i < curr_elem + std::get<0>(result_el["a1"]);
               i++) {
            a1_offsets[i] += curr_var_buffer_size;
          }

          // Adjust current element count;
          curr_elem += std::get<0>(result_el["a1"]);
          curr_var_buffer_size += std::get<1>(result_el["a1"]);

          // Stop on query completion.
          if (query.query_status() == Query::Status::COMPLETE) {
            a1_offsets[curr_elem] = curr_var_buffer_size;
            break;
          }
        }

        // Make sure we did get an overflow of the var buffer that caused more
        // than one submit.
        CHECK(iter > 1);

        // Check the results.
        uint64_t expected_null_count;
        if (dense_) {
          if (set_qc) {
            expected_null_count = 0;
          } else {
            if (set_ranges) {
              expected_null_count = 12;
            } else {
              expected_null_count = 17;
            }
          }
        } else {
          if (set_ranges) {
            expected_null_count = allow_dups_ ? 4 : 3;
          } else {
            expected_null_count = allow_dups_ ? 8 : 7;
          }
        }

        auto result_el = query.result_buffer_elements_nullable();
        CHECK(std::get<1>(result_el["NullCount"]) == 1);
        CHECK(null_count[0] == expected_null_count);

        CHECK(std::get<1>(result_el["NullCount2"]) == 1);
        CHECK(null_count2[0] == expected_null_count);

        validate_data_var(
            query, dim1, dim2, a1_data, a1_offsets, a1_validity, false);
      }
    }
  }

  // Close array.
  array.close();
}

TEST_CASE_METHOD(
    CppAggregatesFx<std::string>,
    "C++ API: Aggregates var overflow, exception",
    "[cppapi][aggregates][var][overflow-exception]") {
  generate_test_params();
  if (!nullable_ || dense_) {
    return;
  }

  create_var_array_and_write_fragments();

  Array array{ctx_, ARRAY_NAME, TILEDB_READ};

  for (bool set_ranges : {true, false}) {
    set_ranges_ = set_ranges;

    // We should always request the data to make sure the test does create an
    // overflow on the var buffer.
    request_data_ = true;
    for (bool set_qc : set_qc_values_) {
      set_qc_ = set_qc;
      for (tiledb_layout_t layout : layout_values_) {
        // Filter invalid combination. The legacy reader does not support
        // aggregates, and we cannot automatically switch to unordered
        // reads if we are requesting both the aggregates and the data.
        if (layout != TILEDB_UNORDERED)
          continue;
        layout_ = layout;
        Query query(ctx_, array, TILEDB_READ);

        QueryChannel default_channel =
            QueryExperimental::get_default_channel(query);
        ChannelOperation operation =
            QueryExperimental::create_unary_aggregate<NullCountOperator>(
                query, "a1");
        default_channel.apply_aggregate("NullCount", operation);

        // Add another aggregator on the second attribute. We will make this
        // attribute get a var size overflow, which should impact the result
        // of the first one hence throw an exception.
        query.ptr()->query_->add_aggregator_to_default_channel(
            "NullCount2",
            std::make_shared<
                tiledb::sm::NullCountAggregator>(tiledb::sm::FieldInfo(
                "a2", true, nullable_, TILEDB_VAR_NUM, tdb_type<std::string>)));

        set_ranges_and_condition_if_needed(array, query, true);

        // Set the data buffer for the aggregator.
        std::vector<uint64_t> null_count(1);
        std::vector<uint64_t> null_count2(1);
        std::vector<uint64_t> dim1(100);
        std::vector<uint64_t> dim2(100);
        std::vector<uint64_t> a1_offsets(100);
        std::string a1_data;
        a1_data.resize(100);
        std::vector<uint8_t> a1_validity(100);
        std::vector<uint64_t> a2_offsets(100);
        std::string a2_data;
        a2_data.resize(100);
        std::vector<uint8_t> a2_validity(100);
        query.set_layout(layout);
        query.set_data_buffer("NullCount", null_count);
        query.set_data_buffer("NullCount2", null_count2);
        query.set_data_buffer("d1", dim1.data(), 100);
        query.set_data_buffer("d2", dim2.data(), 100);
        query.set_data_buffer("a1", a1_data.data(), 100);
        query.set_offsets_buffer("a1", a1_offsets.data(), 100);
        query.set_data_buffer("a2", a2_data.data(), 10);
        query.set_offsets_buffer("a2", a2_offsets.data(), 100);

        if (nullable_) {
          query.set_validity_buffer("a1", a1_validity.data(), 100);
          query.set_validity_buffer("a2", a2_validity.data(), 100);
        }

        // Submit the query.
        CHECK_THROWS_WITH(
            query.submit(),
            Catch::Matchers::EndsWith(
                "Overflow happened after aggregate was computed, aggregate "
                "recompute pass is not yet implemented"));
      }
    }
  }

  // Close array.
  array.close();
}

TEMPLATE_LIST_TEST_CASE_METHOD(
    CppAggregatesFx,
    "C++ API: Aggregates incomplete test",
    "[cppapi][aggregates][incomplete]",
    SumFixedTypesUnderTest) {
  typedef TestType T;
  CppAggregatesFx<T>::generate_test_params();
  CppAggregatesFx<T>::create_array_and_write_fragments();

  Array array{
      CppAggregatesFx<T>::ctx_, CppAggregatesFx<T>::ARRAY_NAME, TILEDB_READ};

  for (bool set_ranges : {true, false}) {
    CppAggregatesFx<T>::set_ranges_ = set_ranges;
    for (bool set_qc : CppAggregatesFx<T>::set_qc_values_) {
      CppAggregatesFx<T>::set_qc_ = set_qc;
      for (tiledb_layout_t layout : CppAggregatesFx<T>::layout_values_) {
        // Filter invalid combination. The legacy reader does not support
        // aggregates, and we cannot automatically switch to unordered
        // reads if we are requesting both the aggregates and the data.
        if (!CppAggregatesFx<T>::dense_ && layout != TILEDB_UNORDERED)
          continue;
        CppAggregatesFx<T>::layout_ = layout;
        Query query(CppAggregatesFx<T>::ctx_, array, TILEDB_READ);

        // Add a count aggregator to the query. We add both sum and count as
        // they are processed separately in the dense case.
        QueryChannel default_channel =
            QueryExperimental::get_default_channel(query);
        default_channel.apply_aggregate("Count", CountOperation());
        ChannelOperation operation2 =
            QueryExperimental::create_unary_aggregate<SumOperator>(query, "a1");
        default_channel.apply_aggregate("Sum", operation2);

        CppAggregatesFx<T>::set_ranges_and_condition_if_needed(
            array, query, false);

        // Set the data buffer for the aggregator.
        uint64_t cell_size = sizeof(T);
        std::vector<uint64_t> count(1);
        std::vector<typename tiledb::sm::sum_type_data<T>::sum_type> sum(1);
        std::vector<uint8_t> sum_validity(1);
        std::vector<uint64_t> dim1(100);
        std::vector<uint64_t> dim2(100);
        std::vector<uint8_t> a1(100 * cell_size);
        std::vector<uint8_t> a1_validity(100);
        query.set_layout(layout);
        query.set_data_buffer("Count", count);
        query.set_data_buffer("Sum", sum);
        if (CppAggregatesFx<T>::nullable_) {
          query.set_validity_buffer("Sum", sum_validity);
        }

        // Here we run a few iterations until the query completes and update
        // the buffers as we go.
        uint64_t curr_elem = 0;
        uint64_t num_elems = CppAggregatesFx<T>::dense_ ? 20 : 4;
        uint64_t num_iters = 0;
        if (CppAggregatesFx<T>::dense_) {
          num_iters = 2;
        } else {
          if (CppAggregatesFx<T>::set_ranges_) {
            num_iters = 2;
          } else {
            num_iters = 4;
          }
        }

        for (uint64_t iter = 0; iter < num_iters; iter++) {
          query.set_data_buffer("d1", dim1.data() + curr_elem, num_elems);
          query.set_data_buffer("d2", dim2.data() + curr_elem, num_elems);
          query.set_data_buffer(
              "a1",
              static_cast<void*>(a1.data() + curr_elem * cell_size),
              num_elems);

          if (CppAggregatesFx<T>::nullable_) {
            query.set_validity_buffer(
                "a1", a1_validity.data() + curr_elem, num_elems);
          }

          // Submit the query.
          query.submit();

          // Adjust current element count;
          auto result_el = query.result_buffer_elements_nullable();
          curr_elem += std::get<1>(result_el["d1"]);

          // Stop on query completion.
          if (iter < num_iters - 1) {
            CHECK(query.query_status() == Query::Status::INCOMPLETE);
          } else {
            CHECK(query.query_status() == Query::Status::COMPLETE);
          }
        }

        // Check the results.
        uint64_t expected_count;
        if (CppAggregatesFx<T>::dense_) {
          expected_count = set_ranges ? 24 : 36;
        } else {
          if (set_ranges) {
            expected_count = CppAggregatesFx<T>::allow_dups_ ? 8 : 7;
          } else {
            expected_count = CppAggregatesFx<T>::allow_dups_ ? 16 : 15;
          }
        }

        typename tiledb::sm::sum_type_data<T>::sum_type expected_sum;
        if (CppAggregatesFx<T>::dense_) {
          if (CppAggregatesFx<T>::nullable_) {
            if (set_ranges) {
              expected_sum = set_qc ? 197 : 201;
            } else {
              expected_sum = set_qc ? 315 : 319;
            }
          } else {
            if (set_ranges) {
              expected_sum = set_qc ? 398 : 402;
            } else {
              expected_sum = set_qc ? 591 : 630;
            }
          }
        } else {
          if (CppAggregatesFx<T>::nullable_) {
            if (set_ranges) {
              expected_sum = 42;
            } else {
              expected_sum = 56;
            }
          } else {
            if (set_ranges) {
              expected_sum = CppAggregatesFx<T>::allow_dups_ ? 88 : 81;
            } else {
              expected_sum = CppAggregatesFx<T>::allow_dups_ ? 120 : 113;
            }
          }
        }

        auto result_el = query.result_buffer_elements_nullable();
        CHECK(std::get<1>(result_el["Count"]) == 1);
        CHECK(count[0] == expected_count);

        CHECK(std::get<1>(result_el["Sum"]) == 1);
        CHECK(sum[0] == expected_sum);

        CppAggregatesFx<T>::validate_data(
            query,
            dim1,
            dim2,
            a1,
            a1_validity,
            [&](uint64_t) -> void {},
            false);
      }
    }
  }

  // Close array.
  array.close();
}

TEST_CASE_METHOD(
    CppAggregatesFx<int32_t>,
    "CPP: Aggregates - Basic",
    "[cppapi][aggregates][args]") {
  dense_ = false;
  nullable_ = false;
  allow_dups_ = false;
  create_array_and_write_fragments();

  Array array{ctx_, ARRAY_NAME, TILEDB_READ};
  Query query(ctx_, array);
  query.set_layout(TILEDB_UNORDERED);

  // This throws for and attribute that doesn't exist
  CHECK_THROWS(QueryExperimental::create_unary_aggregate<SumOperator>(
      query, "nonexistent"));

  QueryChannel default_channel = QueryExperimental::get_default_channel(query);
  ChannelOperation operation =
      QueryExperimental::create_unary_aggregate<SumOperator>(query, "a1");
  default_channel.apply_aggregate("Sum", operation);

  // Duplicated output fields are not allowed
  CHECK_THROWS(default_channel.apply_aggregate("Sum", operation));

  // Transition the query state
  int64_t sum = 0;
  query.set_data_buffer("Sum", &sum, 1);
  REQUIRE(query.submit() == Query::Status::COMPLETE);

  // Check api throws if the query state is already >= initialized
  CHECK_THROWS(
      QueryExperimental::create_unary_aggregate<SumOperator>(query, "a1"));
  CHECK_THROWS(default_channel.apply_aggregate("Something", operation));
}

typedef tuple<std::byte> BlobTypeUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "C++ API: Aggregates basic sum, std::byte",
    "[cppapi][aggregates][basic][sum][byte]",
    BlobTypeUnderTest) {
  const std::string array_name = "test_byte_aggregates";
  auto datatype = GENERATE(
      tiledb_datatype_t::TILEDB_BLOB,
      tiledb_datatype_t::TILEDB_GEOM_WKB,
      tiledb_datatype_t::TILEDB_GEOM_WKT);

  Context& ctx = vanilla_context_cpp();
  VFS vfs(ctx);
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Create domain.
  Domain domain(ctx);
  auto d = Dimension::create<uint64_t>(ctx, "d", {{1, 999}}, 2);
  domain.add_dimension(d);

  // Create array schema.
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(Attribute::create(ctx, "a", datatype));

  // Create array and query.
  Array::create(array_name, schema);
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

  // Add a sum aggregator to the query.
  QueryChannel default_channel = QueryExperimental::get_default_channel(query);
  REQUIRE_THROWS_WITH(
      QueryExperimental::create_unary_aggregate<SumOperator>(query, "a"),
      Catch::Matchers::ContainsSubstring("not a supported Datatype"));

  // Clean up.
  array.close();
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}
