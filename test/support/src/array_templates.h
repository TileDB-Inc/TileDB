/**
 * @file test/support/src/array_templates.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file provides templates for generic programming with respect
 * to array schema, data types, etc.
 */

#ifndef TILEDB_ARRAY_TEMPLATES_H
#define TILEDB_ARRAY_TEMPLATES_H

#include "tiledb.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/query/ast/query_ast.h"
#include "tiledb/type/datatype_traits.h"
#include "tiledb/type/range/range.h"

#include <test/support/assert_helpers.h>
#include <test/support/src/array_schema_templates.h>
#include <test/support/src/error_helpers.h>
#include <test/support/src/helpers.h>
#include <test/support/stdx/fold.h>
#include <test/support/stdx/span.h>
#include <test/support/stdx/traits.h>
#include <test/support/stdx/tuple.h>

#include <algorithm>
#include <concepts>
#include <type_traits>

namespace tiledb::sm {
class Dimension;
}

namespace tiledb::test::templates {

/**
 * Adapts a `std::tuple` whose fields are all `GlobalCellCmp`
 * to itself be `GlobalCellCmp`.
 */
template <typename StdTuple>
struct global_cell_cmp_std_tuple {
  global_cell_cmp_std_tuple(const StdTuple& tup)
      : tup_(tup) {
  }

 private:
  template <typename T>
  static constexpr tiledb::common::UntypedDatumView static_coord_datum(
      const T& field) {
    static_assert(
        stdx::is_fundamental<T> ||
        std::is_same_v<T, StringDimensionCoordView> ||
        std::is_same_v<T, StringDimensionCoordType>);
    if constexpr (stdx::is_fundamental<T>) {
      return UntypedDatumView(&field, sizeof(T));
    } else {
      return UntypedDatumView(field.data(), field.size());
    }
  }

  template <unsigned I>
  static tiledb::common::UntypedDatumView try_dimension_datum(
      const StdTuple& tup, unsigned dim) {
    if (dim == I) {
      return static_coord_datum(std::get<I>(tup));
    } else if constexpr (I + 1 < std::tuple_size_v<StdTuple>) {
      return try_dimension_datum<I + 1>(tup, dim);
    } else {
      // NB: probably not reachable in practice
      throw std::logic_error("Out of bounds access to dimension tuple");
    }
  }

 public:
  tiledb::common::UntypedDatumView dimension_datum(
      const tiledb::sm::Dimension&, unsigned dim_idx) const {
    return try_dimension_datum<0>(tup_, dim_idx);
  }

  const void* coord(unsigned dim) const {
    return try_dimension_datum<0>(tup_, dim).content();
  }

  StdTuple tup_;
};

/**
 * Adapts a span of coordinates for comparison using `GlobalCellCmp`.
 */
template <typename Coord>
struct global_cell_cmp_span {
  global_cell_cmp_span(std::span<const Coord> values)
      : values_(values) {
  }

  tiledb::common::UntypedDatumView dimension_datum(
      const tiledb::sm::Dimension&, unsigned dim_idx) const {
    return UntypedDatumView(&values_[dim_idx], sizeof(Coord));
  }

  const void* coord(unsigned dim) const {
    return &values_[dim];
  }

  std::span<const Coord> values_;
};

/**
 * Forward declaration of query_buffers
 * which will be specialized.
 *
 * The type `T` is a user-level type which logically represents
 * the contents of a cell.
 */
template <typename T>
struct query_buffers {};

/**
 * Constrains types which can be used as columnar data fragment input.
 *
 * Methods `dimensions` and `attributes` return tuples whose fields are each
 * `query_buffers<DimensionType>` and `query_buffers<AttributeType>`
 * respectively.
 */
template <typename T>
concept FragmentType = requires(const T& fragment) {
  { fragment.size() } -> std::convertible_to<uint64_t>;

  // not sure how to specify "returns any tuple whose elements decay to
  // query_buffers"
  fragment.dimensions();
  fragment.attributes();
} and requires(T& fragment) {
  // non-const versions
  fragment.dimensions();
  fragment.attributes();
};

/**
2D)
 * Schema of named fields for simple evaluation of a query condition
 */
template <FragmentType Fragment>
struct QueryConditionEvalSchema {
  std::vector<std::string> field_names_;

  QueryConditionEvalSchema() {
    stdx::decay_tuple<decltype(std::declval<Fragment>().dimensions())> dims;
    stdx::decay_tuple<decltype(std::declval<Fragment>().attributes())> atts;

    auto add_dimension = [&](auto) {
      field_names_.push_back("d" + std::to_string(field_names_.size() + 1));
    };
    auto add_attribute = [&](auto) {
      field_names_.push_back(
          "a" +
          std::to_string(
              field_names_.size() + 1 - std::tuple_size<decltype(dims)>()));
    };

    std::apply([&](const auto... dim) { (add_dimension(dim), ...); }, dims);
    std::apply([&](const auto... att) { (add_attribute(att), ...); }, atts);
  }

  template <typename T, typename Cmp>
  static bool test(
      const T& value, const T& atom, Cmp cmp, tiledb::sm::QueryConditionOp op) {
    switch (op) {
      case tiledb::sm::QueryConditionOp::LT:
        return cmp(value, atom);
      case tiledb::sm::QueryConditionOp::LE:
        return cmp(value, atom) || !cmp(atom, value);
      case tiledb::sm::QueryConditionOp::GT:
        return cmp(atom, value);
      case tiledb::sm::QueryConditionOp::GE:
        return cmp(atom, value) || !cmp(value, atom);
      case tiledb::sm::QueryConditionOp::EQ:
        return !cmp(value, atom) && !cmp(atom, value);
      case tiledb::sm::QueryConditionOp::NE:
        return cmp(value, atom) || cmp(atom, value);
      case tiledb::sm::QueryConditionOp::IN:
      case tiledb::sm::QueryConditionOp::NOT_IN:
      case tiledb::sm::QueryConditionOp::ALWAYS_TRUE:
      case tiledb::sm::QueryConditionOp::ALWAYS_FALSE:
      default:
        // not implemented here, lazy
        stdx::unreachable();
    }
  }

  /**
   * @return true if a value passes a simple condition
   */
  template <stdx::is_fundamental T>
  static bool test(const T& value, const tiledb::sm::ASTNode& condition) {
    const T atom = *static_cast<const T*>(condition.get_value_ptr());
    return test<T>(value, atom, std::less<T>{}, condition.get_op());
  }

  template <stdx::is_fundamental T>
  static bool test(
      const std::optional<T>& value, const tiledb::sm::ASTNode& condition) {
    if (condition.get_value_size() == 0) {
      // null test
      switch (condition.get_op()) {
        case tiledb::sm::QueryConditionOp::EQ:
          // `field IS NULL`
          return !value.has_value();
        default:
          // `field IS NOT NULL`
          return value.has_value();
      }
    } else {
      // normal comparison
      if (value.has_value()) {
        const T atom = *static_cast<const T*>(condition.get_value_ptr());
        return test<T>(value.value(), atom, std::less<T>{}, condition.get_op());
      } else {
        // `null` compared against not null
        return false;
      }
    }
  }

  template <stdx::is_fundamental T>
  static bool test(
      std::span<const T> value, const tiledb::sm::ASTNode& condition) {
    std::span<const T> atom(
        static_cast<const T*>(condition.get_value_ptr()),
        condition.get_value_size() / sizeof(T));
    auto cmp = [](std::span<const T> left, std::span<const T> right) -> bool {
      return std::lexicographical_compare(
          left.begin(), left.end(), right.begin(), right.end());
    };
    return test<std::span<const T>>(value, atom, cmp, condition.get_op());
  }

  template <stdx::is_fundamental T>
  static bool test(
      std::optional<std::span<const T>> value,
      const tiledb::sm::ASTNode& condition) {
    if (value.has_value()) {
      return test<T>(value.value(), condition);
    } else {
      // TODO: how to distinguish between null test and empty string comparison?
      return false;
    }
  }

  /**
   * @return true if `record` passes a simple (i.e. non-combination) query
   * condition
   */
  bool test(
      const Fragment& fragment,
      uint64_t record,
      const tiledb::sm::ASTNode& condition) const {
    using DimensionTuple = stdx::decay_tuple<decltype(fragment.dimensions())>;
    using AttributeTuple = stdx::decay_tuple<decltype(fragment.attributes())>;

    const auto dim_eval = stdx::fold_sequence(
        std::make_index_sequence<std::tuple_size_v<DimensionTuple>>{},
        [&](auto i) {
          if (condition.get_field_name() == field_names_[i]) {
            return test(std::get<i>(fragment.dimensions())[record], condition);
          } else {
            return false;
          }
        });
    if (dim_eval) {
      return dim_eval;
    }

    return stdx::fold_sequence(
        std::make_index_sequence<std::tuple_size_v<AttributeTuple>>{},
        [&](auto i) {
          if (condition.get_field_name() ==
              field_names_[i + std::tuple_size_v<DimensionTuple>]) {
            return test(std::get<i>(fragment.attributes())[record], condition);
          } else {
            return false;
          }
        });
  }
};

/**
 * Specialization of `query_buffers` for single-valued non-nullable cells.
 * The template parameter `T` is a "fundamental type" (i.e. int32_t, float,
 * etc).
 *
 * This scenario requires just one `std::vector<T>` buffer.
 */
template <stdx::is_fundamental T>
struct query_buffers<T> {
  using value_type = T;
  using cell_type = value_type;
  using query_field_size_type = uint64_t;

  using self_type = query_buffers<T>;

  std::vector<T> values_;

  query_buffers() {
  }

  query_buffers(const self_type& other)
      : values_(other.values_) {
  }

  query_buffers(std::vector<T> cells)
      : values_(cells) {
  }

  query_buffers(std::initializer_list<T> cells)
      : values_(cells) {
  }

  bool operator==(const self_type&) const = default;

  uint64_t num_cells() const {
    return values_.size();
  }

  size_t size() const {
    return num_cells();
  }

  void reserve(size_t num_cells) {
    values_.reserve(num_cells);
  }

  void resize(uint64_t num_cells, T value = 0) {
    values_.resize(num_cells, value);
  }

  const cell_type& operator[](uint64_t index) const {
    return values_[index];
  }

  cell_type& operator[](uint64_t index) {
    return values_[index];
  }

  void push_back(cell_type value) {
    values_.push_back(value);
  }

  void extend(const self_type& from) {
    reserve(num_cells() + from.num_cells());
    values_.insert(values_.end(), from.begin(), from.end());
  }

  self_type& operator=(const self_type& other) {
    values_ = other.values_;
    return *this;
  }

  self_type& operator=(const std::vector<T>& values) {
    values_ = values;
    return *this;
  }

  self_type& operator=(const std::initializer_list<T>& values) {
    values_ = values;
    return *this;
  }

  query_field_size_type make_field_size(
      uint64_t offset, uint64_t cell_limit) const {
    return sizeof(T) * std::min<uint64_t>(cell_limit, values_.size() - offset);
  }

  query_field_size_type make_field_size(uint64_t cell_limit) const {
    return make_field_size(0, cell_limit);
  }

  int32_t attach_to_query(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      query_field_size_type& field_size,
      const std::string& name,
      const query_field_size_type& cursor) const {
    const uint64_t cell_offset = query_num_cells(cursor);
    void* ptr = const_cast<void*>(
        static_cast<const void*>(&values_.data()[cell_offset]));
    return tiledb_query_set_data_buffer(
        ctx, query, name.c_str(), ptr, &field_size);
  }

  template <typename Asserter = AsserterRuntimeException>
  uint64_t query_num_cells(const query_field_size_type& field_size) const {
    ASSERTER(field_size % sizeof(T) == 0);
    ASSERTER(field_size <= num_cells() * sizeof(T));
    return field_size / sizeof(T);
  }

  /**
   * Update buffers, stitching them together after multiple reads.
   * For non-var data this is a no-op.
   */
  void apply_cursor(
      const query_field_size_type&, const query_field_size_type&) {
  }

  void accumulate_cursor(
      query_field_size_type& cursor,
      const query_field_size_type& field_sizes) const {
    cursor += field_sizes;
  }

  void resize_to_cursor(const query_field_size_type& cursor) {
    resize(cursor / sizeof(T));
  }

  /*
   * Specializations for what amounts to a std::vector
   */
  template <typename... Args>
  void insert(Args... args) {
    values_.insert(std::forward<Args>(args)...);
  }

  auto begin() {
    return values_.begin();
  }

  auto end() {
    return values_.end();
  }

  auto begin() const {
    return values_.begin();
  }

  auto end() const {
    return values_.end();
  }

  operator std::span<const T>() const {
    return std::span(values_);
  }

  operator std::span<T>() {
    return std::span(values_);
  }
};

/**
 * Specialization of `query_buffers` for single-valued nullable cells.
 * The specialization template parameter `T` is a "fundamental type" (i.e.
 * int32_t, float, etc). The `query_buffers` template parameter is
 * `std::optional<T>` representing a cell which optionally has a value.
 *
 * This scenario requires the values buffer `std::vector<T>` and the validity
 * buffer `std::vector<uint8_t>`.
 */
template <stdx::is_fundamental T>
struct query_buffers<std::optional<T>> {
  using value_type = T;
  using cell_type = std::optional<T>;
  using query_field_size_type = std::pair<uint64_t, uint64_t>;

  using self_type = query_buffers<std::optional<T>>;

  std::vector<T> values_;
  std::vector<uint8_t> validity_;

  query_buffers() {
  }

  query_buffers(const self_type& other) = default;

  bool operator==(const self_type& other) const = default;

  uint64_t num_cells() const {
    return values_.size();
  }

  size_t size() const {
    return num_cells();
  }

  void reserve(size_t num_cells) {
    values_.reserve(num_cells);
    validity_.reserve(num_cells);
  }

  void resize(uint64_t num_cells, T value = 0) {
    values_.resize(num_cells, value);
    validity_.resize(num_cells, 0);
  }

  /**
   * Mutable handle for a cell in the buffers.
   * Used to enable assignment of `std::optional<T>` to a position
   * in the columnar buffers.
   */
  struct cell_handle {
    cell_handle(T& cell, uint8_t& validity)
        : cell_(cell)
        , validity_(validity) {
    }

    operator std::optional<T>() const {
      if (validity_) {
        return cell_;
      } else {
        return std::nullopt;
      }
    }

    cell_handle& operator=(std::optional<T> value) {
      if (value.has_value()) {
        validity_ = 1;
        cell_ = value.value();
      } else {
        validity_ = 0;
      }
      return *this;
    }

    T& cell_;
    uint8_t& validity_;
  };

  /**
   * Iterator over the `std::optional<T>` values in the buffer.
   */
  struct iterator {
    using iterator_category =
        std::forward_iterator_tag;  // NB: could be random access, but lazy
    using difference_type = std::ptrdiff_t;
    using value_type = cell_handle;
    using pointer = value_type*;
    using reference = value_type&;

    iterator(self_type& buffers, uint64_t idx)
        : buffers_(buffers)
        , idx_(idx) {
    }

    value_type operator*() {
      return buffers_[idx_];
    }

    iterator operator++() {
      idx_++;
      return *this;
    }

    iterator operator++(int) {
      auto tmp = *this;
      ++(*this);
      return tmp;
    }

    friend bool operator==(const iterator& a, const iterator& b) {
      return (&a.buffers_) == (&b.buffers_) && a.idx_ == b.idx_;
    }

    friend bool operator!=(const iterator& a, const iterator& b) {
      return !(a == b);
    }

   private:
    self_type& buffers_;
    uint64_t idx_;
  };

  std::optional<T> operator[](uint64_t index) const {
    if (validity_[index]) {
      return values_[index];
    } else {
      return std::nullopt;
    }
  }

  cell_handle operator[](uint64_t index) {
    return cell_handle(values_[index], validity_[index]);
  }

  iterator begin() {
    return iterator(*this, 0);
  }

  iterator end() {
    return iterator(*this, values_.size());
  }

  void push_back(cell_type value) {
    if (value.has_value()) {
      values_.push_back(value.value());
      validity_.push_back(1);
    } else {
      values_.push_back(0);
      validity_.push_back(1);
    }
  }

  void extend(const self_type& from) {
    reserve(num_cells() + from.num_cells());
    values_.insert(values_.end(), from.values_.begin(), from.values_.end());
    validity_.insert(
        validity_.end(), from.validity_.begin(), from.validity_.end());
  }

  self_type& operator=(const self_type& other) {
    values_ = other.values_;
    validity_ = other.validity_;
    return *this;
  }

  query_field_size_type make_field_size(
      uint64_t offset, uint64_t cell_limit) const {
    const uint64_t values_size =
        sizeof(T) * std::min<uint64_t>(cell_limit, values_.size() - offset);
    const uint64_t validity_size =
        sizeof(uint8_t) *
        std::min<uint64_t>(cell_limit, validity_.size() - offset);
    return std::make_pair(values_size, validity_size);
  }

  query_field_size_type make_field_size(uint64_t cell_limit) const {
    return make_field_size(0, cell_limit);
  }

  int32_t attach_to_query(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      query_field_size_type& field_size,
      const std::string& name,
      const query_field_size_type& cursor) const {
    const uint64_t cell_offset = query_num_cells(cursor);
    void* ptr = const_cast<void*>(
        static_cast<const void*>(&values_.data()[cell_offset]));
    RETURN_IF_ERR(tiledb_query_set_data_buffer(
        ctx, query, name.c_str(), ptr, &std::get<0>(field_size)));
    RETURN_IF_ERR(tiledb_query_set_validity_buffer(
        ctx,
        query,
        name.c_str(),
        const_cast<uint8_t*>(validity_.data()),
        &std::get<1>(field_size)));
    return TILEDB_OK;
  }

  template <typename Asserter = AsserterRuntimeException>
  uint64_t query_num_cells(const query_field_size_type& field_size) const {
    const uint64_t values_size = std::get<0>(field_size);
    const uint64_t validity_size = std::get<1>(field_size);
    ASSERTER(values_size % sizeof(T) == 0);
    ASSERTER(validity_size % sizeof(uint8_t) == 0);
    ASSERTER(values_size <= num_cells() * sizeof(T));
    ASSERTER(validity_size <= num_cells() * sizeof(uint8_t));
    ASSERTER(values_size / sizeof(T) == validity_size / sizeof(uint8_t));
    return validity_size / sizeof(uint8_t);
  }

  /**
   * Update buffers, stitching them together after multiple reads.
   * For non-var data this is a no-op.
   */
  void apply_cursor(
      const query_field_size_type&, const query_field_size_type&) {
  }

  void accumulate_cursor(
      query_field_size_type& cursor,
      const query_field_size_type& field_sizes) const {
    std::get<0>(cursor) += std::get<0>(field_sizes);
    std::get<1>(cursor) += std::get<1>(field_sizes);
  }

  void resize_to_cursor(const query_field_size_type& cursor) {
    resize(std::get<0>(cursor) / sizeof(T));
  }
};

/**
 * Specialization of `query_buffers` for variable-length non-nullable cells.
 * The specialization template parameter `T` is a fundamental type.
 * The `query_buffers` template parameter is `std::vector<T>` representing
 * a cell which has a variable number of values.
 *
 * This scenario requires the values buffer `std::vector<T>` and the offsets
 * buffer `std::vector<uint64_t>`. The offsets buffer contains one value
 * per cell whereas the values buffer contains a variable number of values
 * per cell. As such, methods which attach to a query need to treat the
 * size of both buffers separately.
 */
template <stdx::is_fundamental T>
struct query_buffers<std::vector<T>> {
  using value_type = T;
  using cell_type = std::span<const value_type>;
  using query_field_size_type = std::pair<uint64_t, uint64_t>;

  using self_type = query_buffers<std::vector<T>>;

  std::vector<T> values_;
  std::vector<uint64_t> offsets_;

  query_buffers() {
  }

  query_buffers(const self_type& other) = default;

  query_buffers(std::vector<std::vector<T>> cells) {
    uint64_t offset = 0;
    for (const auto& cell : cells) {
      values_.insert(values_.end(), cell.begin(), cell.end());
      offsets_.push_back(offset);
      offset += cell.size() * sizeof(T);
    }
  }

  bool operator==(const self_type&) const = default;

  uint64_t num_cells() const {
    return offsets_.size();
  }

  size_t size() const {
    return num_cells();
  }

  void reserve(size_t num_cells) {
    values_.reserve(16 * num_cells);
    offsets_.reserve(num_cells);
  }

  void resize(size_t num_cells, T value = 0) {
    values_.resize(16 * num_cells, value);
    offsets_.resize(num_cells, 0);
  }

  std::span<const T> operator[](uint64_t index) const {
    if (index + 1 < num_cells()) {
      return std::span(
          values_.begin() + (offsets_[index] / sizeof(T)),
          values_.begin() + (offsets_[index + 1] / sizeof(T)));
    } else {
      return std::span(
          values_.begin() + (offsets_[index] / sizeof(T)), values_.end());
    }
  }

  void push_back(cell_type value) {
    if (offsets_.empty()) {
      offsets_.push_back(0);
    } else {
      offsets_.push_back(values_.size() * sizeof(T));
    }
    values_.insert(values_.end(), value.begin(), value.end());
  }

  void extend(const self_type& from) {
    reserve(num_cells() + from.num_cells());

    const size_t offset_base = values_.size() * sizeof(T);
    for (size_t o : from.offsets_) {
      offsets_.push_back(offset_base + o);
    }

    values_.insert(values_.end(), from.values_.begin(), from.values_.end());
  }

  self_type& operator=(const self_type& other) {
    offsets_ = other.offsets_;
    values_ = other.values_;
    return *this;
  }

  query_field_size_type make_field_size(
      uint64_t cell_offset, uint64_t cell_limit) const {
    const uint64_t num_cells =
        std::min<uint64_t>(cell_limit, offsets_.size() - cell_offset);

    const uint64_t offsets_size =
        sizeof(uint64_t) *
        std::min<uint64_t>(num_cells, offsets_.size() - cell_offset);

    uint64_t values_size;
    if (cell_offset + num_cells + 1 < offsets_.size()) {
      values_size = sizeof(T) *
                    (offsets_[cell_offset + num_cells] - offsets_[cell_offset]);
    } else {
      values_size = sizeof(T) * (values_.size() - offsets_[cell_offset]);
    }

    return std::make_pair(values_size, offsets_size);
  }

  query_field_size_type make_field_size(uint64_t cell_limit) const {
    return make_field_size(0, cell_limit);
  }

  int32_t attach_to_query(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      query_field_size_type& field_size,
      const std::string& name,
      const query_field_size_type& cursor) const {
    const uint64_t cell_offset = query_num_cells(cursor);
    const uint64_t values_offset = std::get<0>(cursor) / sizeof(T);

    void* ptr = const_cast<void*>(
        static_cast<const void*>(&values_.data()[values_offset]));
    RETURN_IF_ERR(tiledb_query_set_data_buffer(
        ctx, query, name.c_str(), ptr, &std::get<0>(field_size)));
    RETURN_IF_ERR(tiledb_query_set_offsets_buffer(
        ctx,
        query,
        name.c_str(),
        const_cast<uint64_t*>(&offsets_.data()[cell_offset]),
        &std::get<1>(field_size)));
    return TILEDB_OK;
  }

  template <typename Asserter = AsserterRuntimeException>
  uint64_t query_num_cells(const query_field_size_type& field_size) const {
    const uint64_t offsets_size = std::get<1>(field_size);
    ASSERTER(offsets_size % sizeof(uint64_t) == 0);
    ASSERTER(offsets_size <= num_cells() * sizeof(uint64_t));
    return offsets_size / sizeof(uint64_t);
  }

  /**
   * Called after a query which read into these buffers with nonzero
   * `cell_offset`. The offsets of the most recent read must be adjusted based
   * on the position where data was placed in `values_`.
   */
  void apply_cursor(
      const query_field_size_type& cursor,
      const query_field_size_type& field_sizes) {
    const uint64_t prev_values_size = std::get<0>(cursor);
    const uint64_t cell_offset = std::get<1>(cursor) / sizeof(uint64_t);
    const uint64_t num_cells =
        query_num_cells<AsserterRuntimeException>(field_sizes);

    for (uint64_t o = 0; o < num_cells; o++) {
      offsets_[cell_offset + o] += prev_values_size;
    }
  }

  void accumulate_cursor(
      query_field_size_type& cursor,
      const query_field_size_type& field_sizes) const {
    std::get<0>(cursor) += std::get<0>(field_sizes);
    std::get<1>(cursor) += std::get<1>(field_sizes);
  }

  void resize_to_cursor(const query_field_size_type& cursor) {
    values_.resize(std::get<0>(cursor) / sizeof(T));
    offsets_.resize(std::get<1>(cursor) / sizeof(uint64_t));
  }
};

/**
 * Specialization of `query_buffers` for variable-length nullable cells.
 * The specialization template parameter `T` is a fundamental type.
 * The `query_buffers` template parameter is `std::optional<std::vector<T>>`
 * representing a cell which may or may not contain a variable number of values.
 *
 * This scenario requires the values buffer `std::vector<T>`, the offsets
 * buffer `std::vector<uint64_t>`, and the validity buffer
 * `std::vector<uint8_t>`. The offsets and validity buffers contain one value
 * per cell whereas the values buffer contains a variable number of values per
 * cell. As such, methods which attach to a query need to treat the size of each
 * buffer separately.
 */
template <stdx::is_fundamental T>
struct query_buffers<std::optional<std::vector<T>>> {
  using value_type = T;
  using cell_type = std::optional<std::span<const value_type>>;
  using query_field_size_type = std::tuple<uint64_t, uint64_t, uint64_t>;

  using self_type = query_buffers<std::optional<std::vector<T>>>;

  std::vector<T> values_;
  std::vector<uint64_t> offsets_;
  std::vector<uint8_t> validity_;

  query_buffers() {
  }

  query_buffers(const self_type& other) = default;

  bool operator==(const self_type&) const = default;

  uint64_t num_cells() const {
    return offsets_.size();
  }

  size_t size() const {
    return num_cells();
  }

  void reserve(size_t num_cells) {
    values_.reserve(16 * num_cells);
    offsets_.reserve(num_cells);
    validity_.reserve(num_cells);
  }

  void resize(size_t num_cells, T value = 0) {
    values_.resize(16 * num_cells, value);
    offsets_.resize(num_cells, 0);
    validity_.resize(num_cells, 0);
  }

  std::optional<std::span<const T>> operator[](uint64_t index) const {
    if (!validity_[index]) {
      return std::nullopt;
    }
    if (index + 1 < num_cells()) {
      return std::span(
          values_.begin() + (offsets_[index] / sizeof(T)),
          values_.begin() + (offsets_[index + 1] / sizeof(T)));
    } else {
      return std::span(
          values_.begin() + (offsets_[index] / sizeof(T)), values_.end());
    }
  }

  void push_back(cell_type value) {
    if (!value.has_value()) {
      offsets_.push_back(values_.size() * sizeof(T));
      validity_.push_back(0);
      return;
    }
    if (offsets_.empty()) {
      offsets_.push_back(0);
    } else {
      offsets_.push_back(values_.size() * sizeof(T));
    }
    values_.insert(values_.end(), value.value().begin(), value.value().end());
    validity_.push_back(1);
  }

  void extend(const self_type& from) {
    reserve(num_cells() + from.num_cells());

    const size_t offset_base = values_.size() * sizeof(T);
    for (size_t o : from.offsets_) {
      offsets_.push_back(offset_base + o);
    }

    values_.insert(values_.end(), from.values_.begin(), from.values_.end());
    validity_.insert(
        validity_.end(), from.validity_.begin(), from.validity_.end());
  }

  self_type& operator=(const self_type& other) {
    offsets_ = other.offsets_;
    values_ = other.values_;
    validity_ = other.validity_;
    return *this;
  }

  query_field_size_type make_field_size(
      uint64_t cell_offset, uint64_t cell_limit) const {
    const uint64_t offsets_size =
        sizeof(uint64_t) *
        std::min<uint64_t>(cell_limit, offsets_.size() - cell_offset);
    const uint64_t validity_size =
        sizeof(uint8_t) *
        std::min<uint64_t>(cell_limit, validity_.size() - cell_offset);

    // NB: unlike the above this can just be the whole buffer
    // since offsets is what determines the values
    const uint64_t values_size = sizeof(T) * values_.size();

    return std::make_tuple(values_size, offsets_size, validity_size);
  }

  query_field_size_type make_field_size(uint64_t cell_limit) const {
    return make_field_size(0, cell_limit);
  }

  int32_t attach_to_query(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      query_field_size_type& field_size,
      const std::string& name,
      const query_field_size_type& cursor) const {
    const uint64_t cell_offset = query_num_cells(cursor);
    const uint64_t values_offset = std::get<0>(cursor) / sizeof(T);

    void* ptr = const_cast<void*>(
        static_cast<const void*>(&values_.data()[values_offset]));
    RETURN_IF_ERR(tiledb_query_set_data_buffer(
        ctx, query, name.c_str(), ptr, &std::get<0>(field_size)));
    RETURN_IF_ERR(tiledb_query_set_offsets_buffer(
        ctx,
        query,
        name.c_str(),
        const_cast<uint64_t*>(&offsets_.data()[cell_offset]),
        &std::get<1>(field_size)));
    RETURN_IF_ERR(tiledb_query_set_validity_buffer(
        ctx,
        query,
        name.c_str(),
        const_cast<uint8_t*>(&validity_.data()[cell_offset]),
        &std::get<2>(field_size)));
    return TILEDB_OK;
  }

  template <typename Asserter = AsserterRuntimeException>
  uint64_t query_num_cells(const query_field_size_type& field_size) const {
    const uint64_t values_size = std::get<0>(field_size);
    const uint64_t offsets_size = std::get<1>(field_size);
    const uint64_t validity_size = std::get<2>(field_size);
    ASSERTER(values_size % sizeof(T) == 0);
    ASSERTER(offsets_size % sizeof(uint64_t) == 0);
    ASSERTER(validity_size % sizeof(uint8_t) == 0);
    ASSERTER(offsets_size <= num_cells() * sizeof(uint64_t));
    ASSERTER(validity_size <= num_cells() * sizeof(uint8_t));
    ASSERTER(
        offsets_size / sizeof(uint64_t) == validity_size / sizeof(uint8_t));
    return validity_size / sizeof(uint8_t);
  }

  /**
   * Called after a query which read into these buffers with nonzero
   * `cell_offset`. The offsets of the most recent read must be adjusted based
   * on the position where data was placed in `values_`.
   */
  void apply_cursor(
      const query_field_size_type& cursor,
      const query_field_size_type& field_sizes) {
    const uint64_t prev_values_size = std::get<0>(cursor);
    const uint64_t cell_offset = std::get<1>(cursor) / sizeof(uint64_t);
    const uint64_t num_cells =
        query_num_cells<AsserterRuntimeException>(field_sizes);

    for (uint64_t o = 0; o < num_cells; o++) {
      offsets_[cell_offset + o] += prev_values_size;
    }
  }

  void accumulate_cursor(
      query_field_size_type& cursor,
      const query_field_size_type& field_sizes) const {
    std::get<0>(cursor) += std::get<0>(field_sizes);
    std::get<1>(cursor) += std::get<1>(field_sizes);
    std::get<2>(cursor) += std::get<2>(field_sizes);
  }

  void resize_to_cursor(const query_field_size_type& cursor) {
    values_.resize(std::get<0>(cursor) / sizeof(T));
    offsets_.resize(std::get<1>(cursor) / sizeof(uint64_t));
    validity_.resize(std::get<2>(cursor) / sizeof(uint8_t));
  }
};

template <typename _DimensionTuple, typename _AttributeTuple>
struct Fragment {
 private:
  template <typename... Ts>
  struct to_query_buffers {
    using value_type = std::tuple<query_buffers<Ts>...>;
    using ref_type = std::tuple<query_buffers<Ts>&...>;
    using const_ref_type = std::tuple<const query_buffers<Ts>&...>;
  };

  template <typename... Ts>
  static to_query_buffers<Ts...>::value_type f_qb_value(std::tuple<Ts...>) {
    return std::declval<to_query_buffers<Ts...>::value_type>();
  }

  template <typename... Ts>
  static to_query_buffers<Ts...>::ref_type f_qb_ref(std::tuple<Ts...>) {
    return std::declval<to_query_buffers<Ts...>::ref_type>();
  }

  template <typename... Ts>
  static to_query_buffers<Ts...>::const_ref_type f_qb_const_ref(
      std::tuple<Ts...>) {
    return std::declval<to_query_buffers<Ts...>::const_ref_type>();
  }

  template <typename T>
  using value_tuple_query_buffers = decltype(f_qb_value(std::declval<T>()));

  template <typename T>
  using ref_tuple_query_buffers = decltype(f_qb_ref(std::declval<T>()));

  template <typename T>
  using const_ref_tuple_query_buffers =
      decltype(f_qb_const_ref(std::declval<T>()));

 public:
  using DimensionTuple = _DimensionTuple;
  using AttributeTuple = _AttributeTuple;

  using self_type = Fragment<DimensionTuple, AttributeTuple>;

  using DimensionBuffers = value_tuple_query_buffers<DimensionTuple>;
  using DimensionBuffersRef = ref_tuple_query_buffers<DimensionTuple>;
  using DimensionBuffersConstRef =
      const_ref_tuple_query_buffers<DimensionTuple>;

  using AttributeBuffers = value_tuple_query_buffers<AttributeTuple>;
  using AttributeBuffersRef = ref_tuple_query_buffers<AttributeTuple>;
  using AttributeBuffersConstRef =
      const_ref_tuple_query_buffers<AttributeTuple>;

  DimensionBuffers dims_;
  AttributeBuffers atts_;

  uint64_t num_cells() const {
    static_assert(
        std::tuple_size<DimensionBuffers>::value > 0 ||
        std::tuple_size<AttributeBuffers>::value > 0);

    if constexpr (std::tuple_size<DimensionBuffers>::value == 0) {
      return std::get<0>(atts_).num_cells();
    } else {
      return std::get<0>(atts_).num_cells();
    }
  }

  uint64_t size() const {
    return num_cells();
  }

  const DimensionBuffersConstRef dimensions() const {
    return std::apply(
        [](const auto&... field) { return std::forward_as_tuple(field...); },
        dims_);
  }

  DimensionBuffersRef dimensions() {
    return std::apply(
        [](auto&... field) { return std::forward_as_tuple(field...); }, dims_);
  }

  const AttributeBuffersConstRef attributes() const {
    return std::apply(
        [](const auto&... field) { return std::forward_as_tuple(field...); },
        atts_);
  }

  AttributeBuffersRef attributes() {
    return std::apply(
        [](auto&... field) { return std::forward_as_tuple(field...); }, atts_);
  }

  void reserve(uint64_t num_cells) {
    std::apply(
        [num_cells]<typename... Ts>(Ts&... field) {
          (field.reserve(num_cells), ...);
        },
        std::tuple_cat(dimensions(), attributes()));
  }

  void resize(uint64_t num_cells) {
    std::apply(
        [num_cells]<typename... Ts>(Ts&... field) {
          (field.resize(num_cells), ...);
        },
        std::tuple_cat(dimensions(), attributes()));
  }

  void extend(const self_type& other) {
    std::apply(
        [&]<typename... Ts>(Ts&... dst) {
          std::apply(
              [&]<typename... Us>(const Us&... src) { (dst.extend(src), ...); },
              std::tuple_cat(other.dimensions(), other.attributes()));
        },
        std::tuple_cat(dimensions(), attributes()));
  }

  bool operator==(const self_type& other) const {
    return dimensions() == other.dimensions() &&
           attributes() == other.attributes();
  }
};

/**
 * Data for a one-dimensional array
 */
template <DimensionType D, AttributeType... Att>
struct Fragment1D : public Fragment<std::tuple<D>, std::tuple<Att...>> {
  using DimensionType = D;

  const query_buffers<D>& dimension() const {
    return std::get<0>(this->dimensions());
  }

  query_buffers<D>& dimension() {
    return std::get<0>(this->dimensions());
  }
};

/**
 * Data for a two-dimensional array
 */
template <DimensionType D1, DimensionType D2, typename... Att>
struct Fragment2D : public Fragment<std::tuple<D1, D2>, std::tuple<Att...>> {
  const query_buffers<D1>& d1() const {
    return std::get<0>(this->dimensions());
  }

  const query_buffers<D2>& d2() const {
    return std::get<1>(this->dimensions());
  }

  query_buffers<D1>& d1() {
    return std::get<0>(this->dimensions());
  }

  query_buffers<D2>& d2() {
    return std::get<1>(this->dimensions());
  }
};

/**
 * Data for a three-dimensional array
 */
template <DimensionType D1, DimensionType D2, DimensionType D3, typename... Att>
struct Fragment3D
    : public Fragment<std::tuple<D1, D2, D3>, std::tuple<Att...>> {
  const query_buffers<D1>& d1() const {
    return std::get<0>(this->dimensions());
  }

  const query_buffers<D2>& d2() const {
    return std::get<1>(this->dimensions());
  }

  const query_buffers<D3>& d3() const {
    return std::get<2>(this->dimensions());
  }

  query_buffers<D1>& d1() {
    return std::get<0>(this->dimensions());
  }

  query_buffers<D2>& d2() {
    return std::get<1>(this->dimensions());
  }

  query_buffers<D3>& d3() {
    return std::get<2>(this->dimensions());
  }
};

/**
 * Binds variadic field data to a tiledb query
 */
template <typename Asserter, typename... Ts>
struct query_applicator {
  /**
   * @return a tuple containing the size of each input field
   */
  static auto make_field_sizes(
      const std::tuple<std::decay_t<Ts>&...> fields,
      uint64_t cell_limit = std::numeric_limits<uint64_t>::max()) {
    std::optional<uint64_t> num_cells;
    auto make_field_size = [&]<typename T>(const query_buffers<T>& field) {
      const uint64_t field_cells =
          std::min<uint64_t>(cell_limit, field.num_cells());
      const auto field_size = field.make_field_size(cell_limit);
      if (num_cells.has_value()) {
        // precondition: each field must have the same number of cells
        ASSERTER(field_cells == num_cells.value());
      } else {
        num_cells.emplace(field_cells);
      }
      return field_size;
    };

    return std::apply(
        [make_field_size](const auto&... field) {
          return std::make_tuple(make_field_size(field)...);
        },
        fields);
  }

  /**
   * @return a tuple containing the size of each input field to write for a
   * range of input cells [cell_offset, cell_offset + cell_limit]
   */
  static auto write_make_field_sizes(
      const std::tuple<const std::decay_t<Ts>&...> fields,
      uint64_t cell_offset,
      uint64_t cell_limit = std::numeric_limits<uint64_t>::max()) {
    std::optional<uint64_t> num_cells;
    auto write_make_field_size = [&]<typename T>(
                                     const query_buffers<T>& field) {
      const auto field_size = field.make_field_size(cell_offset, cell_limit);
      return field_size;
    };

    return std::apply(
        [&](const auto&... field) {
          return std::make_tuple(write_make_field_size(field)...);
        },
        fields);
  }

  /**
   * Sets buffers on `query` for the variadic `fields` and `fields_sizes`
   */
  static void set(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      auto& field_sizes,
      std::tuple<std::decay_t<Ts>&...> fields,
      std::function<std::string(unsigned)> fieldname,
      const auto& field_cursors) {
    auto set_data_buffer = [&](const std::string& name,
                               auto& field,
                               auto& field_size,
                               const auto& field_cursor) {
      const auto rc =
          field.attach_to_query(ctx, query, field_size, name, field_cursor);
      ASSERTER(std::optional<std::string>() == error_if_any(ctx, rc));
    };

    unsigned d = 0;
    std::apply(
        [&](const auto&... field) {
          std::apply(
              [&]<typename... Us>(Us&... field_size) {
                std::apply(
                    [&](const auto&... field_cursor) {
                      (set_data_buffer(
                           fieldname(d++), field, field_size, field_cursor),
                       ...);
                    },
                    field_cursors);
              },
              field_sizes);
        },
        fields);
  }

  /**
   * @return the number of cells written into `fields` by a read query
   */
  static uint64_t num_cells(const auto& fields, const auto& field_sizes) {
    std::optional<uint64_t> num_cells;

    auto check_field = [&]<typename T>(
                           const query_buffers<T>& field, auto field_size) {
      const uint64_t field_num_cells =
          field.template query_num_cells<Asserter>(field_size);
      if (num_cells.has_value()) {
        ASSERTER(num_cells.value() == field_num_cells);
      } else {
        num_cells.emplace(field_num_cells);
      }
    };

    std::apply(
        [&](const auto&... field) {
          std::apply(
              [&]<typename... Us>(const auto&... field_size) {
                (check_field(field, field_size), ...);
              },
              field_sizes);
        },
        fields);

    return num_cells.value();
  }
};

/**
 * Helper namespace for actually using the `query_applicator`.
 * Functions in this namespace help to deduce the template
 * instantiation of `query_applicator`.
 */
namespace query {

/**
 * @return a tuple containing the size of each input field
 */
template <typename Asserter, FragmentType F>
auto make_field_sizes(
    F& fragment, uint64_t cell_limit = std::numeric_limits<uint64_t>::max()) {
  typename F::DimensionBuffersRef dims = fragment.dimensions();
  typename F::AttributeBuffersRef atts = fragment.attributes();
  return [cell_limit]<typename... Ts>(std::tuple<Ts...> fields) {
    return query_applicator<Asserter, Ts...>::make_field_sizes(
        fields, cell_limit);
  }(std::tuple_cat(dims, atts));
}

template <FragmentType F>
using fragment_field_sizes_t =
    decltype(make_field_sizes<AsserterRuntimeException, F>(
        std::declval<F&>(), std::declval<uint64_t>()));

template <typename Asserter, FragmentType F>
fragment_field_sizes_t<F> write_make_field_sizes(
    const F& fragment,
    uint64_t cell_offset,
    uint64_t cell_limit = std::numeric_limits<uint64_t>::max()) {
  typename F::DimensionBuffersConstRef dims = fragment.dimensions();
  typename F::AttributeBuffersConstRef atts = fragment.attributes();
  return [cell_offset, cell_limit]<typename... Ts>(std::tuple<Ts...> fields) {
    return query_applicator<Asserter, std::remove_cvref_t<Ts>...>::
        write_make_field_sizes(fields, cell_offset, cell_limit);
  }(std::tuple_cat(dims, atts));
}

/**
 * Apply field cursor and sizes to each field of `fragment`.
 */
template <FragmentType F>
void apply_cursor(
    F& fragment,
    const fragment_field_sizes_t<F>& cursor,
    const fragment_field_sizes_t<F>& field_sizes) {
  typename F::DimensionBuffersRef dims = fragment.dimensions();
  typename F::AttributeBuffersRef atts = fragment.attributes();
  std::apply(
      [&](auto&... field) {
        std::apply(
            [&](const auto&... field_cursor) {
              std::apply(
                  [&](const auto&... field_size) {
                    (field.apply_cursor(field_cursor, field_size), ...);
                  },
                  field_sizes);
            },
            cursor);
      },
      std::tuple_cat(dims, atts));
}

/**
 * Advances field cursors `cursor` over `fragment` by the amount of data from
 * `field_sizes`
 */
template <FragmentType F>
void accumulate_cursor(
    const F& fragment,
    fragment_field_sizes_t<F>& cursor,
    const fragment_field_sizes_t<F>& field_sizes) {
  std::apply(
      [&](auto&... field) {
        std::apply(
            [&](auto&... field_cursor) {
              std::apply(
                  [&](const auto&... field_size) {
                    (field.accumulate_cursor(field_cursor, field_size), ...);
                  },
                  field_sizes);
            },
            cursor);
      },
      std::tuple_cat(fragment.dimensions(), fragment.attributes()));
}

/**
 * Resizes the fields of `fragment` to the sizes given by `cursor`.
 */
template <FragmentType F>
void resize(F& fragment, const fragment_field_sizes_t<F>& cursor) {
  std::apply(
      [cursor](auto&... field) {
        std::apply(
            [&](const auto&... field_cursor) {
              (field.resize_to_cursor(field_cursor), ...);
            },
            cursor);
      },
      std::tuple_cat(fragment.dimensions(), fragment.attributes()));
}

/**
 * Set buffers on `query` for the tuple of field columns
 */
template <typename Asserter, FragmentType F>
void set_fields(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    fragment_field_sizes_t<F>& field_sizes,
    F& fragment,
    std::function<std::string(unsigned)> dimension_name,
    std::function<std::string(unsigned)> attribute_name,
    const fragment_field_sizes_t<F>& field_cursors =
        fragment_field_sizes_t<F>()) {
  auto split_sizes = stdx::split_tuple<
      std::decay_t<decltype(field_sizes)>,
      std::tuple_size_v<decltype(fragment.dimensions())>>::value(field_sizes);

  auto split_cursors = stdx::split_tuple<
      std::decay_t<decltype(field_cursors)>,
      std::tuple_size_v<decltype(fragment.dimensions())>>::value(field_cursors);

  if constexpr (!std::
                    is_same_v<decltype(fragment.dimensions()), std::tuple<>>) {
    [&]<typename... Ts>(std::tuple<Ts...> fields) {
      query_applicator<Asserter, Ts...>::set(
          ctx,
          query,
          split_sizes.first,
          fields,
          dimension_name,
          split_cursors.first);
    }(fragment.dimensions());
  }

  if constexpr (!std::
                    is_same_v<decltype(fragment.attributes()), std::tuple<>>) {
    [&]<typename... Ts>(std::tuple<Ts...> fields) {
      query_applicator<Asserter, Ts...>::set(
          ctx,
          query,
          split_sizes.second,
          fields,
          attribute_name,
          split_cursors.second);
    }(fragment.attributes());
  }
}

/**
 * @return the number of cells written into `fields` by a read query
 */
template <typename Asserter, FragmentType F>
uint64_t num_cells(const F& fragment, const auto& field_sizes) {
  return [&]<typename... Ts>(auto fields) {
    return query_applicator<Asserter, Ts...>::num_cells(fields, field_sizes);
  }(std::tuple_cat(fragment.dimensions(), fragment.attributes()));
}

/**
 * Writes a fragment to a sparse array.
 */
template <typename Asserter, FragmentType Fragment>
void write_fragment(
    const Fragment& fragment,
    Array& forwrite,
    tiledb_layout_t layout = TILEDB_UNORDERED) {
  Query query(forwrite);
  query.set_layout(layout);

  auto field_sizes =
      make_field_sizes<Asserter, Fragment>(const_cast<Fragment&>(fragment));
  templates::query::set_fields<Asserter, Fragment>(
      query.ctx().ptr().get(),
      query.ptr().get(),
      field_sizes,
      const_cast<Fragment&>(fragment),
      [](unsigned d) { return "d" + std::to_string(d + 1); },
      [](unsigned a) { return "a" + std::to_string(a + 1); });

  const auto status = query.submit();
  ASSERTER(status == Query::Status::COMPLETE);

  if (layout == TILEDB_GLOBAL_ORDER) {
    query.finalize();
  }

  // check that sizes match what we expect
  const uint64_t expect_num_cells = fragment.size();
  const uint64_t num_cells =
      templates::query::num_cells<Asserter>(fragment, field_sizes);

  ASSERTER(num_cells == expect_num_cells);
}

/**
 * Writes a fragment to a dense array.
 */
template <typename Asserter, FragmentType Fragment, DimensionType Coord>
void write_fragment(
    const Fragment& fragment,
    Array& forwrite,
    const sm::NDRange& subarray,
    tiledb_layout_t layout = TILEDB_ROW_MAJOR) {
  Query query(forwrite.context(), forwrite, TILEDB_WRITE);
  query.set_layout(layout);

  std::vector<Coord> coords;
  for (const auto& dim : subarray) {
    coords.push_back(dim.start_as<Coord>());
    coords.push_back(dim.end_as<Coord>());
  }

  Subarray sub(query.ctx(), forwrite);
  sub.set_subarray(coords);
  query.set_subarray(sub);

  auto field_sizes =
      make_field_sizes<Asserter, Fragment>(const_cast<Fragment&>(fragment));
  templates::query::set_fields<Asserter, Fragment>(
      query.ctx().ptr().get(),
      query.ptr().get(),
      field_sizes,
      const_cast<Fragment&>(fragment),
      [](unsigned d) { return "d" + std::to_string(d + 1); },
      [](unsigned a) { return "a" + std::to_string(a + 1); });

  const auto status = query.submit();
  ASSERTER(status == Query::Status::COMPLETE);

  if (layout == TILEDB_GLOBAL_ORDER) {
    query.finalize();
  }

  // check that sizes match what we expect
  const uint64_t expect_num_cells = fragment.size();
  const uint64_t num_cells =
      templates::query::num_cells<Asserter>(fragment, field_sizes);

  ASSERTER(num_cells == expect_num_cells);
}

}  // namespace query

namespace ddl {

template <typename T>
struct cell_type_traits;

template <>
struct cell_type_traits<char> {
  static constexpr sm::Datatype physical_type = sm::Datatype::CHAR;
  static constexpr uint32_t cell_val_num = 1;
  static constexpr bool is_nullable = false;
};

template <>
struct cell_type_traits<int> {
  static constexpr sm::Datatype physical_type = sm::Datatype::INT32;
  static constexpr uint32_t cell_val_num = 1;
  static constexpr bool is_nullable = false;
};

template <>
struct cell_type_traits<uint64_t> {
  static constexpr sm::Datatype physical_type = sm::Datatype::UINT64;
  static constexpr uint32_t cell_val_num = 1;
  static constexpr bool is_nullable = false;
};

template <typename T>
struct cell_type_traits<std::vector<T>> {
  static constexpr sm::Datatype physical_type =
      cell_type_traits<T>::physical_type;
  static constexpr uint32_t cell_val_num = std::numeric_limits<uint32_t>::max();
  static constexpr bool is_nullable = false;
};

template <FragmentType F>
std::vector<std::tuple<Datatype, uint32_t, bool>> physical_type_attributes() {
  std::vector<std::tuple<Datatype, uint32_t, bool>> ret;
  auto attr = [&]<typename T>(const T&) {
    ret.push_back(std::make_tuple(
        cell_type_traits<std::decay_t<T>>::physical_type,
        cell_type_traits<std::decay_t<T>>::cell_val_num,
        cell_type_traits<std::decay_t<T>>::is_nullable));
  };
  std::apply(
      [&](const auto&... value) { (attr(value), ...); },
      typename F::AttributeTuple());

  return ret;
}

/**
 * Creates an array with a schema whose dimensions and attributes
 * come from the simplified arguments.
 * The names of the dimensions are d1, d2, etc.
 * The names of the attributes are a1, a2, etc.
 */
template <Datatype... DimensionDatatypes>
void create_array(
    const std::string& array_name,
    const Context& context,
    const std::tuple<const Dimension<DimensionDatatypes>&...> dimensions,
    std::vector<std::tuple<Datatype, uint32_t, bool>> attributes,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order,
    uint64_t tile_capacity,
    bool allow_duplicates) {
  std::vector<std::string> dimension_names;
  std::vector<tiledb_datatype_t> dimension_types;
  std::vector<void*> dimension_ranges;
  std::vector<void*> dimension_extents;
  auto add_dimension = [&]<Datatype D>(
                           const templates::Dimension<D>& dimension) {
    using CoordType = templates::Dimension<D>::value_type;
    dimension_names.push_back("d" + std::to_string(dimension_names.size() + 1));
    dimension_types.push_back(static_cast<tiledb_datatype_t>(D));
    if constexpr (std::is_same_v<CoordType, StringDimensionCoordType>) {
      dimension_ranges.push_back(nullptr);
      dimension_extents.push_back(nullptr);
    } else {
      dimension_ranges.push_back(
          const_cast<CoordType*>(&dimension.domain.lower_bound));
      dimension_extents.push_back(const_cast<CoordType*>(&dimension.extent));
    }
  };
  std::apply(
      [&]<Datatype... Ds>(const templates::Dimension<Ds>&... dimension) {
        (add_dimension(dimension), ...);
      },
      dimensions);

  std::vector<std::string> attribute_names;
  std::vector<tiledb_datatype_t> attribute_types;
  std::vector<uint32_t> attribute_cell_val_nums;
  std::vector<bool> attribute_nullables;
  std::vector<std::pair<tiledb_filter_type_t, int>> attribute_compressors;
  auto add_attribute = [&](Datatype datatype,
                           uint32_t cell_val_num,
                           bool nullable) {
    attribute_names.push_back("a" + std::to_string(attribute_names.size() + 1));
    attribute_types.push_back(static_cast<tiledb_datatype_t>(datatype));
    attribute_cell_val_nums.push_back(cell_val_num);
    attribute_nullables.push_back(nullable);
    attribute_compressors.push_back(std::make_pair(TILEDB_FILTER_NONE, -1));
  };
  for (const auto& [datatype, cell_val_num, nullable] : attributes) {
    add_attribute(datatype, cell_val_num, nullable);
  }

  tiledb::test::create_array(
      context.ptr().get(),
      array_name,
      TILEDB_SPARSE,
      dimension_names,
      dimension_types,
      dimension_ranges,
      dimension_extents,
      attribute_names,
      attribute_types,
      attribute_cell_val_nums,
      attribute_compressors,
      tile_order,
      cell_order,
      tile_capacity,
      allow_duplicates,
      false,
      {attribute_nullables});
}

}  // namespace ddl

}  // namespace tiledb::test::templates

#endif
