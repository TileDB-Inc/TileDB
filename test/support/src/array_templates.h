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
#include "tiledb/sm/query/ast/query_ast.h"
#include "tiledb/type/datatype_traits.h"
#include "tiledb/type/range/range.h"

#include <test/support/assert_helpers.h>
#include <test/support/src/error_helpers.h>
#include <test/support/stdx/fold.h>
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

  tiledb::common::UntypedDatumView dimension_datum(
      const tiledb::sm::Dimension&, unsigned dim_idx) const {
    return std::apply(
        [&](const auto&... field) {
          size_t sizes[] = {sizeof(std::decay_t<decltype(field)>)...};
          const void* const ptrs[] = {
              static_cast<const void*>(std::addressof(field))...};
          return UntypedDatumView(ptrs[dim_idx], sizes[dim_idx]);
        },
        tup_);
  }

  const void* coord(unsigned dim) const {
    return std::apply(
        [&](const auto&... field) {
          const void* const ptrs[] = {
              static_cast<const void*>(std::addressof(field))...};
          return ptrs[dim];
        },
        tup_);
  }

  StdTuple tup_;
};

/**
 * Constrains types which can be used as the physical type of a dimension.
 */
template <typename D>
concept DimensionType = requires(const D& coord) {
  typename std::is_signed<D>;
  { coord < coord } -> std::same_as<bool>;
  { D(int64_t(coord)) } -> std::same_as<D>;
};

/**
 * Constrains types which can be used as the physical type of an attribute.
 *
 * Right now this doesn't constrain anything, it is just a marker for
 * readability, and someday we might want it do require something.
 */
template <typename T>
concept AttributeType = std::is_fundamental_v<T>;

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
 * A generic, statically-typed range which is inclusive on both ends.
 */
template <DimensionType D>
struct Domain {
  D lower_bound;
  D upper_bound;

  Domain() {
  }

  Domain(D d1, D d2)
      : lower_bound(std::min(d1, d2))
      , upper_bound(std::max(d1, d2)) {
  }

  uint64_t num_cells() const {
    // FIXME: this is incorrect for 64-bit domains which need to check overflow
    if (std::is_signed<D>::value) {
      return static_cast<int64_t>(upper_bound) -
             static_cast<int64_t>(lower_bound) + 1;
    } else {
      return static_cast<uint64_t>(upper_bound) -
             static_cast<uint64_t>(lower_bound) + 1;
    }
  }

  bool contains(D point) const {
    return lower_bound <= point && point <= upper_bound;
  }

  bool intersects(const Domain<D>& other) const {
    return (other.lower_bound <= lower_bound &&
            lower_bound <= other.upper_bound) ||
           (other.lower_bound <= upper_bound &&
            upper_bound <= other.upper_bound) ||
           (lower_bound <= other.lower_bound &&
            other.lower_bound <= upper_bound) ||
           (lower_bound <= other.upper_bound &&
            other.upper_bound <= upper_bound);
  }

  tiledb::type::Range range() const {
    return tiledb::type::Range(lower_bound, upper_bound);
  }
};

/**
 * A description of a dimension as it pertains to its datatype.
 */
template <tiledb::sm::Datatype DATATYPE>
struct Dimension {
  using value_type = tiledb::type::datatype_traits<DATATYPE>::value_type;

  Dimension() = default;
  Dimension(Domain<value_type> domain, value_type extent)
      : domain(domain)
      , extent(extent) {
  }

  Domain<value_type> domain;
  value_type extent;
};

template <Datatype DATATYPE, uint32_t CELL_VAL_NUM, bool NULLABLE>
struct static_attribute {};

template <Datatype DATATYPE>
struct static_attribute<DATATYPE, 1, false> {
  static constexpr Datatype datatype = DATATYPE;
  static constexpr uint32_t cell_val_num = 1;
  static constexpr bool nullable = false;

  using value_type =
      typename tiledb::type::datatype_traits<DATATYPE>::value_type;
  using cell_type = value_type;
};

template <Datatype DATATYPE>
struct static_attribute<DATATYPE, tiledb::sm::cell_val_num_var, false> {
  static constexpr Datatype datatype = DATATYPE;
  static constexpr uint32_t cell_val_num = tiledb::sm::cell_val_num_var;
  static constexpr bool nullable = false;

  using value_type =
      typename tiledb::type::datatype_traits<DATATYPE>::value_type;
  using cell_type = std::vector<value_type>;
};

template <typename static_attribute>
constexpr std::tuple<Datatype, uint32_t, bool> attribute_properties() {
  return {
      static_attribute::datatype,
      static_attribute::cell_val_num,
      static_attribute::nullable};
}

/**
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

  /**
   * @return true if a value passes a simple condition
   */
  template <AttributeType T>
  static bool test(const T& value, const tiledb::sm::ASTNode& condition) {
    switch (condition.get_op()) {
      case tiledb::sm::QueryConditionOp::LT:
        return value < *static_cast<const T*>(condition.get_value_ptr());
      case tiledb::sm::QueryConditionOp::LE:
        return value <= *static_cast<const T*>(condition.get_value_ptr());
      case tiledb::sm::QueryConditionOp::GT:
        return value > *static_cast<const T*>(condition.get_value_ptr());
      case tiledb::sm::QueryConditionOp::GE:
        return value >= *static_cast<const T*>(condition.get_value_ptr());
      case tiledb::sm::QueryConditionOp::EQ:
        return value == *static_cast<const T*>(condition.get_value_ptr());
      case tiledb::sm::QueryConditionOp::NE:
        return value != *static_cast<const T*>(condition.get_value_ptr());
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
   * @return true if `record` passes a simple (i.e. non-combination) query
   * condition
   */
  bool test(
      const Fragment& fragment,
      int record,
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

template <typename T>
struct query_buffers {};

template <stdx::is_fundamental T>
struct query_buffers<T> {
  using value_type = T;
  using cell_type = value_type;
  using query_field_size_type = uint64_t;

  std::vector<T> values_;

  query_buffers() {
  }

  query_buffers(const query_buffers<T>& other)
      : values_(other.values_) {
  }

  query_buffers(std::vector<T> cells)
      : values_(cells) {
  }

  size_t num_cells() const {
    return values_.size();
  }

  size_t size() const {
    return num_cells();
  }

  void reserve(size_t num_cells) {
    values_.reserve(num_cells);
  }

  void resize(size_t num_cells, T value = 0) {
    values_.resize(num_cells, value);
  }

  const cell_type& operator[](int64_t index) const {
    return values_[index];
  }

  cell_type& operator[](int64_t index) {
    return values_[index];
  }

  void push_back(cell_type value) {
    values_.push_back(value);
  }

  bool operator==(const query_buffers<T>&) const = default;

  query_buffers<T>& operator=(const query_buffers<T>& other) {
    values_ = other.values_;
    return *this;
  }

  query_buffers<T>& operator=(const std::vector<T>& values) {
    values_ = values;
    return *this;
  }

  query_field_size_type make_field_size(uint64_t cell_limit) const {
    return sizeof(T) * std::min(cell_limit, values_.size());
  }

  int32_t attach_to_query(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      query_field_size_type& field_size,
      const std::string& name,
      uint64_t cell_offset) const {
    void* ptr = const_cast<void*>(
        static_cast<const void*>(&values_.data()[cell_offset]));
    return tiledb_query_set_data_buffer(
        ctx, query, name.c_str(), ptr, &field_size);
  }

  template <typename Asserter>
  uint64_t query_num_cells(const query_field_size_type& field_size) const {
    ASSERTER(field_size % sizeof(T) == 0);
    ASSERTER(field_size <= num_cells() * sizeof(T));
    return field_size / sizeof(T);
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
 * Data for a one-dimensional array
 */
template <DimensionType D, AttributeType... Att>
struct Fragment1D {
  using DimensionType = D;

  query_buffers<D> dim_;
  std::tuple<query_buffers<Att>...> atts_;

  uint64_t size() const {
    return dim_.num_cells();
  }

  std::tuple<const query_buffers<D>&> dimensions() const {
    return std::tuple<const query_buffers<D>&>(dim_);
  }

  std::tuple<const query_buffers<Att>&...> attributes() const {
    return std::apply(
        [](const query_buffers<Att>&... attribute) {
          return std::tuple<const query_buffers<Att>&...>(attribute...);
        },
        atts_);
  }

  std::tuple<query_buffers<D>&> dimensions() {
    return std::tuple<query_buffers<D>&>(dim_);
  }

  std::tuple<query_buffers<Att>&...> attributes() {
    return std::apply(
        [](query_buffers<Att>&... attribute) {
          return std::tuple<query_buffers<Att>&...>(attribute...);
        },
        atts_);
  }
};

/**
 * Data for a two-dimensional array
 */
template <DimensionType D1, DimensionType D2, typename... Att>
struct Fragment2D {
  query_buffers<D1> d1_;
  query_buffers<D2> d2_;
  std::tuple<query_buffers<Att>...> atts_;

  uint64_t size() const {
    return d1_.num_cells();
  }

  std::tuple<const query_buffers<D1>&, const query_buffers<D2>&> dimensions()
      const {
    return std::tuple<const query_buffers<D1>&, const query_buffers<D2>&>(
        d1_, d2_);
  }

  std::tuple<query_buffers<D1>&, query_buffers<D2>&> dimensions() {
    return std::tuple<query_buffers<D1>&, query_buffers<D2>&>(d1_, d2_);
  }

  std::tuple<const query_buffers<Att>&...> attributes() const {
    return std::apply(
        [](const query_buffers<Att>&... attribute) {
          return std::tuple<const query_buffers<Att>&...>(attribute...);
        },
        atts_);
  }

  std::tuple<query_buffers<Att>&...> attributes() {
    return std::apply(
        [](query_buffers<Att>&... attribute) {
          return std::tuple<query_buffers<Att>&...>(attribute...);
        },
        atts_);
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
      const uint64_t field_cells = std::min(cell_limit, field.num_cells());
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
   * Sets buffers on `query` for the variadic `fields` and `fields_sizes`
   */
  static void set(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      auto& field_sizes,
      std::tuple<std::decay_t<Ts>&...> fields,
      std::function<std::string(unsigned)> fieldname,
      uint64_t cell_offset = 0) {
    auto set_data_buffer =
        [&](const std::string& name, auto& field, auto& field_size) {
          const auto rc =
              field.attach_to_query(ctx, query, field_size, name, cell_offset);
          ASSERTER(std::optional<std::string>() == error_if_any(ctx, rc));
        };

    unsigned d = 0;
    std::apply(
        [&](const auto&... field) {
          std::apply(
              [&]<typename... Us>(Us&... field_size) {
                (set_data_buffer(fieldname(d++), field, field_size), ...);
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
  return [cell_limit]<typename... Ts>(std::tuple<Ts...> fields) {
    return query_applicator<Asserter, Ts...>::make_field_sizes(
        fields, cell_limit);
  }(std::tuple_cat(fragment.dimensions(), fragment.attributes()));
}

/**
 * Set buffers on `query` for the tuple of field columns
 */
template <typename Asserter, FragmentType F>
void set_fields(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    auto& field_sizes,
    F& fragment,
    std::function<std::string(unsigned)> dimension_name,
    std::function<std::string(unsigned)> attribute_name,
    uint64_t cell_offset = 0) {
  auto [dimension_sizes, attribute_sizes] = stdx::split_tuple<
      std::decay_t<decltype(field_sizes)>,
      std::tuple_size_v<decltype(fragment.dimensions())>>::value(field_sizes);

  [&]<typename... Ts>(std::tuple<Ts...> fields) {
    query_applicator<Asserter, Ts...>::set(
        ctx, query, dimension_sizes, fields, dimension_name, cell_offset);
  }(fragment.dimensions());
  [&]<typename... Ts>(std::tuple<Ts...> fields) {
    query_applicator<Asserter, Ts...>::set(
        ctx, query, attribute_sizes, fields, attribute_name, cell_offset);
  }(fragment.attributes());
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

}  // namespace query

}  // namespace tiledb::test::templates

#endif
