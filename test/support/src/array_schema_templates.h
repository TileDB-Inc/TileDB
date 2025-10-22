/**
 * @file test/support/src/array_schema_templates.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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

#ifndef TILEDB_ARRAY_SCHEMA_TEMPLATES_H
#define TILEDB_ARRAY_SCHEMA_TEMPLATES_H

#include "tiledb/type/datatype_traits.h"
#include "tiledb/type/range/range.h"

#include <concepts>
#include <type_traits>

namespace tiledb::test::templates {

using StringDimensionCoordType = std::vector<char>;
using StringDimensionCoordView = std::span<const char>;

/**
 * Constrains types which can be used as the physical type of a dimension.
 */
template <typename D>
concept DimensionType =
    std::is_same_v<D, StringDimensionCoordType> or requires(const D& coord) {
      typename std::is_signed<D>;
      { coord < coord } -> std::same_as<bool>;
      { D(int64_t(coord)) } -> std::same_as<D>;
    };

/**
 * Constrains types which can be used as the physical type of an attribute.
 *
 * Right now this doesn't constrain anything, it is just a marker for
 * readability, and someday we might want it do require something.
 *
 * This used to have
 * ```
 * typename query_buffers<T>::cell_type;
 * ```
 * but that was removed to simplify include whatnot and forward declaration etc
 */
template <typename T>
concept AttributeType = true;

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

  bool operator==(const Domain<D>&) const = default;

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
  using domain_type = Domain<value_type>;

  Dimension() = default;
  Dimension(Domain<value_type> domain, value_type extent)
      : domain(domain)
      , extent(extent) {
  }

  Dimension(value_type lower_bound, value_type upper_bound, value_type extent)
      : Dimension(Domain<value_type>(lower_bound, upper_bound), extent) {
  }

  Domain<value_type> domain;
  value_type extent;

  /**
   * @return the number of tiles spanned by the whole domain of this dimension
   */
  uint64_t num_tiles() const {
    return num_tiles(domain);
  }

  /**
   * @return the number of tiles spanned by a range in this dimension
   */
  uint64_t num_tiles(const domain_type& range) const {
    return (range.num_cells() + extent - 1) / extent;
  }
};

template <tiledb::sm::Datatype DATATYPE, uint32_t CELL_VAL_NUM, bool NULLABLE>
struct static_attribute {};

template <tiledb::sm::Datatype DATATYPE>
struct static_attribute<DATATYPE, 1, false> {
  static constexpr tiledb::sm::Datatype datatype = DATATYPE;
  static constexpr uint32_t cell_val_num = 1;
  static constexpr bool nullable = false;

  using value_type =
      typename tiledb::type::datatype_traits<DATATYPE>::value_type;
  using cell_type = value_type;
};

template <tiledb::sm::Datatype DATATYPE>
struct static_attribute<DATATYPE, 1, true> {
  static constexpr tiledb::sm::Datatype datatype = DATATYPE;
  static constexpr uint32_t cell_val_num = 1;
  static constexpr bool nullable = true;

  using value_type = std::optional<
      typename tiledb::type::datatype_traits<DATATYPE>::value_type>;
  using cell_type = value_type;
};

template <tiledb::sm::Datatype DATATYPE>
struct static_attribute<DATATYPE, tiledb::sm::cell_val_num_var, false> {
  static constexpr tiledb::sm::Datatype datatype = DATATYPE;
  static constexpr uint32_t cell_val_num = tiledb::sm::cell_val_num_var;
  static constexpr bool nullable = false;

  using value_type =
      typename tiledb::type::datatype_traits<DATATYPE>::value_type;
  using cell_type = std::vector<value_type>;
};

template <tiledb::sm::Datatype DATATYPE>
struct static_attribute<DATATYPE, tiledb::sm::cell_val_num_var, true> {
  static constexpr tiledb::sm::Datatype datatype = DATATYPE;
  static constexpr uint32_t cell_val_num = tiledb::sm::cell_val_num_var;
  static constexpr bool nullable = true;

  using value_type =
      typename tiledb::type::datatype_traits<DATATYPE>::value_type;
  using cell_type = std::optional<std::vector<value_type>>;
};

template <typename static_attribute>
constexpr std::tuple<tiledb::sm::Datatype, uint32_t, bool>
attribute_properties() {
  return {
      static_attribute::datatype,
      static_attribute::cell_val_num,
      static_attribute::nullable};
}

}  // namespace tiledb::test::templates

#endif
