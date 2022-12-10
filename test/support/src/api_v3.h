/**
 * @file array_api.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares some test suite helper functions.
 */

#ifndef TILEDB_TEST_ARRAY_API_H
#define TILEDB_TEST_ARRAY_API_H

#include <algorithm>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <tuple>
#include <vector>

#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"

namespace tiledb::test::api::v3 {

/*
  Domain domain(ctx);
domain.add_dimension(Dimension::create<int>(ctx, "rows", {{1, num_rows}}, 4))
    .add_dimension(Dimension::create<int>(ctx, "cols", {{1, num_rows}}, 4));
ArraySchema schema(ctx, array_type);
if (set_dups) {
  schema.set_allows_dups(true);
}
schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
Attribute attr_a = Attribute::create<int>(ctx, "a");
Attribute attr_b = Attribute::create<float>(ctx, "b");
if (array_type == TILEDB_DENSE) {
  attr_a.set_fill_value(&a_fill_value, sizeof(int));
  attr_b.set_fill_value(&b_fill_value, sizeof(float));
} else {
  schema.set_capacity(16);
}
schema.add_attribute(attr_a);
schema.add_attribute(attr_b);
Array::create(array_name, schema);
*/

template<typename T>
class Dimension {
  public:
    Dimension() {
    }

  std::optional<std::string> name;
  std::optional<std::array<T, 2>> range;
  std::optional<T> extent;
};

template<typename... Ts>
class Dimensions {
  public:
    typedef std::tuple<Ts...> coords_type;

    Dimensions() : size_(0) {
    }

    template<size_t I, typename T>
    Dimensions set(
        std::string name,
        std::initializer_list<T> range,
        std::optional<T> extent = std::nullopt) {
      if(range.size() != 2) {
        throw std::logic_error(
            "range size must be 2, not " + std::to_string(range.size()));
      }
      std::vector<T> v(range);
      std::array<T, 2> tmp;
      tmp[0] = v[0];
      tmp[1] = v[1];
      return set<I, T>(name, tmp, extent);
    }

    template<size_t I, typename T>
    Dimensions set(
        std::string name,
        std::optional<std::array<T, 2>> range = std::nullopt,
        std::optional<T> extent = std::nullopt) {

      std::get<I>(elems_).name = name;
      std::get<I>(elems_).range = range;
      std::get<I>(elems_).extent = extent;
      size_ = std::max(size_, I);
      return *this;
    }

    // template <size_t I = 0, typename... Ts>
    // contexpr void generate(std::tuple<Ts...> elems) {
    //   if constexpr (I == sizeof...(Ts)) {
    //     return;
    //   } else {
    //     // Going for next element.
    //     generate<I + 1>(elems);
    //   }
    // }

    size_t size() {
      return size_ + 1;
    }

  private:
    size_t size_;
    std::tuple<Dimension<Ts>...> elems_;
};

template<typename T>
class Attribute {
  public:
    Attribute() {
    }

  std::optional<std::string> name;
  std::optional<T> fill_value;
  std::optional<bool> nullable;
};

template<typename... Ts>
class Attributes {
  public:
    typedef std::tuple<Ts...> cell_type;

    Attributes() : size_(0) {
    }

    template<size_t I, typename T>
    Attributes set(
        std::string name,
        std::optional<T> fill_value = std::nullopt,
        std::optional<bool> nullable = std::nullopt) {

      std::get<I>(elems_).name = name;
      std::get<I>(elems_).fill_value = fill_value;
      std::get<I>(elems_).nullable = nullable;
      size_ = std::max(size_, I);
      return *this;
    }

    size_t size() {
      return size_ + 1;
    }

  private:
    size_t size_;
    std::tuple<Attribute<Ts>...> elems_;
};

template<class SubClass>
class Array {
  public:
    virtual ~Array() = default;

    SubClass set_allow_dups(bool allow_dups) {
      allow_dups_ = allow_dups;
      return *static_cast<SubClass*>(this);
    }

    SubClass set_order(int tiles, std::optional<int> cells = std::nullopt) {
      order_[0] = tiles;
      if (cells.has_value()) {
        order_[1] = *cells;
      }
      return *static_cast<SubClass*>(this);
    }

  protected:
    Array()
      : allow_dups_(false)
      , order_({TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}) {
    }

  private:
    bool allow_dups_ = false;
    std::array<int, 2> order_ = {TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR};
};

// class DenseArray : public Array<DenseArray> {
//   public:
//     DenseArray() : Array() {}
//
//     tiledb_array_type_t array_type() {
//       return TILEDB_DENSE;
//     }
//
//   private:
//
// };

template<class DimClass, class AttrClass>
class SparseArray : public Array<SparseArray<DimClass, AttrClass>> {
  public:
    typedef typename DimClass::coords_type coords_type;
    typedef typename AttrClass::cell_type cell_type;
    typedef SparseArray<DimClass, AttrClass> ArrayClass;

    explicit SparseArray<DimClass, AttrClass>(DimClass dims, AttrClass attrs)
      : Array<SparseArray<DimClass, AttrClass>>()
      , dims_(dims)
      , attrs_(attrs)
      , capacity_(1024) {
    }

    ArrayClass set_capacity(uint64_t capacity) {
      capacity_ = capacity;
      return *this;
    }

    DimClass dims() {
      return dims_;
    }

    AttrClass attrs() {
      return attrs_;
    }

  private:
    DimClass dims_;
    AttrClass attrs_;

    uint64_t capacity_;
};


}  // namespace tiledb::test
#endif  //  TILEDB_TEST_ARRAY_API_H
