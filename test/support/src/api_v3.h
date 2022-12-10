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
    Dimensions() {
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
      std::array<T, 2> tmp;
      tmp[0] = *range.begin();
      tmp[1] = *(range.begin() + 1);
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
      num_elems_ = std::max(num_elems_, I);
      return *this;
    }

  private:
    size_t num_elems_;
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
    Attributes() {
    }

    template<size_t I, typename T>
    Attributes set(
        std::string name,
        std::optional<T> fill_value = std::nullopt,
        std::optional<bool> nullable = std::nullopt) {

      std::get<I>(elems_).name = name;
      std::get<I>(elems_).fill_value = fill_value;
      std::get<I>(elems_).nullable = nullable;
      num_elems_ = std::max(num_elems_, I);
      return *this;
    }

  private:
    size_t num_elems_;
    std::tuple<Attribute<Ts>...> elems_;
};

// template<class SubClass>
// class Array {
//   public:
//     virtual ~Array() = default;
//
//     void set_allow_dups(bool allow_dups) {
//       allow_dups_ = allow_dups;
//     }
//
//     template<typename T>
//     SubClass dim(const std::string name, std::pair<T, T> range, T extent) {
//       dims_.push_back(std::make_shared<Dimension<T>>(name, range, extent));
//       return static_cast<SubClass&>(*this);
//     }
//
//     template<typename T>
//     SubClass attr(const std::string name) {
//       attrs_.push_back(std::make_shared<Attribute<T>>(name));
//       return static_cast<SubClass&>(*this);;
//     }
//
//     template<typename T>
//     SubClass attr(const std::string name, T fill_val) {
//       attrs_.push_back(std::make_shared<Attribute<T>>(name, fill_val));
//       return static_cast<SubClass&>(*this);
//     }
//
//   protected:
//     Array() = default;
//
//   private:
//     bool allow_dups_ = false;
//
//     std::vector<std::shared_ptr<BaseDimension>> dims_;
//     std::vector<std::shared_ptr<BaseAttribute>> attrs_;
// };
//
//
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
//
// class SparseArray : public Array<SparseArray> {
//   public:
//     SparseArray()
//       : Array()
//       , capacity_(1024) {
//     }
//
//     void set_capacity(uint64_t capacity) {
//       capacity_ = capacity;
//     }
//
//     tiledb_array_type_t array_type() {
//       return TILEDB_SPARSE;
//     }
//
//   private:
//     uint64_t capacity_;
// };


}  // namespace tiledb::test
#endif  //  TILEDB_TEST_ARRAY_API_H
