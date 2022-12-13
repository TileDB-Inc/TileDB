/**
 * @file array.h
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
 * Implementation details for the v3 Array class
 */

#ifndef TILEDB_API_V3_DETAILS_ARRAY_H
#define TILEDB_API_V3_DETAILS_ARRAY_H

#include <memory>
#include <optional>
#include <vector>

#include "test/support/api/attribute.h"
#include "test/support/api/dimension.h"

namespace tiledb::test::api::v3::details {

template<class SubClass>
class Array {
  public:
    virtual ~Array() = default;

    SubClass& set_dimensions(std::vector<v3::Dimension> dims) {
      dims_ = dims;
      return *static_cast<SubClass*>(this);
    }

    SubClass& set_dimensions(std::initializer_list<v3::Dimension> dims) {
      std::vector<Dimension> vec_dims(dims);
      return set_dimensions(vec_dims);
    }

    SubClass& set_attributes(std::vector<v3::Attribute> attrs) {
      attrs_ = attrs;
      return *static_cast<SubClass*>(this);
    }

    SubClass& set_allow_dups(bool allow_dups) {
      allow_dups_ = allow_dups;
      return *static_cast<SubClass*>(this);
    }

    SubClass& set_order(int tiles, std::optional<int> cells = std::nullopt) {
      order_[0] = tiles;
      if (cells.has_value()) {
        order_[1] = *cells;
      }
      return *static_cast<SubClass*>(this);
    }

    SubClass& create() {
      return *static_cast<SubClass*>(this);
    }

    std::string name() {
      return name_;
    }

    size_t num_dimensions() {
      return dims_.size();
    }

    size_t num_attributes() {
      return attrs_.size();
    }

    Dimension get_dimensions() {
      return dims_;
    }

    Attribute get_attributes() {
      return attrs_;
    }

  protected:
    Array(std::string name)
      : name_(name)
      , allow_dups_(false)
      , order_({TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}) {
    }

  private:
    std::vector<v3::Dimension> dims_;
    std::vector<v3::Attribute> attrs_;

    std::string name_;
    bool allow_dups_;
    std::array<int, 2> order_;
};

} // namespace tiledb::test::api::v3::details

#endif // TILEDB_API_V3_DETAILS_ARRAY_H

