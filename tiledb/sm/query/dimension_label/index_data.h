/**
 * @file index_data.h
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
 * Class for internally managed index data for dimension labels.
 */

#ifndef TILEDB_INDEX_DATA_H
#define TILEDB_INDEX_DATA_H

#include "tiledb/common/common.h"
#include "tiledb/type/range/range.h"

#include <vector>

using namespace tiledb::common;

namespace tiledb::sm {

enum class Datatype : uint8_t;

/** Base class for internally managed index data. */
class IndexData {
 public:
  virtual ~IndexData() = default;

  /** Returns the count, or number of stored variables.*/
  virtual uint64_t count() const = 0;

  /** Returns pointer to data. */
  virtual void* data() = 0;

  /** Returns pointer to total data size. */
  virtual uint64_t* data_size() = 0;
};

/** Typed class for internally managed index data. */
template <
    typename T,
    typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
class TypedIndexData : public IndexData {
 public:
  /** Default constructor is not C.41 compliant. */
  TypedIndexData() = delete;

  /**
   * Constructor.
   *
   * This will create a vector of index values starting at the range lower bound
   * and continuing to the range upper bound.
   *
   * @param range The range to create index data for.
   */
  TypedIndexData(const type::Range& range)
      : data_{}
      , data_size_{0} {
    auto range_data = static_cast<const T*>(range.data());
    T min_value = range_data[0];
    T max_value = range_data[1];
    if (max_value < min_value) {
      throw std::invalid_argument(
          "Invalid range - cannot have lower bound greater than the upper "
          "bound.");
    }
    data_.reserve(max_value - min_value + 1);
    for (T val = min_value; val <= max_value; ++val) {
      data_.push_back(val);
    }
    data_size_ = sizeof(T) * data_.size();
  }

  /**
   * Constructor.
   *
   * This will create a vector of the index type large enough to store the
   * requested number of values.
   *
   * @param num_values Number of values the data array must be able to store.
   */
  TypedIndexData(const uint64_t num_values)
      : data_(num_values)
      , data_size_{sizeof(T) * num_values} {
  }

  /** Returns the number of stored elements. */
  uint64_t count() const override {
    return data_.size();
  }

  /** Pointer access to index data. */
  void* data() override {
    return data_.data();
  }

  /** Pointer access to data size. */
  uint64_t* data_size() override {
    return &data_size_;
  }

 private:
  /** Vector of index data. */
  std::vector<T> data_;

  /** Size of the index data. */
  uint64_t data_size_;
};

/** Index data factory. */
class IndexDataCreate {
 public:
  /**
   * Creates a buffer of incremental index values in a range, including end
   * points.
   *
   * @param type Datatype of the index data to create.
   * @param input_range Range to create data for.
   * @returns A pointer to an index data object.
   */
  static IndexData* make_index_data(
      const Datatype type, const type::Range& input_range);

  /**
   * Creates a buffer to hold index values.
   *
   * @param Datatype of the index data to create.
   * @param num_values The number of contained data points.
   * @returns A pointer to an index data object.
   */
  static IndexData* make_index_data(
      const Datatype type, const uint64_t num_values);
};

}  // namespace tiledb::sm

#endif
