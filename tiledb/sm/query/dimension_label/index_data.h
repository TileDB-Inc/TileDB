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

#include <numeric>
#include <vector>

using namespace tiledb::common;

namespace tiledb::sm {

enum class Datatype : uint8_t;

/** Base class for internally managed index data. */
class IndexData {
 public:
  virtual ~IndexData() = default;

  /** Returns the count, or number of stored variables.*/
  virtual storage_size_t count() const = 0;

  /** Returns pointer to data. */
  virtual void* data() = 0;

  /** Returns pointer to total data size. */
  virtual storage_size_t* data_size() = 0;

  /**
   * Returns if ranges should be interpreted as points when setting ranges with
   * the index data.
   *
   *  - If this is ``true``, each point represents a range containing just that
   *    value.
   *  - If this is ``false``, data is stored in range start/end pairs.
   */
  virtual bool ranges_are_points() const = 0;
};

/** Typed class for internally managed index data for a QueryBuffer. */
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
      , data_size_{0}
      , ranges_are_points_{true} {
    auto range_data = range.typed_data<T>();
    T min_value = range_data[0];
    T max_value = range_data[1];
    if (max_value < min_value) {
      throw std::invalid_argument(
          "Invalid range - cannot have lower bound greater than the upper "
          "bound.");
    }
    data_.resize(max_value - min_value + 1);
    std::iota(data_.begin(), data_.end(), min_value);
    data_size_ = sizeof(T) * data_.size();
  }

  /**
   * Constructor.
   *
   * This will create a vector of the index type large enough to store the
   * requested number of values. It uses the same initialization pattern as
   * std::vector.
   *
   * @param num_values Number of values the data array must be able to store.
   * @param ranges_are_points If ``true``, it contins point data. Otherwise, the
   *     data contains alternating start/end values of ranges.
   */
  TypedIndexData(const storage_size_t num_values, const bool ranges_are_points)
      : data_(num_values)
      , data_size_{sizeof(T) * num_values}
      , ranges_are_points_{ranges_are_points} {
  }

  /** Returns the number of stored elements. */
  storage_size_t count() const override {
    return data_.size();
  }

  /** Pointer access to index data. */
  void* data() override {
    return data_.data();
  }

  /** Pointer access to data size. */
  storage_size_t* data_size() override {
    return &data_size_;
  }

  /**
   * Returns if ranges should be interpreted as points when setting ranges with
   * the index data.
   *
   *  - If this is ``true``, each point represents a range containing just that
   *    value.
   *  - If this is ``false``, data is stored in range start/end pairs.
   */
  bool ranges_are_points() const override {
    return ranges_are_points_;
  }

 private:
  /** Vector of index data. */
  std::vector<T> data_;

  /** Size of the index data. */
  storage_size_t data_size_;

  /**
   * Flag for interpretting data as ranges.
   *
   *  - If this is ``true``, each point represents a range containing just that
   *    value.
   *  - If this is ``false``, data is stored in range start/end pairs.
   */
  bool ranges_are_points_;
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
   * Creates a buffer that can hold the requested size of data.
   *
   * This follows the same initialization rules as std::vector.
   *
   * @param Datatype of the index data to create.
   * @param num_values The number of contained data points.
   * @param ranges_are_points If ``true``, it contins point data. Otherwise, the
   *     data contains alternating start/end values of ranges.
   * @returns A pointer to an index data object.
   */
  static IndexData* make_index_data(
      const Datatype type,
      const storage_size_t num_values,
      const bool ranges_are_points);
};

}  // namespace tiledb::sm

#endif
