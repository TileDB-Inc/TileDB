/**
 * @file   comparators.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Defines custom comparators to be used in cell position sorting in the case
 * of sparse arrays.
 */

#ifndef TILEDB_COMPARATORS_H
#define TILEDB_COMPARATORS_H

#include <cinttypes>
#include <vector>

namespace tiledb {

/**
 * Wrapper of comparison function for sorting cells; first by the smallest id,
 * and then by column-major order of coordinates.
 */
template <class T>
class SmallerIdCol {
 public:
  /**
   * Constructor.
   *
   * @param buffer The buffer containing the cells to be sorted.
   * @param dim_num The number of dimensions of the cells.
   * @param ids The ids of the cells in the buffer.
   */
  SmallerIdCol(
      const T* buffer, unsigned int dim_num, const std::vector<uint64_t>& ids)
      : buffer_(buffer)
      , dim_num_(dim_num)
      , ids_(ids) {
  }

  /**
   * Comparison operator.
   *
   * @param a The first cell position in the cell buffer.
   * @param b The second cell position in the cell buffer.
   */
  bool operator()(uint64_t a, uint64_t b) {
    if (ids_[a] < ids_[b])
      return true;

    if (ids_[a] > ids_[b])
      return false;

    // a.id_ == b.id_ --> check coordinates
    const T* coords_a = &buffer_[a * dim_num_];
    const T* coords_b = &buffer_[b * dim_num_];

    for (unsigned int i = dim_num_ - 1;; --i) {
      if (coords_a[i] < coords_b[i])
        return true;
      if (coords_a[i] > coords_b[i])
        return false;
      // else coords_a[i] == coords_b[i] --> continue

      if (i == 0)
        break;
    }

    return false;
  }

 private:
  /** Cell buffer. */
  const T* buffer_;
  /** Number of dimensions. */
  unsigned int dim_num_;
  /** The cell ids. */
  const std::vector<uint64_t>& ids_;
};

/**
 * Wrapper of comparison function for sorting cells; first by the smallest id,
 * and then by row-major order of coordinates.
 */
template <class T>
class SmallerIdRow {
 public:
  /**
   * Constructor.
   *
   * @param buffer The buffer containing the cells to be sorted.
   * @param dim_num The number of dimensions of the cells.
   * @param ids The ids of the cells in the buffer.
   */
  SmallerIdRow(
      const T* buffer, unsigned int dim_num, const std::vector<uint64_t>& ids)
      : buffer_(buffer)
      , dim_num_(dim_num)
      , ids_(ids) {
  }

  /**
   * Comparison operator.
   *
   * @param a The first cell position in the cell buffer.
   * @param b The second cell position in the cell buffer.
   */
  bool operator()(uint64_t a, uint64_t b) {
    if (ids_[a] < ids_[b])
      return true;

    if (ids_[a] > ids_[b])
      return false;

    // a.id_ == b.id_ --> check coordinates
    const T* coords_a = &buffer_[a * dim_num_];
    const T* coords_b = &buffer_[b * dim_num_];

    for (unsigned int i = 0; i < dim_num_; ++i) {
      if (coords_a[i] < coords_b[i])
        return true;
      if (coords_a[i] > coords_b[i])
        return false;
      // else coords_a[i] == coords_b[i] --> continue
    }

    return false;
  }

 private:
  /** Cell buffer. */
  const T* buffer_;
  /** Number of dimensions. */
  unsigned int dim_num_;
  /** The cell ids. */
  const std::vector<uint64_t>& ids_;
};

/** Wrapper of comparison function for sorting cells on column-major order. */
template <class T>
class SmallerCol {
 public:
  /**
   * Constructor.
   *
   * @param buffer The buffer containing the cells to be sorted.
   * @param dim_num The number of dimensions of the cells.
   */
  SmallerCol(const T* buffer, unsigned int dim_num)
      : buffer_(buffer)
      , dim_num_(dim_num) {
  }

  /**
   * Comparison operator.
   *
   * @param a The first cell position in the cell buffer.
   * @param b The second cell position in the cell buffer.
   */
  bool operator()(uint64_t a, uint64_t b) {
    const T* coords_a = &buffer_[a * dim_num_];
    const T* coords_b = &buffer_[b * dim_num_];

    for (unsigned int i = dim_num_ - 1;; --i) {
      if (coords_a[i] < coords_b[i])
        return true;
      if (coords_a[i] > coords_b[i])
        return false;
      // else coords_a[i] == coords_b[i] --> continue

      if (i == 0)
        break;
    }

    return false;
  }

 private:
  /** Cell buffer. */
  const T* buffer_;
  /** Number of dimensions. */
  unsigned int dim_num_;
};

/** Wrapper of comparison function for sorting cells on row-major order. */
template <class T>
class SmallerRow {
 public:
  /**
   * Constructor.
   *
   * @param buffer The buffer containing the cells to be sorted.
   * @param dim_num The number of dimensions of the cells.
   */
  SmallerRow(const T* buffer, unsigned int dim_num)
      : buffer_(buffer)
      , dim_num_(dim_num) {
  }

  /**
   * Comparison operator.
   *
   * @param a The first cell position in the cell buffer.
   * @param b The second cell position in the cell buffer.
   */
  bool operator()(uint64_t a, uint64_t b) {
    const T* coords_a = &buffer_[a * dim_num_];
    const T* coords_b = &buffer_[b * dim_num_];

    for (unsigned int i = 0; i < dim_num_; ++i) {
      if (coords_a[i] < coords_b[i])
        return true;
      if (coords_a[i] > coords_b[i])
        return false;
      // else coords_a[i] == coords_b[i] --> continue
    }

    return false;
  }

 private:
  /** Cell buffer. */
  const T* buffer_;
  /** Number of dimensions. */
  unsigned int dim_num_;
};

}  // namespace tiledb

#endif  // TILEDB_COMPARATORS_H
