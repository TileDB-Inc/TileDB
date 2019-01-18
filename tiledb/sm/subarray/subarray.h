/**
 * @file   subarray.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines class Subarray.
 */

#ifndef TILEDB_SUBARRAY_H
#define TILEDB_SUBARRAY_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/tile_overlap.h"

#include <cmath>
#include <iostream>
#include <memory>
#include <vector>

namespace tiledb {
namespace sm {

class Array;

/**
 * A Subarray object is associated with an array, and
 * is oriented by a set of 1D ranges per dimension over the array
 * domain. The ranges may overlap. The subarray essentially represents
 * a new logical domain constructed by choosing cell slabs from
 * the array domain. This is applicable to both dense and sparse
 * array domains.
 *
 * **Example:**
 *
 * Consider the following array with domain [1,3] x [1,3]:
 *
 * 1 2 3
 * 4 5 6
 * 7 8 9
 *
 * A subarray will be defined as an ordered list of 1D ranges per dimension,
 * e.g., for
 *
 * rows dim: ([2,3], [1,1])
 * cols dim: ([1,2])
 *
 * we get a 3x2 array:
 *
 * 4 5
 * 7 8
 * 1 2
 *
 * The 1D ranges may overlap. For instance:
 *
 * rows dim: ([2,3], [1,1])
 * cols dim: ([1,2], [2,3])
 *
 * produces:
 *
 * 4 5 5 6
 * 7 8 8 9
 * 1 2 2 3
 *
 * @note The subarray will certainly have the same type and number of
 *     dimensions as the array domain it is constructed from, but it may
 *     have a different shape.
 */
class Subarray {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** Result size (in bytes) for an attribute used for partitioning. */
  struct ResultSize {
    /** Size for fixed-sized attributes or offsets of var-sized attributes. */
    double size_fixed_;
    /** Size of values for var-sized attributes. */
    double size_var_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Subarray();

  /**
   * Constructor.
   *
   * @param array The array the subarray is associated with.
   * @param layout The layout of the values of the subarray (of the results
   *     if the subarray is used for reads, or of the values provided
   *     by the user for writes).
   */
  Subarray(const Array* array, Layout layout);

  /**
   * Copy constructor. This performs a deep copy (including memcpy of
   * underlying buffers).
   */
  Subarray(const Subarray& subarray);

  /** Move constructor. */
  Subarray(Subarray&& subarray);

  /** Destructor. */
  ~Subarray() = default;

  /**
   * Copy-assign operator. This performs a deep copy (including memcpy
   * of underlying buffers).
   */
  Subarray& operator=(const Subarray& subarray);

  /** Move-assign operator. */
  Subarray& operator=(Subarray&& subarray);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * If no range has been added for a dimension, this function adds
   * a range that covers the whole domain along that dimension.
   *
   * @return Status
   */
  Status add_missing_ranges();

  /** Adds a range along the dimension with the given index. */
  Status add_range(uint32_t dim_idx, const void* range);

  /** Returns the array the subarray is associated with. */
  const Array* array() const;

  /** Clears the contents of the subarray. */
  void clear();

  /** Returns the number of dimensions of the subarray. */
  uint32_t dim_num() const;

  /** Returns the domain the subarray is constructed from. */
  const void* domain() const;

  /** Retrieves a range of a given dimension at a given range index. */
  Status get_range(
      uint32_t dim_idx, uint64_t range_idx, const void** range) const;

  /** Retrieves the number of ranges on the given dimension index. */
  Status get_range_num(uint32_t dim_idx, uint64_t* range_num) const;

  /** Returns the subarray layout. */
  Layout layout() const;

  /** Retrieves the next partition. */
  Status next_partition();

  /** Returns ``true`` if all subarray partitions have been consumed. */
  bool no_more_partitions() const;

  /** Gets result size budget (in bytes) for the input fixed-sized attribute. */
  Status get_result_budget(const char* attr_name, uint64_t* budget);

  /** Gets result size budget (in bytes) for the input var-sized attribute. */
  Status get_result_budget(
      const char* attr_name, uint64_t* budget_off, uint64_t* budget_val);

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized
   * attribute.
   */
  Status get_est_result_size(const char* attr_name, uint64_t* size);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized
   * attribute.
   */
  Status get_est_result_size(
      const char* attr_name, uint64_t* size_off, uint64_t* size_val);

  /** The total number of multi-dimensional ranges in the subarray. */
  uint64_t range_num() const;

  /**
   * Returns the multi-dimensional range with the input id, based on the
   * order imposed on the the subarray ranges by the layout. If ``layout_``
   * is UNORDERED, the the range layout will be the same as the array's
   * cell order, since this will lead to more beneficial tile access
   * patterns upon a read query.
   */
  template <class T>
  std::vector<const T*> range(uint64_t range_idx) const;

  /** Sets result size budget (in bytes) for the input fixed-sized attribute. */
  Status set_result_budget(const char* attr_name, uint64_t budget);

  /** Sets result size budget (in bytes) for the input var-sized attribute. */
  Status set_result_budget(
      const char* attr_name, uint64_t budget_off, uint64_t budget_val);

  /** Returns the subarray domain type. */
  Datatype type() const;

  /**
   * Returns ``true`` if the current partition with the given memory
   * budget is unsplittable.
   */
  bool unsplittable() const;

 private:
  /* ********************************* */
  /*      PRIVATE TYPE DEFINITIONS     */
  /* ********************************* */

  /**
   * Stores a set of 1D ranges.
   */
  struct Ranges {
    /** A buffer where all the ranges are appended to. */
    Buffer buffer_;

    /** The size in bytes of a range. */
    uint64_t range_size_;

    /** The datatype of the ranges. */
    Datatype type_;

    /** Constructor. */
    explicit Ranges(Datatype type)
        : type_(type) {
      range_size_ = 2 * datatype_size(type_);
    }

    /** Adds a range to the buffer. */
    void add_range(const void* range) {
      buffer_.write(range, range_size_);
    }

    /** Gets the range at the give index. */
    const void* get_range(uint64_t idx) const {
      assert(idx < range_num());
      return buffer_.data(idx * range_size_);
    }

    /** Return the number of ranges in this object. */
    uint64_t range_num() const {
      return buffer_.size() / range_size_;
    }
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array the subarray object is associated with. */
  const Array* array_;

  /** Result size budget (in bytes) for all attributes. */
  std::unordered_map<std::string, ResultSize> budget_;

  /** Stores the estimated result size for each array attribute. */
  std::unordered_map<std::string, ResultSize> est_result_size_;

  /**
   * The layout of the subarray (i.e., of the results
   * if the subarray is used for reads, or of the values provided
   * by the user for writes).
   */
  Layout layout_;

  /** Stores a set of ranges per dimension. */
  std::vector<Ranges> ranges_;

  /** Important for computed an ND range index from a flat serialized index. */
  std::vector<uint64_t> range_offsets_;

  /**
   * ``True`` if the estimated result size for all attributes has been
   * computed.
   */
  bool result_est_size_computed_;

  /**
   * Stores info about the overlap of the subarray with tiles
   * of all array fragments. Each element is a vector corresponding
   * to a single range of the subarray. These vectors/ranges are sorted
   * according to ``layout_``.
   */
  std::vector<std::vector<TileOverlap>> tile_overlap_;

  /**
   * ``True`` if the tile overlap for the ranges of the subarray has
   *  been computed.
   */
  bool tile_overlap_computed_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Adds a range along the dimension with the given index.
   *
   * @tparam T The subarray domain type.
   * @param dim_idx The index of the dimension to add the range to.
   * @param range The range to add.
   * @return Status
   */
  template <class T>
  Status add_range(uint32_t dim_idx, const T* range);

  /** Checks if the input range contains NaN. This is a noop for integers. */
  template <
      typename T,
      typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
  Status check_nan(const T* range) const {
    (void)range;
    return Status::Ok();
  }

  /** Checks if the input range contains NaN. */
  template <
      typename T,
      typename std::enable_if<!std::is_integral<T>::value>::type* = nullptr>
  Status check_nan(const T* range) const {
    // Check for NaN
    if (std::isnan(range[0]) || std::isnan(range[1]))
      return LOG_STATUS(Status::SubarrayError(
          "Cannot add range to dimension; Range contains NaN"));
    return Status::Ok();
  }

  /**
   * Computes the range offsets which are important for getting
   * an ND range index from a flat serialized index.
   */
  void compute_range_offsets();

  /** Computes the estimated result size for all attributes. */
  void compute_est_result_size();

  /** Computes the estimated result size for all attributes. */
  template <class T>
  void compute_est_result_size();

  /**
   * Computes the tile overlap with all subarray ranges for
   * all fragments.
   */
  template<class T>
  void compute_tile_overlap();

  /**
   * Computes the estimated result size for a given attribute, fragment index,
   * and tile overlap info.
   *
   * @param attr_name The name of the attribute to focus on.
   * @param var_size Whether the attribute is var-sized or not.
   * @param fragment_idx The id of the fragment to focus on.
   * @param overlap The tile overlap info.
   * @return The result size.
   */
  ResultSize compute_est_result_size(
      const std::string& attr_name,
      bool var_size,
      unsigned fragment_idx,
      const TileOverlap& overlap) const;

  /** Returns a deep copy of this Subarray. */
  Subarray clone() const;

  /**
   * Swaps the contents (all field values) of this subarray with the
   * given subarray.
   */
  void swap(Subarray& subarray);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SUBARRAY_H
