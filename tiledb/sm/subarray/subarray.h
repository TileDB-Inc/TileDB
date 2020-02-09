/**
 * @file   subarray.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/tile_overlap.h"

#include <cmath>
#include <iostream>
#include <map>
#include <memory>
#include <set>
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
 *
 * @note If no 1D ranges are set for some dimension, then the subarray
 *     by default will cover the entire domain of that dimension.
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
    /**
     * Maximum size of overlapping tiles fetched into memory for
     * fixed-sized attributes or offsets of var-sized attributes.
     */
    uint64_t mem_size_fixed_;
    /**
     * Maximum size of overlapping tiles fetched into memory for
     * var-sized attributes.
     */
    uint64_t mem_size_var_;
  };

  /**
   * Stores a set of 1D ranges.
   */
  struct Ranges {
    /** A buffer where all the ranges are appended to. */
    Buffer buffer_;

    /**
     * ``true`` if it has the default entire-domain range
     * that must be replaced the first time a new range
     * is added.
     */
    bool has_default_range_ = false;

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
    void add_range(const void* range, bool is_default = false) {
      if (is_default) {
        buffer_.write(range, range_size_);
        has_default_range_ = true;
      } else {
        if (has_default_range_) {
          buffer_.clear();
          has_default_range_ = false;
        }
        buffer_.write(range, range_size_);
      }
    }

    /** Gets the range at the given index. */
    const void* get_range(uint64_t idx) const {
      assert(idx < range_num());
      return buffer_.data(idx * range_size_);
    }

    /** Gets the range start at the given index. */
    const void* get_range_start(uint64_t idx) const {
      assert(idx < range_num());
      return buffer_.data(idx * range_size_);
    }

    /** Gets the range end at the given index. */
    const void* get_range_end(uint64_t idx) const {
      assert(idx < range_num());
      return buffer_.data(idx * range_size_ + range_size_ / 2);
    }

    /** Return the number of ranges in this object. */
    uint64_t range_num() const {
      return buffer_.size() / range_size_;
    }
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
  Subarray(Subarray&& subarray) noexcept;

  /** Destructor. */
  ~Subarray() = default;

  /**
   * Copy-assign operator. This performs a deep copy (including memcpy
   * of underlying buffers).
   */
  Subarray& operator=(const Subarray& subarray);

  /** Move-assign operator. */
  Subarray& operator=(Subarray&& subarray) noexcept;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Adds a range along the dimension with the given index.
   *
   * @param dim_idx The index of the dimension to add the range to.
   * @param range The range to be added in [low. high] format.
   * @param check_expanded_domain If `true`, the subarray bounds will be
   *     checked against the expanded domain of the array. This is important
   *     in dense consolidation with space tiles not fully dividing the
   *     dimension domain.
   */
  Status add_range(
      uint32_t dim_idx, const void* range, bool check_expanded_domain = false);

  /**
   * Adds a range along the dimension with the given index, in the
   * form of (start, end).
   */
  Status add_range(uint32_t dim_idx, const void* start, const void* end);

  /**
   * Adds a range along the dimension with the given index.
   *
   * @tparam T The subarray domain type.
   * @param dim_idx The index of the dimension to add the range to.
   * @param range The range to add.
   * @param check_expanded_domain If `true`, the subarray bounds will be
   *     checked against the expanded domain of the array. This is important
   *     in dense consolidation with space tiles not fully dividing the
   *     dimension domain.
   * @return Status
   */
  template <class T>
  Status add_range(
      uint32_t dim_idx, const T* range, bool check_expanded_domain);

  /**
   * Adds a range along the dimension with the given index, in the
   * form of (start, end).
   */
  template <class T>
  Status add_range(uint32_t dim_idx, const T* start, const T* end);

  /** Returns the array the subarray is associated with. */
  const Array* array() const;

  /**
   * Returns the number of cells in the ND range with the input id.
   * If the domain is huge and the number of cells overflows, the
   * function returns UINT64_MAX.
   */
  template <class T>
  uint64_t cell_num(uint64_t range_idx) const;

  /** Clears the contents of the subarray. */
  void clear();

  /**
   * Computes the range offsets which are important for getting
   * an ND range index from a flat serialized index.
   */
  void compute_range_offsets();

  /**
   * Computes the tile overlap with all subarray ranges for
   * all fragments.
   */
  Status compute_tile_overlap();

  /**
   * Computes the estimated result size (calibrated using the maximum size)
   * for a given attribute and range id, for all fragments.
   *
   * @tparam T The domain type.
   * @param attr_name The name of the attribute to focus on.
   * @param range_idx The id of the subarray range to focus on.
   * @param var_size Whether the attribute is var-sized or not.
   * @param result_size The result size to be retrieved.
   * @return Status
   */
  template <class T>
  Status compute_est_result_size(
      const std::string& attr_name,
      uint64_t range_idx,
      bool var_size,
      ResultSize* result_size) const;

  /**
   * Returns a cropped version of the subarray, constrained in the
   * tile with the input coordinates. The new subarray will have
   * the input `layout`.
   */
  template <class T>
  Subarray crop_to_tile(const T* tile_coords, Layout layout) const;

  /** Returns the number of dimensions of the subarray. */
  uint32_t dim_num() const;

  /** Returns the domain the subarray is constructed from. */
  const void* domain() const;

  /** ``True`` if the subarray does not contain any ranges. */
  bool empty() const;

  /** Retrieves a range of a given dimension at a given range index. */
  Status get_range(
      uint32_t dim_idx, uint64_t range_idx, const void** range) const;

  /**
   * Retrieves a range of a given dimension at a given range index.
   * The range is in the form (start, end).
   */
  Status get_range(
      uint32_t dim_idx,
      uint64_t range_idx,
      const void** start,
      const void** end) const;

  /** Retrieves the number of ranges on the given dimension index. */
  Status get_range_num(uint32_t dim_idx, uint64_t* range_num) const;

  /** Returns `true` if at least one dimension has non-default ranges set. */
  bool is_set() const;

  /**
   * Returns ``true`` if the subarray is unary, which happens when it consists
   * of a single ND range **and** each 1D range is unary (i.e., consisting of
   * a single point in the 1D domain).
   */
  bool is_unary() const;

  /**
   * Returns ``true`` if the subarray range with the input id is unary
   * (i.e., consisting of a single point in the 1D domain).
   */
  bool is_unary(uint64_t range_idx) const;

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

  /*
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input fixed-sized attribute.
   */
  Status get_max_memory_size(const char* attr_name, uint64_t* size);

  /**
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input fixed-sized attribute.
   */
  Status get_max_memory_size(
      const char* attr_name, uint64_t* size_off, uint64_t* size_val);

  /** Retrieves the query type of the subarray's array. */
  Status get_query_type(QueryType* type) const;

  /**
   * Returns the range coordinates (for all dimensions) given a flattened
   * 1D range id. The range coordinates is a tuple with an index
   * per dimension that uniquely identify a multi-dimensional
   * subarray range.
   */
  std::vector<uint64_t> get_range_coords(uint64_t range_idx) const;

  /**
   * Returns a subarray consisting of the ranges specified by
   * the input.
   *
   * @tparam T The domain type.
   * @param start The subarray will be constructed from ranges in
   *     interval ``[start, end]`` in the flattened range order.
   * @param end The subarray will be constructed from ranges in
   *     interval ``[start, end]`` in the flattened range order.
   */
  template <class T>
  Subarray get_subarray(uint64_t start, uint64_t end) const;

  /** Sets the array layout. */
  void set_layout(Layout layout);

  /** Returns the subarray layout. */
  Layout layout() const;

  /** Returns the flattened 1D id of the range with the input coordinates. */
  uint64_t range_idx(const std::vector<uint64_t>& range_coords) const;

  /** The total number of multi-dimensional ranges in the subarray. */
  uint64_t range_num() const;

  /**
   * Returns the multi-dimensional range with the input id, based on the
   * order imposed on the the subarray ranges by the layout. If ``layout_``
   * is UNORDERED, then the range layout will be the same as the array's
   * cell order, since this will lead to more beneficial tile access
   * patterns upon a read query.
   */
  template <class T>
  std::vector<const T*> range(uint64_t range_idx) const;

  /**
   * Returns the multi-dimensional range with the input id, based on the
   * order imposed on the the subarray ranges by the layout. If ``layout_``
   * is UNORDERED, then the range layout will be the same as the array's
   * cell order, since this will lead to more beneficial tile access
   * patterns upon a read query.
   */
  std::vector<const void*> range(uint64_t range_idx) const;

  /**
   * Returns the `Ranges` for the given dimension index.
   * @note Intended for serialization only
   */
  const Ranges* ranges_for_dim(uint32_t dim_idx) const;

  /**
   * Directly sets the `Ranges` for the given dimension index, making a deep
   * copy of the given `Ranges` instance.
   *
   * @param dim_idx Index of dimension to set
   * @param ranges Ranges instance that will be copied and set
   * @return Status
   *
   * @note Intended for serialization only
   */
  Status set_ranges_for_dim(uint32_t dim_idx, const Ranges& ranges);

  /**
   * Returns the (unique) coordinates of all the tiles that the subarray
   * ranges intersect with.
   */
  const std::vector<std::vector<uint8_t>>& tile_coords() const;

  /**
   * Given the input tile coordinates, it returns a pointer to a tile
   * coords vector in `tile_coords_` (casted to `T`). This is typically
   * to avoid storing a tile coords vector in numerous cell slabs (and
   * instead store only a pointer to the tile coordinates vector
   * stored once in the subarray instance).
   *
   * `aux` should be of the same byte size as `tile_coords`. It is used
   * to avoid repeated allocations of the auxiliary vector need for
   * converting the `tile_coords` vector of type `T` to a vector of
   * type `uint8_t` before searching for `tile_coords` in `tile_coords_`.
   */
  template <class T>
  const T* tile_coords_ptr(
      const std::vector<T>& tile_coords, std::vector<uint8_t>* aux) const;

  /** Returns the tile overlap of the subarray. */
  const std::vector<std::vector<TileOverlap>>& tile_overlap() const;

  /** Returns the subarray domain type. */
  Datatype type() const;

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on the array tile order.
   *
   * @tparam T The subarray datatype.
   */
  template <class T>
  void compute_tile_coords();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array the subarray object is associated with. */
  const Array* array_;

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
  bool est_result_size_computed_;

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

  /**
   * The tile coordinates that the subarray overlaps with. Note that
   * the datatype must be casted to the datatype of the subarray upon use.
   */
  std::vector<std::vector<uint8_t>> tile_coords_;

  /** A map (tile coords) -> (vector element poistion in `tile_coords_`). */
  std::map<std::vector<uint8_t>, size_t> tile_coords_map_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * This function adds a range that covers the whole domain along
   * each dimension. When ``add_range()`` is called for the first
   * time along a dimension, the corresponding default range is
   * replaced.
   */
  void add_default_ranges();

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

  /** Computes the estimated result size for all attributes. */
  Status compute_est_result_size();

  /** Computes the estimated result size for all attributes. */
  template <class T>
  Status compute_est_result_size();

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on col-major tile order.
   *
   * @tparam T The subarray datatype.
   */
  template <class T>
  void compute_tile_coords_col();

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on row-major tile order.
   *
   * @tparam T The subarray datatype.
   */
  template <class T>
  void compute_tile_coords_row();

  /**
   * Computes the tile overlap with all subarray ranges for
   * all fragments.
   */
  template <class T>
  Status compute_tile_overlap();

  /** Returns a deep copy of this Subarray. */
  Subarray clone() const;

  /**
   * Compute the tile overlap between ``range`` and the non-empty domain
   * of the input fragment. Applicable only to dense fragments.
   *
   * @tparam T The domain data type.
   * @param range The range to compute the overlap with.
   * @param fid The id of the fragment to focus on.
   * @return The tile overlap.
   */
  template <class T>
  TileOverlap get_tile_overlap(
      const std::vector<const T*>& range, unsigned fid) const;

  /**
   * Swaps the contents (all field values) of this subarray with the
   * given subarray.
   */
  void swap(Subarray& subarray);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SUBARRAY_H
