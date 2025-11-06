/**
 * @file   subarray.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

#include <atomic>

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/tile_overlap.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/subarray/range_subset.h"
#include "tiledb/sm/subarray/relevant_fragments.h"
#include "tiledb/sm/subarray/subarray_tile_overlap.h"

#include <cmath>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::type {
class Range;
}

namespace tiledb::sm {

struct FieldDataSize {
  /**
   * The size of fixed-length field data in bytes.
   */
  size_t fixed_;

  /**
   * The size of variable-length field data in bytes.
   */
  size_t variable_;

  /**
   * The size of validity data in bytes.
   */
  size_t validity_;
};

class Array;
class ArraySchema;
class OpenedArray;
class DimensionLabel;
class EncryptionKey;
class FragIdx;
class FragmentMetadata;

enum class Layout : uint8_t;
enum class QueryType : uint8_t;

template <class T>
class DenseTileSubarray {
 public:
  /**
   * Stores information about a range along a single dimension. The
   * whole range resides in a single tile.
   */
  struct DenseTileRange {
    /** The start of the range in global coordinates. */
    T start_;
    /** The end of the range in global coordinates. */
    T end_;

    /** Constructor. */
    DenseTileRange(T start, T end)
        : start_(start)
        , end_(end) {
    }

    /** Equality operator. */
    bool operator==(const DenseTileRange& r) const {
      return (r.start_ == start_ && r.end_ == end_);
    }
  };

  /**
   * The type of the allocator. Required to make the type allocator-aware.
   *
   * uint8_t was arbitrarily chosen and does not matter; allocators can convert
   * between any types.
   */
  using allocator_type = tdb::pmr::polymorphic_allocator<uint8_t>;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  DenseTileSubarray() = delete;

  DenseTileSubarray(unsigned dim_num, const allocator_type& alloc = {})
      : original_range_idx_(alloc)
      , ranges_(dim_num, alloc) {
  }

  /** Default copy constructor. */
  DenseTileSubarray(const DenseTileSubarray&) = default;
  /** Default move constructor. */
  DenseTileSubarray(DenseTileSubarray&&) = default;

  /** Allocator-aware copy constructor. */
  DenseTileSubarray(const DenseTileSubarray& other, const allocator_type& alloc)
      : original_range_idx_(other.original_range_idx_, alloc)
      , ranges_(other.ranges_, alloc) {
  }

  /** Allocator-aware move constructor. */
  DenseTileSubarray(DenseTileSubarray&& other, const allocator_type& alloc)
      : original_range_idx_(std::move(other.original_range_idx_), alloc)
      , ranges_(std::move(other.ranges_), alloc) {
  }

  /** Default copy-assign operator. */
  DenseTileSubarray& operator=(const DenseTileSubarray&) = default;
  /** Default move-assign operator. */
  DenseTileSubarray& operator=(DenseTileSubarray&&) = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the orignal range indexes. */
  inline const tdb::pmr::vector<tdb::pmr::vector<uint64_t>>&
  original_range_idx() const {
    return original_range_idx_;
  }

  /** Returns the ranges. */
  inline const tdb::pmr::vector<tdb::pmr::vector<DenseTileRange>>& ranges()
      const {
    return ranges_;
  }

  /** Returns the orignal range indexes to be modified. */
  inline tdb::pmr::vector<tdb::pmr::vector<uint64_t>>&
  original_range_idx_unsafe() {
    return original_range_idx_;
  }

  /**
   * Adds a range along the dimension with the given index, without
   * performing any error checks.
   */
  void add_range_unsafe(uint32_t dim_idx, const Range& range) {
    ranges_[dim_idx].emplace_back(range.start_as<T>(), range.end_as<T>());
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Stores a vector of 1D ranges per dimension. */
  tdb::pmr::vector<tdb::pmr::vector<uint64_t>> original_range_idx_;

  /** Stores the ranges per dimension. */
  tdb::pmr::vector<tdb::pmr::vector<DenseTileRange>> ranges_;
};

/**
 * Interface to implement for a class that can store tile ranges computed by
 * this class.
 */
class ITileRange {
 public:
  /** Destructor. */
  virtual ~ITileRange() = default;

  /** Clears all tile ranges data. */
  virtual void clear_tile_ranges() = 0;

  /**
   * Add a tile range for a fragment.
   *
   * @param f Fragment index.
   * @param min Min tile index for the range.
   * @param max Max tile index for the range.
   */
  virtual void add_tile_range(unsigned f, uint64_t min, uint64_t max) = 0;

  /** Signals we are done adding tile ranges. */
  virtual void done_adding_tile_ranges() = 0;
};

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
  /*         PUBLIC DATA TYPES         */
  /* ********************************* */
  /**
   * Size type for the number of dimensions of an array and for dimension
   * indices.
   *
   * Note: This should be the same as `Domain::dimension_size_type`. We're
   * not including `domain.h`, otherwise we'd use that definition here.
   */
  using dimension_size_type = unsigned int;

  /**
   * Result size (in bytes) for an attribute/dimension used for
   * partitioning.
   */
  struct ResultSize {
    /** Size for fixed-sized attributes/dimensions or offsets of var-sized
     * attributes/dimensions.
     */
    double size_fixed_;

    /** Size of values for var-sized attributes/dimensions. */
    double size_var_;

    /** Size of validity values for nullable attributes. */
    double size_validity_;
  };

  /**
   * Maximum memory size (in bytes) for an attribute/dimension used for
   * partitioning.
   */
  struct MemorySize {
    /**
     * Maximum size of overlapping tiles fetched into memory for
     * fixed-sized attributes/dimensions or offsets of var-sized
     * attributes/dimensions.
     */
    uint64_t size_fixed_;

    /**
     * Maximum size of overlapping tiles fetched into memory for
     * var-sized attributes/dimensions.
     */
    uint64_t size_var_;

    /**
     * Maximum size of overlapping validity tiles fetched into memory for
     * nullable attributes.
     */
    uint64_t size_validity_;
  };

  /**
   * Wrapper for optional<tuple<std::string, RangeSetAndSuperset>> for
   * cleaner data access.
   */
  struct LabelRangeSubset {
   public:
    /**
     * Default constructor is not C.41.
     **/
    LabelRangeSubset() = delete;

    /**
     * Constructor
     *
     * @param ref Dimension label the ranges will be set on.
     * @param coalesce_ranges Set if ranges should be combined when adjacent.
     */
    LabelRangeSubset(const DimensionLabel& ref, bool coalesce_ranges = true);

    /**
     * Constructor
     *
     * @param name The name of the dimension label the ranges will be set on.
     * @param type The type of the label the ranges will be set on.
     * @param coalesce_ranges Set if ranges should be combined when adjacent.
     */
    LabelRangeSubset(
        const std::string& name, Datatype type, bool coalesce_ranges = true);

    /**
     * Constructor
     *
     * @param name The name of the dimension label the ranges will be set on.
     * @param type The type of the label the ranges will be set on.
     * @param ranges The range subset for the dimension label.
     * @param coalesce_ranges Set if ranges should be combined when adjacent.
     */
    LabelRangeSubset(
        const std::string& name,
        Datatype type,
        std::vector<Range> ranges,
        bool coalesce_ranges = true);

    inline std::span<const Range> get_ranges() const {
      return ranges_.ranges();
    }

    /** Name of the dimension label. */
    std::string name_;

    /** The ranges set on the dimension label. */
    RangeSetAndSuperset ranges_;
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
   * @param parent_stats The parent stats to inherit from.
   * @param logger The parent logger to clone and use for logging
   * @param coalesce_ranges When enabled, ranges will attempt to coalesce
   *     with existing ranges as they are added.
   */
  Subarray(
      const Array* array,
      stats::Stats* parent_stats,
      shared_ptr<Logger> logger,
      bool coalesce_ranges = true);

  /**
   * Constructor.
   *
   * @param array The array the subarray is associated with.
   * @param layout The layout of the values of the subarray (of the results
   *     if the subarray is used for reads, or of the values provided
   *     by the user for writes).
   * @param parent_stats The parent stats to inherit from.
   * @param logger The parent logger to clone and use for logging
   * @param coalesce_ranges When enabled, ranges will attempt to coalesce
   *     with existing ranges as they are added.
   */
  Subarray(
      const Array* array,
      Layout layout,
      stats::Stats* parent_stats,
      shared_ptr<Logger> logger,
      bool coalesce_ranges = true);

  /**
   * Constructor.
   *
   * @param opened_array The opened array the subarray is associated with.
   * @param layout The layout of the values of the subarray (of the results
   *     if the subarray is used for reads, or of the values provided
   *     by the user for writes).
   * @param parent_stats The parent stats to inherit from.
   * @param logger The parent logger to clone and use for logging
   * @param coalesce_ranges When enabled, ranges will attempt to coalesce
   *     with existing ranges as they are added.
   */
  Subarray(
      const shared_ptr<OpenedArray> opened_array,
      Layout layout,
      stats::Stats* parent_stats,
      shared_ptr<Logger> logger,
      bool coalesce_ranges = true);

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

  /** Sets config for query-level parameters only. */
  void set_config(const QueryType query_type, const Config& config);

  /**
   * Sets the subarray using a pointer to raw range data that stores one range
   * per dimension.
   *
   * This is only valid for arrays with homogenous dimension data types.
   *
   * @param subarray A pointer to the range data to use.
   */
  void set_subarray(const void* subarray);

  /**
   * Sets the subarray using a pointer to raw range data that stores one range
   * per dimension without performing validity checks.
   *
   * This is only valid for arrays with homogenous dimension data types. This
   * function should only be used for deserializing dense write queries.
   *
   * @param subarray A pointer to the range data to use.
   */
  void set_subarray_unsafe(const void* subarray);

  /**
   * Adds dimension ranges computed from label ranges on the dimension label.
   *
   * @param dim_idx The dimension to add ranges to.
   * @param is_point_ranges If ``true`` the data contains point ranges.
   *     Otherwise, it contains standard ranges.
   * @param start Pointer to the start of the range array.
   * @param count Number of total elements in the range array.
   */
  void add_index_ranges_from_label(
      const uint32_t dim_idx,
      const bool is_point_ranges,
      const void* start,
      const uint64_t count);

  /**
   * Adds a range along the dimension with the given index for the requested
   * dimension label.
   *
   * @param dim_label_ref Cache data about the dimension label definition.
   * @param range The range to add.
   * @param read_range_oob_error If ``true`` return error for input range
   *     larger than the dimension label domain, otherwise return a warning
   *     and truncate the range.
   */
  void add_label_range(
      const DimensionLabel& dim_label_ref,
      Range&& range,
      const bool read_range_oob_error = true);

  /**
   * Adds a range along the dimension with the given index for the requested
   * dimension label.
   *
   * @param label_name The name of the dimension label to add the range to.
   * @param start The range start.
   * @param end The range end.
   */
  void add_label_range(
      const std::string& label_name, const void* start, const void* end);

  /**
   * Adds a variable-sized range along the dimension with the given index for
   * the requested dimension label.
   *
   * @param label_name The name of the dimension label to add the range to.
   * @param start The range start.
   * @param start_size The size of the start data.
   * @param end The range end.
   * @param end_size The size of the end data.
   */
  void add_label_range_var(
      const std::string& label_name,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size);

  /** Adds a range along the dimension with the given index. */
  void add_range(
      uint32_t dim_idx, Range&& range, const bool read_range_oob_error = true);

  /**
   * Adds a range to the subarray on the input dimension by index.
   * The range components must be of the same type as the domain type of the
   * underlying array.
   */
  void add_range(unsigned dim_idx, const void* start, const void* end);

  /**
   * @brief Set point ranges on a fixed-sized dimension
   *
   * @param dim_idx Dimension index.
   * @param start Pointer to start of the array.
   * @param count Number of elements to add.
   * @param check_for_label If ``true``, verify no label ranges set on this
   *   dimension. This should check for labels unless being called by
   *   ``add_index_ranges_from_label`` to update label ranges with index values.
   */
  void add_point_ranges(
      unsigned dim_idx,
      const void* start,
      uint64_t count,
      bool check_for_label = true);

  /**
   * @brief Set point ranges on a variable-sized dimension
   *
   * @param dim_idx Dimension index.
   * @param start Pointer to start of the array.
   * @param start_size Size of the buffer in bytes.
   * @param start_offsets Pointer to the start of the offsets array.
   * @param start_offsets_size Number of offsets in the offsets array.
   * @param check_for_label If ``true``, verify no label ranges set on this
   *   dimension. This should check for labels unless being called by
   *   ``add_index_ranges_from_label`` to update label ranges with index values.
   */
  void add_point_ranges_var(
      unsigned dim_idx,
      const void* start,
      uint64_t start_size,
      const uint64_t* start_offsets,
      uint64_t start_offsets_size,
      bool check_for_label = true);

  /**
   * @brief Set ranges from an array of ranges (paired { begin,end } )
   *
   * @param dim_idx Dimension index.
   * @param start Pointer to start of the array.
   * @param count Number of total elemenst to add. Must contain two elements for
   *     each range.
   * @param check_for_label If ``true``, verify no label ranges set on this
   *   dimension. This should check for labels unless being called by
   *   ``add_index_ranges_from_label`` to update label ranges with index values.
   * @note The pairs list is logically { {begin1,end1}, {begin2,end2}, ...} but
   * because of typing considerations from the C api is simply presented as
   * a linear list of individual items, though they should be multiple of 2
   */
  void add_ranges_list(
      unsigned dim_idx,
      const void* start,
      uint64_t count,
      bool check_for_label = true);

  /**
   * Adds a variable-sized range to the (read/write) query on the input
   * dimension by index, in the form of (start, end).
   */
  void add_range_var(
      unsigned dim_idx,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size);

  /** Returns the orignal range indexes to be modified. */
  inline std::vector<std::vector<uint64_t>>& original_range_idx_unsafe() {
    return original_range_idx_;
  }

  /**
   * Adds a range along the dimension with the given index, without
   * performing any error checks.
   */
  void add_range_unsafe(uint32_t dim_idx, const Range& range);

  /**
   * Adds a range to the (read/write) query on the input dimension by name.
   * The range components must be of the same type as the domain type of the
   * underlying array.
   */
  void add_range_by_name(
      const std::string& dim_name, const void* start, const void* end);

  /**
   * Adds a variable-sized range to the (read/write) query on the input
   * dimension by name, in the form of (start, end).
   */
  void add_range_var_by_name(
      const std::string& dim_name,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size);

  /**
   * Retrieves reference to attribute ranges.
   *
   * @param attr_name Name of the attribute to get ragnes for.
   */
  std::span<const Range> get_attribute_ranges(
      const std::string& attr_name) const;

  /**
   * Get all attribute ranges.
   */
  inline const std::unordered_map<std::string, std::vector<Range>>&
  get_attribute_ranges() const {
    return attr_range_subset_;
  }

  /**
   * Returns the name of the dimension label at the dimension index.
   *
   * @param dim_index Index of the dimension to return the label name for.
   */
  const std::string& get_label_name(const uint32_t dim_index) const;

  /**
   * Retrieves a range from a dimension label name.
   *
   * @param label_name The name of the dimension label to retrieve the range
   *     from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   */
  void get_label_range(
      const std::string& label_name,
      uint64_t range_idx,
      const void** start,
      const void** end) const;

  /**
   * Retrieves the number of ranges of the subarray for the given dimension
   * label name.
   *
   * @param label_name The name of the dimension label to get the number of
   *     ranges on.
   * @param range_num The number of ranges set for the dimension label.
   */
  void get_label_range_num(
      const std::string& label_name, uint64_t* range_num) const;

  /**
   * Retrieves a range's sizes for a variable-length dimension label name
   *
   * @param label_name The name of dimension label to retrieve the range sizes
   *     from.
   * @param range_idx The id of the range to retrieve.
   * @param start_size The range start size in bytes.
   * @param end_size The range end size in bytes.
   */
  void get_label_range_var_size(
      const std::string& label_name,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Retrieves a range from a variable-length dimension label name in the form
   * (start, end).
   *
   * @param label_name The name of dimension label to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start.
   * @param end The range end.
   */
  void get_label_range_var(
      const std::string& label_name,
      uint64_t range_idx,
      void* start,
      void* end) const;

  /**
   * Retrieves the number of ranges of the subarray for the given dimension
   * name.
   */
  void get_range_num_from_name(
      const std::string& dim_name, uint64_t* range_num) const;

  /**
   * Retrieves a range from a dimension name.
   *
   * @param dim_name The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   */
  void get_range_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      const void** start,
      const void** end) const;

  /**
   * Retrieves a range's sizes for a variable-length dimension name
   *
   * @param dim_name The dimension name to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start_size range start size in bytes
   * @param end_size range end size in bytes
   */
  void get_range_var_size_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Retrieves a range from a variable-length dimension name in the form (start,
   * end).
   *
   * @param dim_name The dimension name to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   */
  void get_range_var_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      void* start,
      void* end) const;

  /** Returns the opened array the subarray is associated with. */
  const shared_ptr<OpenedArray> array() const;

  /**
   * Returns the number of cells in the subarray.
   *
   * This only returns the number of cells for dimension ranges, not label or
   * attribute ranges.
   */
  uint64_t cell_num() const;

  /**
   * Returns the number of cells in the input ND range.
   *
   * This only returns the number of cells for dimension ranges, not label or
   * attribute ranges.
   */
  uint64_t cell_num(uint64_t range_idx) const;

  /** Returns the number of cells in the input ND range.
   *
   * This only returns the number of cells for dimension ranges, not label or
   * attribute ranges.
   */
  uint64_t cell_num(const std::vector<uint64_t>& range_coords) const;

  /** Clears the contents of the subarray. */
  void clear();

  /** Clears the contents of the tile overlap. */
  void clear_tile_overlap();

  /**
   * Returns the size of the tile overlap data.
   *
   * @return Size of the tile overlap data.
   */
  uint64_t tile_overlap_byte_size() const;

  /**
   * Returns true if the subarray is unary and it coincides with
   * tile boundaries.
   */
  bool coincides_with_tiles() const;

  /**
   * Checks if the Subarray is OOB for the domain.
   * Throws if any range if found to be OOB.
   */
  void check_oob();

  /**
   * Computes the range offsets which are important for getting
   * an ND range index from a flat serialized index.
   */
  void compute_range_offsets();

  /**
   * Precomputes the tile overlap with all subarray ranges for
   * all fragments. The state is cached internally and accessible
   * through the `get_subarray_tile_overlap` API.
   *
   * This routine may not compute tile overlap for the entire range.
   * This only guarantees that:
   *   1. Tile overlap will be computed for at least one range.
   *   2. Tile overlap is computed starting from `start_range_idx`.
   *
   * The caller is responsible for checking the range indexes that
   * were computed through the `get_subarray_tile_overlap` API.
   *
   * @param start_range_idx The start range index.
   * @param end_range_idx The target end range index.
   * @param config The config object.
   * @param compute_tp The compute thread pool.
   * @param override_memory_constraint When true, this forces the
   *    routine to compute tile overlap for all ranges.
   */
  void precompute_tile_overlap(
      uint64_t start_range_idx,
      uint64_t end_range_idx,
      const Config* config,
      ThreadPool* compute_tp,
      bool override_memory_constraint = false);

  /**
   * Precomputes the tile overlap with all subarray ranges for all fragments.
   *
   * @param compute_tp The compute thread pool.
   * @param frag_tile_idx The current tile index, per fragment.
   * @param tile_ranges The resulting tile ranges.
   */
  void precompute_all_ranges_tile_overlap(
      ThreadPool* compute_tp,
      const std::vector<FragIdx>& frag_tile_idx,
      ITileRange* tile_ranges);

  /**
   * Computes the estimated result size (calibrated using the maximum size)
   * for a vector of given attributes/dimensions and range id, for all
   * fragments. The function focuses only on fragments relevant to the
   * subarray query.
   *
   * @param encryption_key The encryption key of the array.
   * @param array_schema The array schema.
   * @param all_dims_same_type Whether or not all dimensions have the
   *     same type.
   * @param all_dims_fixed Whether or not all dimensions are fixed-sized.
   * @param fragment_meta The fragment metadata of the array.
   * @param names The name vector of the attributes/dimensions to focus on.
   * @param var_sizes A vector indicating which attribute/dimension is
   *     var-sized.
   * @param nullable A vector indicating which attribute is nullable.
   * @param range_idx The id of the subarray range to focus on.
   * @param range_coords The coordinates of the subarray range to focus on.
   * @param result_sizes The result sizes to be retrieved for all given names.
   * @param frag_tiles The set of unique (fragment id, tile id) pairs across
   *   all ranges, which is update by this function in a thread-safe manner.
   */
  void compute_relevant_fragment_est_result_sizes(
      const ArraySchema& array_schema,
      bool all_dims_same_type,
      bool all_dims_fixed,
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_meta,
      const std::vector<std::string>& name,
      const std::vector<bool>& var_sizes,
      const std::vector<bool>& nullable,
      uint64_t range_idx,
      const std::vector<uint64_t>& range_coords,
      std::vector<ResultSize>* result_sizes,
      std::set<std::pair<unsigned, uint64_t>>* frag_tiles);

  /**
   * Returns a cropped version of the subarray, constrained in the
   * tile with the input coordinates. The new subarray will have
   * the input `layout`.
   */
  template <class T>
  Subarray crop_to_tile(const T* tile_coords, Layout layout) const;

  /**
   * Returns a cropped version of the subarray, constrained in the
   * tile with the input coordinates. The new subarray will have
   * the input `layout`.
   */
  template <class T>
  void crop_to_tile(DenseTileSubarray<T>& ret, const T* tile_coords) const;

  /**
   * Returns the number of cells in a specific tile for this subarray.
   */
  template <class T>
  uint64_t tile_cell_num(const T* tile_coords) const;

  /** Returns the number of dimensions of the subarray. */
  uint32_t dim_num() const;

  /** Returns the domain the subarray is constructed from. */
  NDRange domain() const;

  /** ``True`` if the dimension of the subarray does not contain any ranges. */
  bool empty(uint32_t dim_idx) const;

  /**
   * `True`` if the subarray does not contain ranges on any of the dimensions.
   *
   * This function does not check for ranges on labels or attributes.
   */
  bool empty() const;

  /**
   * Retrieves a range of a given dimension at a given range index.
   *
   * @note Note that the retrieved range may be invalid if
   *     Subarray::add_range() is called after this function. In that case,
   *     make sure to make a copy in the caller function.
   */
  void get_range(
      uint32_t dim_idx, uint64_t range_idx, const Range** range) const;

  /**
   * Retrieves a range of a given dimension at a given range index.
   * The range is in the form (start, end).
   *
   * @note Note that the retrieved range may be invalid if
   *     Subarray::add_range() is called after this function. In that case,
   *     make sure to make a copy in the caller function.
   */
  void get_range(
      uint32_t dim_idx,
      uint64_t range_idx,
      const void** start,
      const void** end) const;

  /**
   * Retrieves a range's start and end size for a given variable-length
   * dimensions at a given range index.
   *
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start_size range start size in bytes
   * @param end_size range end size in bytes
   */
  void get_range_var_size(
      uint32_t dim_idx,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Retrieves a range from a variable-length dimension index in the form
   * (start, end).
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   */
  void get_range_var(
      unsigned dim_idx, uint64_t range_idx, void* start, void* end) const;

  /** Retrieves the number of ranges on the given dimension index. */
  void get_range_num(uint32_t dim_idx, uint64_t* range_num) const;

  /**
   * ``True`` if the specified dimension is set by default.
   *
   * @param dim_index
   * @return returns true if the specified dimension is set to default subarray
   */
  bool is_default(uint32_t dim_index) const;

  /** Returns `true` if at least one dimension has non-default ranges set. */
  bool is_set() const;

  /** Returns the number of dimensions with non-default (set) ranges. */
  int32_t count_set_ranges() const;

  /** Returns `true` if the input dimension has non-default range set. */
  bool is_set(unsigned dim_idx) const;

  /**
   * Returns ``true`` if the subarray is unary, which happens when it consists
   * of a single ND range **and** each 1D range is unary (i.e., consisting of
   * a single point in the 1D domain).
   */
  bool is_unary() const;

  /** Returns whether the estimated result size has been computed or not. */
  bool est_result_size_computed();

  /**
   * The estimated result size in bytes for a field.
   *
   * This function handles all fields. The field may be a dimension or
   * attribute, fixed-length or variable, nullable or not. If a particular
   * size is not relevant to the field type, then it's returned as zero.
   */
  FieldDataSize get_est_result_size(
      std::string_view field_name,
      const Config* config,
      ThreadPool* compute_tp);

  /**
   * The maximum memory in bytes required for a field.
   *
   * This function handles all fields. The field may be a dimension or
   * attribute, fixed-length or variable, nullable or not. If a particular
   * size is not relevant to the field type, then it's returned as zero.
   */
  FieldDataSize get_max_memory_size(
      const char* name, const Config* config, ThreadPool* compute_tp);

  /**
   * Returns the range coordinates (for all dimensions) given a flattened
   * 1D range id. The range coordinates is a tuple with an index
   * per dimension that uniquely identify a multi-dimensional
   * subarray range.
   */
  std::vector<uint64_t> get_range_coords(uint64_t range_idx) const;

  /**
   * Returns a subarray consisting of the dimension ranges specified by
   * the input.
   *
   * @param start The subarray will be constructed from ranges in
   *     interval ``[start, end]`` in the flattened range order.
   * @param end The subarray will be constructed from ranges in
   *     interval ``[start, end]`` in the flattened range order.
   */
  Subarray get_subarray(uint64_t start, uint64_t end) const;

  /**
   * Returns ``true`` if the any dimension in the subarray have label ranges
   * set.
   */
  bool has_label_ranges() const;

  /**
   * Returns ``true`` if the dimension index has label ranges set.
   *
   * @param dim_idx The dimension index to check for ranges.
   */
  bool has_label_ranges(const uint32_t dim_index) const;

  /**
   * Returns the number of dimensions that have label ranges set
   */
  int label_ranges_num() const;

  /**
   * Set default indicator for dimension subarray. Used by serialization only
   *
   * @param dim_index
   * @param is_default
   */
  void set_is_default(uint32_t dim_index, bool is_default);

  /** Sets the subarray layout. */
  void set_layout(Layout layout);

  /** Sets coalesce_ranges flag, intended for use by CAPI, to alloc matching
   * default coalesc-ranges=true semantics of internal class constructor, but
   * giving capi clients ability to turn off if desired.
   */
  void set_coalesce_ranges(bool coalesce_ranges = true);

  /**
   * Flattens the subarray ranges in a byte vector. Errors out
   * if the subarray is not unary.
   *
   * @note Only used in test helper `subarray_equiv`; keep it that way.
   */
  void to_byte_vec(std::vector<uint8_t>* byte_vec) const;

  /** Returns the subarray layout. */
  Layout layout() const;

  /** Returns the flattened 1D id of the range with the input coordinates. */
  uint64_t range_idx(const std::vector<uint64_t>& range_coords) const;

  /** Returns the orignal range indexes. */
  inline const std::vector<std::vector<uint64_t>>& original_range_idx() const {
    return original_range_idx_;
  }

  /**
   * The total number of multi-dimensional ranges in the subarray.
   *
   * This only returns the number of multi-dimension ranges on the dimension
   * space. It does not include ranges set on labels or attributes.
   */
  uint64_t range_num() const;

  /**
   * Returns the multi-dimensional range with the input id, based on the
   * order imposed on the the subarray ranges by the layout. If ``layout_``
   * is UNORDERED, then the range layout will be the same as the array's
   * cell order, since this will lead to more beneficial tile access
   * patterns upon a read query.
   */
  NDRange ndrange(uint64_t range_idx) const;

  /** Returns the NDRange corresponding to the input range coordinates. */
  NDRange ndrange(const std::vector<uint64_t>& range_coords) const;

  /**
   * Returns the `Range` vector for the given dimension index.
   * @note Intended for serialization only
   */
  inline std::span<const Range> ranges_for_dim(uint32_t dim_idx) const {
    return range_subset_[dim_idx].ranges();
  }

  /**
   * Adds ranges for an attribute.
   *
   * This method is designed to copy label ranges from a parent subarray to
   * attribute ranges in a dimension label array. The ranges will only be
   * accessed by the dimension label readers, and it is assumed all checks on
   * validity of the ranges has already been ran when adding the label ranges to
   * the parent subarray.
   *
   * @param attr_name Name of the attribute to add the ranges for.
   * @param ranges Ranges to add.
   */
  void set_attribute_ranges(
      const std::string& attr_name, std::span<const Range> ranges);

  /**
   * Returns the `Range` vector for the given dimension label.
   *
   * @param label_name Name of the label to return ranges for.
   * @returns Vector of ranges on the requested dimension label.
   */
  std::span<const Range> ranges_for_label(const std::string& label_name) const;

  /**
   * Directly sets the `Range` vector for the given dimension index, making
   * a deep copy.
   *
   * @param dim_idx Index of dimension to set
   * @param ranges `Range` vector that will be copied and set
   *
   * @note Intended for serialization only
   */
  void set_ranges_for_dim(uint32_t dim_idx, std::span<const Range> ranges);

  /**
   * Directly sets the dimension label ranges for the given dimension index,
   * making a deep copy.
   *
   * @param dim_idx Index of dimension to set
   * @param name Name of the dimension label to set
   * @param ranges `Range` vector that will be copied and set
   *
   * @note Intended for serialization only
   */
  void set_label_ranges_for_dim(
      const uint32_t dim_idx,
      const std::string& name,
      std::span<const Range> ranges);

  /**
   * Splits the subarray along the splitting dimension and value into
   * two new subarrays `r1` and `r2`.
   */
  void split(
      unsigned splitting_dim,
      const ByteVecValue& splitting_value,
      Subarray* r1,
      Subarray* r2) const;

  /**
   * Splits the subarray along the splitting range, dimension and value
   * into two new subarrays `r1` and `r2`.
   */
  void split(
      uint64_t splitting_range,
      unsigned splitting_dim,
      const ByteVecValue& splitting_value,
      Subarray* r1,
      Subarray* r2) const;

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

  /**
   * Returns the internal `TileOverlap` instance. The caller is responsible
   * for ensuring that the input indexes are valid.
   *
   * @param fragment_idx The fragment index.
   * @param range_idx The range index.
   */
  inline const TileOverlap* tile_overlap(
      uint64_t fragment_idx, uint64_t range_idx) const {
    return tile_overlap_.at(fragment_idx, range_idx);
  }

  /**
   * Returns the precomputed tile overlap. This may not contain
   * tile overlap information for each range in this subarray. The
   * caller is responsible for inspecting the returned object to
   * determine the ranges it has information for.
   */
  const SubarrayTileOverlap* subarray_tile_overlap() const;

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on the array tile order.
   *
   * @tparam T The subarray datatype.
   */
  template <class T>
  void compute_tile_coords();

  /**
   * Computes the estimated result sizes for the input attribute/dimension
   * names, assuming the tile overlap is already computed. This function focuses
   * only on fragments relevant to the subarray and the input range ids.
   * The result retrieved contains the estimated result sizes per
   * dimension/attribute per range.
   *
   * The function also retrieves a `mem_sizes` vector of vectors, which
   * holds values for each range and each attribute. Each value represents
   * the **unique** bytes that the correpsonding range contributes to
   * the maximum memory size for all ranges (i.e., based on whether
   * it overlaps a unique tile versus all previous ranges in the vector).
   */
  void compute_relevant_fragment_est_result_sizes(
      const std::vector<std::string>& names,
      uint64_t range_start,
      uint64_t range_end,
      std::vector<std::vector<ResultSize>>* result_sizes,
      std::vector<std::vector<MemorySize>>* mem_sizes,
      ThreadPool* compute_tp);

  /**
   * Used by serialization to set the estimated result size
   *
   * @param est_result_size map to set
   * @param max_mem_size map to set
   */
  void set_est_result_size(
      std::unordered_map<std::string, ResultSize>& est_result_size,
      std::unordered_map<std::string, MemorySize>& max_mem_size);

  /**
   * Used by serialization to get the map of result sizes
   * @return
   */
  std::unordered_map<std::string, ResultSize> get_est_result_size_map(
      const Config* config, ThreadPool* compute_tp);

  /**
   * Used by serialization to get the map of max mem sizes
   * @return
   */
  std::unordered_map<std::string, MemorySize> get_max_mem_size_map(
      const Config* config, ThreadPool* compute_tp);

  /**
   * Return relevant fragments as computed
   */
  const RelevantFragments& relevant_fragments() const;

  /**
   * Return relevant fragments as computed
   */
  RelevantFragments& relevant_fragments();

  /**
   * For flattened ("total order") start/end range indexes,
   * return the starting and ending ND-range coordinates that
   * contain the minimum space to contain all flattened ranges.
   *
   * @param range_idx_start flattened range starting index, inclusive.
   * @param range_idx_end flattened range ending index, inclusive.
   * @param start_coords mutated to contain the nd-range start coordinates.
   * @param end_coords mutated to contain the nd-range end coordinates.
   */
  void get_expanded_coordinates(
      uint64_t range_idx_start,
      uint64_t range_idx_end,
      std::vector<uint64_t>* start_coords,
      std::vector<uint64_t>* end_coords) const;

  /** Returns `stats_`. */
  const stats::Stats& stats() const;

  /**
   * Populate the owned stats instance with data.
   * To be removed when the class will get a C41 constructor.
   *
   * @param data Data to populate the stats with.
   */
  void set_stats(const stats::StatsData& data);

  /** Stores a vector of 1D ranges per dimension. */
  std::vector<std::vector<uint64_t>> original_range_idx_;

  /** Returns if dimension ranges are sorted. */
  bool ranges_sorted() {
    return ranges_sorted_;
  }

  /** Sort and merge ranges per dimension. */
  void sort_and_merge_ranges(ThreadPool* const compute_tp);

  /** Returns if all ranges for this subarray are non overlapping. */
  bool non_overlapping_ranges(ThreadPool* const compute_tp);

  /** Returns if ranges will be coalesced as they are added. */
  inline bool coalesce_ranges() const {
    return coalesce_ranges_;
  }

  /**
   * Initialize the label ranges vector to nullopt for every
   * dimension
   *
   * @param dim_num Total number of dimensions of the schema
   */
  void add_default_label_ranges(dimension_size_type dim_num);

  /**
   * Reset ranges to default if possible before a read operation for sparse
   * reads. We have a lot of optimizations in the sparse readers when no ranges
   * are specified. Python will set ranges that are equal to the non empty
   * domain, which will negate those optimizations. When the non empty domain is
   * computed for the array, it is low performance cost to see if the ranges set
   * are equal to the non empty domain. If they are, we can reset them to be
   * default.
   */
  void reset_default_ranges();

  /** Loads the R-Trees of all relevant fragments in parallel. */
  void load_relevant_fragment_rtrees(ThreadPool* compute_tp) const;

 private:
  /* ********************************* */
  /*        PRIVATE DATA TYPES         */
  /* ********************************* */

  /**
   * An opaque context to be used between successive calls
   * to `compute_relevant_fragment_tile_overlap`.
   */
  struct ComputeRelevantTileOverlapCtx {
    ComputeRelevantTileOverlapCtx()
        : range_idx_offset_(0)
        , range_len_(0) {
    }

    /**
     * The current range index offset. This points to the ending
     * index from the last invocation of
     * `compute_relevant_fragment_tile_overlap`.
     */
    uint64_t range_idx_offset_;

    /** The number of ranges. */
    uint64_t range_len_;
  };

  /**
   * A hash function capable of hashing std::vector<uint8_t> for use by
   * the tile_coords_map_ unordered_map for caching coords indices.
   */
  struct CoordsHasher {
    /**
     * Compute a hash value of the provided key.
     *
     * @param key The uint8_t vector to hash.
     * @return std::size_t The hash value.
     */
    std::size_t operator()(const std::vector<uint8_t>& key) const {
      // The awkward cast here is because std::string_view doesn't accept
      // a uint8_t* in its constructor. Since compilers won't let us cast
      // directly from unsigned to signed, we have to static cast to void*
      // first.
      auto data =
          static_cast<const char*>(static_cast<const void*>(key.data()));
      std::string_view str_key(data, key.size());
      return std::hash<std::string_view>()(str_key);
    }
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The class stats. */
  stats::Stats* stats_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /** The array the subarray object is associated with. */
  shared_ptr<OpenedArray> array_;

  /** Stores the estimated result size for each array attribute/dimension. */
  std::unordered_map<std::string, ResultSize> est_result_size_;

  /** Stores the maximum memory size for each array attribute/dimension. */
  std::unordered_map<std::string, MemorySize> max_mem_size_;

  /**
   * The layout of the subarray (i.e., of the results
   * if the subarray is used for reads, or of the values provided
   * by the user for writes).
   */
  Layout layout_;

  /**
   * The array cell order. We explicitly store it as it may be used
   * numerous times and we'd better have it readily available.
   */
  Layout cell_order_;

  /**
   * Stores a vector of RangeSetAndSuperset objects, one per dimension, for
   * handling operations on ranges.
   */
  std::vector<RangeSetAndSuperset> range_subset_;

  /**
   * Stores LabelRangeSubset objects for handling ranges on dimension labels.
   *
   * Users cannot set label ranges on dimensions that already have normal ranges
   * set. Once the label query on a dimension is finished, the query will add
   * the dimension ranges that correspond to the same regions as the label
   * ranges.
   *
   * Valid states for each dimension:
   *  - No label ranges.
   *  - Label ranges with no dimension ranges.
   *  - Label ranges and dimension ranges that correspond to the same regions.
   */
  std::vector<optional<LabelRangeSubset>> label_range_subset_;

  /**
   * Stores ranges for attributes.
   */
  std::unordered_map<std::string, std::vector<Range>> attr_range_subset_;

  /**
   * Flag storing if each dimension is a default value or not.
   *
   * TODO: Remove this variable and store `is_default` directly with NDRange.
   */
  std::vector<bool> is_default_;

  /** Important for computed an ND range index from a flat serialized index. */
  std::vector<uint64_t> range_offsets_;

  /**
   * ``True`` if the estimated result size for all attributes/dimensions has
   * been computed.
   */
  bool est_result_size_computed_;

  /**
   * The array fragment ids whose non-empty domain intersects at
   * least one subarray range.
   */
  RelevantFragments relevant_fragments_;

  /**
   * The precomputed tile overlap state. Is not guaranteed to be
   * computed for all ranges in this subarray.
   */
  SubarrayTileOverlap tile_overlap_;

  /**
   * ``True`` if ranges should attempt to be coalesced as they are added.
   */
  bool coalesce_ranges_;

  /**
   * The tile coordinates that the subarray overlaps with. Note that
   * the datatype must be casted to the datatype of the subarray upon use.
   */
  std::vector<std::vector<uint8_t>> tile_coords_;

  /** A map (tile coords) -> (vector element position in `tile_coords_`). */
  std::unordered_map<std::vector<uint8_t>, size_t, CoordsHasher>
      tile_coords_map_;

  /** State of specific Config item needed from multiple locations. */
  bool err_on_range_oob_ = true;

  /** Indicate if dimension ranges are sorted. */
  bool ranges_sorted_;

  /** Mutext to protect sorting ranges. */
  std::mutex ranges_sort_mtx_;

  /** Merge overlapping ranges setting. */
  bool merge_overlapping_ranges_;

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

  /** Computes the estimated result size for all attributes/dimensions. */
  void compute_est_result_size(const Config* config, ThreadPool* compute_tp);

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

  /** Returns a deep copy of this Subarray. */
  Subarray clone() const;

  /**
   * Compute the tile overlap between ``range`` and the non-empty domain
   * of the input fragment. Applicable only to dense fragments.
   *
   * @param range_idx The id of the range to compute the overlap with.
   * @param fid The id of the fragment to focus on.
   * @return The tile overlap.
   */
  TileOverlap compute_tile_overlap(uint64_t range_idx, unsigned fid) const;

  /**
   * Compute the tile overlap between ``range`` and the non-empty domain
   * of the input fragment. Applicable only to dense fragments.
   *
   * @tparam T The domain data type.
   * @param range_idx The id of the range to compute the overlap with.
   * @param fid The id of the fragment to focus on.
   * @return The tile overlap.
   */
  template <class T>
  TileOverlap compute_tile_overlap(uint64_t range_idx, unsigned fid) const;

  /**
   * Swaps the contents (all field values) of this subarray with the
   * given subarray.
   */
  void swap(Subarray& subarray);

  /**
   * Computes the tile overlap for each range and relevant fragment.
   *
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param tile_overlap Mutated to store the computed tile overlap.
   * @param fn_ctx An opaque context object to be used between successive
   * invocations.
   */
  void compute_relevant_fragment_tile_overlap(
      ThreadPool* compute_tp,
      SubarrayTileOverlap* tile_overlap,
      ComputeRelevantTileOverlapCtx* fn_ctx);

  /**
   * Load the var-sized tile sizes for the input names and from the
   * relevant fragments.
   */
  void load_relevant_fragment_tile_var_sizes(
      const std::vector<std::string>& names, ThreadPool* compute_tp) const;

  /**
   * Determine if ranges for a dimension are non overlapping.
   *
   * @param dim_idx dimension index.
   * @return true if the ranges are non overlapping, false otherwise.
   */
  bool non_overlapping_ranges_for_dim(uint64_t dim_idx);

  /**
   * Returns a cropped version of the subarray, constrained in the
   * tile with the input coordinates. The new subarray will have
   * the input `layout`.
   */
  template <class T, class SubarrayT>
  void crop_to_tile_impl(const T* tile_coords, SubarrayT& ret) const;
};

}  // namespace tiledb::sm

#endif  // TILEDB_SUBARRAY_H
