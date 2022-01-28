/**
 * @file   range_manager.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines the class RangeManager.
 */

#ifndef TILEDB_RANGE_MANAGER_H
#define TILEDB_RANGE_MANAGER_H

#include "tiledb/common/heap_memory.h"
#include "tiledb/sm/misc/resource_pool.h"
#include "tiledb/sm/misc/types.h"

#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class RangeManager {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  RangeManager();

  /**
   * Constructor for a specific dimension of an array.
   *
   * This will set the dimension to use the default range manager strategy.
   */
  RangeManager(
      const tiledb::Array* const array,
      uint32_t dim_index,
      bool coalesce_ranges = true,
      bool read_range_oob_error = true);

  /** Copy constructor. */
  RangeManager(const RangeManager&);

  /** Move constructor. */
  RangeManager(RangeManager&&);

  /** Destructor. */
  ~RangeManager() = default;

  /** Copy-assign operator.*/
  RangeManager& operator=(const RangeManager&);

  /** Move-assign operator. */
  RangeManager& operator=(RangeManager&&);

  /**
   * Adds a range to the range manager. If a default range manager strategy is,
   * then first update the range strategy.
   *
   * @param ranges The current ranges in the subarray (remove after refactor).
   * @param new_range The range to add.
   */
  Status add_range(std::vector<std::vector<Range>>& ranges, Range&& new_range);

  /**
   * Adds a range to the range manager without performing any checkes. If a
   * default strategy is set, then first update the range strategy.
   *
   * @param ranges The current ranges in the subarray (remove after refactor).
   * @param new_range The range to add.
   */
  Satus add_range_unsafe(
      std::vector<std::vector<Range>>& ranges, Range&& new_range);

  /**
   * Returns the number of cells in the input ND range.
   *
   * @param ranges The current ranges in the subarray (remove after refactor).
   */
  uint64_t cell_num(std::vector<std::vector<Range>>& ranges) const;

  /** Reset the range manager to a default state. */
  void clear();

  /**
   * Returns the start and stop values for all ranges stored for this
   * dimension.
   *
   * @param ranges The current ranges in the subarray (remove after refactor).
   */
  std::tuple<Status, void**, void**> get_range(
      std::vector<std::vector<Range>>& ranges) const;

  /**
   * Returns the range at the requested index.
   *
   * @param ranges The current ranges in the subarray (remove after refactor).
   * @param range_index Index for the range using local indexing.
   */
  std::tuple<Status, Range**> get_range(
      std::vector<std::vector<Range>>& ranges, uint64_t range_index) const;

  /**
   * Returns all ranges managed this manager.
   *
   * @param ranges The current ranges in the subarray (remove after refactor).
   */
  std::vector<Range>& get_ranges(std::vector<std::vector<Range>>& ranges) const;

  /** Returns ``true`` if the current range is the default range.*/
  bool is_default() const;

  /**
   * Returns ``true`` of the ranges are unary (i.e. consists of a single point.
   */
  bool is_unary(const std::vector<std::vector<Range>>& ranges) const;

  /**
   * Number of ranges contained in the range manager.
   *
   * @param ranges The current ranges in the subarray (remove after refactor).
   */
  uint64_t range_num(const std::vector<std::vector<Range>>& ranges) const;

  /**
   * Clear current ranges and add the provided ranges.
   *
   * @param
   * @param
   */
  Status set_ranges(
      std::vector<std::vector<Range>>& ranges,
      const std::vector<Range>& new_ranges);

  Status sort_ranges(
      std::vector<std::vector<Range>>& range, ThreadPool* const compute_tp);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Dimension index of the dimension this is managing the range for. */
  uint32_t dim_index_;

  /**
   * If ``true``, when adding ranges, coalesce if possible.
   *
   * Used when constructing a non-default range management strategy.
   */
  bool coalesce_ranges_;

  /**
   * If ``true``, throw an error when a range is out of bounds. Otherwise,
   * truncate the bounds when a range is out of bounds.
   *
   * Used when constructing a non-default range management strategy.
   */
  bool error_on_oob_;  // Used for setting non-default strategy.

  /** Strategy for managing (adding, getting, etc.) ranges. */
  tdb_shared_ptr<RangeStrategyBase> strategy_;
};

#endif
