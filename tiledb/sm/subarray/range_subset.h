/**
 * @file   range_subset.h
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
 * This file defines the class RangeMultiSubset.
 */

#ifndef TILEDB_RANGE_SUBSET_H
#define TILEDB_RANGE_SUBSET_H

#include "tiledb/common/heap_memory.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/misc/types.h"

#include <cmath>
#include <optional>
#include <type_traits>
#include <vector>

using namespace tiledb::common;

namespace tiledb::sm {

namespace detail {

/** Default add strategy: simple add. */
template <typename T, bool Coalesce, typename Enable = T>
struct AddStrategy {
  static Status add_range(std::vector<Range>& ranges, const Range& new_range) {
    ranges.emplace_back(new_range);
    return Status::Ok();
  };
};

/** Specialization for coalescing integer-type ranges. */
template <typename T>
struct AddStrategy<
    T,
    true,
    typename std::enable_if<std::is_integral<T>::value, T>::type> {
  static Status add_range(std::vector<Range>& ranges, const Range& new_range) {
    if (ranges.empty()) {
      ranges.emplace_back(new_range);
      return Status::Ok();
    }

    // If the start index of `range` immediately follows the end of the
    // last range on `ranges`, they are contiguous and will be coalesced.
    Range& last_range = ranges.back();
    const bool contiguous_after =
        *static_cast<const T*>(last_range.end()) !=
            std::numeric_limits<T>::max() &&
        *static_cast<const T*>(last_range.end()) + 1 ==
            *static_cast<const T*>(new_range.start());

    // Coalesce `range` with `last_range` if they are contiguous.
    if (contiguous_after) {
      last_range.set_end(new_range.end());
    } else {
      ranges.emplace_back(new_range);
    }
    return Status::Ok();
  };
};

/**
 * Sort algorithm for ranges.
 *
 * Default behavior: sorting is not enable.
 */
template <typename T, typename Enable = T>
struct SortStrategy {
  static Status sort(ThreadPool* const, std::vector<Range>&);
};

/**
 * Sort algorithm for ranges.
 *
 * Sorting for numeric types.
 */
template <typename T>
struct SortStrategy<
    T,
    typename std::enable_if<std::is_arithmetic<T>::value, T>::type> {
  static Status sort(ThreadPool* const compute_tp, std::vector<Range>& ranges) {
    parallel_sort(
        compute_tp,
        ranges.begin(),
        ranges.end(),
        [&](const Range& a, const Range& b) {
          const T* a_data = static_cast<const T*>(a.start());
          const T* b_data = static_cast<const T*>(b.start());
          return a_data[0] < b_data[0] ||
                 (a_data[0] == b_data[0] && a_data[1] < b_data[1]);
        });
    return Status::Ok();
  };
};

/**
 * Sort algorithm for ranges.
 *
 * Sort for ASCII strings.
 */
template <>
struct SortStrategy<std::string, std::string> {
  static Status sort(ThreadPool* const compute_tp, std::vector<Range>& ranges) {
    parallel_sort(
        compute_tp,
        ranges.begin(),
        ranges.end(),
        [&](const Range& a, const Range& b) {
          return a.start_str() < b.start_str() ||
                 (a.start_str() == b.start_str() && a.end_str() < b.end_str());
        });
    return Status::Ok();
  };
};

class RangeMultiSubsetImpl {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Destructor. */
  virtual ~RangeMultiSubsetImpl() = default;

  /**
   * Adds a range to the range manager without performing any checkes. If a
   * default strategy is set, then first update the range strategy.
   *
   * @param ranges The current ranges in the subarray (remove after
   refactor).
   * @param range The range to add.
   */
  virtual Status add_range(
      std::vector<Range>& ranges, const Range& range) const = 0;

  /**
   * Checks the range is a valid subset of the superset.
   *
   * @param range The range to verify is a subset.
   * @return Status that returns error if range is not a valid subset.
   */
  virtual Status check_is_valid_subset(const Range& range) const = 0;

  /**
   * Updates a range to be a subset of another range.
   *
   * This method will return an ok status if the Range is not mutated and an
   * error status if the intersection changes the bounds of the range.
   *
   * @param intersector The Range to take an intersetction with.
   * @param range The Range to replace with the intersection.
   * @return Status that returns an error if range is mutated.
   */
  virtual Status intersect(Range& range) const = 0;

  /**
   * Sorts the ranges in the range manager.
   *
   * @param compute_tp The compute thread pool.
   */
  virtual Status sort_ranges(
      ThreadPool* const compute_tp, std::vector<Range>& ranges) const = 0;
};

template <typename T, bool CoalesceAdds>
class TypedRangeMultiSubsetImpl : public RangeMultiSubsetImpl {
 public:
  TypedRangeMultiSubsetImpl() = delete;
  TypedRangeMultiSubsetImpl(const Range& superset)
      : superset_(superset){};

  Status add_range(
      std::vector<Range>& ranges, const Range& new_range) const override {
    return AddStrategy<T, CoalesceAdds>::add_range(ranges, new_range);
  };

  Status check_is_valid_subset(const Range& range) const override {
    RETURN_NOT_OK(RangeOperations<T>::check_is_valid_range(range));
    RETURN_NOT_OK(superset_.check_is_subset(range));
    return Status::Ok();
  };

  Status intersect(Range& range) const override {
    return superset_.intersect(range);
  };

  Status sort_ranges(
      ThreadPool* const compute_tp, std::vector<Range>& ranges) const override {
    return SortStrategy<T>::sort(compute_tp, ranges);
  };

 private:
  RangeSuperset<T> superset_;
};

/**
 * Implementation for the RangeMultiSubset when the superset is the full
 * typeset. This skips subset checks.
 */
template <typename T, bool CoalesceAdds>
class TypedRangeMultisetImpl : public RangeMultiSubsetImpl {
 public:
  Status add_range(
      std::vector<Range>& ranges, const Range& new_range) const override {
    return AddStrategy<T, CoalesceAdds>::add_range(ranges, new_range);
  };

  Status check_is_valid_subset(const Range& range) const override {
    return RangeOperations<T>::check_is_valid_range(range);
  };

  Status intersect(Range&) const override {
    return Status::Ok();
  };

  Status sort_ranges(
      ThreadPool* const compute_tp, std::vector<Range>& ranges) const override {
    return SortStrategy<T>::sort(compute_tp, ranges);
  };
};

}  // namespace detail

/**
 * A RangeMultiSubset object is a collection of possibly overlapping or
 * duplicate Ranges that are assumed to be subsets of a given superset with a
 * defined TileDB datatype.
 *
 * If constructed with the ``implicitly_initialize`` flag set to ``true``, the
 * superset will be added to the Ranges in the set until any additional ranages
 * are added.
 *
 */
class RangeMultiSubset {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  RangeMultiSubset() = default;

  /** General constructor
   *
   * @param datatype The TileDB datatype of of the ranges.
   * @param implicitly_initialize If ``true``, set the ranges to contain the
   * full superset until a new range is explicitly added.
   * @param coalesce_ranges If ``true``, when adding a new range, attempt to
   * combine with the first left-adjacent range found.
   **/
  RangeMultiSubset(
      Datatype datatype,
      const Range& superset,
      bool implicitly_initialize,
      bool coalesce_ranges);

  /** Destructor. */
  ~RangeMultiSubset() = default;

  /**
   * Access specified element.
   *
   * @param range_index Index of the range to access.
   */
  inline const Range& operator[](const uint64_t range_index) const {
    return ranges_[range_index];
  };

  /**
   * Adds a range that is subset.
   *
   * Checks the range is a valid range, and that it is infact a subset.
   *
   * @param range The range to add.
   */
  Status add_subset(const Range& range);

  /**
   * Adds a range that is a subset without performing any checkes.
   *
   * If the ranges are currently implicitly initialized, then they will be
   * cleared before the new range is added.
   *
   * @param range The range to add.
   */
  Status add_subset_unrestricted(const Range& range);

  /** Returns a const reference to the stored ranges. */
  inline const std::vector<Range>& ranges() const {
    return ranges_;
  };

  /**
   * Replaces the range with its intersection with the superset.
   *
   * This method will return an ok status if the Range is not mutated and an
   * error status if the intersection changes the bounds of the range.
   *
   * @param intersector The Range to take an intersetction with.
   * @param range The Range to replace with the intersection.
   * @return Status that returns error if range is mutated.
   */
  inline Status intersect_with_superset(Range& range) const {
    return impl_->intersect(range);
  };

  /**
   * Returns ``true`` if the current range is implicitly set to the full subset.
   **/
  inline bool is_implicitly_initialized() const {
    return is_implicitly_initialized_;
  };

  /** Returns ``true`` if the range subset is the empty set. */
  inline bool is_empty() const {
    return ranges_.empty();
  };

  /**
   * Returns ``false`` if the subset contains a range other than the default
   * range.
   **/
  inline bool is_explicitly_initialized() const {
    return !is_implicitly_initialized_ && !ranges_.empty();
  };

  /**
   * Returns ``true`` if there is exactly one range with one element in the
   * subset.
   */
  inline bool has_single_element() const {
    return (ranges_.size() == 1) && ranges_[0].unary();
  };

  /** Returns the number of distinct ranges stored in the range manager. */
  inline uint64_t num_ranges() const {
    return ranges_.size();
  };

  /**
   * Sorts the stored ranges.
   *
   * @param compute_tp The compute thread pool.
   */
  Status sort_ranges(ThreadPool* const compute_tp);

 private:
  /** Internal implementation. */
  tdb_shared_ptr<detail::RangeMultiSubsetImpl> impl_ = nullptr;

  /**
   * If ``true``, the range contains the full domain for the dimension (the
   * default value for a subarray before any other values is set. Otherwise,
   * some values has been explicitly set to the range.
   */
  bool is_implicitly_initialized_ = true;

  /** Stored ranges. */
  std::vector<Range> ranges_{};
};

}  // namespace tiledb::sm

#endif
