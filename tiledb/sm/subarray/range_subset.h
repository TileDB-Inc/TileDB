/**
 * @file   range_subset.h
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
 * This file defines the class RangeSetAndSuperset.
 */

#ifndef TILEDB_RANGE_SUBSET_H
#define TILEDB_RANGE_SUBSET_H

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/type/range/range.h"

#include <optional>
#include <string>
#include <type_traits>
#include <vector>

using namespace tiledb::common;
using namespace tiledb::type;

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
        last_range.end_as<T>() != std::numeric_limits<T>::max() &&
        last_range.end_as<T>() + 1 == new_range.start_as<T>();

    // Coalesce `range` with `last_range` if they are contiguous.
    if (contiguous_after) {
      last_range.set_end_fixed(new_range.end_fixed());
    } else {
      ranges.emplace_back(new_range);
    }
    return Status::Ok();
  };
};

/**
 * Sort algorithm for ranges.
 *
 * Default behavior: sorting is not enabled.
 */
template <typename T, typename Enable = T>
struct SortStrategy {
  static void sort(ThreadPool* const, std::vector<Range>&);
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
  static void sort(ThreadPool* const compute_tp, std::vector<Range>& ranges) {
    parallel_sort(
        compute_tp,
        ranges.begin(),
        ranges.end(),
        [&](const Range& a, const Range& b) {
          const T* a_data = static_cast<const T*>(a.start_fixed());
          const T* b_data = static_cast<const T*>(b.start_fixed());
          return a_data[0] < b_data[0] ||
                 (a_data[0] == b_data[0] && a_data[1] < b_data[1]);
        });
  };
};

/**
 * Sort algorithm for ranges.
 *
 * Sort for ASCII strings.
 */
template <>
struct SortStrategy<std::string, std::string> {
  static void sort(ThreadPool* const compute_tp, std::vector<Range>& ranges) {
    parallel_sort(
        compute_tp,
        ranges.begin(),
        ranges.end(),
        [&](const Range& a, const Range& b) {
          return a.start_str() < b.start_str() ||
                 (a.start_str() == b.start_str() && a.end_str() < b.end_str());
        });
  };
};

/** Default merge behavior: merging is not enabled. */
template <typename T, typename Enable = T>
struct MergeStrategy {
  static void merge_sorted_ranges(std::vector<Range>&);
};

/** Merge algorithm for numeric-type ranges. */
template <typename T>
struct MergeStrategy<
    T,
    typename std::enable_if<std::is_arithmetic<T>::value, T>::type> {
  static void merge_sorted_ranges(std::vector<Range>& ranges) {
    auto head = ranges.begin();
    size_t merged_cells = 0;

    // Merge
    for (auto tail = head + 1; tail != ranges.end(); tail++) {
      // For integer ranges, we can merge two ranges if the start of a range is
      // the next integer after the end of the other. For this reason, we use
      // end + 1 for integer values. For floating point ranges, this is not the
      // case and we simply use the end.
      auto head_end = std::is_integral<T>::value ? head->end_as<T>() + 1 :
                                                   head->end_as<T>();
      const bool can_merge =
          head->end_as<T>() != std::numeric_limits<T>::max() &&
          tail->start_as<T>() <= head_end;

      if (can_merge) {
        // Only update the end if the merging end is greater.
        if (tail->end_as<T>() > head->end_as<T>()) {
          head->set_end_fixed(tail->end_fixed());
        }
        merged_cells++;
      } else {
        head++;
        std::swap(*head, *tail);
      }
    }

    // Resize
    ranges.resize(ranges.size() - merged_cells);
  };
};

/** Merge algorithm for string-ASCII-type ranges. */
template <>
struct MergeStrategy<std::string, std::string> {
  static void merge_sorted_ranges(std::vector<Range>& ranges) {
    auto head = ranges.begin();
    size_t merged_cells = 0;

    // Merge
    for (auto tail = head + 1; tail != ranges.end(); tail++) {
      const bool can_merge = tail->start_str() <= head->end_str();

      if (can_merge) {
        auto start_str = head->start_str();
        std::string start{start_str.data(), start_str.size()};
        auto end_str = tail->end_str();
        std::string end{end_str.data(), end_str.size()};
        head->set_range_var(start.data(), start.size(), end.data(), end.size());
        merged_cells++;
      } else {
        head++;
        std::swap(*head, *tail);
      }
    }

    // Resize
    ranges.resize(ranges.size() - merged_cells);
  };
};

class RangeSetAndSupersetImpl {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Destructor. */
  virtual ~RangeSetAndSupersetImpl() = default;

  /**
   * Adds a range to the range manager without performing any checks. If a
   * default strategy is set, then first update the range strategy.
   *
   * @param ranges The current ranges in the subarray (remove after
   refactor).
   * @param new_range The range to add.
   */
  virtual Status add_range(
      std::vector<Range>& ranges, const Range& range) const = 0;

  /**
   * Performs correctness checks for a valid range.
   *
   * @param range The range to check.
   **/
  virtual void check_range_is_valid(const Range& range) const = 0;

  /**
   * Checks a range is a subset of the superset of this ``RangeSetAndSuperset``.
   *
   * @param range The range to check.
   * @return Status of the subset check.
   **/
  virtual Status check_range_is_subset(const Range& range) const = 0;

  /**
   * Crops a range to the superset of this ``RangeSetAndSuperset``.
   *
   * If the range is cropped, a string is returned with a warning for the
   * logger.
   *
   * @param range The range to crop
   * @return An optional string with a warning message if the range is cropped.
   **/
  virtual optional<std::string> crop_range_with_warning(Range& range) const = 0;

  /**
   * Sorts the ranges in the range manager.
   *
   * @param compute_tp The compute thread pool.
   */
  virtual void sort_ranges(
      ThreadPool* const compute_tp, std::vector<Range>& ranges) const = 0;

  /**
   * Merges sorted ranges in the range manager.
   *
   * @param ranges The sorted ranges to be merged.
   */
  virtual void merge_ranges(std::vector<Range>& ranges) const = 0;
};

template <typename T, bool CoalesceAdds>
class TypedRangeSetAndSupersetImpl : public RangeSetAndSupersetImpl {
 public:
  TypedRangeSetAndSupersetImpl(const Range& superset)
      : superset_(superset){};

  Status add_range(
      std::vector<Range>& ranges, const Range& new_range) const override {
    return AddStrategy<T, CoalesceAdds>::add_range(ranges, new_range);
  }

  void check_range_is_valid(const Range& range) const override {
    type::check_range_is_valid<T>(range);
  }

  Status check_range_is_subset(const Range& range) const override {
    return type::check_range_is_subset<T>(superset_, range);
  }

  optional<std::string> crop_range_with_warning(Range& range) const override {
    auto domain = (const T*)superset_.data();
    auto r = (const T*)range.data();
    if (r[0] < domain[0] || r[1] > domain[1]) {
      std::string warn_message{
          "Range [" + std::to_string(r[0]) + ", " + std::to_string(r[1]) +
          "] is out of domain bounds [" + std::to_string(domain[0]) + ", " +
          std::to_string(domain[1]) + "]"};
      type::crop_range<T>(superset_, range);
      warn_message += "; Adjusting range to [" + std::to_string(r[0]) + ", " +
                      std::to_string(r[1]) + "]";
      return warn_message;
    }
    return nullopt;
  }

  void sort_ranges(
      ThreadPool* const compute_tp, std::vector<Range>& ranges) const override {
    SortStrategy<T>::sort(compute_tp, ranges);
  }

  void merge_ranges(std::vector<Range>& ranges) const override {
    MergeStrategy<T>::merge_sorted_ranges(ranges);
  }

 private:
  /** Maximum possible range. */
  Range superset_{};
};

template <typename T, bool CoalesceAdds>
class TypedRangeSetAndFullsetImpl : public RangeSetAndSupersetImpl {
 public:
  TypedRangeSetAndFullsetImpl() = default;

  Status add_range(
      std::vector<Range>& ranges, const Range& new_range) const override {
    return AddStrategy<T, CoalesceAdds>::add_range(ranges, new_range);
  }

  void check_range_is_valid(const Range& range) const override {
    type::check_range_is_valid<T>(range);
  }

  Status check_range_is_subset(const Range&) const override {
    // No check needed.
    return Status::Ok();
  }

  optional<std::string> crop_range_with_warning(Range&) const override {
    // No check needed.
    return nullopt;
  }

  void sort_ranges(
      ThreadPool* const compute_tp, std::vector<Range>& ranges) const override {
    SortStrategy<T>::sort(compute_tp, ranges);
  }

  void merge_ranges(std::vector<Range>& ranges) const override {
    MergeStrategy<T>::merge_sorted_ranges(ranges);
  }
};

/**
 * Implementation for the RangeSetAndSuperset for string ranges. Assumes
 * superset is always the full typeset.
 */
template <bool CoalesceAdds>
class TypedRangeSetAndFullsetImpl<std::string, CoalesceAdds>
    : public RangeSetAndSupersetImpl {
 public:
  TypedRangeSetAndFullsetImpl() = default;

  Status add_range(
      std::vector<Range>& ranges, const Range& new_range) const override {
    return AddStrategy<std::string, CoalesceAdds>::add_range(ranges, new_range);
  }

  void check_range_is_valid(const Range&) const override {
    // No checks for string ranges.
  }

  Status check_range_is_subset(const Range&) const override {
    // Range is always necessarily a subset of the full typeset.
    return Status::Ok();
  }

  optional<std::string> crop_range_with_warning(Range&) const override {
    // No-op. Superset is always full typeset.
    return nullopt;
  }

  void sort_ranges(
      ThreadPool* const compute_tp, std::vector<Range>& ranges) const override {
    SortStrategy<std::string>::sort(compute_tp, ranges);
  }

  void merge_ranges(std::vector<Range>& ranges) const override {
    MergeStrategy<std::string>::merge_sorted_ranges(ranges);
  }
};

}  // namespace detail

/**
 * A RangeSetAndSuperset object is a collection of possibly overlapping or
 * duplicate Ranges that are assumed to be subsets of a given superset with a
 * defined TileDB datatype.
 *
 * If constructed with the ``implicitly_initialize`` flag set to ``true``, the
 * superset will be added to the Ranges in the set until any additional ranges
 * are added.
 *
 * Current state of the RangeSetAndSuperset:
 *
 *  * The only way to add Ranges is with an "unrestricted" method that does not
 *  check the range is in fact a subset of the superset.
 *
 * Planned updates:
 *
 *  * When adding a new range, this will verify that Range is a subset of the
 *  RangeSetAndSuperset by using ``is_subset`` and ``intersection`` methods.
 *
 */
class RangeSetAndSuperset {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  RangeSetAndSuperset() = default;

  /** General constructor
   *
   * @param datatype The TileDB datatype of of the ranges.
   * @param implicitly_initialize If ``true``, set the ranges to contain the
   * full superset until a new range is explicitly added.
   * @param coalesce_ranges If ``true``, when adding a new range, attempt to
   * combine with the first left-adjacent range found.
   **/
  RangeSetAndSuperset(
      Datatype datatype,
      const Range& superset,
      bool implicitly_initialize,
      bool coalesce_ranges);

  /** Destructor. */
  ~RangeSetAndSuperset() = default;

  /**
   * Access specified element.
   *
   * @param range_index Index of the range to access.
   */
  inline const Range& operator[](const uint64_t range_index) const {
    return ranges_[range_index];
  };

  /**
   * Adds a range to the range set after checking validity.
   *
   * If the ranges are currently implicitly initialized, then they will be
   * cleared before the new range is added.
   *
   * @param range The range to add.
   * @param read_range_oob_err Flag for behavior when a range is out of bounds.
   * If ``true``, an error is returned. If ``false``, the range is cropped and a
   * warning message is returned.
   * @return {error_status, warning_message} Returns a status with any errors
   * and a warning message.
   **/
  tuple<Status, optional<std::string>> add_range(
      Range& range, const bool read_range_oob_error = true);

  /**
   * Adds a range to the range manager without performing any checkes.
   *
   * If the ranges are currently implicitly initialized, then they will be
   * cleared before the new range is added.
   *
   * @param range The range to add.
   */
  Status add_range_unrestricted(const Range& range);

  /**
   * Removes all ranges.
   *
   * Note: This will make it so it is no longer implicititly set.
   */
  inline void clear() {
    ranges_.clear();
    is_implicitly_initialized_ = false;
  }

  /** Returns a const reference to the stored ranges. */
  inline const std::vector<Range>& ranges() const {
    return ranges_;
  };

  /**
   * Returns ``true`` if the current range is implicitly set to the full
   * subset.
   **/
  inline bool is_implicitly_initialized() const {
    return is_implicitly_initialized_;
  };

  /** Returns ``true`` if the range subset is the empty set. */
  inline bool is_empty() const {
    return ranges_.empty();
  };

  /**
   * Checks if Subarray ranges are all valid. Throws is any range is found to be
   * invalid.
   */
  void check_oob();

  /**
   * Returns ``true`` if the range subset was set after instantiation and
   * ``false`` if the range subset was implicitly set at instantiation or is
   * empty.
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
   * @param merge If true, the ranges will be merged after sorting.
   */
  void sort_and_merge_ranges(ThreadPool* const compute_tp, bool merge = false);

 private:
  /** Pointer to typed implementation details. */
  shared_ptr<detail::RangeSetAndSupersetImpl> impl_ = nullptr;

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
