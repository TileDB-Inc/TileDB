/*
 * @file uniform_mapping.h
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
 * This file defines the mapping from the label to index for a dimension label
 * that is a virtual uniform or evenly-spaced dimension.
 */

#ifndef TILEDB_UNIFORM_MAPPING_H
#define TILEDB_UNIFORM_MAPPING_H

#include <cmath>
#include <stdexcept>
#include <string>
#include "tiledb/common/common.h"
#include "tiledb/sm/dimension_label/dimension_label_mapping.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/types.h"

namespace tiledb::sm {

/**
 * Uniform mapping for the uniform dimension label.
 *
 * An uniform dimension label maps an uniform grid to an integer index. This is
 * a virtual dimesnion label; no data is stored in disk.
 */
template <
    typename TLABEL,
    typename TINDEX,
    typename ENABLE_LABEL = TLABEL,
    typename ENABLE_INDEX = TINDEX>
class UniformMapping
    : public VirtualLabelMapping<TLABEL, TINDEX, ENABLE_LABEL, ENABLE_INDEX> {
 public:
  /** Default constructor is not C.41 compliant. */
  UniformMapping() = delete;

  /** Constructor
   *
   * @param x_min Minimum value of the label dimension.
   * @param x_max Minimum value of the label dimension.
   * @param n_min Minimum value of the index dimension.
   * @param n_max Maximum value of the index dimension.
   */
  UniformMapping(
      const TLABEL& x_min,
      const TLABEL& x_max,
      const TINDEX& n_min,
      const TINDEX& n_max);

  /**
   * Returns a pointer to a UniformMapping for specificed domains.
   *
   * @param label_domain Domain of the label. Pair of [lower, upper] bounds.
   * @param index_domain Domain of the index. Pair of [lower, upper] bounds.
   * @returns Pointer to the UniformMapping.
   */
  static shared_ptr<UniformMapping> create(
      const Range& label_domain, const Range& index_domain);

  /**
   * Returns a pointer to a UniformMapping for specificed domains, skipping
   * validity checks.
   *
   * @param label_domain Domain of the label. Pair of [lower, upper] bounds.
   * @param index_domain Domain of the index. Pair of [lower, upper] bounds.
   * @returns Pointer to the UniformMapping.
   */
  static shared_ptr<UniformMapping> create_unrestricted(
      const Range& label_domain, const Range& index_domain);

 protected:
  /**
   * Returns the index value matching the requested label.
   *
   * If the label is between indices, it will round up. This is used for the
   * lower bound of a region.
   *
   * A logic error is thrown if the label is larger than the maximum label
   * value.
   *
   * @param label Input label coordinate value.
   * @returns The cooresponding coordinate value of the original dimension.
   */
  TINDEX index_lower_bound(const TLABEL label) const override;

  /**
   * Returns the index value matching the requested label.
   *
   * If the label is between indices, it will round down. This is used for the
   * upper bound of a region.
   *
   * A logic error is thrown if the label is smaller than the minimum label
   * value.
   *
   * @param label Input label coordinate value.
   * @returns The cooresponding coordinate value of the original dimension.
   */
  TINDEX index_upper_bound(const TLABEL label) const override;
};

/** Uniform mapping for dimension label with floating-point labels. */
template <typename T>
class UniformMapping<
    T,
    uint64_t,
    typename std::enable_if<std::is_floating_point<T>::value, T>::type,
    uint64_t> : public VirtualLabelMapping<T, uint64_t, T, uint64_t> {
 public:
  UniformMapping() = delete;

  UniformMapping(
      const T x_min, const T x_max, const uint64_t n_min, const uint64_t n_max)
      : n_min_(n_min)
      , n_max_(n_max)
      , x_min_(x_min)
      , x_max_(x_max) {
    // Check x_min_ and x_max_ are valid.
    if (x_min_ > x_max_)
      throw std::invalid_argument(
          "Label domain cannot have minimum value " + std::to_string(x_min_) +
          " greater than maximum value " + std::to_string(x_max_));
    if (std::isnan(x_min_) || std::isnan(x_max_))
      throw std::invalid_argument("Label domain cannot contain a NaN value");
    if (std::isinf(x_min_) || std::isinf(x_max_))
      throw std::invalid_argument(
          "Label domain cannot contain an infinite value");
    // Check n_min_ and n_max_ are valid.
    if (n_min_ > n_max_)
      throw std::invalid_argument(
          "Index domain cannot have minimum value " + std::to_string(n_min_) +
          " greater than maximum value " + std::to_string(n_max_));
    // Set dx_ and check if interval is only a single point.
    if (n_min_ == n_max_) {
      if (x_min_ != x_max_)
        throw std::invalid_argument(
            "If the index domain contains only a single point, then the label "
            "domain must only contain a single point.");
      dx_ = 1.0;
    } else {
      if (x_min_ == x_max_)
        throw std::invalid_argument(
            "If the label contains only a single point, then the index domain "
            "must contain only a single point.");
      dx_ = (x_max_ - x_min_) / static_cast<T>(n_max_ - n_min_);
    }
  };

  static shared_ptr<UniformMapping> create(
      const Range& label_domain, const Range& index_domain) {
    // Get index domain data and verify it is non-empty.
    if (index_domain.empty())
      throw std::invalid_argument("Index domain cannot be empty");
    auto index_data = static_cast<const uint64_t*>(index_domain.data());
    // Get the label domain data nad verify it is non-empty.
    if (label_domain.empty())
      throw std::invalid_argument("Label domain cannot be empty");
    auto label_data = static_cast<const T*>(label_domain.data());
    // Return ponter.
    return make_shared<UniformMapping>(
        HERE(), label_data[0], label_data[1], index_data[0], index_data[1]);
  };

  static shared_ptr<UniformMapping> create_unrestricted(
      const Range& label_domain, const Range& index_domain) {
    auto index_data = static_cast<const uint64_t*>(index_domain.data());
    auto label_data = static_cast<const T*>(label_domain.data());
    return make_shared<UniformMapping>(
        HERE(),
        label_data[0],
        label_data[1],
        index_data[0],
        index_data[1],
        label_data[1] - label_data[0],
        static_cast<T>(index_data[1] - index_data[0]));
  }

 protected:
  uint64_t index_lower_bound(const T label) const override {
    if (label > x_max_)
      throw std::out_of_range(
          "Lower bound value " + std::to_string(label) +
          " is greater than the maximum label value " + std::to_string(x_max_));
    if (label <= x_min_)
      return n_min_;
    return static_cast<uint64_t>(std::ceil((label - x_min_) / dx_)) + n_min_;
  };

  uint64_t index_upper_bound(const T label) const override {
    if (label < x_min_)
      throw std::out_of_range(
          "Upper bound value " + std::to_string(label) +
          " is less than the minimum label value " + std::to_string(x_min_));
    if (label >= x_max_)
      return n_max_;
    return static_cast<uint64_t>(std::floor((label - x_min_) / dx_)) + n_min_;
  };

 private:
  /**
   * Private constructor that skips checks.
   *
   * @param x_min Minimum value of the label dimension.
   * @param x_max Minimum value of the label dimension.
   * @param n_min Minimum value of the index dimension.
   * @param n_max Maximum value of the index dimension.
   * @param dx Spacing of a single interval, (x_max - x_min)/(n_max - n_min).
   */
  UniformMapping(
      const T x_min,
      const T x_max,
      const uint64_t n_min,
      const uint64_t n_max,
      const T dx)
      : n_min_(n_min)
      , n_max_(n_max)
      , x_min_(x_min)
      , x_max_(x_max)
      , dx_(dx){};

  /** Minimum index value for the index domain. */
  uint64_t n_min_{};

  /** Maximum index value for the index domain. */
  uint64_t n_max_{};

  /** Minimum label value for the label domain. */
  T x_min_{};

  /** Maximum label value for the label domain. */
  T x_max_{};

  /** The width of a single interval, (x_max_ - x_min_) / (n_max_ - n_min_). */
  T dx_{};
};

/** Uniform mapping for dimension label with integer-type labels. */
template <typename T>
class UniformMapping<
    T,
    uint64_t,
    typename std::enable_if<std::is_integral<T>::value, T>::type,
    uint64_t> : public VirtualLabelMapping<T, uint64_t, T, uint64_t> {
 public:
  UniformMapping() = delete;

  UniformMapping(
      const T x_min, const T x_max, const uint64_t n_min, const uint64_t n_max)
      : n_min_(n_min)
      , n_max_(n_max)
      , x_min_(x_min)
      , x_max_(x_max) {
    // Check x_min_ and x_max_ are valid.
    if (x_min_ > x_max_)
      throw std::invalid_argument(
          "Label domain cannot have minimum value " + std::to_string(x_min_) +
          " greater than maximum value " + std::to_string(x_max_));
    // Check n_min_ and n_max_ are valid.
    if (n_min_ > n_max_)
      throw std::invalid_argument(
          "Index domain cannot have minimum value " + std::to_string(n_min_) +
          " greater than maximum value " + std::to_string(n_max_));
    // Check for over-flow error and set nintervals_
    if (n_min_ == n_max_) {
      if (x_min_ != x_max_)
        throw std::invalid_argument(
            "If the index domain contains only a single point, then the label "
            "domain must only contain a single point.");
      dx_ = 1;
    } else {
      if (x_min_ == x_max_)
        throw std::invalid_argument(
            "If the label contains only a single point, then the index domain "
            "must contain only a single point.");
      const auto ninterval = n_max_ - n_min_;
      const uint64_t width = static_cast<uint64_t>(x_max_ - x_min_);
      if (width % ninterval != 0)
        throw std::invalid_argument(
            "The uniform dimension label must align on valid points.");
      dx_ = static_cast<T>(width / ninterval);
    }
  };

  static shared_ptr<UniformMapping> create(
      const Range& label_domain, const Range& index_domain) {
    // Get index domain data and verify it is non-empty.
    if (index_domain.empty())
      throw std::invalid_argument("Index domain cannot be empty");
    auto index_data = static_cast<const uint64_t*>(index_domain.data());
    // Get the label domain data nad verify it is non-empty.
    if (label_domain.empty())
      throw std::invalid_argument("Label domain cannot be empty");
    auto label_data = static_cast<const T*>(label_domain.data());
    // Return ponter.
    return make_shared<UniformMapping>(
        HERE(), label_data[0], label_data[1], index_data[0], index_data[1]);
  };

  static shared_ptr<UniformMapping> create_unrestricted(
      const Range& label_domain, const Range& index_domain) {
    auto index_data = static_cast<const uint64_t*>(index_domain.data());
    auto label_data = static_cast<const T*>(label_domain.data());
    return make_shared<UniformMapping>(
        HERE(),
        label_data[0],
        label_data[1],
        index_data[0],
        index_data[1],
        static_cast<uint64_t>(label_data[1] - label_data[0]) /
            (index_data[1] - index_data[0]));
  }

 protected:
  uint64_t index_lower_bound(const T label) const override {
    if (label > x_max_)
      throw std::out_of_range(
          "Lower bound value " + std::to_string(label) +
          " is greater than the maximum label value " + std::to_string(x_max_));
    if (label <= x_min_)
      return n_min_;
    return ((label - x_min_) % dx_ == 0) ?
               (static_cast<uint64_t>((label - x_min_) / dx_) + n_min_) :
               (static_cast<uint64_t>((label - x_min_) / dx_) + n_min_ + 1);
  };

  uint64_t index_upper_bound(const T label) const override {
    if (label < x_min_)
      throw std::out_of_range(
          "Upper bound value " + std::to_string(label) +
          " is less than the minimum label value " + std::to_string(x_min_));
    if (label >= x_max_)
      return n_max_;
    return static_cast<uint64_t>((label - x_min_) / dx_) + n_min_;
  };

 private:
  /**
   * Private constructor that skips checks.
   *
   * @param x_min Minimum value of the label dimension.
   * @param x_max Minimum value of the label dimension.
   * @param n_min Minimum value of the index dimension.
   * @param n_max Maximum value of the index dimension.
   * @param dx Spacing of a single interval, (x_max - x_min)/(n_max - n_min).
   */
  UniformMapping(
      const T x_min,
      const T x_max,
      const uint64_t n_min,
      const uint64_t n_max,
      const T dx)
      : n_min_(n_min)
      , n_max_(n_max)
      , x_min_(x_min)
      , x_max_(x_max)
      , dx_(dx){};

  /** Minimum index value for the index domain. */
  uint64_t n_min_{};

  /** Maximum index value for the index domain. */
  uint64_t n_max_{};

  /** Minimum label value for the label domain. */
  T x_min_{};

  /** Maximum label value for the label domain. */
  T x_max_{};

  /** The width of a single interval, (x_max_ - x_min_) / (n_min_ - n_max_). */
  T dx_{};
};

/**
 * Factory function for creating a dimension label mapping using the virtual
 * uniform mapping.
 *
 * @param label_datatype The TileDB datatype of the label.
 * @param label_domain The domain of the label. Pair of [lower, upper] bounds.
 * @param index_datatype The TileDB datatype of the index.
 * @param index_domain The domain of the index. Pair of [lower, upper] bounds.
 * @returns A pointer to a new ``UniformMapping`` over the requested domains.
 **/
shared_ptr<DimensionLabelMapping> create_uniform_mapping(
    Datatype label_datatype,
    const Range& label_domain,
    Datatype index_datatype,
    const Range& index_domain);

}  // namespace tiledb::sm

#endif
