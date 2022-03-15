/**
 * @file   range_multi_subset.cc
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

#include "tiledb/sm/subarray/range_multi_subset.h"

#include <iostream>

using namespace tiledb::common;

namespace tiledb::sm {

template <typename T>
tdb_shared_ptr<detail::RangeMultiSubsetImpl> create_range_subset_internals(
    const Range& superset, bool coalesce_ranges) {
  if (coalesce_ranges) {
    return make_shared<detail::TypedRangeMultiSubsetImpl<T, true>>(
        HERE(), superset);
  }
  return make_shared<detail::TypedRangeMultiSubsetImpl<T, false>>(
      HERE(), superset);
};

tdb_shared_ptr<detail::RangeMultiSubsetImpl> range_subset_internals(
    Datatype datatype, const Range& superset, bool coalesce_ranges) {
  switch (datatype) {
    case Datatype::INT8:
      return create_range_subset_internals<int8_t>(superset, coalesce_ranges);
    case Datatype::UINT8:
      return create_range_subset_internals<uint8_t>(superset, coalesce_ranges);
    case Datatype::INT16:
      return create_range_subset_internals<int16_t>(superset, coalesce_ranges);
    case Datatype::UINT16:
      return create_range_subset_internals<uint16_t>(superset, coalesce_ranges);
    case Datatype::INT32:
      return create_range_subset_internals<int32_t>(superset, coalesce_ranges);
    case Datatype::UINT32:
      return create_range_subset_internals<uint32_t>(superset, coalesce_ranges);
    case Datatype::INT64:
      return create_range_subset_internals<int64_t>(superset, coalesce_ranges);
    case Datatype::UINT64:
      return create_range_subset_internals<uint64_t>(superset, coalesce_ranges);
    case Datatype::FLOAT32:
      return create_range_subset_internals<float>(superset, coalesce_ranges);
    case Datatype::FLOAT64:
      return create_range_subset_internals<double>(superset, coalesce_ranges);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return create_range_subset_internals<int64_t>(superset, coalesce_ranges);
    case Datatype::STRING_ASCII:
      // Set STRING_ASCII to use implementation where the superset is the
      // full typeset.
      return make_shared<detail::TypedRangeMultisetImpl<std::string, false>>(
          HERE());
    default:
      LOG_ERROR("Unexpected dimension datatype " + datatype_str(datatype));
      return nullptr;
  }
}

RangeMultiSubset::RangeMultiSubset(
    Datatype datatype,
    const Range& superset,
    bool implicitly_initialize,
    bool coalesce_ranges)
    : impl_(range_subset_internals(datatype, superset, coalesce_ranges))
    , is_implicitly_initialized_(implicitly_initialize) {
  if (implicitly_initialize)
    ranges_.emplace_back(superset);
}

tuple<Status, Status> RangeMultiSubset::add_subset(
    Range& range, const bool read_range_oob_error) {
  // Set out-of-bounds status to Ok. Only warn if range is modified by
  // intersect.
  Status warn_oob_status = Status::Ok();
  // Check range is a valid range. Abort if it is not.
  auto error_status = impl_->check_is_valid_range(range);
  if (!error_status.ok())
    return {error_status, warn_oob_status};
  // Check the range is not out of bound. Abort if oob and oob is an error.
  if (read_range_oob_error) {
    error_status = impl_->check_is_valid_subset(range);
    if (!error_status.ok())
      return {error_status, warn_oob_status};
  } else {
    warn_oob_status = impl_->intersect(range);
  }
  // Add subset and return error and warning statuses.
  error_status = add_subset_unrestricted(range);
  return {error_status, warn_oob_status};
}

Status RangeMultiSubset::add_subset_unrestricted(const Range& range) {
  if (is_implicitly_initialized_) {
    ranges_.clear();
    is_implicitly_initialized_ = false;
  }
  return impl_->add_range(ranges_, range);
}

Status RangeMultiSubset::sort_ranges(ThreadPool* const compute_tp) {
  return impl_->sort_ranges(compute_tp, ranges_);
}

}  // namespace tiledb::sm
