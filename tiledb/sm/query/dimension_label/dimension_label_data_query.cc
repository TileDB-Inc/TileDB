/**
 * @file dimension_label_query.cc
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
 * Classes for querying (reading/writing) a dimension label using the index
 * dimension for setting the subarray.
 */

#include "tiledb/sm/query/dimension_label/dimension_label_data_query.h"
#include "tiledb/common/common.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/query/dimension_label/index_data.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/subarray/subarray.h"

#include <algorithm>
#include <functional>

using namespace tiledb::common;

namespace tiledb::sm {

DimensionLabelReadDataQuery::DimensionLabelReadDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    const Subarray& parent_subarray,
    const QueryBuffer& label_buffer,
    const uint32_t dim_idx)
    : Query(storage_manager, dimension_label->indexed_array(), nullopt) {
  // Set the layout (ordered, 1D).
  throw_if_not_ok(set_layout(Layout::ROW_MAJOR));

  // Set the subarray if it has index ranges added to it.
  if (!parent_subarray.is_default(dim_idx) &&
      !parent_subarray.has_label_ranges(dim_idx)) {
    Subarray subarray{*this->subarray()};
    throw_if_not_ok(subarray.set_ranges_for_dim(
        0, parent_subarray.ranges_for_dim(dim_idx)));
    throw_if_not_ok(set_subarray(subarray));
  }

  // Set the label data buffer.
  set_dimension_label_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
}

bool DimensionLabelReadDataQuery::completed() const {
  return status() == QueryStatus::COMPLETED;
}

/**
 * Typed implementation to check if data is sorted.
 *
 * TODO: This is a quick-and-dirty implementation while we decide where
 * sorting is handled for ordered dimension labels. If we keep this design,
 * we should consider optimizing (parallelizing?) the loops in this check.
 *
 * @param stats Stats object for timing method.
 * @param buffer Buffer to check for sort.
 * @param buffer_size Total size of the buffer.
 * @param increasing If ``true`` check if the data is stricly increasing. If
 *     ``false``, check if the data is strictly decreasing.
 */
template <
    typename T,
    typename Op,
    typename std::enable_if<std::is_arithmetic<T>::value>::type* = nullptr>
bool is_sorted_buffer_impl(
    stats::Stats* stats, const T* buffer, const uint64_t* buffer_size) {
  auto timer_se = stats->start_timer("check_data_sort");
  uint64_t num_values = *buffer_size / sizeof(T);
  Op compare;
  for (uint64_t index{0}; index < num_values - 1; ++index) {
    if (compare(buffer[index + 1], buffer[index])) {
      return false;
    }
  }
  return true;
}

/**
 * Checks if the input buffer is sorted for variable length buffer.
 *
 * TODO: This is a quick-and-dirty implementation while we decide where
 * sorting is handled for ordered dimension labels. If we keep this design,
 * we should consider optimizing (parallelizing?) the loops in this check.
 *
 * @param stats Stats object for timing method.
 * @param offsets_buffer Buffer with offsets for data.
 * @param offsets_buffer_size Total size of the offsets buffer.
 * @param buffer Data buffer to check for sort.
 * @param buffer_size Total size of the data buffer.
 */
template <typename Op>
bool is_sorted_buffer_str_impl(
    stats::Stats* stats,
    const uint64_t* offsets_buffer,
    const uint64_t* offsets_buffer_size,
    const char* buffer,
    const uint64_t* buffer_size) {
  auto timer_se = stats->start_timer("check_data_sort");
  uint64_t num_values = *offsets_buffer_size / sizeof(uint64_t);
  Op compare;
  for (uint64_t index{0}; index < num_values - 1; ++index) {
    uint64_t i0 = offsets_buffer[index];
    uint64_t i1 = offsets_buffer[index + 1];
    uint64_t i2 = index + 1 < num_values ? offsets_buffer[index + 2] :
                                           *buffer_size / sizeof(char);
    if (compare(
            std::string_view(&buffer[i1], i2 - i1),
            std::string_view(&buffer[i0], i1 - i0))) {
      return false;
    }
  }
  return true;
}

/**
 * Pass through method that calss is_sorted_buffer_impl with the appropriate
 * sort type.
 *
 * @param buffer Buffer to check for sort.
 * @param buffer_size Total size of the buffer.
 * @param increasing If ``true`` check if the data is stricly increasing. If
 *     ``false``, check if the data is strictly decreasing.
 */
template <
    typename T,
    typename std::enable_if<std::is_arithmetic<T>::value>::type* = nullptr>
inline bool is_sorted_buffer_as(
    stats::Stats* stats,
    const T* buffer,
    const uint64_t* buffer_size,
    bool increasing) {
  return increasing ? is_sorted_buffer_impl<T, std::less_equal<T>>(
                          stats, buffer, buffer_size) :
                      is_sorted_buffer_impl<T, std::greater_equal<T>>(
                          stats, buffer, buffer_size);
}
/**
 * Checks if the input buffer is sorted.
 *
 * @param buffer Buffer to check for sort.
 * @param type Datatype of the input buffer.
 * @param increasing If ``true`` check if the data is strictly increasing. If
 *     ``false``, check if the data is strictly decreasing.
 */
bool is_sorted_buffer(
    stats::Stats* stats,
    const QueryBuffer& buffer,
    const Datatype type,
    bool increasing) {
  switch (type) {
    case Datatype::INT8:
      return is_sorted_buffer_as<int8_t>(
          stats,
          buffer.data_buffer_as<int8_t>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::UINT8:
      return is_sorted_buffer_as<uint8_t>(
          stats,
          buffer.data_buffer_as<uint8_t>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::INT16:
      return is_sorted_buffer_as<int16_t>(
          stats,
          buffer.data_buffer_as<int16_t>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::UINT16:
      return is_sorted_buffer_as<uint16_t>(
          stats,
          buffer.data_buffer_as<uint16_t>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::INT32:
      return is_sorted_buffer_as<int32_t>(
          stats,
          buffer.data_buffer_as<int32_t>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::UINT32:
      return is_sorted_buffer_as<uint32_t>(
          stats,
          buffer.data_buffer_as<uint32_t>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::INT64:
      return is_sorted_buffer_as<int64_t>(
          stats,
          buffer.data_buffer_as<int64_t>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::UINT64:
      return is_sorted_buffer_as<uint64_t>(
          stats,
          buffer.data_buffer_as<uint64_t>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::FLOAT32:
      return is_sorted_buffer_as<float>(
          stats,
          buffer.data_buffer_as<float>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::FLOAT64:
      return is_sorted_buffer_as<double>(
          stats,
          buffer.data_buffer_as<double>(),
          buffer.buffer_size_,
          increasing);
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
      return is_sorted_buffer_as<int64_t>(
          stats,
          buffer.data_buffer_as<int64_t>(),
          buffer.buffer_size_,
          increasing);
    case Datatype::STRING_ASCII:
      return increasing ?
                 is_sorted_buffer_str_impl<std::less_equal<std::string_view>>(
                     stats,
                     buffer.offsets_buffer(),
                     buffer.buffer_size_,
                     buffer.data_buffer_as<char>(),
                     buffer.buffer_var_size_) :
                 is_sorted_buffer_str_impl<
                     std::greater_equal<std::string_view>>(
                     stats,
                     buffer.offsets_buffer(),
                     buffer.buffer_size_,
                     buffer.data_buffer_as<char>(),
                     buffer.buffer_var_size_);
    default:
      throw DimensionLabelDataQueryStatusException(
          "Unexpected label datatype " + datatype_str(type));
  }

  return true;
}

OrderedWriteDataQuery::OrderedWriteDataQuery(
    StorageManager* storage_manager,
    stats::Stats* stats,
    DimensionLabel* dimension_label,
    const Subarray& parent_subarray,
    const QueryBuffer& label_buffer,
    const QueryBuffer& index_buffer,
    const uint32_t dim_idx,
    optional<std::string> fragment_name)
    : Query(storage_manager, dimension_label->indexed_array(), fragment_name) {
  // Set query layout.
  throw_if_not_ok(set_layout(Layout::ROW_MAJOR));

  // Verify the label data is sorted in the correct order and set label buffer.
  if (!is_sorted_buffer(
          stats,
          label_buffer,
          dimension_label->label_attribute()->type(),
          dimension_label->label_order() == LabelOrder::INCREASING_LABELS)) {
    throw DimensionLabelDataQueryStatusException(
        "Failed to create dimension label query. The label data is not in the "
        "expected order.");
  }
  set_dimension_label_buffer(
      dimension_label->label_attribute()->name(), label_buffer);

  // Set the subarray.
  if (index_buffer.buffer_ == nullptr) {
    // Set the subarray if it has index ranges added to it.
    if (!parent_subarray.is_default(dim_idx)) {
      Subarray subarray{*this->subarray()};
      throw_if_not_ok(subarray.set_ranges_for_dim(
          0, parent_subarray.ranges_for_dim(dim_idx)));
      throw_if_not_ok(set_subarray(subarray));
    }

  } else {
    // Set the subarray using the points from the index buffer. Throw an error
    // if more than one range is created (only happens if index data is not
    // ordered).
    uint64_t count = *index_buffer.buffer_size_ /
                     datatype_size(dimension_label->index_dimension()->type());
    Subarray subarray{*this->subarray()};
    throw_if_not_ok(subarray.set_coalesce_ranges(true));
    throw_if_not_ok(subarray.add_point_ranges(0, index_buffer.buffer_, count));
    if (subarray.range_num() > 1) {
      throw DimensionLabelDataQueryStatusException(
          "Failed to create dimension label query. The index data must be for "
          "consecutive values.");
    }
    throw_if_not_ok(set_subarray(subarray));
  }
}

bool OrderedWriteDataQuery::completed() const {
  return status() == QueryStatus::COMPLETED;
}

UnorderedWriteDataQuery::UnorderedWriteDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    const Subarray& parent_subarray,
    const QueryBuffer& label_buffer,
    const QueryBuffer& index_buffer,
    const uint32_t dim_idx,
    optional<std::string> fragment_name)
    : Query(storage_manager, dimension_label->indexed_array(), fragment_name) {
  // Create locally stored index data if the index buffer is empty.
  bool use_local_index = index_buffer.buffer_ == nullptr;
  if (use_local_index) {
    // Check only one range on the subarray is set.
    if (!parent_subarray.is_default(dim_idx)) {
      const auto& ranges = parent_subarray.ranges_for_dim(dim_idx);
      if (ranges.size() != 1) {
        throw DimensionLabelDataQueryStatusException(
            "Failed to create dimension label query. Dimension label writes "
            "can only be set for a single range.");
      }
    }

    // Create the index data.
    index_data_ = tdb_unique_ptr<IndexData>(IndexDataCreate::make_index_data(
        dimension_label->index_dimension()->type(),
        parent_subarray.ranges_for_dim(dim_idx)[0]));
  }

  // Set-up indexed array query (sparse array).
  throw_if_not_ok(set_layout(Layout::UNORDERED));
  set_dimension_label_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
  if (use_local_index) {
    throw_if_not_ok(set_data_buffer(
        dimension_label->index_dimension()->name(),
        index_data_->data(),
        index_data_->data_size(),
        true));
  } else {
    set_dimension_label_buffer(
        dimension_label->index_dimension()->name(), index_buffer);
  }
}
bool UnorderedWriteDataQuery::completed() const {
  return status() == QueryStatus::COMPLETED;
}

}  // namespace tiledb::sm
