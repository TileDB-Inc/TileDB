/**
 * @file mean_aggregator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file implements class MeanAggregator.
 */

#include "tiledb/sm/query/readers/aggregators/mean_aggregator.h"

#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/sum_aggregator.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class MeanAggregatorStatusException : public StatusException {
 public:
  explicit MeanAggregatorStatusException(const std::string& message)
      : StatusException("MeanAggregator", message) {
  }
};

template <typename T>
MeanAggregator<T>::MeanAggregator(const FieldInfo field_info)
    : field_info_(field_info)
    , sum_(0)
    , count_(0)
    , validity_value_(
          field_info_.is_nullable_ ? std::make_optional(0) : nullopt)
    , sum_overflowed_(false) {
  if (field_info_.var_sized_) {
    throw MeanAggregatorStatusException(
        "Mean aggregates must not be requested for var sized attributes.");
  }

  if (field_info_.cell_val_num_ != 1) {
    throw MeanAggregatorStatusException(
        "Mean aggregates must not be requested for attributes with more than "
        "one value.");
  }
}

template <typename T>
void MeanAggregator<T>::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw MeanAggregatorStatusException("Result buffer doesn't exist.");
  }

  auto& result_buffer = buffers[output_field_name];
  if (result_buffer.buffer_ == nullptr) {
    throw MeanAggregatorStatusException(
        "Mean aggregates must have a fixed size buffer.");
  }

  if (result_buffer.buffer_var_ != nullptr) {
    throw MeanAggregatorStatusException(
        "Mean aggregates must not have a var buffer.");
  }

  if (result_buffer.original_buffer_size_ != 8) {
    throw MeanAggregatorStatusException(
        "Mean aggregates fixed size buffer should be for one element only.");
  }

  bool exists_validity = result_buffer.validity_vector_.buffer();
  if (field_info_.is_nullable_) {
    if (!exists_validity) {
      throw MeanAggregatorStatusException(
          "Mean aggregates for nullable attributes must have a validity "
          "buffer.");
    }

    if (*result_buffer.validity_vector_.buffer_size() != 1) {
      throw MeanAggregatorStatusException(
          "Mean aggregates validity vector should be for one element only.");
    }
  } else {
    if (exists_validity) {
      throw MeanAggregatorStatusException(
          "Mean aggregates for non nullable attributes must not have a "
          "validity "
          "buffer.");
    }
  }
}

template <typename T>
void MeanAggregator<T>::aggregate_data(AggregateBuffer& input_data) {
  tuple<double, uint64_t, optional<uint8_t>> res{0, 0, nullopt};

  bool overflow = false;
  try {
    if (input_data.is_count_bitmap()) {
      res = mean<uint64_t>(input_data);
    } else {
      res = mean<uint8_t>(input_data);
    }
  } catch (std::overflow_error&) {
    overflow = true;
  }

  {
    // This might be called on multiple threads, the final result stored in sum_
    // should be computed in a thread safe manner. The mutex also protects
    // sum_overflowed_ which indicates when the sum has overflowed.
    std::unique_lock lock(mean_mtx_);

    // A previous operation already overflowed the sum, return.
    if (sum_overflowed_) {
      return;
    }

    // If we have an overflow, signal it, else it's business as usual.
    if (overflow) {
      sum_overflowed_ = true;
      sum_ = std::get<0>(res);
      count_ = std::numeric_limits<uint64_t>::max();
      return;
    } else {
      // This sum might overflow as well.
      try {
        safe_sum(std::get<0>(res), sum_);
        safe_sum(std::get<1>(res), count_);
      } catch (std::overflow_error&) {
        sum_overflowed_ = true;
      }
    }
  }

  if (field_info_.is_nullable_ && std::get<2>(res).value() == 1) {
    validity_value_ = 1;
  }
}

template <typename T>
void MeanAggregator<T>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  auto& result_buffer = buffers[output_field_name];
  *static_cast<double*>(result_buffer.buffer_) = sum_ / count_;

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(double);
  }

  if (field_info_.is_nullable_) {
    *static_cast<uint8_t*>(result_buffer.validity_vector_.buffer()) =
        validity_value_.value();

    if (result_buffer.validity_vector_.buffer_size()) {
      *result_buffer.validity_vector_.buffer_size() = 1;
    }
  }
}

template <typename T>
template <typename BITMAP_T>
tuple<double, uint64_t, optional<uint8_t>> MeanAggregator<T>::mean(
    AggregateBuffer& input_data) {
  double sum{0};
  uint64_t count{0};
  optional<uint8_t> validity{nullopt};
  auto values = input_data.fixed_data_as<T>();

  // Run different loops for bitmap versus no bitmap and nullable versus non
  // nullable. The bitmap tells us which cells was already filtered out by
  // ranges or query conditions.
  if (input_data.has_bitmap()) {
    auto bitmap_values = input_data.bitmap_data_as<BITMAP_T>();

    if (field_info_.is_nullable_) {
      validity = 0;
      auto validity_values = input_data.validity_data();

      // Process for nullable sums with bitmap.
      for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
        if (validity_values[c] != 0 && bitmap_values[c] != 0) {
          validity = 1;

          auto value = static_cast<double>(values[c]);
          for (BITMAP_T i = 0; i < bitmap_values[c]; i++) {
            count++;
            safe_sum(value, sum);
          }
        }
      }
    } else {
      // Process for non nullable sums with bitmap.
      for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
        auto value = static_cast<double>(values[c]);

        for (BITMAP_T i = 0; i < bitmap_values[c]; i++) {
          count++;
          safe_sum(value, sum);
        }
      }
    }
  } else {
    if (field_info_.is_nullable_) {
      validity = 0;
      auto validity_values = input_data.validity_data();

      // Process for nullable sums with no bitmap.
      for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
        if (validity_values[c] != 0) {
          validity = 1;

          auto value = static_cast<double>(values[c]);
          count++;
          safe_sum(value, sum);
        }
      }
    } else {
      // Process for non nullable sums with no bitmap.
      for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
        auto value = static_cast<double>(values[c]);
        count++;
        safe_sum(value, sum);
      }
    }
  }

  return {sum, count, validity};
}

// Explicit template instantiations
template MeanAggregator<int8_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<int16_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<int32_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<int64_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<uint8_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<uint16_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<uint32_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<uint64_t>::MeanAggregator(const FieldInfo);
template MeanAggregator<float>::MeanAggregator(const FieldInfo);
template MeanAggregator<double>::MeanAggregator(const FieldInfo);

}  // namespace sm
}  // namespace tiledb
