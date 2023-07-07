/**
 * @file sum_aggregator.cc
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
 * This file implements class SumAggregator.
 */

#include "tiledb/sm/query/readers/aggregators/sum_aggregator.h"

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class SumAggregatorStatusException : public StatusException {
 public:
  explicit SumAggregatorStatusException(const std::string& message)
      : StatusException("SumAggregator", message) {
  }
};

/** Specialization of safe_sum for int64_t sums. */
template <>
void safe_sum<int64_t>(int64_t value, int64_t& sum) {
  if (sum > 0 && value > 0 &&
      (sum > (std::numeric_limits<int64_t>::max() - value))) {
    sum = std::numeric_limits<int64_t>::max();
    throw std::overflow_error("overflow on sum");
  }

  if (sum < 0 && value < 0 &&
      (sum < (std::numeric_limits<int64_t>::min() - value))) {
    sum = std::numeric_limits<int64_t>::min();
    throw std::overflow_error("overflow on sum");
  }

  sum += value;
}

/** Specialization of safe_sum for uint64_t sums. */
template <>
void safe_sum<uint64_t>(uint64_t value, uint64_t& sum) {
  if (sum > (std::numeric_limits<uint64_t>::max() - value)) {
    sum = std::numeric_limits<uint64_t>::max();
    throw std::overflow_error("overflow on sum");
  }

  sum += value;
}

/** Specialization of safe_sum for double sums. */
template <>
void safe_sum<double>(double value, double& sum) {
  if ((sum < 0.0) == (value < 0.0) &&
      (std::abs(sum) >
       (std::numeric_limits<double>::max() - std::abs(value)))) {
    sum = sum < 0.0 ? std::numeric_limits<double>::lowest() :
                      std::numeric_limits<double>::max();
    throw std::overflow_error("overflow on sum");
  }

  sum += value;
}

template <typename T>
SumAggregator<T>::SumAggregator(
    const std::string field_name,
    const bool var_sized,
    const bool is_nullable,
    const unsigned cell_val_num)
    : is_nullable_(is_nullable)
    , field_name_(field_name)
    , sum_(0)
    , validity_value_(0)
    , sum_overflowed_(false) {
  if (var_sized) {
    throw SumAggregatorStatusException(
        "Sum aggregates must not be requested for var sized attributes.");
  }

  if (cell_val_num != 1) {
    throw SumAggregatorStatusException(
        "Sum aggregates must not be requested for attributes with more than "
        "one value.");
  }
}

template <typename T>
void SumAggregator<T>::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw SumAggregatorStatusException("Result buffer doesn't exist.");
  }

  auto& result_buffer = buffers[output_field_name];
  if (result_buffer.buffer_ == nullptr) {
    throw SumAggregatorStatusException(
        "Sum aggregates must have a fixed size buffer.");
  }

  if (result_buffer.buffer_var_ != nullptr) {
    throw SumAggregatorStatusException(
        "Sum aggregates must not have a var buffer.");
  }

  if (result_buffer.original_buffer_size_ != 8) {
    throw SumAggregatorStatusException(
        "Sum aggregates fixed size buffer should be for one element only.");
  }

  bool exists_validity = result_buffer.validity_vector_.buffer() != nullptr;
  if (is_nullable_) {
    if (!exists_validity) {
      throw SumAggregatorStatusException(
          "Sum aggregates for nullable attributes must have a validity "
          "buffer.");
    }

    if (*result_buffer.validity_vector_.buffer_size() != 1) {
      throw SumAggregatorStatusException(
          "Sum aggregates validity vector should be for one element only.");
    }
  } else {
    if (exists_validity) {
      throw SumAggregatorStatusException(
          "Sum aggregates for non nullable attributes must not have a validity "
          "buffer.");
    }
  }
}

template <typename T>
void SumAggregator<T>::aggregate_data(AggregateBuffer& input_data) {
  tuple<typename sum_type_data<T>::sum_type, uint8_t> res{0, 0};

  bool overflow = false;
  try {
    if (input_data.is_count_bitmap()) {
      res = sum<typename sum_type_data<T>::sum_type, uint64_t>(input_data);
    } else {
      res = sum<typename sum_type_data<T>::sum_type, uint8_t>(input_data);
    }
  } catch (std::overflow_error&) {
    overflow = true;
  }

  {
    std::unique_lock lock(sum_mtx_);

    // A previous operation already overflowed the sum, return.
    if (sum_overflowed_) {
      return;
    }

    // If we have an overflow, signal it, else it's business as usual.
    if (overflow) {
      sum_overflowed_ = true;
      sum_ = std::get<0>(res);
    } else {
      // This sum might overflow as well.
      try {
        safe_sum(std::get<0>(res), sum_);
      } catch (std::overflow_error&) {
        sum_overflowed_ = true;
      }
    }
  }

  if (is_nullable_ && std::get<1>(res) == 1) {
    validity_value_ = 1;
  }
}

template <typename T>
void SumAggregator<T>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  auto& result_buffer = buffers[output_field_name];
  *static_cast<typename sum_type_data<T>::sum_type*>(result_buffer.buffer_) =
      sum_;

  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(typename sum_type_data<T>::sum_type);
  }

  if (is_nullable_) {
    *static_cast<uint8_t*>(result_buffer.validity_vector_.buffer()) =
        validity_value_;

    if (result_buffer.validity_vector_.buffer_size()) {
      *result_buffer.validity_vector_.buffer_size() = 1;
    }
  }
}

template <typename T>
template <typename SUM_T, typename BITMAP_T>
tuple<SUM_T, uint8_t> SumAggregator<T>::sum(AggregateBuffer& input_data) {
  SUM_T sum{0};
  uint8_t validity{0};
  auto values = input_data.fixed_data_as<T>();

  // Run different loops for bitmap versus no bitmap and nullable versus non
  // nullable.
  if (input_data.has_bitmap()) {
    auto bitmap_values = input_data.bitmap_data_as<BITMAP_T>();

    if (is_nullable_) {
      auto validity_values = input_data.validity_data();

      // Process for nullable sums with bitmap.
      for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
        if (validity_values[c] != 0 && bitmap_values[c] != 0) {
          validity = 1;

          auto value = static_cast<SUM_T>(values[c]);
          for (BITMAP_T i = 0; i < bitmap_values[c]; i++) {
            safe_sum(value, sum);
          }
        }
      }
    } else {
      // Process for non nullable sums with bitmap.
      for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
        auto value = static_cast<SUM_T>(values[c]);

        for (BITMAP_T i = 0; i < bitmap_values[c]; i++) {
          safe_sum(value, sum);
        }
      }
    }
  } else {
    if (is_nullable_) {
      auto validity_values = input_data.validity_data();

      // Process for nullable sums with no bitmap.
      for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
        if (validity_values[c] != 0) {
          validity = 1;

          auto value = static_cast<SUM_T>(values[c]);
          safe_sum(value, sum);
        }
      }
    } else {
      // Process for non nullable sums with no bitmap.
      for (uint64_t c = input_data.min_cell(); c < input_data.max_cell(); c++) {
        auto value = static_cast<SUM_T>(values[c]);
        safe_sum(value, sum);
      }
    }
  }

  return {sum, validity};
}

// Explicit template instantiations
template SumAggregator<int8_t>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);
template SumAggregator<int16_t>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);
template SumAggregator<int32_t>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);
template SumAggregator<int64_t>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);
template SumAggregator<uint8_t>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);
template SumAggregator<uint16_t>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);
template SumAggregator<uint32_t>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);
template SumAggregator<uint64_t>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);
template SumAggregator<float>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);
template SumAggregator<double>::SumAggregator(
    const std::string, const bool, const bool, const unsigned);

}  // namespace sm
}  // namespace tiledb
