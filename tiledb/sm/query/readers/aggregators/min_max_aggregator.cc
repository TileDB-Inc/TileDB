/**
 * @file min_max_aggregator.cc
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
 * This file implements class MinMaxAggregator.
 */

#include "tiledb/sm/query/readers/aggregators/min_max_aggregator.h"

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class MinMaxAggregatorStatusException : public StatusException {
 public:
  explicit MinMaxAggregatorStatusException(const std::string& message)
      : StatusException("MinMaxAggregator", message) {
  }
};

template <typename T>
template <typename VALUE_T>
VALUE_T ComparatorAggregatorBase<T>::value_at(
    const void* fixed_data, const char*, const uint64_t cell_idx) const {
  return static_cast<const T*>(fixed_data)[cell_idx];
}

template <>
template <>
std::string_view ComparatorAggregatorBase<std::string>::value_at(
    const void* fixed_data,
    const char* var_data,
    const uint64_t cell_idx) const {
  if (field_info_.var_sized_) {
    auto offsets = static_cast<const uint64_t*>(fixed_data);
    // Return the var sized string.
    uint64_t offset = offsets[cell_idx];
    uint64_t next_offset = offsets[cell_idx + 1];

    return std::string_view(var_data + offset, next_offset - offset);
  } else {
    // Return the fixed size string.
    return std::string_view(
        static_cast<const char*>(fixed_data) +
            field_info_.cell_val_num_ * cell_idx,
        field_info_.cell_val_num_);
  }
}

template <typename T>
template <typename VALUE_T>
VALUE_T ComparatorAggregatorBase<T>::last_var_value(
    const void*, const char*, const AggregateBuffer&) const {
  return 0;
}

template <>
template <>
std::string_view ComparatorAggregatorBase<std::string>::last_var_value(
    const void* fixed_data,
    const char* var_data,
    const AggregateBuffer& input_data) const {
  auto offsets = static_cast<const uint64_t*>(fixed_data);

  // Return the var sized string.
  uint64_t offset = offsets[input_data.max_cell() - 1];
  uint64_t next_offset = input_data.var_data_size();

  return std::string_view(var_data + offset, next_offset - offset);
}

template <typename T>
void ComparatorAggregatorBase<T>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) const {
  auto& result_buffer = buffers[output_field_name];

  *static_cast<T*>(result_buffer.buffer_) = value_.value_or(0);
  if (result_buffer.buffer_size_) {
    *result_buffer.buffer_size_ = sizeof(T);
  }

  if (field_info_.is_nullable_) {
    *static_cast<uint8_t*>(result_buffer.validity_vector_.buffer()) =
        validity_value_.value();

    if (result_buffer.validity_vector_.buffer_size()) {
      *result_buffer.validity_vector_.buffer_size() = 1;
    }
  }
}

template <>
void ComparatorAggregatorBase<std::string>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) const {
  auto& result_buffer = buffers[output_field_name];

  if (field_info_.var_sized_) {
    // For var sized string, we need to set the offset value of 0 and the var
    // data buffer.
    *static_cast<uint64_t*>(result_buffer.buffer_) = 0;
    if (result_buffer.buffer_size_) {
      *result_buffer.buffer_size_ = constants::cell_var_offset_size;
    }

    if (value_.has_value()) {
      if (result_buffer.original_buffer_var_size_ < value_.value().size()) {
        throw MinMaxAggregatorStatusException(
            "Min/max buffer not big enough for " + field_info_.name_ +
            ". Required: " + std::to_string(value_.value().size()));
      }

      memcpy(
          result_buffer.buffer_var_,
          value_.value().data(),
          value_.value().size());
    }
  } else {
    if (value_.has_value()) {
      // For fixed size strings, just set the fixed buffer.
      memcpy(
          result_buffer.buffer_, value_.value().data(), value_.value().size());
    }
  }

  // Set the var buffer size.
  if (result_buffer.buffer_var_size_) {
    *result_buffer.buffer_var_size_ =
        value_.has_value() ? value_.value().size() : 0;
  }

  if (field_info_.is_nullable_) {
    *static_cast<uint8_t*>(result_buffer.validity_vector_.buffer()) =
        validity_value_.value();

    if (result_buffer.validity_vector_.buffer_size()) {
      *result_buffer.validity_vector_.buffer_size() = 1;
    }
  }
}

template <typename T, typename Op>
ComparatorAggregator<T, Op>::ComparatorAggregator(const FieldInfo& field_info)
    : ComparatorAggregatorBase<T>(field_info) {
  if (field_info.var_sized_ && !std::is_same<T, std::string>::value) {
    throw MinMaxAggregatorStatusException(
        "Min/max aggregates must not be requested for var sized non-string "
        "attributes.");
  }

  if (field_info.cell_val_num_ != 1 && !std::is_same<T, std::string>::value) {
    throw MinMaxAggregatorStatusException(
        "Min/max aggregates must not be requested for attributes with more "
        "than one value.");
  }
}

template <typename T, typename Op>
void ComparatorAggregator<T, Op>::validate_output_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  if (buffers.count(output_field_name) == 0) {
    throw MinMaxAggregatorStatusException("Result buffer doesn't exist.");
  }

  auto& result_buffer = buffers[output_field_name];
  if (result_buffer.buffer_ == nullptr) {
    throw MinMaxAggregatorStatusException(
        "Min/max aggregates must have a fixed size buffer.");
  }

  if (ComparatorAggregatorBase<T>::field_info_.var_sized_) {
    if (result_buffer.buffer_var_ == nullptr) {
      throw MinMaxAggregatorStatusException(
          "Var sized min/max aggregates must have a var buffer.");
    }

    if (result_buffer.original_buffer_size_ !=
        constants::cell_var_offset_size) {
      throw MinMaxAggregatorStatusException(
          "Var sized min/max aggregates offset buffer should be for one "
          "element only.");
    }

    if (ComparatorAggregatorBase<T>::field_info_.cell_val_num_ !=
        constants::var_num) {
      throw MinMaxAggregatorStatusException(
          "Var sized min/max aggregates should have TILEDB_VAR_NUM cell val "
          "num.");
    }
  } else {
    if (result_buffer.buffer_var_ != nullptr) {
      throw MinMaxAggregatorStatusException(
          "Fixed min/max aggregates must not have a var buffer.");
    }

    // If cell val num is one, this is a normal fixed size attritube. If not, it
    // is a fixed sized string.
    if (ComparatorAggregatorBase<T>::field_info_.cell_val_num_ == 1) {
      if (result_buffer.original_buffer_size_ != sizeof(T)) {
        throw MinMaxAggregatorStatusException(
            "Fixed size min/max aggregates fixed buffer should be for one "
            "element only.");
      }
    } else {
      if (result_buffer.original_buffer_size_ !=
          ComparatorAggregatorBase<T>::field_info_.cell_val_num_) {
        throw MinMaxAggregatorStatusException(
            "Fixed size min/max aggregates fixed buffer should be for one "
            "element only.");
      }
    }
  }

  bool exists_validity = result_buffer.validity_vector_.buffer();
  if (ComparatorAggregatorBase<T>::field_info_.is_nullable_) {
    if (!exists_validity) {
      throw MinMaxAggregatorStatusException(
          "Min/max aggregates for nullable attributes must have a validity "
          "buffer.");
    }

    if (*result_buffer.validity_vector_.buffer_size() != 1) {
      throw MinMaxAggregatorStatusException(
          "Min/max aggregates validity vector should be for one element only.");
    }
  } else {
    if (exists_validity) {
      throw MinMaxAggregatorStatusException(
          "Min/max aggregates for non nullable attributes must not have a "
          "validity buffer.");
    }
  }
}

template <typename T, typename Op>
void ComparatorAggregator<T, Op>::aggregate_data(AggregateBuffer& input_data) {
  optional<T> res{nullopt};

  if (input_data.is_count_bitmap()) {
    res = min_max<uint64_t>(input_data);
  } else {
    res = min_max<uint8_t>(input_data);
  }

  {
    // This might be called on multiple threads, the final result stored in
    // value_ should be computed in a thread safe manner.
    std::unique_lock lock(value_mtx_);
    if (res.has_value() &&
        (ComparatorAggregatorBase<T>::value_ == std::nullopt ||
         op_(res.value(), ComparatorAggregatorBase<T>::value_.value()))) {
      ComparatorAggregatorBase<T>::value_ = res.value();
    }

    if (ComparatorAggregatorBase<T>::field_info_.is_nullable_ &&
        res.has_value()) {
      ComparatorAggregatorBase<T>::validity_value_ = 1;
    }
  }
}

template <typename T, typename Op>
void ComparatorAggregator<T, Op>::copy_to_user_buffer(
    std::string output_field_name,
    std::unordered_map<std::string, QueryBuffer>& buffers) {
  ComparatorAggregatorBase<T>::copy_to_user_buffer(output_field_name, buffers);
}

template <typename T, typename Op>
template <typename BITMAP_T>
optional<T> ComparatorAggregator<T, Op>::min_max(AggregateBuffer& input_data) {
  optional<VALUE_T> value;
  auto fixed_data = input_data.fixed_data_as<void*>();
  auto var_data = ComparatorAggregatorBase<T>::field_info_.var_sized_ ?
                      input_data.var_data() :
                      nullptr;

  // Run different loops for bitmap versus no bitmap and nullable versus non
  // nullable. The bitmap tells us which cells was already filtered out by
  // ranges or query conditions.
  const uint64_t max_cell = input_data.includes_last_var_cell() ?
                                input_data.max_cell() - 1 :
                                input_data.max_cell();

  if (input_data.has_bitmap()) {
    auto bitmap_values = input_data.bitmap_data_as<BITMAP_T>();

    if (ComparatorAggregatorBase<T>::field_info_.is_nullable_) {
      auto validity_values = input_data.validity_data();

      // Process for nullable min/max with bitmap.
      for (uint64_t c = input_data.min_cell(); c < max_cell; c++) {
        if (validity_values[c] != 0 && bitmap_values[c] != 0) {
          update_min_max(value, fixed_data, var_data, c);
        }
      }

      // Process last var cell.
      if (input_data.includes_last_var_cell()) {
        if (validity_values[max_cell] != 0 && bitmap_values[max_cell] != 0) {
          update_last_var_min_max(value, fixed_data, var_data, input_data);
        }
      }
    } else {
      // Process for non nullable min/max with bitmap.
      for (uint64_t c = input_data.min_cell(); c < max_cell; c++) {
        if (bitmap_values[c]) {
          update_min_max(value, fixed_data, var_data, c);
        }
      }

      // Process last var cell.
      if (input_data.includes_last_var_cell()) {
        if (bitmap_values[max_cell] != 0) {
          update_last_var_min_max(value, fixed_data, var_data, input_data);
        }
      }
    }
  } else {
    if (ComparatorAggregatorBase<T>::field_info_.is_nullable_) {
      auto validity_values = input_data.validity_data();

      // Process for nullable min/max with no bitmap.
      for (uint64_t c = input_data.min_cell(); c < max_cell; c++) {
        if (validity_values[c] != 0) {
          update_min_max(value, fixed_data, var_data, c);
        }
      }

      // Process last var cell.
      if (input_data.includes_last_var_cell()) {
        if (validity_values[max_cell] != 0) {
          update_last_var_min_max(value, fixed_data, var_data, input_data);
        }
      }
    } else {
      // Process for non nullable min/max with no bitmap.
      for (uint64_t c = input_data.min_cell(); c < max_cell; c++) {
        update_min_max(value, fixed_data, var_data, c);
      }

      // Process last var cell.
      if (input_data.includes_last_var_cell()) {
        update_last_var_min_max(value, fixed_data, var_data, input_data);
      }
    }
  }

  if (value.has_value()) {
    return std::make_optional<T>(value.value());
  }

  return nullopt;
}

// Explicit template instantiations
template MinAggregator<int8_t>::MinAggregator(const FieldInfo);
template MinAggregator<int16_t>::MinAggregator(const FieldInfo);
template MinAggregator<int32_t>::MinAggregator(const FieldInfo);
template MinAggregator<int64_t>::MinAggregator(const FieldInfo);
template MinAggregator<uint8_t>::MinAggregator(const FieldInfo);
template MinAggregator<uint16_t>::MinAggregator(const FieldInfo);
template MinAggregator<uint32_t>::MinAggregator(const FieldInfo);
template MinAggregator<uint64_t>::MinAggregator(const FieldInfo);
template MinAggregator<float>::MinAggregator(const FieldInfo);
template MinAggregator<double>::MinAggregator(const FieldInfo);
template MinAggregator<std::string>::MinAggregator(const FieldInfo);
template MaxAggregator<int8_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<int16_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<int32_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<int64_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<uint8_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<uint16_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<uint32_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<uint64_t>::MaxAggregator(const FieldInfo);
template MaxAggregator<float>::MaxAggregator(const FieldInfo);
template MaxAggregator<double>::MaxAggregator(const FieldInfo);
template MaxAggregator<std::string>::MaxAggregator(const FieldInfo);

}  // namespace sm
}  // namespace tiledb
