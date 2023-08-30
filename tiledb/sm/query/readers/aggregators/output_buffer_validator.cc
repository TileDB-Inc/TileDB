/**
 * @file output_buffer_validator.cc
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
 * This file implements class OutputBufferValidator.
 */

#include "tiledb/sm/query/readers/aggregators/output_buffer_validator.h"

#include "tiledb/sm/query/query_buffer.h"

namespace tiledb {
namespace sm {

class OutputBufferValidatorStatusException : public StatusException {
 public:
  explicit OutputBufferValidatorStatusException(const std::string& message)
      : StatusException("OutputBufferValidator", message) {
  }
};

void OutputBufferValidator::validate_output_buffer_arithmetic(
    QueryBuffer& buffer) {
  if (buffer.buffer_ == nullptr) {
    throw OutputBufferValidatorStatusException(
        "Aggregate must have a fixed size buffer.");
  }

  if (buffer.buffer_var_ != nullptr) {
    throw OutputBufferValidatorStatusException(
        "Aggregate must not have a var buffer.");
  }

  if (buffer.original_buffer_size_ != 8) {
    throw OutputBufferValidatorStatusException(
        "Aggregate fixed size buffer should be for one element only.");
  }

  bool exists_validity = buffer.validity_vector_.buffer();
  if (field_info_.is_nullable_) {
    if (!exists_validity) {
      throw OutputBufferValidatorStatusException(
          "Aggregate for nullable attributes must have a validity "
          "buffer.");
    }

    if (*buffer.validity_vector_.buffer_size() != 1) {
      throw OutputBufferValidatorStatusException(
          "Aggregate validity vector should be for one element only.");
    }
  } else {
    if (exists_validity) {
      throw OutputBufferValidatorStatusException(
          "Aggregate for non nullable attributes must not have a "
          "validity buffer.");
    }
  }
}

void OutputBufferValidator::validate_output_buffer_count(QueryBuffer& buffer) {
  if (buffer.buffer_ == nullptr) {
    throw OutputBufferValidatorStatusException(
        "Count aggregates must have a fixed size buffer.");
  }

  if (buffer.buffer_var_ != nullptr) {
    throw OutputBufferValidatorStatusException(
        "Count aggregates must not have a var buffer.");
  }

  if (buffer.original_buffer_size_ != 8) {
    throw OutputBufferValidatorStatusException(
        "Count aggregates fixed size buffer should be for one element only.");
  }

  bool exists_validity = buffer.validity_vector_.buffer();
  if (exists_validity) {
    throw OutputBufferValidatorStatusException(
        "Count aggregates must not have a validity buffer.");
  }
}

template <class T>
void OutputBufferValidator::validate_output_buffer_var(QueryBuffer& buffer) {
  if (buffer.buffer_ == nullptr) {
    throw OutputBufferValidatorStatusException(
        "Aggregates must have a fixed size buffer.");
  }

  if (field_info_.var_sized_) {
    if (buffer.buffer_var_ == nullptr) {
      throw OutputBufferValidatorStatusException(
          "Var sized aggregates must have a var buffer.");
    }

    if (buffer.original_buffer_size_ != constants::cell_var_offset_size) {
      throw OutputBufferValidatorStatusException(
          "Var sized aggregates offset buffer should be for one element only.");
    }

    if (field_info_.cell_val_num_ != constants::var_num) {
      throw OutputBufferValidatorStatusException(
          "Var sized aggregates should have TILEDB_VAR_NUM cell val num.");
    }
  } else {
    if (buffer.buffer_var_ != nullptr) {
      throw OutputBufferValidatorStatusException(
          "Fixed aggregates must not have a var buffer.");
    }

    // If cell val num is one, this is a normal fixed size attritube. If not, it
    // is a fixed sized string.
    if (field_info_.cell_val_num_ == 1) {
      if (buffer.original_buffer_size_ != sizeof(T)) {
        throw OutputBufferValidatorStatusException(
            "Fixed size aggregates fixed buffer should be for one element "
            "only.");
      }
    } else {
      if (buffer.original_buffer_size_ != field_info_.cell_val_num_) {
        throw OutputBufferValidatorStatusException(
            "Fixed size aggregates fixed buffer should be for one element "
            "only.");
      }
    }
  }

  bool exists_validity = buffer.validity_vector_.buffer();
  if (field_info_.is_nullable_) {
    if (!exists_validity) {
      throw OutputBufferValidatorStatusException(
          "Aggregates for nullable attributes must have a validity buffer.");
    }

    if (*buffer.validity_vector_.buffer_size() != 1) {
      throw OutputBufferValidatorStatusException(
          "Aggregates validity vector should be for one element only.");
    }
  } else {
    if (exists_validity) {
      throw OutputBufferValidatorStatusException(
          "Aggregates for non nullable attributes must not have a validity "
          "buffer.");
    }
  }
}

// Explicit template instantiations
template void OutputBufferValidator::validate_output_buffer_var<int8_t>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<int16_t>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<int32_t>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<int64_t>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<uint8_t>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<uint16_t>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<uint32_t>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<uint64_t>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<float>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<double>(
    QueryBuffer&);
template void OutputBufferValidator::validate_output_buffer_var<std::string>(
    QueryBuffer&);

}  // namespace sm
}  // namespace tiledb
