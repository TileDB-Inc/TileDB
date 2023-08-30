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

void OutputBufferValidator::validate_has_fixed_buffer(QueryBuffer& buffer) {
  if (buffer.buffer_ == nullptr) {
    throw OutputBufferValidatorStatusException(
        "Aggregate must have a fixed size buffer.");
  }
}

void OutputBufferValidator::validate_no_var_buffer(QueryBuffer& buffer) {
  if (buffer.buffer_var_ != nullptr) {
    throw OutputBufferValidatorStatusException(
        "Aggregate must not have a var buffer.");
  }
}

void OutputBufferValidator::validate_one_element(
    QueryBuffer& buffer, uint64_t element_size) {
  if (buffer.original_buffer_size_ != element_size) {
    throw OutputBufferValidatorStatusException(
        "Aggregate fixed size buffer should be for one element only.");
  }
}

void OutputBufferValidator::validate_validity(QueryBuffer& buffer) {
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

void OutputBufferValidator::validate_output_buffer_arithmetic(
    QueryBuffer& buffer) {
  validate_has_fixed_buffer(buffer);
  validate_no_var_buffer(buffer);
  validate_one_element(buffer, 8);
  validate_validity(buffer);
}

void OutputBufferValidator::validate_output_buffer_count(QueryBuffer& buffer) {
  validate_has_fixed_buffer(buffer);
  validate_no_var_buffer(buffer);
  validate_one_element(buffer, 8);

  bool exists_validity = buffer.validity_vector_.buffer();
  if (exists_validity) {
    throw OutputBufferValidatorStatusException(
        "Count aggregates must not have a validity buffer.");
  }
}

template <class T>
void OutputBufferValidator::validate_output_buffer_var(QueryBuffer& buffer) {
  validate_has_fixed_buffer(buffer);

  if (field_info_.var_sized_) {
    if (buffer.buffer_var_ == nullptr) {
      throw OutputBufferValidatorStatusException(
          "Var sized aggregates must have a var buffer.");
    }

    validate_one_element(buffer, constants::cell_var_offset_size);

    if (field_info_.cell_val_num_ != constants::var_num) {
      throw OutputBufferValidatorStatusException(
          "Var sized aggregates should have TILEDB_VAR_NUM cell val num.");
    }
  } else {
    validate_no_var_buffer(buffer);

    // If cell val num is one, this is a normal fixed size attritube. If not, it
    // is a fixed sized string.
    if (field_info_.cell_val_num_ == 1) {
      validate_one_element(buffer, sizeof(T));
    } else {
      validate_one_element(buffer, field_info_.cell_val_num_);
    }
  }

  validate_validity(buffer);
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
