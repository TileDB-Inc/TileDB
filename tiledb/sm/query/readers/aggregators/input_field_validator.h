/**
 * @file   input_field_validator.h
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
 * This file defines class InputFieldValidator.
 */

#ifndef TILEDB_INPUT_FIELD_VALIDATOR_H
#define TILEDB_INPUT_FIELD_VALIDATOR_H

#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/field_info.h"

namespace tiledb::sm {

class InputFieldValidatorStatusException : public StatusException {
 public:
  explicit InputFieldValidatorStatusException(const std::string& message)
      : StatusException("InputFieldValidator", message) {
  }
};

class InputFieldValidator {
 protected:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  InputFieldValidator() {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Ensure the input field is numeric.
   *
   * @param field_info Field info.
   */
  inline static void ensure_field_numeric(const FieldInfo& field_info) {
    if (field_info.var_sized_) {
      throw InputFieldValidatorStatusException(
          "Aggregate is not supported for var sized non-string fields.");
    }
    if (field_info.cell_val_num_ != 1) {
      throw InputFieldValidatorStatusException(
          "Aggregate is not supported for non-string fields with cell_val_num "
          "greater than one.");
    }
  }

  /**
   * Ensure the input field is nullable.
   *
   * @param field_info Field info.
   */
  inline static void ensure_field_nullable(const FieldInfo& field_info) {
    if (!field_info.is_nullable_) {
      throw InputFieldValidatorStatusException(
          "Aggregate must only be requested for nullable fields.");
    }
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_INPUT_FIELD_VALIDATOR_H
