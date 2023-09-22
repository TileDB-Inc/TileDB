/**
 * @file   output_buffer_validator.h
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
 * This file defines class OutputBufferValidator.
 */

#ifndef TILEDB_OUTPUT_BUFFER_VALIDATOR_H
#define TILEDB_OUTPUT_BUFFER_VALIDATOR_H

#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/field_info.h"

namespace tiledb::sm {

class QueryBuffer;

class OutputBufferValidator {
 protected:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  OutputBufferValidator(const FieldInfo field_info)
      : field_info_(field_info) {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Validate the output buffer has a fixed buffer.
   *
   * @param buffer Output buffer.
   */
  void ensure_has_fixed_buffer(QueryBuffer& buffer);

  /**
   * Validate the output buffer has no var buffer.
   *
   * @param buffer Output buffer.
   */
  void ensure_no_var_buffer(QueryBuffer& buffer);

  /**
   * Validate the output buffer has one element.
   *
   * @param buffer Output buffer.
   * @param element_size Element size.
   */
  void ensure_one_element(QueryBuffer& buffer, uint64_t element_size);

  /**
   * Validate the output buffer has the correct validity buffer.
   *
   * @param buffer Output buffer.
   */
  void ensure_correct_validity_buffer(QueryBuffer& buffer);

  /**
   * Validate the output buffer can receive an arithmetic operation result.
   *
   * @param buffer Output buffer.
   */
  void ensure_output_buffer_arithmetic(QueryBuffer& buffer);

  /**
   * Validate the output buffer can receive a count operation result.
   *
   * @param buffer Output buffer.
   */
  void ensure_output_buffer_count(QueryBuffer& buffer);

  /**
   * Validate the output buffer can receive a result that can be var sized.
   *
   * @tparam T fixed data type.
   * @param buffer Output buffer.
   */
  template <class T>
  void ensure_output_buffer_var(QueryBuffer& buffer);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Field information. */
  const FieldInfo field_info_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_OUTPUT_BUFFER_VALIDATOR_H
