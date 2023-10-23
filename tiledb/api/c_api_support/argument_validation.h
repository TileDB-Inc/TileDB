/**
 * @file tiledb/api/c_api_support/api_argument_validation.h
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
 * This file declares argument validator functions used in the c-api
 * implementation
 */

#ifndef TILEDB_CAPI_SUPPORT_ARGUMENT_VALIDATION_H
#define TILEDB_CAPI_SUPPORT_ARGUMENT_VALIDATION_H

#include "tiledb/common/exception/exception.h"

namespace tiledb::api {

class CAPIException : public common::StatusException {
 public:
  explicit CAPIException(const std::string& message)
      : StatusException("C API", message) {
  }
};
// Legacy alias to forestall massive sudden code change
using CAPIStatusException = CAPIException;

/*
 * Validation functions
 *
 * Some functions are only declared here. Others are defined here and declared
 * as inline:
 *
 * * Inline validation functions should do simple tests only. Anything more than
 *   that should be a function call.
 * * Validation should rely on exception handling in the API wrapper.
 */

/**
 * Validates a pointer to an output object, either new or existing.
 *
 * Logs and saves any error encountered, if possible.
 *
 * @param ctx Context
 * @param p Output pointer
 * @return true iff context is valid and pointer is not null
 */
inline void ensure_output_pointer_is_valid(void* p) {
  if (p == nullptr) {
    throw CAPIException("Invalid output pointer for object");
  }
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_SUPPORT_ARGUMENT_VALIDATION_H