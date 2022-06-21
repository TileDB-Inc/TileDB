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

#include "tiledb/api/c_api/context/context_internal.h"
#include "tiledb/common/exception/exception.h"
#include "tiledb/sm/storage_manager/context.h"

// Forward declaration
struct tiledb_error_t;

/* Saves a status inside the context object. */
bool save_error(tiledb_ctx_t* ctx, const tiledb::common::Status& st);

bool create_error(tiledb_error_t** error, const tiledb::common::Status& st);

namespace tiledb::api {

class CAPIStatusException : public common::StatusException {
 public:
  explicit CAPIStatusException(const std::string& message)
      : StatusException("C API", message) {
  }
};

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
 * Ensures the context is sufficient for `save_error` to be called on it.
 *
 * Intended to be called only by the API wrapper function. Wrapped functions
 * should rely on the wrapper to validate contexts.
 *
 * @param ctx A context of unknown validity
 * @throw StatusException
 */
inline void ensure_context_is_valid_enough_for_errors(tiledb_ctx_t* ctx) {
  if (ctx == nullptr) {
    throw CAPIStatusException("Null context pointer");
  }
  if (ctx->ctx_ == nullptr) {
    throw CAPIStatusException("Empty context structure");
  }
}

/**
 * Ensure the context is valid.
 *
 * Intended to be called only by the API wrapper function. Wrapped functions
 * should rely on the wrapper to validate contexts.
 *
 * TRANSITIONAL: The context constructor should throw if it doesn't have a valid
 * storage manager object. Until that class is fully C.41-compliant, we'll leave
 * this validation check in place.
 *
 * @pre `ensure_context_is_valid_enough_for_errors` would return successfully
 *
 * @param ctx A partially-validated context
 * @throw StatusException
 */
inline void ensure_context_is_fully_valid(tiledb_ctx_t* ctx) {
  if (ctx->ctx_->storage_manager() == nullptr) {
    throw CAPIStatusException("Context is missing its storage manager");
  }
}

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
    throw CAPIStatusException("Invalid output pointer for object");
  }
}

/**
 * Action to take when an input pointer is invalid.
 *
 * @pre `ctx` is valid
 *
 * @param ctx Context
 * @param type_name API name of object typew
 */
inline void action_invalid_object(const std::string& type_name) {
  throw CAPIStatusException(std::string("Invalid TileDB object: ") + type_name);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_SUPPORT_ARGUMENT_VALIDATION_H