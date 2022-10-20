/**
 * @file   api_exception_safety_experimental.h
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
 * implementation for experimental features.
 */

#ifndef TILEDB_CAPI_HELPERS_EXPERIMENTAL_H
#define TILEDB_CAPI_HELPERS_EXPERIMENTAL_H

#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/c_api/experimental/tiledb_struct_def.h"

inline int32_t sanity_check(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_label_schema_t* dim_label_schema) {
  if (dim_label_schema == nullptr ||
      dim_label_schema->dim_label_schema_ == nullptr) {
    auto st = Status_Error("Invalid TileDB dimension label schema object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

#endif
