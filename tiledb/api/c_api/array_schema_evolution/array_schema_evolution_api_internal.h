/**
 * @file
 * tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares the internals of the array schema evolution section of
 * the C API.
 */

#ifndef TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_INTERNAL_H
#define TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_INTERNAL_H

#include "array_schema_evolution_api_experimental.h"

#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema_evolution.h"

/** Handle `struct` for API array schema evolution objects. */
struct tiledb_array_schema_evolution_t {
  tiledb::sm::ArraySchemaEvolution* array_schema_evolution_ = nullptr;
};

namespace tiledb::api {

inline int32_t sanity_check(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_evolution_t* schema_evolution) {
  if (schema_evolution == nullptr ||
      schema_evolution->array_schema_evolution_ == nullptr) {
    auto st = Status_Error("Invalid TileDB array schema evolution object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_ARRAY_SCHEMA_EVOLUTION_INTERNAL_H
