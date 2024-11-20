/**
 * @file tiledb/api/c_api/query_plan/query_plan_api.cc
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
 * This file defines the query_plan C API of TileDB.
 **/

#include "../string/string_api_internal.h"
#include "query_plan_api_external_experimental.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

#include "tiledb/sm/query_plan/query_plan.h"

namespace tiledb::api {

capi_return_t tiledb_query_get_plan(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_string_handle_t** rv) {
  // unused for now
  (void)ctx;

  if (query == nullptr) {
    throw CAPIStatusException("argument `query` may not be nullptr");
  }

  sm::QueryPlan plan(*query->query_);
  *rv = make_handle<tiledb_string_handle_t>(plan.dump_json());

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    query_get_plan,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_string_handle_t** rv) {
  return api_entry_with_context<tiledb::api::tiledb_query_get_plan>(
      ctx, query, rv);
}
