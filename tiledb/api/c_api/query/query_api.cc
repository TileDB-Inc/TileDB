/**
 * @file tiledb/api/c_api/query/query_api.cc
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
 * This file defines the query section of the C API for TileDB.
 */

#include "query_api_external.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/enums/query_type.h"

namespace tiledb::api {

int32_t tiledb_query_type_to_str(
    tiledb_query_type_t query_type, const char** str) {
  const auto& strval =
      tiledb::sm::query_type_str((tiledb::sm::QueryType)query_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_query_type_from_str(
    const char* str, tiledb_query_type_t* query_type) {
  tiledb::sm::QueryType val = tiledb::sm::QueryType::READ;
  if (!tiledb::sm::query_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *query_type = (tiledb_query_type_t)val;
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;

CAPI_INTERFACE(
    query_type_to_str, tiledb_query_type_t query_type, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_query_type_to_str>(
      query_type, str);
}

CAPI_INTERFACE(
    query_type_from_str, const char* str, tiledb_query_type_t* query_type) {
  return api_entry_plain<tiledb::api::tiledb_query_type_from_str>(
      str, query_type);
}
