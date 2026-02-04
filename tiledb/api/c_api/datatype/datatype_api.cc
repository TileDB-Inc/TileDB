/**
 * @file tiledb/api/c_api/datatype/datatype_api.cc
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
 * This file defines the datatype section of the C API for TileDB.
 */

#include "datatype_api_external.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/enums/datatype.h"

namespace tiledb::api {

capi_return_t tiledb_datatype_to_str(
    tiledb_datatype_t datatype, const char** str) {
  const auto& strval = tiledb::sm::datatype_str((tiledb::sm::Datatype)datatype);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_datatype_from_str(
    const char* str, tiledb_datatype_t* datatype) {
  *datatype = (tiledb_datatype_t)tiledb::sm::datatype_enum(str);
  return TILEDB_OK;
}

uint64_t tiledb_datatype_size(tiledb_datatype_t type) {
  return tiledb::sm::datatype_size(static_cast<tiledb::sm::Datatype>(type));
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;

CAPI_INTERFACE(datatype_to_str, tiledb_datatype_t datatype, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_datatype_to_str>(datatype, str);
}

CAPI_INTERFACE(
    datatype_from_str, const char* str, tiledb_datatype_t* datatype) {
  return api_entry_plain<tiledb::api::tiledb_datatype_from_str>(str, datatype);
}

CAPI_INTERFACE_VALUE(uint64_t, datatype_size, tiledb_datatype_t type) {
  return api_entry_plain<tiledb::api::tiledb_datatype_size>(type);
}
