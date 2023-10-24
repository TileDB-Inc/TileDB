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
  tiledb::sm::Datatype val = tiledb::sm::Datatype::UINT8;
  if (!tiledb::sm::datatype_enum(str, &val).ok()) {
    return TILEDB_ERR;
  }
  *datatype = (tiledb_datatype_t)val;
  return TILEDB_OK;
}

uint64_t tiledb_datatype_size(tiledb_datatype_t type) {
  return tiledb::sm::datatype_size(static_cast<tiledb::sm::Datatype>(type));
}

}  // namespace tiledb::api

TILEDB_CAPI_NAME_TRAIT(tiledb::api::tiledb_datatype_from_str);
TILEDB_CAPI_NAME_TRAIT(tiledb::api::tiledb_datatype_to_str);

using tiledb::api::api_entry_plain;

capi_return_t tiledb_datatype_to_str(
    tiledb_datatype_t datatype, const char** str) noexcept {
  return api_entry_plain<tiledb::api::tiledb_datatype_to_str>(datatype, str);
}

capi_return_t tiledb_datatype_from_str(
    const char* str, tiledb_datatype_t* datatype) noexcept {
  return api_entry_plain<tiledb::api::tiledb_datatype_from_str>(str, datatype);
}

uint64_t tiledb_datatype_size(tiledb_datatype_t type) noexcept {
  return tiledb::sm::datatype_size(static_cast<tiledb::sm::Datatype>(type));
}
