/**
 * @file tiledb/api/c_api/data_order/data_order_api.cc
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
 * This file defines the data order section of the C API for TileDB.
 */

#include "data_order_api_external.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/enums/data_order.h"

namespace tiledb::api {

capi_return_t tiledb_data_order_to_str(
    tiledb_data_order_t data_order, const char** str) {
  const auto& strval = tiledb::sm::data_order_str(
      static_cast<tiledb::sm::DataOrder>(data_order));
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_data_order_from_str(
    const char* str, tiledb_data_order_t* data_order) {
  *data_order =
      static_cast<tiledb_data_order_t>(tiledb::sm::data_order_from_str(str));
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;

CAPI_INTERFACE(
    data_order_to_str, tiledb_data_order_t data_order, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_data_order_to_str>(
      data_order, str);
}

CAPI_INTERFACE(
    data_order_from_str, const char* str, tiledb_data_order_t* data_order) {
  return api_entry_plain<tiledb::api::tiledb_data_order_from_str>(
      str, data_order);
}
