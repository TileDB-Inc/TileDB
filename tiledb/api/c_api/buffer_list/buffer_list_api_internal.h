/**
 * @file tiledb/api/c_api/buffer_list/buffer_list_api_internal.h
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
 * This file declares the internal buffer list section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_BUFFER_LIST_API_INTERNAL_H
#define TILEDB_CAPI_BUFFER_LIST_API_INTERNAL_H

//#include "../../c_api_support/handle/handle.h"
#include "tiledb/sm/buffer/buffer_list.h"

namespace tiledb::api {

/**
 * Returns if the argument is a valid buffer list: non-null, valid as a handle
 *
 * @param buffer_list A buffer list of unknown validity
 */
inline int32_t ensure_buffer_list_is_valid(
    const tiledb_buffer_list_t* buffer_list) {
  // #TODO Change to inline void, ensure_handle_is_valid(buffer_list)
  if (buffer_list == nullptr || buffer_list->buffer_list_ == nullptr) {
    auto st = Status_Error("Invalid TileDB buffer list object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_BUFFER_LIST_API_INTERNAL_H
