/**
 * @file   tiledb_cpp_api_core_interface.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * Interface between core and C++ API. This is used for C++ specific funcs,
 * implemented in tiledb.cc
 */

#ifndef TILEDB_CPP_API_CORE_INTERFACE_H
#define TILEDB_CPP_API_CORE_INTERFACE_H

#include "tiledb.h"

#include <functional>
#include <string>

namespace tiledb {
namespace impl {

/**
 * @cond
 * Doxygen is disabled for this function, as it conflicts with the C
 * API function of the same name.
 *
 * Submits a TileDB query in asynchronous mode.
 *
 * @param ctx The TileDB context.
 * @param query The query to be submitted.
 * @param callback A pointer to a std::function<void(void*)>.
 * The function is copied.
 * @param callback_data Data to pass callback
 * @return TILEDB_OK for success and TILEDB_ERR for error.
 */
TILEDB_EXPORT int tiledb_query_submit_async_func(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void* callback_func,
    void* callback_data = nullptr);
/** @endcond */

inline size_t type_size(tiledb_datatype_t type) {
  return tiledb_datatype_size(type);
}

/** Convert a tiledb_datatype_t to a string repersentation. **/
inline std::string to_str(const tiledb_datatype_t& type) {
  switch (type) {
    case TILEDB_CHAR:
      return "char";
    case TILEDB_INT8:
      return "int8";
    case TILEDB_UINT8:
      return "uint8";
    case TILEDB_INT16:
      return "int16";
    case TILEDB_UINT16:
      return "uint16";
    case TILEDB_INT32:
      return "int32";
    case TILEDB_UINT32:
      return "uint32";
    case TILEDB_INT64:
      return "int64";
    case TILEDB_UINT64:
      return "uint64";
    case TILEDB_FLOAT32:
      return "float32";
    case TILEDB_FLOAT64:
      return "float64";
    case TILEDB_STRING_ASCII:
      return "string_ascii";
    case TILEDB_STRING_UTF8:
      return "string_utf8";
    case TILEDB_STRING_UTF16:
      return "string_utf16";
    case TILEDB_STRING_UTF32:
      return "string_utf32";
    case TILEDB_STRING_UCS2:
      return "string_ucs2";
    case TILEDB_STRING_UCS4:
      return "string_ucs4";
    case TILEDB_ANY:
      return "any";
  }
  return "";
}

}  // namespace impl
}  // namespace tiledb

#endif  // TILEDB_CPP_API_CORE_INTERFACE_H
