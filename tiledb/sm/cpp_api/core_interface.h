/**
 * @file   tiledb_cpp_api_core_interface.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

/** Convert a tiledb_datatype_t to a string representation. **/
inline std::string to_str(const tiledb_datatype_t& type) {
  const char* c_str = nullptr;
  tiledb_datatype_to_str(type, &c_str);
  return std::string(c_str);
}

}  // namespace impl
}  // namespace tiledb

#endif  // TILEDB_CPP_API_CORE_INTERFACE_H
