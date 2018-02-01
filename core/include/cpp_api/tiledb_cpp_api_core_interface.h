/**
 * @file   tdbpp_cpp_api_core_interface.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#ifndef TILEDB_TILEDB_CPP_API_CORE_INTERFACE_H
#define TILEDB_TILEDB_CPP_API_CORE_INTERFACE_H

#include "tiledb.h"

#include <functional>

namespace tdb {
namespace impl {

/**
 * Submits a TileDB query in asynchronous mode.
 *
 * @param ctx The TileDB context.
 * @param query The query to be submitted.
 * @param callback The function to be called when the query completes.
 * @param callback_data Data to pass callback
 * @return TILEDB_OK for success and TILEDB_OOM or TILEDB_ERR for error.
 *
 * @note This function essentially opens the array associated with the query.
 *     Some metadata structures are loaded in main memory for this array.
 */
int tiledb_query_submit_async(tiledb_ctx_t* ctx,
                              tiledb_query_t* query,
                              std::function<void(void*)> callback,
                              void* callback_data=nullptr);

}
}

#endif //TILEDB_TILEDB_CPP_API_CORE_INTERFACE_H
