/**
 * @file   libcurl_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 This file initializes the libcurl state, if libcurl is present.
 */

#include "tiledb/sm/global_state/libcurl_state.h"
#include "tiledb/common/logger.h"

#ifdef TILEDB_SERIALIZATION
#include <curl/curl.h>
#endif

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace global_state {

Status init_libcurl() {
#ifdef TILEDB_SERIALIZATION
  auto rc = curl_global_init(CURL_GLOBAL_DEFAULT);
  if (rc != 0)
    return LOG_STATUS(Status_Error(
        "Cannot initialize libcurl global state: got non-zero return code " +
        std::to_string(rc)));
#endif

  return Status::Ok();
}

}  // namespace global_state
}  // namespace sm
}  // namespace tiledb
