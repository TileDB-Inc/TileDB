/**
 * @file curl_init.cc
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

#include <mutex>

#include "curl_init.h"
#include "tiledb/common/exception/exception.h"

#ifdef TILEDB_SERIALIZATION
#include <curl/curl.h>
// This ifdef'ed definition of LIBCURL_INIT only exists
// because the preprocessor is unable to parse
// CURL_GLOBAL_DEFAULT properly inside a constexpr block
// when the curl.h header hasn't been included.
#define LIBCURL_INIT curl_global_init(CURL_GLOBAL_DEFAULT)
#else
#define LIBCURL_INIT 0
#endif

namespace tiledb::sm::curl {

/* ********************************* */
/*          GLOBAL VARIABLES         */
/* ********************************* */

/** Ensures that the cURL library is only initialized once per process. */
static std::once_flag curl_lib_initialized;

LibCurlInitializer::LibCurlInitializer() {
  std::call_once(curl_lib_initialized, []() {
    auto rc = LIBCURL_INIT;
    if (rc != 0) {
      throw common::StatusException(
          "[TileDB::CurlInit]",
          "Cannot initialize libcurl global state: got non-zero return code " +
              std::to_string(rc));
    }
  });
}

}  // namespace tiledb::sm::curl
