/**
 * @file   curl.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2019 TileDB, Inc.
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
 * This file declares a high-level libcurl helper class.
 */

#ifndef TILEDB_CURL_H
#define TILEDB_CURL_H

#ifdef TILEDB_SERIALIZATION

#include <curl/curl.h>
#include <cstdlib>
#include <string>

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/storage_manager/config.h"

namespace tiledb {
namespace sm {

/**
 * Helper class offering a high-level wrapper over some libcurl functions.
 *
 * Note: because the underlying libcurl handle is not threadsafe, the interface
 * presented by this class is not threadsafe either. See
 * https://curl.haxx.se/libcurl/c/threadsafe.html
 */
class Curl {
 public:
  /** Constructor. */
  Curl();

  /**
   * Initializes the class.
   *
   * @param config TileDB config storing server/auth information
   * @return Status
   */
  Status init(const Config* config);

  /**
   * Escapes the given URL.
   *
   * @param url URL to escape
   * @return Escaped URL
   */
  std::string url_escape(const std::string& url) const;

  /**
   * Simple wrapper for posting data to server.
   *
   * @param url URL to post to
   * @param serialization_type Serialization type to use
   * @param data Encoded data buffer for posting
   * @param returned_data Buffer to store response data
   * @return Status
   */
  Status post_data(
      const std::string& url,
      SerializationType serialization_type,
      Buffer* data,
      Buffer* returned_data);

  /**
   * Simple wrapper for getting data from server
   *
   * @param url URL to get
   * @param serialization_type Serialization type to use
   * @param returned_data Buffer to store response data
   * @return Status
   */
  Status get_data(
      const std::string& url,
      SerializationType serialization_type,
      Buffer* returned_data);

  /**
   * Simple wrapper for sending delete requests to server
   *
   * @param config TileDB config storing server/auth information
   * @param url URL to delete
   * @param serialization_type Serialization type to use
   * @param returned_data Buffer to store response data
   * @return Status
   */
  Status delete_data(
      const std::string& url,
      SerializationType serialization_type,
      Buffer* returned_data);

 private:
  /** TileDB config parameters. */
  const Config* config_;

  /** Underlying C curl instance. */
  std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl_;

  /**
   * Sets authorization (token or username+password) on the curl instance using
   * the given config instance.
   *
   * @param headers Headers (may be modified)
   * @return Status
   */
  Status set_auth(struct curl_slist** headers) const;

  /**
   * Sets the appropriate Content-Type header for the given serialization type.
   *
   * @param serialization_type Serialization type
   * @param headers Headers to be modified
   * @return Status
   */
  Status set_content_type(
      SerializationType serialization_type, struct curl_slist** headers) const;

  /**
   * Makes the configured curl request to the given URL, storing response data
   * in the given buffer.
   *
   * @param url URL to fetch
   * @param curl_code Set to the return value of the curl call
   * @param returned_data Buffer that will store the response data
   * @return Status
   */
  Status make_curl_request(
      const char* url, CURLcode* curl_code, Buffer* returned_data) const;

  /**
   * Check the given curl code for errors, returning a TileDB error status if
   * so.
   *
   * @param curl_code Curl return code to check for errors
   * @param operation String describing operation that was performed
   * @param returned_data Buffer containing any response data from server
   * @return Status
   */
  Status check_curl_errors(
      CURLcode curl_code,
      const std::string& operation,
      const Buffer* returned_data) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION

#endif  // TILEDB_CURL_H
