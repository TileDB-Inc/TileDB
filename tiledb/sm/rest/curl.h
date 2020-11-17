/**
 * @file   curl.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/serialization_type.h"

using namespace tiledb::common;

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
   * @param extra_headers Any additional headers to attach to each request.
   * @return Status
   */
  Status init(
      const Config* config,
      const std::unordered_map<std::string, std::string>& extra_headers,
      const std::unordered_map<std::string, std::string>& res_headers);

  /**
   * Escapes the given URL.
   *
   * @param url URL to escape
   * @return Escaped URL
   */
  std::string url_escape(const std::string& url) const;

  /**
   * Wrapper for posting data to server and returning
   * the unbuffered response body.
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
      const BufferList* data,
      Buffer* returned_data);

  /**
   * Callback defined by the caller of the 'post_data' variant for
   * receiving buffered response data.
   *
   * @param reset True if this is the first callback from a new request
   * @param contents Response data
   * @param content_nbytes Response data size in 'contents'
   * @param skip_retries Output argument that can be set to 'true' to skip
   *        retries in the curl layer.
   * @return Number of acknowledged bytes.
   */
  typedef std::function<size_t(
      bool reset, void* contents, size_t content_nbytes, bool* skip_retries)>
      PostResponseCb;

  /**
   * Wrapper for posting data to server and returning
   * a buffered response body.
   *
   * @param url URL to post to
   * @param serialization_type Serialization type to use
   * @param data Encoded data buffer for posting
   * @param write_cb Invoked as response body buffers are received.
   * @return Status
   */
  Status post_data(
      const std::string& url,
      SerializationType serialization_type,
      const BufferList* data,
      PostResponseCb&& write_cb);

  /**
   * Common code shared between variants of 'post_data'.
   *
   * @param serialization_type Serialization type to use
   * @param data Encoded data buffer for posting
   * @param headers Request headers that must be freed after the curl
   *    request is complete. If this routine returns a non-OK status,
   *    the value returned through this parameter should be ignored.
   * @return Status
   */
  Status post_data_common(
      SerializationType serialization_type,
      const BufferList* data,
      struct curl_slist** headers);

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

  /** String buffer that will be used by libcurl to store error messages. */
  Buffer curl_error_buffer_;

  /** Extra headers to attach to each request. */
  std::unordered_map<std::string, std::string> extra_headers_;

  /** Response headers attached to each response. */
  std::unordered_map<std::string, std::string> res_headers_;

  /**
   * This callback function gets called by libcurl as soon as a header has been
   * received. libcurl buffers headers and delivers only "full" headers, one by
   * one, to this callback. This callback should return the number of bytes
   * actually taken care of if that number differs from the number passed to
   * your callback function, it signals an error condition to the library
   *
   * @param res_data points to the delivered data
   * @param tSize the size of that data is size multiplied with nmemb
   * @param tCount number of bytes actually taken care of
   * @param userdata the userdata argument is set with CURLOPT_HEADERDATA
   * @return
   */
  static size_t OnReceiveData(
      void* res_data, size_t tSize, size_t tCount, void* userdata);

  /**
   * Populates the curl slist with authorization (token or username+password),
   * and any extra headers.
   *
   * @param headers Headers list (will be modified)
   * @return Status
   */
  Status set_headers(struct curl_slist** headers) const;

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
   * Makes the configured curl request to the given URL, writing partial
   * response data to the 'cb' callback as the response is received.
   *
   * @param url URL to fetch
   * @param curl_code Set to the return value of the curl call
   * @param write_cb Callback to invoke as response data is received
   * @return Status
   */
  Status make_curl_request(
      const char* url, CURLcode* curl_code, PostResponseCb&& write_cb) const;

  /**
   * Common code shared between variants of 'make_curl_request'.
   *
   * @param url URL to fetch
   * @param curl_code Set to the return value of the curl call
   * @param write_cb Callback to invoke as response data is received.
   * @param write_arg Opaque memory address passed to 'write_cb'.
   * @return Status
   */
  Status make_curl_request_common(
      const char* url,
      CURLcode* curl_code,
      size_t (*write_cb)(void*, size_t, size_t, void*),
      void* write_arg) const;

  /**
   * Check the given curl code for errors, returning a TileDB error status if
   * so.
   *
   * @param curl_code Curl return code to check for errors
   * @param operation String describing operation that was performed
   * @param returned_data Buffer containing any response data from server.
   * Optional.
   * @return Status
   */
  Status check_curl_errors(
      CURLcode curl_code,
      const std::string& operation,
      const Buffer* returned_data = NULL) const;

  /**
   * Gets as detailed an error message as possible from libcurl.
   *
   * @param curl_code Curl return code to check for errors
   * @return Possibly empty error message string
   */
  std::string get_curl_errstr(CURLcode curl_code) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION

#endif  // TILEDB_CURL_H
