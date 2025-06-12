/**
 * @file   curl.h
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
 * This file declares a high-level libcurl helper class.
 */

#ifndef TILEDB_CURL_H
#define TILEDB_CURL_H

#ifdef TILEDB_SERIALIZATION

#if !defined(NOMINMAX)
#define NOMINMAX  // curl may include windows headers
#endif

#include <curl/curl.h>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/buffer_list.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/curl/curl_init.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/stats/stats.h"

using namespace tiledb::common;

namespace tiledb {

namespace common {
class Logger;
}

namespace sm {

/**
 * Exception class with `RestClient` origin
 */
class CurlException : public StatusException {
 public:
  explicit CurlException(
      const std::string& message,
      int curl_code = CURL_NONE,
      int http_code = HTTP_NONE)
      : StatusException("Curl", message)
      , curl_code_(curl_code)
      , http_code_(http_code) {
  }

  [[nodiscard]] int curl_code() const {
    return curl_code_;
  }

  [[nodiscard]] int http_code() const {
    return http_code_;
  }

  enum { CURL_NONE = -2, HTTP_NONE };

 private:
  int curl_code_;
  int http_code_;
};

/**
 * Helper class offering a high-level wrapper over some libcurl functions.
 *
 * Note: because the underlying libcurl handle is not threadsafe, the interface
 * presented by this class is not threadsafe either. See
 * https://curl.haxx.se/libcurl/c/threadsafe.html
 */

/**
 * Wraps opaque user data to be invoked with a header callback.
 */
struct HeaderCbData {
  /** The output of parse::rest_components from url -> array_ns:array_uri */
  const std::string* uri;

  /** A pointer to the map in REST client caching the redirections */
  std::unordered_map<std::string, std::string>* redirect_uri_map;

  /** A pointer to the lock attached to the shared resource of the cache map */
  std::mutex* redirect_uri_map_lock;

  /** True if the uri should be stored in URI cache map, false if not */
  bool should_cache_redirect;
};

/**
 * Wraps opaque user data to be invoked with a write callback.
 */
struct WriteCbState {
  /** Default constructor. */
  WriteCbState()
      : reset(true)
      , arg(NULL)
      , skip_retries(false) {
  }

  /** True if this is the first write callback invoked in a request retry. */
  bool reset;

  /** The opaque user data to pass to the write callback. */
  void* arg;

  /** True if the internal curl retries should be skipped. */
  bool skip_retries;
};

/**
 * This callback function gets called by libcurl as soon as a header has been
 * received. libcurl buffers headers and delivers only "full" headers, one by
 * one, to this callback. This callback should return the number of bytes
 * actually taken care of if that number differs from the number passed to
 * your callback function, it signals an error condition to the library
 *
 * @param res_data points to the delivered data
 * @param size the size of that data is size multiplied with nmemb
 * @param count number of bytes actually taken care of
 * @param userdata the userdata argument is set with CURLOPT_HEADERDATA
 * @return
 */
size_t write_header_callback(
    void* res_data, size_t size, size_t count, void* userdata);

class Curl {
 public:
  /** Constructor. */
  explicit Curl(const std::shared_ptr<Logger>& logger);

  /** Destructor. */
  ~Curl() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(Curl);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Curl);

  /**
   * Initializes the class.
   *
   * @param config TileDB config storing server/auth information
   * @param extra_headers Any additional headers to attach to each request.
   * @param res_ns_uri Pointer to Array namespace : Array URI cache key
   * @param res_headers Pointer to cache map
   * @param res_mtx Pointer to mtx that handles the lock of the cache map
   * @return Status
   */
  Status init(
      const Config* config,
      const std::unordered_map<std::string, std::string>& extra_headers,
      std::unordered_map<std::string, std::string>* res_headers,
      std::mutex* res_mtx,
      bool should_cache_redirect = true);

  /**
   * Escapes the given URL.
   *
   * @param url URL to escape
   * @return Escaped URL
   */
  std::string url_escape(const std::string& url) const;

  /**
   * Escapes the given namespace REST component.
   * For REST 3.0 we escape the workspace and teamspace components, preserving
   * the path seperator between them.
   * Legacy REST namespace will be returned as-is, since naming requirements
   * disallows using characters that require URL encoding.
   *
   * @param ns The namespace or workspace/teamspace to escape.
   * @return Escaped namespace component.
   */
  std::string url_escape_namespace(const std::string& ns) const;

  /**
   * Wrapper for sending patch request to server and returning
   * the unbuffered response body.
   *
   * @param stats The stats instance to record into
   * @param url URL to post to
   * @param serialization_type Serialization type to use
   * @param data Encoded data buffer for posting
   * @param returned_data Buffer to store response data
   * @param res_ns_uri Array Namespace and URI
   * @return Status
   */
  Status patch_data(
      stats::Stats* stats,
      const std::string& url,
      SerializationType serialization_type,
      BufferList* data,
      Buffer* returned_data,
      const std::string& res_ns_uri);

  /**
   * Wrapper for posting data to server and returning
   * the unbuffered response body.
   *
   * @param stats The stats instance to record into
   * @param url URL to post to
   * @param serialization_type Serialization type to use
   * @param data Encoded data buffer for posting
   * @param returned_data Buffer to store response data
   * @param res_ns_uri Array Namespace and URI
   * @return Status
   */
  Status post_data(
      stats::Stats* stats,
      const std::string& url,
      SerializationType serialization_type,
      BufferList* data,
      Buffer* returned_data,
      const std::string& res_ns_uri);

  /**
   * Wrapper for sending put request to server and returning
   * the unbuffered response body.
   *
   * @param stats The stats instance to record into
   * @param url URL to post to
   * @param serialization_type Serialization type to use
   * @param data Encoded data buffer for posting
   * @param returned_data Buffer to store response data
   * @param res_ns_uri Array Namespace and URI
   * @return Status
   */
  Status put_data(
      stats::Stats* stats,
      const std::string& url,
      SerializationType serialization_type,
      BufferList* data,
      Buffer* returned_data,
      const std::string& res_ns_uri);

  /**
   * Wrapper for sending options request to server.
   *
   * @param stats The stats instance to record into
   * @param url URL to post to
   * @param serialization_type Serialization type to use
   * @param returned_data Buffer to store response data
   * @param res_ns_uri Array Namespace and URI
   * @return Status
   */
  Status options(
      stats::Stats* const stats,
      const std::string& url,
      SerializationType serialization_type,
      Buffer* returned_data,
      const std::string& res_ns_uri);

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
   * @param stats The stats instance to record into
   * @param url URL to post to
   * @param serialization_type Serialization type to use
   * @param data Encoded data buffer for posting
   * @param returned_data Buffer to store response data
   * @param write_cb Invoked as response body buffers are received.
   * @param res_ns_uri Array Namespace and URI
   * @return Status
   */
  Status post_data(
      stats::Stats* stats,
      const std::string& url,
      SerializationType serialization_type,
      BufferList* data,
      Buffer* returned_data,
      PostResponseCb&& write_cb,
      const std::string& res_ns_uri);

  /**
   * Common code shared between variants of 'patch_data'.
   *
   * @param serialization_type Serialization type to use
   * @param data Encoded data buffer for posting
   * @param headers Request headers that must be freed after the curl
   *    request is complete. If this routine returns a non-OK status,
   *    the value returned through this parameter should be ignored.
   * @return Status
   */
  Status patch_data_common(
      SerializationType serialization_type,
      BufferList* data,
      struct curl_slist** headers);

  /**
   * Common code shared between variants of 'put_data'.
   *
   * @param serialization_type Serialization type to use
   * @param data Encoded data buffer for posting
   * @param headers Request headers that must be freed after the curl
   *    request is complete. If this routine returns a non-OK status,
   *    the value returned through this parameter should be ignored.
   * @return Status
   */
  Status put_data_common(
      SerializationType serialization_type,
      BufferList* data,
      struct curl_slist** headers);

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
      BufferList* data,
      struct curl_slist** headers);

  /**
   * Simple wrapper for getting data from server
   *
   * @param stats The stats instance to record into
   * @param url URL to get
   * @param serialization_type Serialization type to use
   * @param returned_data Buffer to store response data
   * @param res_ns_uri Array Namespace and URI
   */
  void get_data(
      stats::Stats* stats,
      const std::string& url,
      SerializationType serialization_type,
      Buffer* returned_data,
      const std::string& res_ns_uri);

  /**
   * Simple wrapper for sending delete requests to server
   *
   * @param stats The stats instance to record into
   * @param config TileDB config storing server/auth information
   * @param url URL to delete
   * @param serialization_type Serialization type to use
   * @param returned_data Buffer to store response data
   * @param res_ns_uri Array Namespace and URI
   * @return Status
   */
  Status delete_data(
      stats::Stats* stats,
      const std::string& url,
      SerializationType serialization_type,
      Buffer* returned_data,
      const std::string& res_ns_uri);

  /**
   * Get HTTP status code of last request
   *
   * @return tuple of status and last_request
   */
  tuple<Status, optional<long>> last_http_status_code();

  /**
   * Checks if the request should be retried
   *
   * @param curl_code the curl code of the attempted request
   * @param http_code http code to check
   * @return retry or not
   */
  bool should_retry_request(CURLcode curl_code, long http_request) const;

 private:
  /**
   * A libcurl initializer instance. This should remain
   * the first member variable to ensure that libcurl is
   * initialized before any calls that may require it.
   */
  tiledb::sm::curl::LibCurlInitializer curl_inited_;

  /** TileDB config parameters. */
  const Config* config_;

  /** Underlying C curl instance. */
  std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl_;

  /** String buffer that will be used by libcurl to store error messages. */
  Buffer curl_error_buffer_;

  /** Extra headers to attach to each request. */
  std::unordered_map<std::string, std::string> extra_headers_;

  /** Response headers attached to each response. */
  HeaderCbData headerData;

  /** Number of times to attempt retry. */
  uint64_t retry_count_;

  /** Retry backoff factor. */
  double retry_delay_factor_;

  /** Initial delay in milliseconds before attempting retry. */
  uint64_t retry_initial_delay_ms_;

  /** List of http status codes to retry. */
  std::vector<uint32_t> retry_http_codes_;

  /** The class logger. */
  std::shared_ptr<Logger> logger_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** Verbose logging in curl. */
  bool verbose_;

  /** Max curl buffer size for received data. */
  uint64_t curl_buffer_size_;

  /* Retry requests with curl errors */
  bool retry_curl_errors_;

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
   * @param stats The stats instance to record into
   * @param url URL to fetch
   * @param curl_code Set to the return value of the curl call
   * @param data Encoded data buffer for sending, if applicable to the request
   * @param returned_data Buffer that will store the response data
   * @return Status
   */
  Status make_curl_request(
      stats::Stats* const stats,
      const char* url,
      CURLcode* curl_code,
      BufferList* data,
      Buffer* returned_data) const;

  /**
   * Makes the configured curl request to the given URL, writing partial
   * response data to the 'cb' callback as the response is received.
   *
   * @param stats The stats instance to record into
   * @param url URL to fetch
   * @param curl_code Set to the return value of the curl call
   * @param data Encoded data buffer for sending, if applicable to the request
   * @param write_cb Callback to invoke as response data is received
   * @return Status
   */
  Status make_curl_request(
      stats::Stats* stats,
      const char* url,
      CURLcode* curl_code,
      BufferList* data,
      PostResponseCb&& write_cb) const;

  /**
   * Common code shared between variants of 'make_curl_request'.
   *
   * @param stats The stats instance to record into
   * @param url URL to fetch
   * @param curl_code Set to the return value of the curl call
   * @param data Encoded data buffer for sending, if applicable to the request
   * @param write_cb Callback to invoke as response data is received.
   * @param write_arg Opaque memory address passed to 'write_cb'.
   * @return Status
   */
  Status make_curl_request_common(
      stats::Stats* stats,
      const char* url,
      CURLcode* curl_code,
      BufferList* data,
      size_t (*write_cb)(void*, size_t, size_t, void*),
      void* write_arg) const;

  /**
   * Instruments and then executes curl_easy_perform. Instrumentation
   * is meant to provide crucial information on core-to-REST-server HTTP
   * operations which is essential for analyzing and minimizing remote-request
   * latencies. An indispensable counterpart to Jaeger tracing, while Jaeger
   * tracing isn't enough to give us a full picture on all interactions in all
   * contexts.
   *
   * Easiest instrumentation enable:
   *     export TILEDB_CONFIG_LOGGING_LEVEL=5
   *
   * @param url URL to fetch.
   * @param retry_number The time this request is being retried.
   */
  CURLcode curl_easy_perform_instrumented(
      const char* const url, const uint8_t retry_number) const;

  /**
   * Set needed options on the curl request
   *
   * @param url URL to fetch
   * @param write_cb Callback to invoke as response data is received.
   * @param write_cb_state A data pointer to pass to the write callback.
   */
  void set_curl_request_options(
      const char* const url,
      size_t (*write_cb)(void*, size_t, size_t, void*),
      WriteCbState& write_cb_state) const;

  /**
   * Check the given curl code for errors, returning a TileDB error status if
   * so.
   *
   * @param curl_code Curl return code to check for errors
   * @param operation String describing operation that was performed
   * @param returned_data Optional buffer containing response data from server.
   * @return Status
   */
  Status check_curl_errors(
      CURLcode curl_code,
      const std::string& operation,
      const Buffer* returned_data = NULL) const;

  /**
   * Check the given curl code for errors, throwing CurlException if an error is
   * found.
   *
   * @param curl_code Curl return code to check for errors
   * @param operation String describing operation that was performed
   * @param returned_data Optional buffer containing response data from server.
   */
  void check_curl_errors_v2(
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

  /**
   * Checks the curl http status code to see if it matches a list of http
   * requests to retry
   * @param http_code http code to check
   * @return true if should be retried, false otherwise
   */
  bool should_retry_based_on_http_status(long http_code) const;

  /**
   * Checks the curl return code to see if the request should be retried
   *
   * @param curl_code the curl code to check
   * @return retry or not
   */
  bool should_retry_based_on_curl_code(CURLcode curl_code) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION

#endif  // TILEDB_CURL_H
