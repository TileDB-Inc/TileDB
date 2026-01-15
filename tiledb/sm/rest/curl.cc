/**
 * @file   curl.cc
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
 * This file defines a high-level libcurl helper class.
 */

#include "tiledb/sm/rest/curl.h"
#include "tiledb/common/assert.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/filesystem/ssl_config.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/stats/global_stats.h"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>

#ifdef _WIN32
#include <locale>  //provides needed visibility of two-argument tolower() prototype for win32
#include "curl.h"
#endif

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * @brief Callback for saving unbuffered libcurl response data
 *
 * This is called by libcurl as soon as there is data received that needs
 * to be saved.
 *
 * @param contents Pointer to received data
 * @param size Size of a member in the received data
 * @param nmemb Number of members in the received data
 * @param userdata User data attached to the callback. This points to
 *      a WriteCbState instance that wraps an instance of a Buffer.
 * @return Number of bytes copied from the received data
 */
size_t write_memory_callback(
    void* contents, size_t size, size_t nmemb, void* userdata) {
  const size_t content_nbytes = size * nmemb;
  auto write_cb_state = static_cast<WriteCbState*>(userdata);
  auto buffer = static_cast<Buffer*>(write_cb_state->arg);

  if (write_cb_state->reset) {
    buffer->set_size(0);
    buffer->reset_offset();
    write_cb_state->reset = false;
  }

  auto st = buffer->write(contents, content_nbytes);
  if (!st.ok()) {
    LOG_ERROR(
        "Cannot copy libcurl response data; buffer write failed: " +
        st.to_string());
    return 0;
  }

  return content_nbytes;
}

/**
 * @brief Callback for saving buffered libcurl response data
 *
 * This is called by libcurl as soon as there is data received that needs
 * to be saved.
 *
 * @param contents Pointer to received data
 * @param size Size of a member in the received data
 * @param nmemb Number of members in the received data
 * @param userdata User data attached to the callback This points to
 *      a WriteCbState instance that wraps an instance of a nested
 *      PostResponseCb callback.
 * @return Number of bytes copied from the received data
 */
size_t write_memory_callback_cb(
    void* contents, size_t size, size_t nmemb, void* userdata) {
  const size_t content_nbytes = size * nmemb;
  auto write_cb_state = static_cast<WriteCbState*>(userdata);
  auto user_cb = static_cast<Curl::PostResponseCb*>(write_cb_state->arg);

  const size_t bytes_received = (*user_cb)(
      write_cb_state->reset,
      contents,
      content_nbytes,
      &write_cb_state->skip_retries);
  write_cb_state->reset = false;

  return bytes_received;
}

/**
 * Callback for reading data to POST.
 *
 * This is called by libcurl when there is data from a BufferList being POSTed.
 *
 * @param dest Destination buffer to read into
 * @param size Size of a member in the dest buffer
 * @param nmemb Max number of members in the dest buffer
 * @param userdata User data attached to the callback (in our case will point to
 *      a BufferList instance)
 * @return Number of bytes copied into the dest buffer
 */
size_t buffer_list_read_memory_callback(
    void* dest, size_t size, size_t nmemb, void* userdata) {
  auto buffer_list = static_cast<BufferList*>(userdata);
  const size_t max_nbytes = size * nmemb;

  // The buffer list tracks the current offset internally.
  return buffer_list->read_at_most(dest, max_nbytes);
}

/**
 * Seek function to handle curl redirects
 * @param userp user data (buffer list)
 * @param offset offset to seek to
 * @param origin whence to seek from
 * @return SEEKFUNC status
 */
static int buffer_list_seek_callback(
    void* userp, curl_off_t offset, int origin) {
  BufferList* data = static_cast<BufferList*>(userp);
  try {
    data->seek(offset, origin);
    return CURL_SEEKFUNC_OK;
  } catch (...) {
    return CURL_SEEKFUNC_FAIL;
  }
}

size_t write_header_callback(
    void* res_data, size_t size, size_t count, void* userdata) {
  const size_t header_length = size * count;
  auto* const header_buffer = static_cast<char*>(res_data);
  auto* const pmHeader = static_cast<HeaderCbData*>(userdata);

  // If we have enabled caching of the redirect URI ensure it's not empty.
  // If disabled for this request, do not treat an empty asset URI as an error.
  if (pmHeader->should_cache_redirect && pmHeader->uri->empty()) {
    LOG_ERROR("Rest components as array_ns and array_uri cannot be empty");
    return 0;
  }

  std::string header(header_buffer, header_length);
  const size_t header_key_end_pos = header.find(": ");

  if (header_key_end_pos != std::string::npos) {
    std::string header_key = header.substr(0, header_key_end_pos);
    std::transform(
        header_key.begin(), header_key.end(), header_key.begin(), ::tolower);

    if (header_key == constants::redirection_header_key) {
      // Fetch the header value. Subtract 2 from the `header_length` to
      // remove the trailing CR LF ("\r\n") and subtract another 2
      // for the ": ".
      const std::string header_value = header.substr(
          header_key_end_pos + 2, header_length - header_key_end_pos - 4);

      // Find the http scheme
      const size_t header_scheme_end_pos = header_value.find("://");

      if (header_scheme_end_pos == std::string::npos) {
        LOG_ERROR(
            "The header `location` should have a value that includes the "
            "scheme in the URI.");
        return 0;
      }

      const std::string header_scheme =
          header_value.substr(0, header_scheme_end_pos);

      // Find the domain
      const std::string header_value_scheme_excl =
          header_value.substr(header_scheme_end_pos + 3, header_value.length());
      const size_t header_domain_end_pos = header_value_scheme_excl.find("/");

      if (header_domain_end_pos == std::string::npos) {
        LOG_ERROR(
            "The header `location` should have a value that includes the "
            "domain in the URI.");
        return 0;
      }

      const std::string header_value_domain =
          header_value_scheme_excl.substr(0, header_domain_end_pos);

      if (pmHeader->should_cache_redirect) {
        const std::string redirection_value =
            header_scheme + "://" + header_value_domain;
        std::unique_lock<std::mutex> rd_lck(*(pmHeader->redirect_uri_map_lock));
        (*pmHeader->redirect_uri_map)[*pmHeader->uri] = redirection_value;
      }
    }
  }

  return size * count;
}

Curl::Curl(const std::shared_ptr<Logger>& logger)
    : config_(nullptr)
    , curl_(nullptr, curl_easy_cleanup)
    , retry_count_(0)
    , retry_delay_factor_(0)
    , retry_initial_delay_ms_(0)
    , logger_(logger->clone("curl ", ++logger_id_))
    , verbose_(false)
    , retry_curl_errors_(true) {
}

Status Curl::init(
    const Config* config,
    const std::unordered_map<std::string, std::string>& extra_headers,
    std::unordered_map<std::string, std::string>* const res_headers,
    std::mutex* const res_mtx,
    bool should_cache_redirect) {
  if (config == nullptr)
    return LOG_STATUS(
        Status_RestError("Error initializing libcurl; config is null."));

  config_ = config;
  curl_.reset(curl_easy_init());
  extra_headers_ = extra_headers;
  headerData.redirect_uri_map = res_headers;
  headerData.redirect_uri_map_lock = res_mtx;
  headerData.should_cache_redirect = should_cache_redirect;

  // See https://curl.haxx.se/libcurl/c/threadsafe.html
  CURLcode rc = curl_easy_setopt(curl_.get(), CURLOPT_NOSIGNAL, 1);
  if (rc != CURLE_OK)
    return LOG_STATUS(Status_RestError(
        "Error initializing libcurl; failed to set CURLOPT_NOSIGNAL"));

  // For human-readable error messages
  RETURN_NOT_OK(curl_error_buffer_.realloc(CURL_ERROR_SIZE));
  std::memset(curl_error_buffer_.data(), 0, CURL_ERROR_SIZE);
  rc = curl_easy_setopt(
      curl_.get(), CURLOPT_ERRORBUFFER, curl_error_buffer_.data());
  if (rc != CURLE_OK)
    return LOG_STATUS(Status_RestError(
        "Error initializing libcurl; failed to set CURLOPT_ERRORBUFFER"));

  rc = curl_easy_setopt(
      curl_.get(), CURLOPT_HEADERFUNCTION, write_header_callback);
  if (rc != CURLE_OK)
    return LOG_STATUS(Status_RestError(
        "Error initializing libcurl; failed to set CURLOPT_HEADERFUNCTION"));

  /* set url to fetch */
  rc = curl_easy_setopt(curl_.get(), CURLOPT_HEADERDATA, &headerData);
  if (rc != CURLE_OK)
    return LOG_STATUS(Status_RestError(
        "Error initializing libcurl; failed to set CURLOPT_HEADERDATA"));

  SSLConfig ssl_cfg = RestSSLConfig(*config_);

  if (ssl_cfg.verify() == false) {
    curl_easy_setopt(curl_.get(), CURLOPT_SSL_VERIFYHOST, 0);
    curl_easy_setopt(curl_.get(), CURLOPT_SSL_VERIFYPEER, 0);
  }

  if (!ssl_cfg.ca_file().empty()) {
    curl_easy_setopt(curl_.get(), CURLOPT_CAINFO, ssl_cfg.ca_file().c_str());
  }

  if (!ssl_cfg.ca_path().empty()) {
    curl_easy_setopt(curl_.get(), CURLOPT_CAPATH, ssl_cfg.ca_path().c_str());
  }

  bool tcp_keepalive =
      config_->get<bool>("rest.curl.tcp_keepalive", Config::must_find);

  curl_easy_setopt(curl_.get(), CURLOPT_TCP_KEEPALIVE, tcp_keepalive ? 1L : 0L);

  retry_count_ = config_->get<uint64_t>("rest.retry_count", Config::must_find);
  retry_delay_factor_ =
      config_->get<double>("rest.retry_delay_factor", Config::must_find);
  retry_initial_delay_ms_ =
      config_->get<uint64_t>("rest.retry_initial_delay_ms", Config::must_find);
  {
    bool found;
    RETURN_NOT_OK(config_->get_vector<uint32_t>(
        "rest.retry_http_codes", &retry_http_codes_, &found));
    passert(found);
  }
  verbose_ = config_->get<bool>("rest.curl.verbose", Config::must_find);
  curl_buffer_size_ =
      config_->get<uint64_t>("rest.curl.buffer_size", Config::must_find);
  retry_curl_errors_ =
      config_->get<bool>("rest.curl.retry_errors", Config::must_find);

  return Status::Ok();
}

std::string Curl::url_escape(const std::string& url) const {
  if (curl_.get() == nullptr)
    return "";

  char* escaped_c =
      curl_easy_escape(curl_.get(), url.c_str(), (int)url.length());
  std::string escaped(escaped_c);
  curl_free(escaped_c);
  return escaped;
}

std::string Curl::url_escape_namespace(const std::string& ns) const {
  if (curl_.get() == nullptr) {
    return "";
  }

  // If the namespace contains a path seperator we are talking to 3.0 REST.
  auto separator = ns.find('/');
  if (separator != std::string::npos) {
    // Encode workspace and teamspace, preserving the path separator.
    std::string workspace, teamspace;
    workspace = ns.substr(0, separator);
    teamspace = ns.substr(separator + 1, ns.size() - 1);
    return url_escape(workspace) + "/" + url_escape(teamspace);
  } else {
    // Legacy namespaces can only contain letters, numbers, and `-`. Encoding
    // this would be a noop so we can return as-is.
    return ns;
  }
}

Status Curl::set_headers(struct curl_slist** headers) const {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status_RestError("Cannot set auth; curl instance is null."));

  // Determine which authentication method to use based on priorities
  RestAuthMethod auth_method;
  try {
    auth_method = config_->get_effective_rest_auth_method();
  } catch (const StatusException& e) {
    return LOG_STATUS(e.extract_status());
  }

  if (auth_method == RestAuthMethod::TOKEN) {
    const char* token = nullptr;
    RETURN_NOT_OK(config_->get("rest.token", &token));

    *headers = curl_slist_append(
        *headers, (std::string("X-TILEDB-REST-API-Key: ") + token).c_str());
    if (*headers == nullptr)
      return LOG_STATUS(Status_RestError(
          "Cannot set curl auth; curl_slist_append returned null."));
  } else if (auth_method == RestAuthMethod::USERNAME_PASSWORD) {
    const char* username = nullptr;
    const char* password = nullptr;
    RETURN_NOT_OK(config_->get("rest.username", &username));
    RETURN_NOT_OK(config_->get("rest.password", &password));

    std::string basic_auth = username + std::string(":") + password;
    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
    curl_easy_setopt(curl, CURLOPT_USERPWD, basic_auth.c_str());
  } else {
    // auth_method == RestAuthMethod::NONE
    return LOG_STATUS(Status_RestError(
        "Missing TileDB authentication: either token or username/password "
        "must be set using the appropriate configuration parameters."));
  }

  // Add any extra headers.
  for (const auto& it : extra_headers_) {
    std::string hdr = it.first + ": " + it.second;
    *headers = curl_slist_append(*headers, hdr.c_str());
    if (*headers == nullptr) {
      curl_slist_free_all(*headers);
      return LOG_STATUS(Status_RestError(
          "Cannot set extra headers; curl_slist_append returned null."));
    }
  }

  return Status::Ok();
}

Status Curl::set_content_type(
    SerializationType serialization_type, struct curl_slist** headers) const {
  switch (serialization_type) {
    case SerializationType::JSON:
      *headers = curl_slist_append(*headers, "Content-Type: application/json");
      break;
    case SerializationType::CAPNP:
      *headers = curl_slist_append(*headers, "Content-Type: application/capnp");
      break;
    default:
      return LOG_STATUS(Status_RestError(
          "Cannot set content-type header; unknown serialization format."));
  }

  if (*headers == nullptr)
    return LOG_STATUS(Status_RestError(
        "Cannot set content-type header; curl_slist_append returned null."));

  return Status::Ok();
}

Status Curl::make_curl_request(
    stats::Stats* const stats,
    const char* url,
    CURLcode* curl_code,
    BufferList* data,
    Buffer* returned_data) const {
  return make_curl_request_common(
      stats,
      url,
      curl_code,
      data,
      &write_memory_callback,
      static_cast<void*>(returned_data));
}

Status Curl::make_curl_request(
    stats::Stats* const stats,
    const char* url,
    CURLcode* curl_code,
    BufferList* data,
    PostResponseCb&& cb) const {
  return make_curl_request_common(
      stats,
      url,
      curl_code,
      data,
      &write_memory_callback_cb,
      static_cast<void*>(&cb));
}

CURLcode Curl::curl_easy_perform_instrumented(
    const char* const url, const uint8_t retry_number) const {
  CURL* curl = curl_.get();
  // Time the curl transfer
  uint64_t t1 = utils::time::timestamp_now_ms();
  auto curl_code = curl_easy_perform(curl);
  uint64_t t2 = utils::time::timestamp_now_ms();
  uint64_t dt = t2 - t1;
  long http_code = 0;
  if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code) != CURLE_OK) {
    http_code = 999;
  }

  // Log time and details about the request
  std::stringstream ss;
  ss.precision(3);
  ss.setf(std::ios::fixed, std::ios::floatfield);
  ss << "OP=CORE-TO-REST";
  ss << ",SECONDS=" << (float)dt / 1000.0;
  ss << ",RETRY=" << int(retry_number);
  ss << ",CODE=" << http_code;
  ss << ",URL=" << url;
  logger_->trace(ss);

  return curl_code;
}

void Curl::set_curl_request_options(
    const char* const url,
    size_t (*write_cb)(void*, size_t, size_t, void*),
    WriteCbState& write_cb_state) const {
  CURL* curl = curl_.get();
  if (curl == nullptr) {
    throw std::runtime_error(
        "Cannot make curl request; curl instance is null.");
  }
  /* set url to fetch */
  CURLcode rc = curl_easy_setopt(curl, CURLOPT_URL, url);
  if (rc != CURLE_OK) {
    throw CurlException(
        "Error initializing libcurl; failed to set URL to fetch, libcurl error "
        "message: " +
        get_curl_errstr(rc));
  }

  /* set callback function */
  rc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
  if (rc != CURLE_OK) {
    throw CurlException(
        "Error initializing libcurl; failed to set callback function, libcurl "
        "error message: " +
        get_curl_errstr(rc));
  }

  /* pass fetch buffer pointer */
  rc = curl_easy_setopt(
      curl, CURLOPT_WRITEDATA, static_cast<void*>(&write_cb_state));
  if (rc != CURLE_OK) {
    throw CurlException(
        "Error initializing libcurl; failed to fetch buffer pointer, libcurl "
        "error message: " +
        get_curl_errstr(rc));
  }

  /* Set curl verbose mode */
  rc = curl_easy_setopt(curl, CURLOPT_VERBOSE, verbose_);
  if (rc != CURLE_OK) {
    throw CurlException(
        "Error initializing libcurl; failed to set curl verbose mode, libcurl "
        "error message: " +
        get_curl_errstr(rc));
  }

  /* set compression */
  const char* compressor = nullptr;
  throw_if_not_ok(config_->get("rest.http_compressor", &compressor));

  if (compressor != nullptr) {
    // curl expects lowecase strings so let's convert
    std::string comp(compressor);
    std::locale loc;
    for (std::string::size_type j = 0; j < comp.length(); ++j)
      comp[j] = std::tolower(comp[j], loc);

    if (comp != "none") {
      if (comp == "any") {
        comp = "";
      }
      rc = curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, comp.c_str());
      if (rc != CURLE_OK) {
        throw CurlException(
            "Error initializing libcurl; failed to set encoding, libcurl error "
            "message: " +
            get_curl_errstr(rc));
      }
    }
  }

  /* enable location redirects */
  rc = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
  if (rc != CURLE_OK) {
    throw CurlException(
        "Error initializing libcurl; failed to fetch buffer pointer, libcurl "
        "error message: " +
        get_curl_errstr(rc));
  }

  /* set maximum allowed redirects */
  rc = curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 1);
  if (rc != CURLE_OK) {
    throw CurlException(
        "Error initializing libcurl; failed to set maximum allowed redirects, "
        "libcurl error message: " +
        get_curl_errstr(rc));
  }
  /* enable forwarding auth to redirects */
  rc = curl_easy_setopt(curl, CURLOPT_UNRESTRICTED_AUTH, 1L);
  if (rc != CURLE_OK) {
    throw CurlException(
        "Error initializing libcurl; failed to enable forwarding auth to "
        "redirects, libcurl error message: " +
        get_curl_errstr(rc));
  }
  /* Set max buffer size */
  rc = curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, curl_buffer_size_);
  if (rc != CURLE_OK) {
    throw CurlException(
        "Error initializing libcurl; failed to set max buffer size, libcurl "
        "error message: " +
        get_curl_errstr(rc));
  }
}

Status Curl::make_curl_request_common(
    stats::Stats* const stats,
    const char* const url,
    CURLcode* const curl_code,
    BufferList* data,
    size_t (*write_cb)(void*, size_t, size_t, void*),
    void* const write_cb_arg) const {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status_RestError("Cannot make curl request; curl instance is null."));

  *curl_code = CURLE_OK;
  uint64_t retry_delay = retry_initial_delay_ms_;

  // Save the offsets before the request in case we need to retry
  size_t current_buffer_index = 0;
  uint64_t current_relative_offset = 0;
  if (data != nullptr) {
    std::tie(current_buffer_index, current_relative_offset) =
        data->get_offset();
  }

  stats->add_counter("rest_http_requests", 1);
  // <= because the 0ths retry is actually the initial request
  for (uint8_t i = 0; i <= retry_count_; i++) {
    WriteCbState write_cb_state;
    write_cb_state.arg = write_cb_arg;

    // If this is a retry we need to reset the offsets in the data buffer list
    // to the initial position before the failed request so that we send the
    // correct data.
    if (data != nullptr && retry_count_ > 0) {
      data->set_offset(current_buffer_index, current_relative_offset);
    }

    // set the necessary curl options on the request
    set_curl_request_options(url, write_cb, write_cb_state);

    /* perform the blocking network transfer */
    *curl_code = curl_easy_perform_instrumented(url, i);

    long http_code = 0;
    if (*curl_code == CURLE_OK) {
      if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code) !=
          CURLE_OK) {
        return LOG_STATUS(Status_RestError(
            "Error checking curl error; could not get HTTP code."));
      }
    }

    // Exit if the request failed and we don't want to retry based on curl or
    // HTTP code, or if the write callback has elected to skip retries
    if (!should_retry_request(*curl_code, http_code) ||
        write_cb_state.skip_retries) {
      break;
    }

    // Set up the actual retry logic
    // Only sleep if this isn't the last failed request allowed
    if (i < retry_count_ - 1) {
      if (*curl_code != CURLE_OK) {
        global_logger().debug(
            "Request to {} failed with Curl error message \"{}\", will sleep "
            "{}ms, "
            "retry count {}",
            url,
            get_curl_errstr(*curl_code),
            retry_delay,
            i);
      } else {
        global_logger().debug(
            "Request to {} failed with http response code {}, will sleep {}ms, "
            "retry count {}",
            url,
            http_code,
            retry_delay,
            i);
      }
      // Increment counter for number of retries
      stats->add_counter("rest_http_retries", 1);
      stats->add_counter("rest_http_retry_time", retry_delay);
      // Sleep for retry delay
      std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay));
      // Increment retry delay, cast to uint64_t and we can ignore any rounding
      retry_delay = static_cast<uint64_t>(retry_delay * retry_delay_factor_);
    }
  }

  return Status::Ok();
}

bool Curl::should_retry_based_on_http_status(long http_code) const {
  // Check http code for list of retries
  for (const auto& retry_code : retry_http_codes_) {
    if (http_code == (decltype(http_code))retry_code) {
      return true;
    }
  }

  return false;
}

bool Curl::should_retry_based_on_curl_code(CURLcode curl_code) const {
  if (!retry_curl_errors_) {
    return false;
  }

  switch (curl_code) {
    // Curl status of okay or non transient errors shouldn't be retried
    case CURLE_OK:
    case CURLE_UNSUPPORTED_PROTOCOL: /* 1 */
    case CURLE_URL_MALFORMAT:        /* 3 */
    case CURLE_SSL_ENGINE_NOTFOUND:  /* 53 - SSL crypto engine not found */
    case CURLE_SSL_ENGINE_SETFAILED: /* 54 - can not set SSL crypto engine as
                                       default */
    case CURLE_SSL_CERTPROBLEM: /* 58 - problem with the local certificate */
    case CURLE_SSL_CIPHER:      /* 59 - couldn't use specified cipher */
    case CURLE_PEER_FAILED_VERIFICATION: /* 60 - peer's certificate or
                                           fingerprint wasn't verified fine */
    case CURLE_SSL_ENGINE_INITFAILED:    /* 66 - failed to initialise ENGINE */
    case CURLE_SSL_CACERT_BADFILE:  /* 77 - could not load CACERT file, missing
                                      or wrong format */
    case CURLE_SSL_SHUTDOWN_FAILED: /* 80 - Failed to shut down the SSL
                                      connection */
    case CURLE_SSL_CRL_BADFILE:     /* 82 - could not load CRL file, missing or
                                      wrong format (Added in 7.19.0) */
    case CURLE_SSL_ISSUER_ERROR:    /* 83 - Issuer check failed.  (Added in
                                      7.19.0) */
    case CURLE_SSL_PINNEDPUBKEYNOTMATCH: /* 90 - specified pinned public key did
                                         not match */
    case CURLE_SSL_INVALIDCERTSTATUS:    /* 91 - invalid certificate status */
    case CURLE_AUTH_ERROR:     /* 94 - an authentication function returned an
                                 error */
    case CURLE_SSL_CLIENTCERT: /* 98 - client-side certificate required */
      return false;
    case CURLE_FAILED_INIT:           /* 2 */
    case CURLE_NOT_BUILT_IN:          /* 4 - [was obsoleted in August 2007 for
                                        7.17.0, reused in April 2011 for 7.21.5] */
    case CURLE_COULDNT_RESOLVE_PROXY: /* 5 */
    case CURLE_COULDNT_RESOLVE_HOST:  /* 6 */
    case CURLE_COULDNT_CONNECT:       /* 7 */
    case CURLE_WEIRD_SERVER_REPLY:    /* 8 */
    case CURLE_REMOTE_ACCESS_DENIED:  /* 9 a service was denied by the server
                                        due to lack of access - when login fails
                                        this is not returned. */
    case CURLE_FTP_ACCEPT_FAILED:     /* 10 - [was obsoleted in April 2006 for
                                        7.15.4, reused in Dec 2011 for 7.24.0]*/
    case CURLE_FTP_WEIRD_PASS_REPLY:  /* 11 */
    case CURLE_FTP_ACCEPT_TIMEOUT:    /* 12 - timeout occurred accepting server
                                        [was obsoleted in August 2007 for 7.17.0,
                                        reused in Dec 2011 for 7.24.0]*/
    case CURLE_FTP_WEIRD_PASV_REPLY:  /* 13 */
    case CURLE_FTP_WEIRD_227_FORMAT:  /* 14 */
    case CURLE_FTP_CANT_GET_HOST:     /* 15 */
    case CURLE_HTTP2: /* 16 - A problem in the http2 framing layer.
                        [was obsoleted in August 2007 for 7.17.0,
                        reused in July 2014 for 7.38.0] */
    case CURLE_FTP_COULDNT_SET_TYPE:   /* 17 */
    case CURLE_PARTIAL_FILE:           /* 18 */
    case CURLE_FTP_COULDNT_RETR_FILE:  /* 19 */
    case CURLE_OBSOLETE20:             /* 20 - NOT USED */
    case CURLE_QUOTE_ERROR:            /* 21 - quote command failure */
    case CURLE_HTTP_RETURNED_ERROR:    /* 22 */
    case CURLE_WRITE_ERROR:            /* 23 */
    case CURLE_OBSOLETE24:             /* 24 - NOT USED */
    case CURLE_UPLOAD_FAILED:          /* 25 - failed upload "command" */
    case CURLE_READ_ERROR:             /* 26 - couldn't open/read from file */
    case CURLE_OUT_OF_MEMORY:          /* 27 */
    case CURLE_OPERATION_TIMEDOUT:     /* 28 - the timeout time was reached */
    case CURLE_OBSOLETE29:             /* 29 - NOT USED */
    case CURLE_FTP_PORT_FAILED:        /* 30 - FTP PORT operation failed */
    case CURLE_FTP_COULDNT_USE_REST:   /* 31 - the REST command failed */
    case CURLE_OBSOLETE32:             /* 32 - NOT USED */
    case CURLE_RANGE_ERROR:            /* 33 - RANGE "command" didn't work */
    case CURLE_HTTP_POST_ERROR:        /* 34 */
    case CURLE_SSL_CONNECT_ERROR:      /* 35 - wrong when connecting with SSL */
    case CURLE_BAD_DOWNLOAD_RESUME:    /* 36 - couldn't resume download */
    case CURLE_FILE_COULDNT_READ_FILE: /* 37 */
    case CURLE_LDAP_CANNOT_BIND:       /* 38 */
    case CURLE_LDAP_SEARCH_FAILED:     /* 39 */
    case CURLE_OBSOLETE40:             /* 40 - NOT USED */
    case CURLE_FUNCTION_NOT_FOUND:     /* 41 - NOT USED starting with 7.53.0 */
    case CURLE_ABORTED_BY_CALLBACK:    /* 42 */
    case CURLE_BAD_FUNCTION_ARGUMENT:  /* 43 */
    case CURLE_OBSOLETE44:             /* 44 - NOT USED */
    case CURLE_INTERFACE_FAILED:       /* 45 - CURLOPT_INTERFACE failed */
    case CURLE_OBSOLETE46:             /* 46 - NOT USED */
    case CURLE_TOO_MANY_REDIRECTS:     /* 47 - catch endless re-direct loops */
    case CURLE_UNKNOWN_OPTION:       /* 48 - User specified an unknown option */
    case CURLE_SETOPT_OPTION_SYNTAX: /* 49 - Malformed setopt option */
    case CURLE_OBSOLETE50:           /* 50 - NOT USED */
    case CURLE_OBSOLETE51:           /* 51 - NOT USED */
    case CURLE_GOT_NOTHING:          /* 52 - when this is a specific error */
    case CURLE_SEND_ERROR:           /* 55 - failed sending network data */
    case CURLE_RECV_ERROR: /* 56 - failure in receiving network data */
    case CURLE_OBSOLETE57: /* 57 - NOT IN USE */
    case CURLE_BAD_CONTENT_ENCODING:  /* 61 - Unrecognized/bad encoding */
    case CURLE_OBSOLETE62:            /* 62 - NOT IN USE since 7.82.0 */
    case CURLE_FILESIZE_EXCEEDED:     /* 63 - Maximum file size exceeded */
    case CURLE_USE_SSL_FAILED:        /* 64 - Requested FTP SSL level failed */
    case CURLE_SEND_FAIL_REWIND:      /* 65 - Sending the data requires a rewind
                                        that failed */
    case CURLE_LOGIN_DENIED:          /* 67 - user, password or similar was not
                                        accepted and we failed to login */
    case CURLE_TFTP_NOTFOUND:         /* 68 - file not found on server */
    case CURLE_TFTP_PERM:             /* 69 - permission problem on server */
    case CURLE_REMOTE_DISK_FULL:      /* 70 - out of disk space on server */
    case CURLE_TFTP_ILLEGAL:          /* 71 - Illegal TFTP operation */
    case CURLE_TFTP_UNKNOWNID:        /* 72 - Unknown transfer ID */
    case CURLE_REMOTE_FILE_EXISTS:    /* 73 - File already exists */
    case CURLE_TFTP_NOSUCHUSER:       /* 74 - No such user */
    case CURLE_OBSOLETE75:            /* 75 - NOT IN USE since 7.82.0 */
    case CURLE_OBSOLETE76:            /* 76 - NOT IN USE since 7.82.0 */
    case CURLE_REMOTE_FILE_NOT_FOUND: /* 78 - remote file not found */
    case CURLE_SSH:                   /* 79 - error from the SSH layer, somewhat
                                        generic so the error message will be of
                                        interest when this has happened */

    case CURLE_AGAIN:              /* 81 - socket is not ready for send/recv,
                                     wait till it's ready and try again (Added
                                     in 7.18.2) */
    case CURLE_FTP_PRET_FAILED:    /* 84 - a PRET command failed */
    case CURLE_RTSP_CSEQ_ERROR:    /* 85 - mismatch of RTSP CSeq numbers */
    case CURLE_RTSP_SESSION_ERROR: /* 86 - mismatch of RTSP Session Ids */
    case CURLE_FTP_BAD_FILE_LIST:  /* 87 - unable to parse FTP file list */
    case CURLE_CHUNK_FAILED:       /* 88 - chunk callback reported error */
    case CURLE_NO_CONNECTION_AVAILABLE: /* 89 - No connection available, the
                                        session will be queued */
    case CURLE_HTTP2_STREAM:       /* 92 - stream error in HTTP/2 framing layer
                                    */
    case CURLE_RECURSIVE_API_CALL: /* 93 - an api function was called from
                                     inside a callback */
    case CURLE_HTTP3:              /* 95 - An HTTP/3 layer problem */
    case CURLE_QUIC_CONNECT_ERROR: /* 96 - QUIC connection error */
    case CURLE_PROXY:              /* 97 - proxy handshake error */
    case CURLE_UNRECOVERABLE_POLL: /* 99 - poll/select returned fatal error */
    case CURLE_TOO_LARGE:          /* 100 - a value/data met its maximum */
    case CURLE_ECH_REQUIRED:       /* 101 - ECH tried but failed */
      return true;
    default:
      return false;
  }
}

bool Curl::should_retry_request(CURLcode curl_code, long http_code) const {
  if (curl_code != CURLE_OK) {
    return should_retry_based_on_curl_code(curl_code);
  } else {
    return should_retry_based_on_http_status(http_code);
  }
}

tuple<Status, optional<long>> Curl::last_http_status_code() {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return {
        Status_RestError("Error checking curl error; curl instance is null."),
        std::nullopt};

  long http_code = 0;
  if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code) != CURLE_OK)
    return {
        Status_RestError("Error checking curl error; could not get HTTP code."),
        std::nullopt};

  return {Status::Ok(), http_code};
}

Status Curl::check_curl_errors(
    CURLcode curl_code,
    const std::string& operation,
    const Buffer* returned_data) const {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status_RestError("Error checking curl error; curl instance is null."));

  if (curl_code != CURLE_OK) {
    std::stringstream msg;
    msg << "Error in libcurl " << operation
        << " operation: libcurl error message '" << get_curl_errstr(curl_code)
        << "; ";
    return Status_RestError(msg.str());
  }

  long http_code = 0;
  if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code) != CURLE_OK)
    return LOG_STATUS(Status_RestError(
        "Error checking curl error; could not get HTTP code."));

  if (http_code >= 400) {
    // TODO: Should see if message has error data object
    std::stringstream msg;
    msg << "Error in libcurl " << operation
        << " operation: libcurl error message '" << get_curl_errstr(curl_code)
        << "'; HTTP code " << http_code << "; ";
    if (returned_data) {
      if (returned_data->size() > 0) {
        msg << "server response data '"
            << std::string(
                   reinterpret_cast<const char*>(returned_data->data()),
                   returned_data->size())
            << "'.";
      } else {
        msg << "server response was empty.";
      }
    }
    return Status_RestError(msg.str());
  }

  return Status::Ok();
}

void Curl::check_curl_errors_v2(
    CURLcode curl_code,
    const std::string& operation,
    const Buffer* returned_data) const {
  CURL* curl = curl_.get();
  if (curl == nullptr) {
    throw CurlException(
        "Error checking curl error; curl instance is null.", curl_code);
  }

  if (curl_code != CURLE_OK) {
    std::stringstream msg;
    msg << "Error in libcurl " << operation
        << " operation: libcurl error message '" << get_curl_errstr(curl_code)
        << "; ";
    throw CurlException(msg.str(), curl_code);
  }

  long http_code = 0;
  if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code) != CURLE_OK) {
    throw CurlException(
        "Error checking curl error; could not get HTTP code.", curl_code);
  }

  if (http_code >= 400) {
    // TODO: Should see if message has error data object
    std::stringstream msg;
    msg << "Error in libcurl " << operation
        << " operation: libcurl error message '" << get_curl_errstr(curl_code)
        << "'; HTTP code " << http_code << "; ";
    if (returned_data) {
      if (returned_data->size() > 0) {
        msg << "server response data '"
            << std::string(
                   reinterpret_cast<const char*>(returned_data->data()),
                   returned_data->size())
            << "'.";
      } else {
        msg << "server response was empty.";
      }
    }
    throw CurlException(msg.str(), curl_code, http_code);
  }
}

std::string Curl::get_curl_errstr(CURLcode curl_code) const {
  if (curl_code == CURLE_OK)
    return "CURLE_OK";

  // First check the error buffer for a detailed message.
  auto err_str_ptr = static_cast<const char*>(curl_error_buffer_.data());
  size_t err_str_len = 0;
  while (err_str_len < CURL_ERROR_SIZE && err_str_ptr[err_str_len] != '\0')
    err_str_len++;
  if (err_str_len > 0 && err_str_len < CURL_ERROR_SIZE)
    return std::string(err_str_ptr, err_str_len);

  // Fall back to a generic error message
  return std::string(curl_easy_strerror(curl_code));
}

Status Curl::post_data(
    stats::Stats* const stats,
    const std::string& url,
    const SerializationType serialization_type,
    BufferList* data,
    Buffer* const returned_data,
    const std::string& res_uri) {
  struct curl_slist* headers;
  RETURN_NOT_OK(post_data_common(serialization_type, data, &headers));

  logger_->debug("posting {} bytes to {}", data->total_size(), url);

  CURLcode ret;
  headerData.uri = &res_uri;
  auto st = make_curl_request(stats, url.c_str(), &ret, data, returned_data);
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "POST", returned_data));

  return Status::Ok();
}

Status Curl::post_data(
    stats::Stats* const stats,
    const std::string& url,
    const SerializationType serialization_type,
    BufferList* data,
    Buffer* const returned_data,
    PostResponseCb&& cb,
    const std::string& res_uri) {
  struct curl_slist* headers;
  RETURN_NOT_OK(post_data_common(serialization_type, data, &headers));

  CURLcode ret;
  headerData.uri = &res_uri;
  auto st = make_curl_request(stats, url.c_str(), &ret, data, std::move(cb));
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "POST", returned_data));

  return Status::Ok();
}

Status Curl::post_data_common(
    const SerializationType serialization_type,
    BufferList* data,
    struct curl_slist** headers) {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status_RestError("Error posting data; curl instance is null."));

  // TODO: If you post more than 2GB, use CURLOPT_POSTFIELDSIZE_LARGE.
  const uint64_t post_size_limit = uint64_t(2) * 1024 * 1024 * 1024;
  if (data->total_size() > post_size_limit) {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, data->total_size());
  } else {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data->total_size());
  }

  // Set auth and content-type for request
  *headers = nullptr;
  RETURN_NOT_OK_ELSE(set_headers(headers), curl_slist_free_all(*headers));
  RETURN_NOT_OK_ELSE(
      set_content_type(serialization_type, headers),
      curl_slist_free_all(*headers));

  /* HTTP POST please */
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(
      curl, CURLOPT_READFUNCTION, buffer_list_read_memory_callback);
  curl_easy_setopt(curl, CURLOPT_READDATA, data);

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, *headers);

  /* set seek for handling redirects */
  curl_easy_setopt(curl, CURLOPT_SEEKFUNCTION, &buffer_list_seek_callback);
  curl_easy_setopt(curl, CURLOPT_SEEKDATA, data);

  return Status::Ok();
}

void Curl::get_data(
    stats::Stats* const stats,
    const std::string& url,
    SerializationType serialization_type,
    Buffer* returned_data,
    const std::string& res_ns_uri) {
  CURL* curl = curl_.get();
  if (curl == nullptr) {
    throw CurlException("Error getting data; curl instance is null.");
  }

  // Set auth and content-type for request
  struct curl_slist* headers = nullptr;
  CURLcode ret;
  try {
    throw_if_not_ok(set_headers(&headers));
    throw_if_not_ok(set_content_type(serialization_type, &headers));

    // Pass our list of custom-made headers.
    CURLcode rc = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    if (rc != CURLE_OK) {
      throw CurlException(
          "Error initializing libcurl; failed to set custom headers, libcurl "
          "error message: " +
          get_curl_errstr(rc));
    }

    headerData.uri = &res_ns_uri;
    throw_if_not_ok(
        make_curl_request(stats, url.c_str(), &ret, nullptr, returned_data));
  } catch (...) {
    curl_slist_free_all(headers);
    throw;
  }

  // Throws CurlException containing the error, if any is found.
  check_curl_errors_v2(ret, "GET", returned_data);
}

Status Curl::options(
    stats::Stats* const stats,
    const std::string& url,
    SerializationType serialization_type,
    Buffer* returned_data,
    const std::string& res_ns_uri) {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status_RestError("Error getting data; curl instance is null."));

  // Set auth and content-type for request
  struct curl_slist* headers = nullptr;
  RETURN_NOT_OK_ELSE(set_headers(&headers), curl_slist_free_all(headers));
  RETURN_NOT_OK_ELSE(
      set_content_type(serialization_type, &headers),
      curl_slist_free_all(headers));

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  /* HTTP OPTIONS please */
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "OPTIONS");

  /* HEAD */
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1);

  CURLcode ret;
  headerData.uri = &res_ns_uri;
  auto st = make_curl_request(stats, url.c_str(), &ret, nullptr, returned_data);
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "OPTIONS", returned_data));

  return Status::Ok();
}

Status Curl::delete_data(
    stats::Stats* const stats,
    const std::string& url,
    SerializationType serialization_type,
    Buffer* returned_data,
    const std::string& res_uri) {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status_RestError("Error deleting data; curl instance is null."));

  // Set auth and content-type for request
  struct curl_slist* headers = nullptr;
  RETURN_NOT_OK_ELSE(set_headers(&headers), curl_slist_free_all(headers));
  RETURN_NOT_OK_ELSE(
      set_content_type(serialization_type, &headers),
      curl_slist_free_all(headers));

  /* HTTP DELETE please */
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode ret;
  headerData.uri = &res_uri;
  auto st = make_curl_request(stats, url.c_str(), &ret, nullptr, returned_data);

  // Erase record in case of de-registered array
  std::unique_lock<std::mutex> rd_lck(*(headerData.redirect_uri_map_lock));
  headerData.redirect_uri_map->erase(*(headerData.uri));
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "DELETE", returned_data));

  return Status::Ok();
}

Status Curl::patch_data(
    stats::Stats* const stats,
    const std::string& url,
    const SerializationType serialization_type,
    BufferList* data,
    Buffer* const returned_data,
    const std::string& res_uri) {
  struct curl_slist* headers;
  RETURN_NOT_OK(patch_data_common(serialization_type, data, &headers));

  CURLcode ret;
  headerData.uri = &res_uri;
  auto st = make_curl_request(stats, url.c_str(), &ret, data, returned_data);
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "PATCH", returned_data));

  return Status::Ok();
}

Status Curl::patch_data_common(
    const SerializationType serialization_type,
    BufferList* data,
    struct curl_slist** headers) {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status_RestError("Error patching data; curl instance is null."));

  const uint64_t post_size_limit = uint64_t(2) * 1024 * 1024 * 1024;
  logger_->debug("patching {} bytes to", data->total_size());
  if (data->total_size() > post_size_limit) {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, data->total_size());
  } else {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data->total_size());
  }

  // Set auth and content-type for request
  *headers = nullptr;
  RETURN_NOT_OK_ELSE(set_headers(headers), curl_slist_free_all(*headers));
  RETURN_NOT_OK_ELSE(
      set_content_type(serialization_type, headers),
      curl_slist_free_all(*headers));

  /* Set POST so curl sends the body */
  curl_easy_setopt(curl, CURLOPT_POST, 1);

  /* HTTP PATCH please */
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
  curl_easy_setopt(
      curl, CURLOPT_READFUNCTION, buffer_list_read_memory_callback);
  curl_easy_setopt(curl, CURLOPT_READDATA, data);

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, *headers);

  /* set seek for handling redirects */
  curl_easy_setopt(curl, CURLOPT_SEEKFUNCTION, &buffer_list_seek_callback);
  curl_easy_setopt(curl, CURLOPT_SEEKDATA, data);

  return Status::Ok();
}

Status Curl::put_data(
    stats::Stats* const stats,
    const std::string& url,
    const SerializationType serialization_type,
    BufferList* data,
    Buffer* const returned_data,
    const std::string& res_uri) {
  struct curl_slist* headers;
  RETURN_NOT_OK(put_data_common(serialization_type, data, &headers));

  CURLcode ret;
  headerData.uri = &res_uri;
  auto st = make_curl_request(stats, url.c_str(), &ret, data, returned_data);
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "PUT", returned_data));

  return Status::Ok();
}

Status Curl::put_data_common(
    const SerializationType serialization_type,
    BufferList* data,
    struct curl_slist** headers) {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status_RestError("Error putting data; curl instance is null."));

  const uint64_t post_size_limit = uint64_t(2) * 1024 * 1024 * 1024;
  logger_->debug("putting {} bytes to", data->total_size());
  if (data->total_size() > post_size_limit) {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, data->total_size());
  } else {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data->total_size());
  }

  // Set auth and content-type for request
  *headers = nullptr;
  RETURN_NOT_OK_ELSE(set_headers(headers), curl_slist_free_all(*headers));
  RETURN_NOT_OK_ELSE(
      set_content_type(serialization_type, headers),
      curl_slist_free_all(*headers));

  /* Set POST so curl sends the body */
  curl_easy_setopt(curl, CURLOPT_POST, 1);

  /* HTTP PUT please */
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
  curl_easy_setopt(
      curl, CURLOPT_READFUNCTION, buffer_list_read_memory_callback);
  curl_easy_setopt(curl, CURLOPT_READDATA, data);

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, *headers);

  /* set seek for handling redirects */
  curl_easy_setopt(curl, CURLOPT_SEEKFUNCTION, &buffer_list_seek_callback);
  curl_easy_setopt(curl, CURLOPT_SEEKDATA, data);

  return Status::Ok();
}

}  // namespace tiledb::sm
