/**
 * @file   curl.cc
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
 * This file defines a high-level libcurl helper class.
 */

#include "tiledb/sm/rest/curl.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/global_state/global_state.h"
#include "tiledb/sm/misc/utils.h"

#include <cstring>
#include <iostream>
#include <sstream>
// TODO: replace this with config option
#define CURL_MAX_RETRIES 3

using namespace tiledb::common;

namespace tiledb {
namespace sm {

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
    buffer->set_offset(0);
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
  uint64_t num_read = 0;
  auto st = buffer_list->read_at_most(dest, max_nbytes, &num_read);
  if (!st.ok()) {
    LOG_ERROR(
        "Cannot copy libcurl POST data; BufferList read failed: " +
        st.to_string());
    return CURL_READFUNC_ABORT;
  }

  return num_read;
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
  Status status = data->seek(offset, origin);
  if (status.ok())
    return CURL_SEEKFUNC_OK;

  return CURL_SEEKFUNC_FAIL;
}

Curl::Curl()
    : config_(nullptr)
    , curl_(nullptr, curl_easy_cleanup) {
}

Status Curl::init(
    const Config* config,
    const std::unordered_map<std::string, std::string>& extra_headers,
    const std::unordered_map<std::string, std::string>& res_headers) {
  if (config == nullptr)
    return LOG_STATUS(
        Status::RestError("Error initializing libcurl; config is null."));

  config_ = config;
  curl_.reset(curl_easy_init());
  extra_headers_ = extra_headers;
  res_headers_ = res_headers;
  // See https://curl.haxx.se/libcurl/c/threadsafe.html
  CURLcode rc = curl_easy_setopt(curl_.get(), CURLOPT_NOSIGNAL, 1);
  if (rc != CURLE_OK)
    return LOG_STATUS(Status::RestError(
        "Error initializing libcurl; failed to set CURLOPT_NOSIGNAL"));

  // For human-readable error messages
  RETURN_NOT_OK(curl_error_buffer_.realloc(CURL_ERROR_SIZE));
  std::memset(curl_error_buffer_.data(), 0, CURL_ERROR_SIZE);
  rc = curl_easy_setopt(
      curl_.get(), CURLOPT_ERRORBUFFER, curl_error_buffer_.data());
  if (rc != CURLE_OK)
    return LOG_STATUS(Status::RestError(
        "Error initializing libcurl; failed to set CURLOPT_ERRORBUFFER"));

  rc = curl_easy_setopt(curl_.get(), CURLOPT_HEADERFUNCTION, OnReceiveData);
  if (rc != CURLE_OK)
    return LOG_STATUS(Status::RestError(
        "Error initializing libcurl; failed to set CURLOPT_HEADERFUNCTION"));

  rc = curl_easy_setopt(curl_.get(), CURLOPT_HEADER, 1);
  if (rc != CURLE_OK)
    return LOG_STATUS(Status::RestError(
        "Error initializing libcurl; failed to set CURLOPT_HEADERFUNCTION"));

  /* set url to fetch */
  rc = curl_easy_setopt(curl_.get(), CURLOPT_HEADERDATA, &res_headers_);
  if (rc != CURLE_OK)
    return LOG_STATUS(Status::RestError(
        "Error initializing libcurl; failed to set CURLOPT_HEADERFUNCTION"));

  // Ignore ssl validation if the user has set rest.ignore_ssl_validation = true
  const char* ignore_ssl_validation_str = nullptr;
  RETURN_NOT_OK(
      config_->get("rest.ignore_ssl_validation", &ignore_ssl_validation_str));

  bool ignore_ssl_validation = false;
  if (ignore_ssl_validation_str != nullptr)
    RETURN_NOT_OK(tiledb::sm::utils::parse::convert(
        ignore_ssl_validation_str, &ignore_ssl_validation));

  if (ignore_ssl_validation) {
    curl_easy_setopt(curl_.get(), CURLOPT_SSL_VERIFYHOST, 0);
    curl_easy_setopt(curl_.get(), CURLOPT_SSL_VERIFYPEER, 0);
  }

#ifdef __linux__
  // Get CA Cert bundle file from global state. This is initialized and cached
  // if detected. We have only had issues with finding the certificate path on
  // Linux.
  const std::string cert_file =
      global_state::GlobalState::GetGlobalState().cert_file();
  // If we have detected a ca cert bundle let's set the curl option for CAINFO
  if (!cert_file.empty()) {
    curl_easy_setopt(curl_.get(), CURLOPT_CAINFO, cert_file.c_str());
  }
#endif

  return Status::Ok();
}

size_t Curl::OnReceiveData(
    void* res_data, size_t tSize, size_t tCount, void* userdata) {
  size_t length = tSize * tCount, index = 0;
  while (index < length) {
    unsigned char* temp = (unsigned char*)res_data + index;
    if ((temp[0] == '\r') || (temp[0] == '\n'))
      break;
    index++;
  }

  std::string str((unsigned char*)res_data, (unsigned char*)res_data + index);
  std::unordered_map<std::string, std::string>* pmHeader =
      (std::unordered_map<std::string, std::string>*)userdata;
  size_t pos = str.find(": ");
  if (pos != std::string::npos) {
    pmHeader->insert(std::make_pair<std::string, std::string>(
        str.substr(0, pos), str.substr(pos + 2, index - 1)));
  }
  return (tCount);
}

std::string Curl::url_escape(const std::string& url) const {
  if (curl_.get() == nullptr)
    return "";

  char* escaped_c = curl_easy_escape(curl_.get(), url.c_str(), url.length());
  std::string escaped(escaped_c);
  curl_free(escaped_c);
  return escaped;
}

Status Curl::set_headers(struct curl_slist** headers) const {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Cannot set auth; curl instance is null."));

  const char* token = nullptr;
  RETURN_NOT_OK(config_->get("rest.token", &token));

  if (token != nullptr) {
    *headers = curl_slist_append(
        *headers, (std::string("X-TILEDB-REST-API-Key: ") + token).c_str());
    if (*headers == nullptr)
      return LOG_STATUS(Status::RestError(
          "Cannot set curl auth; curl_slist_append returned null."));
  } else {
    // Try username+password instead of token
    const char* username = nullptr;
    const char* password = nullptr;
    RETURN_NOT_OK(config_->get("rest.username", &username));
    RETURN_NOT_OK(config_->get("rest.password", &password));

    // Check for no auth.
    if (username == nullptr || password == nullptr)
      return LOG_STATUS(
          Status::RestError("Cannot set curl auth; either token or "
                            "username/password must be set."));

    std::string basic_auth = username + std::string(":") + password;
    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
    curl_easy_setopt(curl, CURLOPT_USERPWD, basic_auth.c_str());
  }

  // Add any extra headers.
  for (const auto& it : extra_headers_) {
    std::string hdr = it.first + ": " + it.second;
    *headers = curl_slist_append(*headers, hdr.c_str());
    if (*headers == nullptr) {
      curl_slist_free_all(*headers);
      return LOG_STATUS(Status::RestError(
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
      return LOG_STATUS(Status::RestError(
          "Cannot set content-type header; unknown serialization format."));
  }

  if (*headers == nullptr)
    return LOG_STATUS(Status::RestError(
        "Cannot set content-type header; curl_slist_append returned null."));

  return Status::Ok();
}

Status Curl::make_curl_request(
    const char* url, CURLcode* curl_code, Buffer* returned_data) const {
  return make_curl_request_common(
      url,
      curl_code,
      &write_memory_callback,
      static_cast<void*>(returned_data));
}

Status Curl::make_curl_request(
    const char* url, CURLcode* curl_code, PostResponseCb&& cb) const {
  return make_curl_request_common(
      url, curl_code, &write_memory_callback_cb, static_cast<void*>(&cb));
}

Status Curl::make_curl_request_common(
    const char* const url,
    CURLcode* const curl_code,
    size_t (*write_cb)(void*, size_t, size_t, void*),
    void* const write_cb_arg) const {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Cannot make curl request; curl instance is null."));

  *curl_code = CURLE_OK;
  for (uint8_t i = 0; i < CURL_MAX_RETRIES; i++) {
    WriteCbState write_cb_state;
    write_cb_state.arg = write_cb_arg;
    /* set url to fetch */
    curl_easy_setopt(curl, CURLOPT_URL, url);

    /* set callback function */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);

    /* pass fetch buffer pointer */
    curl_easy_setopt(
        curl, CURLOPT_WRITEDATA, static_cast<void*>(&write_cb_state));

    /* set default user agent */
    // curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");

    /* Fail on error */
    // curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);

    /* set timeout */
    // curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5);

    /* set compression */
    const char* compressor = nullptr;
    RETURN_NOT_OK(config_->get("rest.http_compressor", &compressor));

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
        curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, comp.c_str());
      }
    }

    /* enable location redirects */
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);

    /* set maximum allowed redirects */
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 1);

    /* fetch the url */
    CURLcode tmp_curl_code = curl_easy_perform(curl);
    /* If Curl call was successful (not http status, but no socket error, etc)
     * break */
    if (tmp_curl_code == CURLE_OK) {
      break;
    }

    /* Only store the first non-OK curl code, because it will likely be more
     * useful than the curl codes from the retries. */
    if (*curl_code == CURLE_OK) {
      *curl_code = tmp_curl_code;
    }

    /* Retry on curl errors, unless the write callback has elected
     * to skip it. */
    if (write_cb_state.skip_retries) {
      break;
    }
  }

  return Status::Ok();
}

Status Curl::check_curl_errors(
    CURLcode curl_code,
    const std::string& operation,
    const Buffer* returned_data) const {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Error checking curl error; curl instance is null."));

  long http_code = 0;
  if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code) != CURLE_OK)
    return LOG_STATUS(Status::RestError(
        "Error checking curl error; could not get HTTP code."));

  if (curl_code != CURLE_OK || http_code >= 400) {
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

    return LOG_STATUS(Status::RestError(msg.str()));
  }

  return Status::Ok();
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
    const std::string& url,
    const SerializationType serialization_type,
    const BufferList* data,
    Buffer* const returned_data) {
  struct curl_slist* headers;
  RETURN_NOT_OK(post_data_common(serialization_type, data, &headers));

  CURLcode ret;
  auto st = make_curl_request(url.c_str(), &ret, returned_data);
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "POST", returned_data));

  return Status::Ok();
}

Status Curl::post_data(
    const std::string& url,
    const SerializationType serialization_type,
    const BufferList* data,
    PostResponseCb&& cb) {
  struct curl_slist* headers;
  RETURN_NOT_OK(post_data_common(serialization_type, data, &headers));

  CURLcode ret;
  auto st = make_curl_request(url.c_str(), &ret, std::move(cb));
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "POST"));

  return Status::Ok();
}

Status Curl::post_data_common(
    const SerializationType serialization_type,
    const BufferList* data,
    struct curl_slist** headers) {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Error posting data; curl instance is null."));

  // TODO: If you post more than 2GB, use CURLOPT_POSTFIELDSIZE_LARGE.
  const uint64_t post_size_limit = uint64_t(2) * 1024 * 1024 * 1024;
  if (data->total_size() > post_size_limit)
    return LOG_STATUS(
        Status::RestError("Error posting data; buffer size > 2GB"));

  // Set auth and content-type for request
  *headers = nullptr;
  RETURN_NOT_OK_ELSE(set_headers(headers), curl_slist_free_all(*headers));
  RETURN_NOT_OK_ELSE(
      set_content_type(serialization_type, headers),
      curl_slist_free_all(*headers));

  /* HTTP PUT please */
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(
      curl, CURLOPT_READFUNCTION, buffer_list_read_memory_callback);
  curl_easy_setopt(curl, CURLOPT_READDATA, data);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data->total_size());

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, *headers);

  /* set seek for handling redirects */
  curl_easy_setopt(curl, CURLOPT_SEEKFUNCTION, &buffer_list_seek_callback);
  curl_easy_setopt(curl, CURLOPT_SEEKDATA, data);

  return Status::Ok();
}

Status Curl::get_data(
    const std::string& url,
    SerializationType serialization_type,
    Buffer* returned_data) {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Error getting data; curl instance is null."));

  // Set auth and content-type for request
  struct curl_slist* headers = nullptr;
  RETURN_NOT_OK_ELSE(set_headers(&headers), curl_slist_free_all(headers));
  RETURN_NOT_OK_ELSE(
      set_content_type(serialization_type, &headers),
      curl_slist_free_all(headers));

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HEADER, 1);
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode ret;
  auto st = make_curl_request(url.c_str(), &ret, returned_data);
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "GET", returned_data));

  return Status::Ok();
}

Status Curl::delete_data(
    const std::string& url,
    SerializationType serialization_type,
    Buffer* returned_data) {
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Error deleting data; curl instance is null."));

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
  auto st = make_curl_request(url.c_str(), &ret, returned_data);
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "DELETE", returned_data));

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
