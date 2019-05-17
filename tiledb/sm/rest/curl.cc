/**
 * @file   curl.cc
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
 * This file defines a high-level libcurl helper class.
 */

#include "tiledb/sm/rest/curl.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

#include <cstring>

// TODO: replace this with config option
#define CURL_MAX_RETRIES 3

namespace tiledb {
namespace sm {

/**
 * @brief Callback for saving libcurl response data
 *
 * This is called by libcurl as soon as there is data received that needs
 * to be saved.
 *
 * @param contents Pointer to received data
 * @param size Size of a member in the received data
 * @param nmemb Number of members in the received data
 * @param userdata User data attached to the callback (in our case will point to
 *      a Buffer instance)
 * @return Number of bytes copied from the received data
 */
size_t write_memory_callback(
    void* contents, size_t size, size_t nmemb, void* userdata) {
  const size_t content_nbytes = size * nmemb;
  auto buffer = (Buffer*)userdata;
  auto st = buffer->write(contents, content_nbytes);
  if (!st.ok()) {
    LOG_ERROR(
        "Cannot copy libcurl response data; buffer write failed: " +
        st.to_string());
    return 0;
  }

  return content_nbytes;
}

Curl::Curl()
    : config_(nullptr)
    , curl_(nullptr, curl_easy_cleanup) {
}

Status Curl::init(const Config* config) {
  if (config == nullptr)
    return LOG_STATUS(
        Status::RestError("Error initializing libcurl; config is null."));

  config_ = config;
  curl_.reset(curl_easy_init());

  // See https://curl.haxx.se/libcurl/c/threadsafe.html
  CURLcode rc = curl_easy_setopt(curl_.get(), CURLOPT_NOSIGNAL, 1);
  if (rc != CURLE_OK)
    return LOG_STATUS(Status::RestError(
        "Error initializing libcurl; failed to set CURLOPT_NOSIGNAL"));

  return Status::Ok();
}

std::string Curl::url_escape(const std::string& url) const {
  if (curl_.get() == nullptr)
    return "";

  char* escaped_c = curl_easy_escape(curl_.get(), url.c_str(), url.length());
  std::string escaped(escaped_c);
  curl_free(escaped_c);
  return escaped;
}

Status Curl::set_auth(struct curl_slist** headers) const {
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
  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Cannot make curl request; curl instance is null."));

  *curl_code = CURLE_OK;
  for (uint8_t i = 0; i < CURL_MAX_RETRIES; i++) {
    returned_data->set_size(0);
    returned_data->set_offset(0);

    /* set url to fetch */
    curl_easy_setopt(curl, CURLOPT_URL, url);

    /* set callback function */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_memory_callback);

    /* pass fetch buffer pointer */
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)returned_data);

    /* set default user agent */
    // curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");

    /* Fail on error */
    // curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);

    /* set timeout */
    // curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5);

    /* enable location redirects */
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);

    /* set maximum allowed redirects */
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 1);

    /* fetch the url */
    *curl_code = curl_easy_perform(curl);
    /* If Curl call was successful (not http status, but no socket error, etc)
     * break */
    if (*curl_code == CURLE_OK)
      break;

    /* Retry on curl errors */
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
    msg << "Error in curl " << operation << " operation ; HTTP code "
        << http_code << "; ";
    if (returned_data->size() > 0) {
      msg << " response data '"
          << std::string(
                 reinterpret_cast<const char*>(returned_data->data()),
                 returned_data->size())
          << "'.";
    } else {
      msg << " server response was empty.";
    }

    return LOG_STATUS(Status::RestError(msg.str()));
  }

  return Status::Ok();
}

Status Curl::post_data(
    const std::string& url,
    SerializationType serialization_type,
    Buffer* data,
    Buffer* returned_data) {
  STATS_FUNC_IN(rest_curl_post);

  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Error posting data; curl instance is null."));

  // TODO: If you post more than 2GB, use CURLOPT_POSTFIELDSIZE_LARGE.
  const uint64_t post_size_limit = uint64_t(2) * 1024 * 1024 * 1024;
  if (data->size() > post_size_limit)
    return LOG_STATUS(
        Status::RestError("Error posting data; buffer size > 2GB"));

  // Set auth and content-type for request
  struct curl_slist* headers = nullptr;
  RETURN_NOT_OK_ELSE(set_auth(&headers), curl_slist_free_all(headers));
  RETURN_NOT_OK_ELSE(
      set_content_type(serialization_type, &headers),
      curl_slist_free_all(headers));

  /* HTTP PUT please */
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data->data());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data->size());

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode ret;
  auto st = make_curl_request(url.c_str(), &ret, returned_data);
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "POST", returned_data));

  return Status::Ok();

  STATS_FUNC_OUT(rest_curl_post);
}

Status Curl::get_data(
    const std::string& url,
    SerializationType serialization_type,
    Buffer* returned_data) {
  STATS_FUNC_IN(rest_curl_get);

  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Error getting data; curl instance is null."));

  // Set auth and content-type for request
  struct curl_slist* headers = nullptr;
  RETURN_NOT_OK_ELSE(set_auth(&headers), curl_slist_free_all(headers));
  RETURN_NOT_OK_ELSE(
      set_content_type(serialization_type, &headers),
      curl_slist_free_all(headers));

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode ret;
  auto st = make_curl_request(url.c_str(), &ret, returned_data);
  curl_slist_free_all(headers);
  RETURN_NOT_OK(st);

  // Check for errors
  RETURN_NOT_OK(check_curl_errors(ret, "GET", returned_data));

  return Status::Ok();

  STATS_FUNC_OUT(rest_curl_get);
}

Status Curl::delete_data(
    const std::string& url,
    SerializationType serialization_type,
    Buffer* returned_data) {
  STATS_FUNC_IN(rest_curl_delete);

  CURL* curl = curl_.get();
  if (curl == nullptr)
    return LOG_STATUS(
        Status::RestError("Error deleting data; curl instance is null."));

  // Set auth and content-type for request
  struct curl_slist* headers = nullptr;
  RETURN_NOT_OK_ELSE(set_auth(&headers), curl_slist_free_all(headers));
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

  STATS_FUNC_OUT(rest_curl_delete);
}

}  // namespace sm
}  // namespace tiledb
