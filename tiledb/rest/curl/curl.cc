/**
 * @file   curl.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines high-level libcurl helper functions.
 */

#include "tiledb/rest/curl/curl.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

#include <cstring>

// TODO: replace this with config option
#define CURL_MAX_RETRIES 3

namespace tiledb {
namespace curl {

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
  auto buffer = (tiledb::sm::Buffer*)userdata;
  auto st = buffer->write(contents, content_nbytes);
  if (!st.ok()) {
    tiledb::sm::LOG_ERROR(
        "Cannot copy libcurl response data; buffer write failed: " +
        st.to_string());
    return 0;
  }

  return content_nbytes;
}

/**
 * Sets authorization (token or username+password) on the curl instance using
 * the given config instance.
 *
 * @param curl Curl instance to set authorization on
 * @param config TileDB config instance
 * @param headers Headers (may be modified)
 * @return Status
 */
tiledb::sm::Status set_auth(
    CURL* curl, const tiledb::sm::Config* config, struct curl_slist** headers) {
  const char* token = nullptr;
  RETURN_NOT_OK(config->get("rest.token", &token));

  if (token != nullptr) {
    *headers = curl_slist_append(
        *headers, (std::string("X-TILEDB-REST-API-Key: ") + token).c_str());
  } else {
    // Try username+password instead of token
    const char* username = nullptr;
    const char* password = nullptr;
    RETURN_NOT_OK(config->get("rest.username", &username));
    RETURN_NOT_OK(config->get("rest.password", &password));

    // Check for no auth.
    if (username == nullptr || password == nullptr)
      return LOG_STATUS(
          tiledb::sm::Status::RestError("Cannot set curl auth; either token or "
                                        "username/password must be set."));

    std::string basic_auth = username + std::string(":") + password;
    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
    curl_easy_setopt(curl, CURLOPT_USERPWD, basic_auth.c_str());
  }

  return tiledb::sm::Status::Ok();
}

/**
 * Fetches the contents of the given URL into the given buffer.
 *
 * @param curl pointer to curl instance
 * @param url URL to fetch
 * @param fetch Buffer that will store the response data
 * @return CURLcode
 */
CURLcode curl_fetch_url(
    CURL* curl, const char* url, tiledb::sm::Buffer* fetch) {
  STATS_FUNC_IN(serialization_curl_fetch_url);

  CURLcode rcode = CURLE_OK;
  for (uint8_t i = 0; i < CURL_MAX_RETRIES; i++) {
    fetch->set_size(0);
    fetch->set_offset(0);

    /* set url to fetch */
    curl_easy_setopt(curl, CURLOPT_URL, url);

    /* set callback function */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_memory_callback);

    /* pass fetch buffer pointer */
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)fetch);

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
    rcode = curl_easy_perform(curl);
    /* If Curl call was successful (not http status, but no socket error, etc)
     * break */
    if (rcode == CURLE_OK)
      break;

    /* Retry on curl errors */
  }

  return rcode;

  STATS_FUNC_OUT(serialization_curl_fetch_url);
}

tiledb::sm::Status post_data(
    CURL* curl,
    const tiledb::sm::Config* config,
    const std::string& url,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Buffer* data,
    tiledb::sm::Buffer* returned_data) {
  STATS_FUNC_IN(serialization_post_data);

  // TODO: If you post more than 2GB, use CURLOPT_POSTFIELDSIZE_LARGE.
  const uint64_t post_size_limit = uint64_t(2) * 1024 * 1024 * 1024;
  if (data->size() > post_size_limit)
    return LOG_STATUS(
        tiledb::sm::Status::RestError("Error posting data; buffer size > 2GB"));

  // Set auth for server
  struct curl_slist* headers = nullptr;
  RETURN_NOT_OK(set_auth(curl, config, &headers));

  if (serialization_type == tiledb::sm::SerializationType::JSON)
    headers = curl_slist_append(headers, "Content-Type: application/json");
  else
    headers = curl_slist_append(headers, "Content-Type: application/capnp");

  /* HTTP PUT please */
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data->data());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data->size());

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode ret = curl_fetch_url(curl, url.c_str(), returned_data);
  curl_slist_free_all(headers);

  // Check for errors
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  if (ret != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error data object
    return LOG_STATUS(tiledb::sm::Status::RestError(
        std::string("Error posting data: ") +
        ((returned_data->size() > 0) ?
             static_cast<const char*>(returned_data->data()) :
             " No error message from server")));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_post_data);
}

tiledb::sm::Status get_data(
    CURL* curl,
    const tiledb::sm::Config* config,
    const std::string& url,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Buffer* returned_data) {
  STATS_FUNC_IN(serialization_get_data);

  // Set auth for server
  struct curl_slist* headers = nullptr;
  RETURN_NOT_OK(set_auth(curl, config, &headers));

  if (serialization_type == tiledb::sm::SerializationType::JSON)
    headers = curl_slist_append(headers, "Content-Type: application/json");
  else
    headers = curl_slist_append(headers, "Content-Type: application/capnp");

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode ret = curl_fetch_url(curl, url.c_str(), returned_data);
  curl_slist_free_all(headers);

  // Check for errors
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  if (ret != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error data object
    return LOG_STATUS(tiledb::sm::Status::RestError(
        std::string("Error getting data: ") +
        ((returned_data->size() > 0) ?
             static_cast<const char*>(returned_data->data()) :
             " No error message from server")));
  }
  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_get_data);
}

tiledb::sm::Status delete_data(
    CURL* curl,
    const tiledb::sm::Config* config,
    const std::string& url,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Buffer* returned_data) {
  STATS_FUNC_IN(serialization_delete_data);

  // Set auth for server
  struct curl_slist* headers = nullptr;
  RETURN_NOT_OK(set_auth(curl, config, &headers));

  if (serialization_type == tiledb::sm::SerializationType::JSON)
    headers = curl_slist_append(headers, "Content-Type: application/json");
  else
    headers = curl_slist_append(headers, "Content-Type: application/capnp");

  /* HTTP DELETE please */
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");

  /* pass our list of custom made headers */
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode ret = curl_fetch_url(curl, url.c_str(), returned_data);
  curl_slist_free_all(headers);

  // Check for errors
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  if (ret != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error data object
    return LOG_STATUS(tiledb::sm::Status::RestError(
        std::string("Error deleting data: ") +
        ((returned_data->size() > 0) ?
             static_cast<const char*>(returned_data->data()) :
             " No error message from server")));
  }
  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_delete_data);
}

}  // namespace curl
}  // namespace tiledb
