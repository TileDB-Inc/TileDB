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
 * This file implements curl fetch functions.
 */
#include "curl.h"
#include <cstring>
#include "tiledb/sm/misc/stats.h"

/**
 * @Brief Call back for saving libcurl response body
 *
 * @param contents
 * @param size
 * @param nmemb
 * @param userp
 * @return
 */
size_t WriteMemoryCallback(
    void* contents, size_t size, size_t nmemb, void* userp) {
  // STATS_FUNC_IN(serialization_write_memory_callback);
  size_t realsize = size * nmemb;
  struct MemoryStruct* mem = (struct MemoryStruct*)userp;

  mem->memory = (char*)realloc(mem->memory, mem->size + realsize + 1);
  if (mem->memory == nullptr) {
    /* out of memory! */
    printf("not enough memory (realloc returned NULL)\n");
    return 0;
  }

  memcpy(&(mem->memory[mem->size]), contents, realsize);
  mem->size += realsize;
  mem->memory[mem->size] = 0;
  return realsize;
  // STATS_FUNC_OUT(serialization_write_memory_callback);
}

/**
 * @brief fetch and return url body via curl
 * @param curl
 * @param url
 * @param fetch
 * @return
 */
CURLcode curl_fetch_url(
    CURL* curl, const char* url, struct MemoryStruct* fetch) {
  STATS_FUNC_IN(serialization_curl_fetch_url);
  CURLcode rcode; /* curl result code */

  for (uint8_t i = 0; i < CURL_MAX_RETRIES; i++) {
    /* init memory */
    fetch->memory = (char*)calloc(1, sizeof(fetch->memory));

    /* check memory */
    if (fetch->memory == nullptr) {
      /* log error */
      fprintf(stderr, "ERROR: Failed to allocate memory in curl_fetch_url");
      /* return error */
      return CURLE_FAILED_INIT;
    }

    /* init size */
    fetch->size = 0;

    /* set url to fetch */
    curl_easy_setopt(curl, CURLOPT_URL, url);

    /* set calback function */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);

    /* pass fetch struct pointer */
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
    /* If we are here, then curl returned an error */
    /* Free allocated memory for return as we are now going to retry */
    free(fetch->memory);
    fetch->memory = nullptr;
    fetch->size = 0;
  }

  /* return */
  return rcode;
  STATS_FUNC_OUT(serialization_curl_fetch_url);
}

tiledb::sm::Status set_auth(
    CURL* curl, tiledb::sm::Config* config, struct curl_slist** headers) {
  const char* token = nullptr;
  if (config->get("rest.token", &token).ok()) {
    *headers = curl_slist_append(
        *headers, (std::string("X-TILEDB-REST-API-Key: ") + token).c_str());
  } else {
    // Get username
    const char* username = nullptr;
    auto st = config->get("rest.username", &username);
    if (!st.ok())
      return st;
    // Get password
    const char* password = nullptr;
    st = config->get("rest.password", &password);
    if (!st.ok())
      return st;
    std::string basic_auth = username + std::string(":") + password;
    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
    curl_easy_setopt(curl, CURLOPT_USERPWD, basic_auth.c_str());
  }
  return tiledb::sm::Status::Ok();
}

tiledb::sm::Status post_data(
    CURL* curl,
    tiledb::sm::Config* config,
    std::string url,
    tiledb::sm::SerializationType serialization_type,
    MemoryStruct* data,
    MemoryStruct* returned_data) {
  STATS_FUNC_IN(serialization_post_data);

  struct curl_slist* headers = nullptr;
  // Set auth for server
  auto st = set_auth(curl, config, &headers);
  if (!st.ok())
    return st;

  /* HTTP PUT please */
  curl_easy_setopt(curl, CURLOPT_POST, 1L);

  // TODO: If you post more than 2GB, use CURLOPT_POSTFIELDSIZE_LARGE.
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data->memory);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data->size);

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
    return tiledb::sm::Status::Error(
        std::string("rest post_data() failed: ") +
        ((returned_data->size > 0) ? returned_data->memory :
                                     " No error message from server"));
  }

  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_post_data);
}

tiledb::sm::Status get_data(
    CURL* curl,
    tiledb::sm::Config* config,
    std::string url,
    tiledb::sm::SerializationType serialization_type,
    MemoryStruct* returned_data) {
  STATS_FUNC_IN(serialization_get_data);

  // Set auth for server
  struct curl_slist* headers = nullptr;
  auto st = set_auth(curl, config, &headers);
  if (!st.ok())
    return st;

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
    return tiledb::sm::Status::Error(
        std::string("rest get_data() failed: ") +
        ((returned_data->size > 0) ? returned_data->memory :
                                     " No error message from server"));
  }
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_get_data);
}

tiledb::sm::Status delete_data(
    CURL* curl,
    tiledb::sm::Config* config,
    std::string url,
    tiledb::sm::SerializationType serialization_type,
    MemoryStruct* returned_data) {
  STATS_FUNC_IN(serialization_delete_data);

  // Set auth for server
  struct curl_slist* headers = nullptr;
  auto st = set_auth(curl, config, &headers);
  if (!st.ok())
    return st;

  /* HTTP DELETE please */
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");

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
    return tiledb::sm::Status::Error(
        std::string("rest delete_data() failed: ") +
        ((returned_data->size > 0) ? returned_data->memory :
                                     " No error message from server"));
  }
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_delete_data);
}
