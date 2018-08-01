/**
 * @file   client.cc
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
 * This file implements curl client helper functions.
 */
#include "client.h"
#include "curl.h"
#include "tiledb/sm/misc/stats.h"

tiledb::sm::Status get_array_schema_json_from_rest(
    std::string rest_server, std::string uri, char** json_returned) {
  STATS_FUNC_IN(serialization_get_array_schema_json_from_rest);
  // init the curl session
  CURL* curl = curl_easy_init();

  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/arrays/group/group1/project/project1/uri/" +
                    uri_escaped;
  curl_free(uri_escaped);

  struct MemoryStruct memoryStruct;
  CURLcode res = get_json(curl, url, &memoryStruct);
  // Check for errors
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  curl_easy_cleanup(curl);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error json object
    return tiledb::sm::Status::Error(
        std::string("rest array get() failed: ") +
        ((memoryStruct.size > 0) ? memoryStruct.memory :
                                   " No error message from server"));
  }

  // Copy the return message
  *json_returned = (char*)std::malloc(memoryStruct.size * sizeof(char));
  strcpy(*json_returned, memoryStruct.memory);

  std::free(memoryStruct.memory);
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_get_array_schema_json_from_rest);
}

tiledb::sm::Status post_array_schema_json_to_rest(
    std::string rest_server, std::string uri, char* json) {
  STATS_FUNC_IN(serialization_post_array_schema_json_from_rest);
  // init the curl session
  CURL* curl = curl_easy_init();

  // Build the url
  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/arrays/group/group1/project/project1/uri/" +
                    uri_escaped;
  curl_free(uri_escaped);

  struct MemoryStruct memoryStruct;
  CURLcode res = post_json(curl, url, json, &memoryStruct);
  /* Check for errors */
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  curl_easy_cleanup(curl);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error json object

    return tiledb::sm::Status::Error(
        std::string("rest array post() failed: ") +
        ((memoryStruct.size > 0) ? memoryStruct.memory :
                                   " No error message from server"));
  }

  std::free(memoryStruct.memory);
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_post_array_schema_json_from_rest);
}

tiledb::sm::Status submit_query_json_to_rest(
    std::string rest_server,
    std::string uri,
    char* json,
    char** json_returned) {
  STATS_FUNC_IN(serialization_submit_query_json_to_rest);
  // init the curl session
  CURL* curl = curl_easy_init();

  // Build the url
  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/queries/group/group1/project/project1/uri/" +
                    uri_escaped + "/submit";
  curl_free(uri_escaped);

  struct MemoryStruct memoryStruct;
  CURLcode res = post_json(curl, url, json, &memoryStruct);
  /* Check for errors */
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  curl_easy_cleanup(curl);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error json object

    return tiledb::sm::Status::Error(
        std::string("rest submit query post() failed: ") +
        ((memoryStruct.size > 0) ? memoryStruct.memory :
                                   " No error message from server"));
  }

  if (memoryStruct.size > 0) {
    // Copy the return message
    *json_returned = (char*)std::malloc(memoryStruct.size * sizeof(char));
    strcpy(*json_returned, memoryStruct.memory);
  }

  std::free(memoryStruct.memory);
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_submit_query_json_to_rest);
}

tiledb::sm::Status finalize_query_json_to_rest(
    std::string rest_server,
    std::string uri,
    char* json,
    char** json_returned) {
  STATS_FUNC_IN(serialization_finalize_query_json_to_rest);
  // init the curl session
  CURL* curl = curl_easy_init();

  // Build the url
  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/queries/group/group1/project/project1/uri/" +
                    uri_escaped + "/finalize";
  curl_free(uri_escaped);

  struct MemoryStruct memoryStruct;
  CURLcode res = post_json(curl, url, json, &memoryStruct);
  /* Check for errors */
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  curl_easy_cleanup(curl);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error json object

    return tiledb::sm::Status::Error(
        std::string("rest finalize query post() failed: ") +
        ((memoryStruct.size > 0) ? memoryStruct.memory :
                                   " No error message from server"));
  }

  if (memoryStruct.size > 0) {
    // Copy the return message
    *json_returned = (char*)std::malloc(memoryStruct.size * sizeof(char));
    strcpy(*json_returned, memoryStruct.memory);
  }

  std::free(memoryStruct.memory);
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_finalize_query_json_to_rest);
}
