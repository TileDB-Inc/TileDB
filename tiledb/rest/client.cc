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

tiledb::sm::Status get_array_schema_json_from_rest(
    std::string rest_server, std::string uri, char** json_returned) {
  // init the curl session
  CURL* curl = curl_easy_init();

  std::string url = std::string(rest_server) +
                    "/v1/arrays/group/group1/project/project1/uri/" +
                    curl_easy_escape(curl, uri.c_str(), uri.length());
  struct MemoryStruct memoryStruct;
  CURLcode res = get_json(curl, url, &memoryStruct);
  // Check for errors
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error json object
    return tiledb::sm::Status::Error(
        std::string("rest array get() failed: ") +
        ((memoryStruct.size > 0) ? memoryStruct.memory :
                                   " No error message from server"));
  }

  // Copy the return message
  *json_returned = (char*)std::malloc(memoryStruct.size * sizeof(char));
  std::memcpy(*json_returned, memoryStruct.memory, memoryStruct.size);

  std::free(memoryStruct.memory);
  return tiledb::sm::Status::Ok();
}

tiledb::sm::Status post_array_schema_json_to_rest(
    std::string rest_server, std::string uri, char* json) {
  // init the curl session
  CURL* curl = curl_easy_init();

  // Build the url
  std::string url = std::string(rest_server) +
                    "/v1/arrays/group/group1/project/project1/uri/" +
                    curl_easy_escape(curl, uri.c_str(), uri.length());
  struct MemoryStruct memoryStruct;
  CURLcode res = post_json(curl, url, json, &memoryStruct);
  /* Check for errors */
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error json object

    return tiledb::sm::Status::Error(
        std::string("rest array post() failed: ") +
        ((memoryStruct.size > 0) ? memoryStruct.memory :
                                   " No error message from server"));
  }

  std::free(memoryStruct.memory);
  return tiledb::sm::Status::Ok();
}
tiledb::sm::Status submit_query_json_to_rest(
    std::string rest_server,
    std::string uri,
    char* json,
    char** json_returned) {
  // init the curl session
  CURL* curl = curl_easy_init();

  // Build the url
  std::string url = std::string(rest_server) +
                    "/v1/queries/group/group1/project/project1/uri/" +
                    curl_easy_escape(curl, uri.c_str(), uri.length()) +
                    "/submit";
  struct MemoryStruct memoryStruct;
  CURLcode res = post_json(curl, url, json, &memoryStruct);
  /* Check for errors */
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error json object

    return tiledb::sm::Status::Error(
        std::string("rest array post() failed: ") +
        ((memoryStruct.size > 0) ? memoryStruct.memory :
                                   " No error message from server"));
  }

  if (memoryStruct.size > 0) {
    // Copy the return message
    *json_returned = (char*)std::malloc(memoryStruct.size * sizeof(char));
    std::memcpy(*json_returned, memoryStruct.memory, memoryStruct.size);
  }

  std::free(memoryStruct.memory);
  return tiledb::sm::Status::Ok();
}
