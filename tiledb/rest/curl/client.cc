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
#include "tiledb/rest/curl/client.h"
#include "tiledb/rest/capnp/array.h"
#include "tiledb/rest/capnp/query.h"
#include "tiledb/rest/curl/curl.h"
#include "tiledb/sm/misc/stats.h"
namespace tiledb {
namespace rest {
tiledb::sm::Status get_array_schema_from_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::ArraySchema* array_schema) {
  STATS_FUNC_IN(serialization_get_array_schema_from_rest);
  // init the curl session
  CURL* curl = curl_easy_init();

  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/arrays/organization_place_holder/" + uri_escaped;
  curl_free(uri_escaped);

  struct MemoryStruct returned_data = {nullptr, 0};
  CURLcode res = get_data(curl, url, serialization_type, &returned_data);
  // Check for errors
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  curl_easy_cleanup(curl);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error data object
    return tiledb::sm::Status::Error(
        std::string("rest array get() failed: ") +
        ((returned_data.size > 0) ? returned_data.memory :
                                    " No error message from server"));
  }

  if (returned_data.memory == nullptr || returned_data.size == 0)
    return tiledb::sm::Status::Error("Server returned no data!");

  tiledb::sm::Status status = array_schema_deserialize(
      &array_schema,
      serialization_type,
      returned_data.memory,
      returned_data.size);
  free(returned_data.memory);
  return status;
  STATS_FUNC_OUT(serialization_get_array_schema_from_rest);
}

tiledb::sm::Status post_array_schema_to_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::ArraySchema* array_schema) {
  STATS_FUNC_IN(serialization_post_array_schema_to_rest);
  struct MemoryStruct data = {nullptr, 0};
  tiledb::sm::Status status = array_schema_serialize(
      array_schema, serialization_type, &data.memory, &data.size);
  if (!status.ok()) {
    if (data.memory != nullptr)
      delete[] data.memory;
    return status;
  }
  // init the curl session
  CURL* curl = curl_easy_init();

  // Build the url
  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/arrays/organization_place_holder/" + uri_escaped;
  curl_free(uri_escaped);

  struct MemoryStruct returned_data = {nullptr, 0};
  CURLcode res =
      post_data(curl, url, serialization_type, &data, &returned_data);
  /* Check for errors */
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  curl_easy_cleanup(curl);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error data object

    return tiledb::sm::Status::Error(
        std::string("rest array post() failed: ") +
        ((returned_data.size > 0) ? returned_data.memory :
                                    " No error message from server"));
  }

  std::free(returned_data.memory);
  delete[] data.memory;
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_post_array_schema_to_rest);
}

tiledb::sm::Status delete_array_schema_from_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type) {
  STATS_FUNC_IN(serialization_delete_array_schema_from_rest);
  // init the curl session
  CURL* curl = curl_easy_init();

  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/arrays/group/group1/project/project1/uri/" +
                    uri_escaped;
  curl_free(uri_escaped);

  struct MemoryStruct returned_data = {nullptr, 0};
  CURLcode res = delete_data(curl, url, serialization_type, &returned_data);
  // Check for errors
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  curl_easy_cleanup(curl);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error data object
    return tiledb::sm::Status::Error(
        std::string("rest array get() failed: ") +
        ((returned_data.size > 0) ? returned_data.memory :
                                    " No error message from server"));
  }

  free(returned_data.memory);
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_delete_array_schema_from_rest);
}

tiledb::sm::Status submit_query_to_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Query* query) {
  STATS_FUNC_IN(serialization_submit_query_to_rest);
  // Serialize data to send
  struct MemoryStruct data = {nullptr, 0};
  tiledb::sm::Status status =
      query_serialize(query, serialization_type, &data.memory, &data.size);
  if (!status.ok()) {
    if (data.memory != nullptr)
      delete[] data.memory;
    return status;
  }

  // init the curl session
  CURL* curl = curl_easy_init();

  // Build the url
  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/arrays/organization_place_holder/" + uri_escaped +
                    "/query/submit";
  curl_free(uri_escaped);

  struct MemoryStruct returned_data = {nullptr, 0};
  CURLcode res =
      post_data(curl, url, serialization_type, &data, &returned_data);
  /* Check for errors */
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  curl_easy_cleanup(curl);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error data object

    return tiledb::sm::Status::Error(
        std::string("rest submit query post() failed: ") +
        ((returned_data.size > 0) ? returned_data.memory :
                                    " No error message from server"));
  }

  if (returned_data.memory == nullptr || returned_data.size == 0)
    return tiledb::sm::Status::Error("Server returned no data!");

  // Deserialize data returned
  status = query_deserialize(
      query, serialization_type, returned_data.memory, returned_data.size);
  free(returned_data.memory);

  return status;
  STATS_FUNC_OUT(serialization_submit_query_to_rest);
}

tiledb::sm::Status finalize_query_to_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Query* query) {
  STATS_FUNC_IN(serialization_finalize_query_to_rest);
  // Serialize data to send
  struct MemoryStruct data = {nullptr, 0};
  tiledb::sm::Status status =
      query_serialize(query, serialization_type, &data.memory, &data.size);
  if (!status.ok()) {
    if (data.memory != nullptr)
      delete[] data.memory;
    return status;
  }

  // init the curl session
  CURL* curl = curl_easy_init();

  // Build the url
  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/arrays/organization_place_holder/" + uri_escaped +
                    "/query/finalize";
  curl_free(uri_escaped);

  struct MemoryStruct returned_data = {nullptr, 0};
  CURLcode res =
      post_data(curl, url, serialization_type, &data, &returned_data);
  /* Check for errors */
  long httpCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
  curl_easy_cleanup(curl);
  if (res != CURLE_OK || httpCode >= 400) {
    // TODO: Should see if message has error data object

    return tiledb::sm::Status::Error(
        std::string("rest finalize query post() failed: ") +
        ((returned_data.size > 0) ? returned_data.memory :
                                    " No error message from server"));
  }

  if (returned_data.memory == nullptr || returned_data.size == 0)
    return tiledb::sm::Status::Error("Server returned no data!");
  // Deserialize data returned
  status = query_deserialize(
      query, serialization_type, returned_data.memory, returned_data.size);
  free(returned_data.memory);
  return status;
  STATS_FUNC_OUT(serialization_finalize_query_to_rest);
}

}  // namespace rest
}  // namespace tiledb