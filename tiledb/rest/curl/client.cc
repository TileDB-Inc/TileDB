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
#include "capnp/compat/json.h"
#include "tiledb/rest/capnp/array.h"
#include "tiledb/rest/capnp/query.h"
#include "tiledb/rest/capnp/tiledb-rest.capnp.h"
#include "tiledb/rest/curl/curl.h"
#include "tiledb/sm/misc/stats.h"
namespace tiledb {
namespace rest {
tiledb::sm::Status get_array_schema_from_rest(
    tiledb::sm::Config* config,
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::ArraySchema** array_schema) {
  STATS_FUNC_IN(serialization_get_array_schema_from_rest);
  // init the curl session
  CURL* curl = curl_easy_init();

  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/arrays/organization_place_holder/" + uri_escaped;
  curl_free(uri_escaped);

  struct MemoryStruct returned_data = {nullptr, 0};
  tiledb::sm::Status res =
      get_data(curl, config, url, serialization_type, &returned_data);
  curl_easy_cleanup(curl);
  // Check for errors
  if (!res.ok()) {
    if (returned_data.memory != nullptr)
      std::free(returned_data.memory);
    return res;
  }

  if (returned_data.memory == nullptr || returned_data.size == 0)
    return tiledb::sm::Status::Error("Server returned no data!");

  tiledb::sm::Status status = array_schema_deserialize(
      array_schema,
      serialization_type,
      returned_data.memory,
      returned_data.size);
  free(returned_data.memory);
  return status;
  STATS_FUNC_OUT(serialization_get_array_schema_from_rest);
}

tiledb::sm::Status post_array_schema_to_rest(
    tiledb::sm::Config* config,
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
  tiledb::sm::Status res =
      post_data(curl, config, url, serialization_type, &data, &returned_data);
  /* Check for errors */
  curl_easy_cleanup(curl);
  // Cleanup initial data
  delete[] data.memory;
  if (!res.ok()) {
    if (returned_data.memory != nullptr)
      std::free(returned_data.memory);
    return res;
  }

  std::free(returned_data.memory);
  delete[] data.memory;
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_post_array_schema_to_rest);
}

tiledb::sm::Status delete_array_schema_from_rest(
    tiledb::sm::Config* config,
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
  tiledb::sm::Status res =
      delete_data(curl, config, url, serialization_type, &returned_data);
  curl_easy_cleanup(curl);
  // Check for errors
  if (!res.ok()) {
    if (returned_data.memory != nullptr)
      std::free(returned_data.memory);
    return res;
  }

  free(returned_data.memory);
  return tiledb::sm::Status::Ok();
  STATS_FUNC_OUT(serialization_delete_array_schema_from_rest);
}

tiledb::sm::Status get_array_non_empty_domain(
    tiledb::sm::Config* config,
    std::string rest_server,
    tiledb::sm::Array* array,
    void* domain,
    bool* is_empty) {
  STATS_FUNC_IN(serialization_get_array_non_empty_domain);
  // init the curl session
  CURL* curl = curl_easy_init();

  if (array == nullptr)
    return tiledb::sm::Status::Error(
        "Error in get array non_empty_domain, openArray passed was null");

  if (array->array_uri().to_string().empty())
    return tiledb::sm::Status::Error(
        "Error in get array non_empty_domain, array uri is null");

  std::string uri = array->array_uri().to_string();
  char* uri_escaped = curl_easy_escape(curl, uri.c_str(), uri.length());
  std::string url = std::string(rest_server) +
                    "/v1/arrays/organization_place_holder/" + uri_escaped +
                    "/non_empty_domain";
  curl_free(uri_escaped);

  struct MemoryStruct returned_data;
  tiledb::sm::Status res = get_data(
      curl, config, url, tiledb::sm::SerializationType::JSON, &returned_data);
  curl_easy_cleanup(curl);
  // Check for errors
  if (!res.ok()) {
    if (returned_data.memory != nullptr)
      std::free(returned_data.memory);
    return res;
  }

  if (returned_data.memory == nullptr || returned_data.size == 0)
    return tiledb::sm::Status::Error("Server returned no data!");

  // Currently only json data is supported, so let's decode it here.
  capnp::JsonCodec json;
  capnp::MallocMessageBuilder message_builder;
  ::NonEmptyDomain::Builder nonEmptyDomainBuilder =
      message_builder.initRoot<::NonEmptyDomain>();
  json.decode(kj::StringPtr(returned_data.memory), nonEmptyDomainBuilder);
  ::NonEmptyDomain::Reader nonEmptyDomainReader =
      nonEmptyDomainBuilder.asReader();
  *is_empty = nonEmptyDomainReader.getIsEmpty();

  tiledb::sm::Datatype domainType = array->array_schema()->domain()->type();

  // If there is a nonEmptyDomain we need to set domain variables
  if (nonEmptyDomainReader.hasNonEmptyDomain()) {
    ::Map<capnp::Text, ::DomainArray>::Reader nonEmptyDomainList =
        nonEmptyDomainReader.getNonEmptyDomain();

    size_t domainPosition = 0;
    // Loop through dimension's domain in order
    for (auto entry : nonEmptyDomainList.getEntries()) {
      ::DomainArray::Reader domainArrayReader = entry.getValue();
      switch (domainType) {
        case tiledb::sm::Datatype::INT8: {
          if (domainArrayReader.hasInt8()) {
            ::capnp::List<int8_t>::Reader domainList =
                domainArrayReader.getInt8();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int8_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::UINT8: {
          if (domainArrayReader.hasUint8()) {
            ::capnp::List<uint8_t>::Reader domainList =
                domainArrayReader.getUint8();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint8_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::INT16: {
          if (domainArrayReader.hasInt16()) {
            ::capnp::List<int16_t>::Reader domainList =
                domainArrayReader.getInt16();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int16_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::UINT16: {
          if (domainArrayReader.hasUint16()) {
            ::capnp::List<uint16_t>::Reader domainList =
                domainArrayReader.getUint16();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint16_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::INT32: {
          if (domainArrayReader.hasInt32()) {
            ::capnp::List<int32_t>::Reader domainList =
                domainArrayReader.getInt32();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int32_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::UINT32: {
          if (domainArrayReader.hasUint32()) {
            ::capnp::List<uint32_t>::Reader domainList =
                domainArrayReader.getUint32();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint32_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::INT64: {
          if (domainArrayReader.hasInt64()) {
            ::capnp::List<int64_t>::Reader domainList =
                domainArrayReader.getInt64();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int64_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::UINT64: {
          if (domainArrayReader.hasUint64()) {
            ::capnp::List<uint64_t>::Reader domainList =
                domainArrayReader.getUint64();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint64_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::FLOAT32: {
          if (domainArrayReader.hasFloat32()) {
            ::capnp::List<float>::Reader domainList =
                domainArrayReader.getFloat32();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<float*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::FLOAT64: {
          if (domainArrayReader.hasFloat64()) {
            ::capnp::List<double>::Reader domainList =
                domainArrayReader.getFloat64();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<double*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        default:
          return tiledb::sm::Status::Error(
              "unknown domain type in trying to get non_empty_domain from "
              "rest");
      }
    }
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_get_array_non_empty_domain);
}

tiledb::sm::Status submit_query_to_rest(
    tiledb::sm::Config* config,
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
  std::string url =
      std::string(rest_server) + "/v1/arrays/organization_place_holder/" +
      uri_escaped +
      "/query/submit?type=" + tiledb::sm::query_type_str(query->type());
  curl_free(uri_escaped);

  struct MemoryStruct returned_data = {nullptr, 0};
  tiledb::sm::Status res =
      post_data(curl, config, url, serialization_type, &data, &returned_data);

  // Cleanup initial data
  curl_easy_cleanup(curl);
  /* Check for errors */
  delete[] data.memory;
  if (!res.ok()) {
    if (returned_data.memory != nullptr)
      std::free(returned_data.memory);
    return res;
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
    tiledb::sm::Config* config,
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
  std::string url =
      std::string(rest_server) + "/v1/arrays/organization_place_holder/" +
      uri_escaped +
      "/query/finalize?type=" + tiledb::sm::query_type_str(query->type());
  curl_free(uri_escaped);

  struct MemoryStruct returned_data = {nullptr, 0};
  tiledb::sm::Status res =
      post_data(curl, config, url, serialization_type, &data, &returned_data);
  curl_easy_cleanup(curl);
  // Cleanup initial data
  delete[] data.memory;
  /* Check for errors */
  if (!res.ok()) {
    if (returned_data.memory != nullptr)
      std::free(returned_data.memory);
    return res;
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