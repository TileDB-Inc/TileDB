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

/**
 * Calls curl_easy_init(), wrapping the returned object in a safe pointer
 * that will be cleaned up automatically.
 */
std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> init_curl_safe() {
  return std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>(
      curl_easy_init(), curl_easy_cleanup);
}

tiledb::sm::Status get_array_schema_from_rest(
    const tiledb::sm::Config* config,
    const std::string& rest_server,
    const std::string& uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::ArraySchema** array_schema) {
  STATS_FUNC_IN(serialization_get_array_schema_from_rest);

  const char* organization;
  RETURN_NOT_OK(config->get("rest.organization", &organization));
  if (organization == nullptr)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error getting array schema from REST; "
        "config param rest.organization cannot be null."));

  // Escape the URL and make a copy
  auto curl = init_curl_safe();
  char* uri_escaped = curl_easy_escape(curl.get(), uri.c_str(), uri.length());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + uri_escaped;
  curl_free(uri_escaped);

  // Get the data
  tiledb::sm::Buffer returned_data;
  RETURN_NOT_OK(curl::get_data(
      curl.get(), config, url, serialization_type, &returned_data));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error getting array schema from REST; server returned no data."));

  return rest::capnp::array_schema_deserialize(
      array_schema, serialization_type, returned_data);

  STATS_FUNC_OUT(serialization_get_array_schema_from_rest);
}

tiledb::sm::Status post_array_schema_to_rest(
    const tiledb::sm::Config* config,
    const std::string& rest_server,
    const std::string& uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::ArraySchema* array_schema) {
  STATS_FUNC_IN(serialization_post_array_schema_to_rest);

  const char* organization;
  RETURN_NOT_OK(config->get("rest.organization", &organization));
  if (organization == nullptr)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error posting array schema to REST; "
        "config param rest.organization cannot be null."));

  tiledb::sm::Buffer serialized;
  RETURN_NOT_OK(rest::capnp::array_schema_serialize(
      array_schema, serialization_type, &serialized));

  // Escape the URL and make a copy
  auto curl = init_curl_safe();
  char* uri_escaped = curl_easy_escape(curl.get(), uri.c_str(), uri.length());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + uri_escaped;
  curl_free(uri_escaped);

  tiledb::sm::Buffer returned_data;
  return curl::post_data(
      curl.get(), config, url, serialization_type, &serialized, &returned_data);

  STATS_FUNC_OUT(serialization_post_array_schema_to_rest);
}

tiledb::sm::Status deregister_array_from_rest(
    const tiledb::sm::Config* config,
    const std::string& rest_server,
    const std::string& uri,
    tiledb::sm::SerializationType serialization_type) {
  const char* organization;
  RETURN_NOT_OK(config->get("rest.organization", &organization));
  if (organization == nullptr)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error deregistering array schema on REST; "
        "config param rest.organization cannot be null."));

  // Escape the URL and make a copy
  auto curl = init_curl_safe();
  char* uri_escaped = curl_easy_escape(curl.get(), uri.c_str(), uri.length());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + uri_escaped + "/deregister";
  curl_free(uri_escaped);

  tiledb::sm::Buffer returned_data;
  return curl::delete_data(
      curl.get(), config, url, serialization_type, &returned_data);
}

tiledb::sm::Status get_array_non_empty_domain(
    const tiledb::sm::Config* config,
    const std::string& rest_server,
    tiledb::sm::Array* array,
    void* domain,
    bool* is_empty) {
  STATS_FUNC_IN(serialization_get_array_non_empty_domain);

  if (array == nullptr)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Cannot get array non-empty domain; array is null"));
  if (array->array_uri().to_string().empty())
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Cannot get array non-empty domain; array URI is empty"));

  const char* organization;
  RETURN_NOT_OK(config->get("rest.organization", &organization));
  if (organization == nullptr)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Cannot get array non-empty domain; "
        "config param rest.organization cannot be null."));

  // Escape the URL and make a copy
  auto curl = init_curl_safe();
  const std::string& uri = array->array_uri().to_string();
  char* uri_escaped = curl_easy_escape(curl.get(), uri.c_str(), uri.length());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + uri_escaped + "/non_empty_domain";
  curl_free(uri_escaped);

  tiledb::sm::Buffer returned_data;
  RETURN_NOT_OK(curl::get_data(
      curl.get(),
      config,
      url,
      tiledb::sm::SerializationType::JSON,
      &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        tiledb::sm::Status::RestError("Error getting array non-empty domain "
                                      "from REST; server returned no data."));

  // Currently only json data is supported, so let's decode it here.
  ::capnp::JsonCodec json;
  ::capnp::MallocMessageBuilder message_builder;
  rest::capnp::NonEmptyDomain::Builder nonEmptyDomainBuilder =
      message_builder.initRoot<rest::capnp::NonEmptyDomain>();
  json.decode(
      kj::StringPtr(static_cast<const char*>(returned_data.data())),
      nonEmptyDomainBuilder);
  rest::capnp::NonEmptyDomain::Reader nonEmptyDomainReader =
      nonEmptyDomainBuilder.asReader();
  *is_empty = nonEmptyDomainReader.getIsEmpty();

  tiledb::sm::Datatype domainType = array->array_schema()->domain()->type();

  // If there is a nonEmptyDomain we need to set domain variables
  if (nonEmptyDomainReader.hasNonEmptyDomain()) {
    rest::capnp::Map<::capnp::Text, rest::capnp::DomainArray>::Reader
        nonEmptyDomainList = nonEmptyDomainReader.getNonEmptyDomain();

    size_t domainPosition = 0;
    // Loop through dimension's domain in order
    for (auto entry : nonEmptyDomainList.getEntries()) {
      rest::capnp::DomainArray::Reader domainArrayReader = entry.getValue();
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
    const tiledb::sm::Config* config,
    const std::string& rest_server,
    const std::string& uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Query* query) {
  STATS_FUNC_IN(serialization_submit_query_to_rest);

  const char* organization;
  RETURN_NOT_OK(config->get("rest.organization", &organization));
  if (organization == nullptr)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error submitting query to REST; "
        "config param rest.organization cannot be null."));

  // Serialize data to send
  tiledb::sm::Buffer serialized;
  RETURN_NOT_OK(
      rest::capnp::query_serialize(query, serialization_type, &serialized));

  // Escape the URL and make a copy
  auto curl = init_curl_safe();
  char* uri_escaped = curl_easy_escape(curl.get(), uri.c_str(), uri.length());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + uri_escaped + "/query/submit?type=" +
                    tiledb::sm::query_type_str(query->type());
  curl_free(uri_escaped);

  tiledb::sm::Buffer returned_data;
  auto st = curl::post_data(
      curl.get(), config, url, serialization_type, &serialized, &returned_data);

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error submitting query to REST; server returned no data."));

  // Deserialize data returned
  return rest::capnp::query_deserialize(
      query, serialization_type, returned_data);

  STATS_FUNC_OUT(serialization_submit_query_to_rest);
}

tiledb::sm::Status finalize_query_to_rest(
    const tiledb::sm::Config* config,
    const std::string& rest_server,
    const std::string& uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Query* query) {
  STATS_FUNC_IN(serialization_finalize_query_to_rest);

  const char* organization;
  RETURN_NOT_OK(config->get("rest.organization", &organization));
  if (organization == nullptr)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Cannot finalize query; "
        "config param rest.organization cannot be null."));

  // Serialize data to send
  tiledb::sm::Buffer serialized;
  RETURN_NOT_OK(
      rest::capnp::query_serialize(query, serialization_type, &serialized));

  // Escape the URL and make a copy
  auto curl = init_curl_safe();
  char* uri_escaped = curl_easy_escape(curl.get(), uri.c_str(), uri.length());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + uri_escaped + "/query/finalize?type=" +
                    tiledb::sm::query_type_str(query->type());
  curl_free(uri_escaped);

  tiledb::sm::Buffer returned_data;
  auto st = curl::post_data(
      curl.get(), config, url, serialization_type, &serialized, &returned_data);

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error finalizing query; server returned no data."));

  // Deserialize data returned
  return rest::capnp::query_deserialize(
      query, serialization_type, returned_data);

  STATS_FUNC_OUT(serialization_finalize_query_to_rest);
}

}  // namespace rest
}  // namespace tiledb