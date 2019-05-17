/**
 * @file   rest_client.cc
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
 * This file declares a REST client class.
 */

#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/capnp_utils.h"
#include "tiledb/sm/serialization/query.h"

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/rest/curl.h"
#include "tiledb/sm/serialization/tiledb-rest.capnp.h"
#endif

namespace tiledb {
namespace sm {

#ifdef TILEDB_SERIALIZATION

RestClient::RestClient()
    : config_(nullptr)
    , serialization_type_(constants::serialization_default_format) {
}

Status RestClient::init(const Config* config) {
  if (config == nullptr)
    return LOG_STATUS(
        Status::RestError("Error initializing rest client; config is null."));

  config_ = config;

  const char* c_str;
  RETURN_NOT_OK(config_->get("rest.server_address", &c_str));
  if (c_str != nullptr)
    rest_server_ = std::string(c_str);
  if (rest_server_.empty())
    return LOG_STATUS(Status::RestError(
        "Error initializing rest client; server address is empty."));

  RETURN_NOT_OK(config_->get("rest.server_serialization_format", &c_str));
  if (c_str != nullptr)
    RETURN_NOT_OK(serialization_type_enum(c_str, &serialization_type_));

  return Status::Ok();
}

Status RestClient::get_array_schema_from_rest(
    const URI& uri, ArraySchema** array_schema) {
  STATS_FUNC_IN(rest_array_get_schema);

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = rest_server_ + "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri);

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.get_data(url, serialization_type_, &returned_data));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status::RestError(
        "Error getting array schema from REST; server returned no data."));

  return serialization::array_schema_deserialize(
      array_schema, serialization_type_, returned_data);

  STATS_FUNC_OUT(rest_array_get_schema);
}

Status RestClient::post_array_schema_to_rest(
    const URI& uri, ArraySchema* array_schema) {
  STATS_FUNC_IN(rest_array_create);

  Buffer serialized;
  RETURN_NOT_OK(serialization::array_schema_serialize(
      array_schema, serialization_type_, &serialized));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = rest_server_ + "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri);

  Buffer returned_data;
  return curlc.post_data(url, serialization_type_, &serialized, &returned_data);

  STATS_FUNC_OUT(rest_array_create);
}

Status RestClient::deregister_array_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = rest_server_ + "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) + "/deregister";

  Buffer returned_data;
  return curlc.delete_data(url, serialization_type_, &returned_data);
}

Status RestClient::get_array_non_empty_domain(
    Array* array, void* domain, bool* is_empty) {
  if (array == nullptr)
    return LOG_STATUS(
        Status::RestError("Cannot get array non-empty domain; array is null"));
  if (array->array_schema() == nullptr)
    return LOG_STATUS(Status::RestError(
        "Cannot get array non-empty domain; array schema is null"));
  if (array->array_uri().to_string().empty())
    return LOG_STATUS(Status::RestError(
        "Cannot get array non-empty domain; array URI is empty"));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(array->array_uri().get_rest_components(&array_ns, &array_uri));
  std::string url = rest_server_ + "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) + "/non_empty_domain";

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.get_data(url, serialization_type_, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        Status::RestError("Error getting array non-empty domain "
                          "from REST; server returned no data."));

  // Deserialize data returned
  return serialization::nonempty_domain_deserialize(
      array, returned_data, serialization_type_, domain, is_empty);

  return Status::Ok();
}

Status RestClient::submit_query_to_rest(const URI& uri, Query* query) {
  STATS_FUNC_IN(rest_query_submit);

  // Serialize data to send
  Buffer serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = rest_server_ + "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) +
                    "/query/submit?type=" + query_type_str(query->type());

  Buffer returned_data;
  RETURN_NOT_OK(
      curlc.post_data(url, serialization_type_, &serialized, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status::RestError(
        "Error submitting query to REST; server returned no data."));

  // Deserialize data returned
  return serialization::query_deserialize(
      returned_data, serialization_type_, true, query);

  STATS_FUNC_OUT(rest_query_submit);
}

Status RestClient::finalize_query_to_rest(const URI& uri, Query* query) {
  // Serialize data to send
  Buffer serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = rest_server_ + "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) +
                    "/query/finalize?type=" + query_type_str(query->type());

  Buffer returned_data;
  RETURN_NOT_OK(
      curlc.post_data(url, serialization_type_, &serialized, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        Status::RestError("Error finalizing query; server returned no data."));

  // Deserialize data returned
  return serialization::query_deserialize(
      returned_data, serialization_type_, true, query);
}

#else

RestClient::RestClient() {
  (void)config_;
  (void)rest_server_;
  (void)serialization_type_;
}

Status RestClient::init(const Config*) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::get_array_schema_from_rest(const URI&, ArraySchema**) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::post_array_schema_to_rest(const URI&, ArraySchema*) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::deregister_array_from_rest(const URI&) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::get_array_non_empty_domain(Array*, void*, bool*) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::submit_query_to_rest(const URI&, Query*) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::finalize_query_to_rest(const URI&, Query*) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace sm
}  // namespace tiledb