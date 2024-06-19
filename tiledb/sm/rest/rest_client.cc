/**
 * @file   rest_client.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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

// clang-format off
#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/array_schema_evolution.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/config.h"
#include "tiledb/sm/serialization/consolidation.h"
#include "tiledb/sm/serialization/enumeration.h"
#include "tiledb/sm/serialization/fragment_info.h"
#include "tiledb/sm/serialization/fragments.h"
#include "tiledb/sm/serialization/group.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/serialization/query_plan.h"
#include "tiledb/sm/serialization/tiledb-rest.capnp.h"
#include "tiledb/sm/serialization/vacuum.h"
#include "tiledb/sm/rest/curl.h" // must be included last to avoid Windows.h
#endif
// clang-format on

// Something, somewhere seems to be defining TIME_MS as a macro
#if defined(TIME_MS)
#undef TIME_MS
#endif

#include <cassert>

#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/config/config_iter.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/endian.h"
#include "tiledb/sm/misc/parse_argument.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_remote_buffer_storage.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/type/apply_with_type.h"

using namespace tiledb::common;

namespace tiledb::sm {

class RestClientException : public StatusException {
 public:
  explicit RestClientException(const std::string& message)
      : StatusException("RestClient", message) {
  }
};

class RestClientDisabledException : public RestClientException {
 public:
  explicit RestClientDisabledException()
      : RestClientException(
            "Cannot use rest client; serialization not enabled.") {
  }
};

#ifdef TILEDB_SERIALIZATION

RestClient::RestClient()
    : stats_(nullptr)
    , config_(nullptr)
    , compute_tp_(nullptr)
    , resubmit_incomplete_(true)
    , memory_tracker_(nullptr) {
  auto st = utils::parse::convert(
      Config::REST_SERIALIZATION_DEFAULT_FORMAT, &serialization_type_);
  throw_if_not_ok(st);
}

Status RestClient::init(
    stats::Stats* const parent_stats,
    const Config* config,
    ThreadPool* compute_tp,
    const std::shared_ptr<Logger>& logger,
    ContextResources& resources) {
  if (config == nullptr)
    return LOG_STATUS(
        Status_RestError("Error initializing rest client; config is null."));

  stats_ = parent_stats->create_child("RestClient");

  logger_ = logger->clone("curl ", ++logger_id_);

  config_ = config;
  compute_tp_ = compute_tp;

  // Setting the type of the memory tracker as MemoryTrackerType::REST_CLIENT
  // for now. This is because the class is used in many places not directly tied
  // to an array.
  memory_tracker_ = resources.create_memory_tracker();
  memory_tracker_->set_type(MemoryTrackerType::REST_CLIENT);

  const char* c_str;
  RETURN_NOT_OK(config_->get("rest.server_address", &c_str));
  if (c_str != nullptr) {
    rest_server_ = std::string(c_str);
    if (rest_server_.ends_with('/')) {
      size_t pos = rest_server_.find_last_not_of('/');
      rest_server_.resize(pos + 1);
    }
  }
  if (rest_server_.empty())
    return LOG_STATUS(Status_RestError(
        "Error initializing rest client; server address is empty."));

  RETURN_NOT_OK(config_->get("rest.server_serialization_format", &c_str));
  if (c_str != nullptr)
    RETURN_NOT_OK(serialization_type_enum(c_str, &serialization_type_));

  bool found = false;
  RETURN_NOT_OK(config_->get<bool>(
      "rest.resubmit_incomplete", &resubmit_incomplete_, &found));

  load_headers(*config_);

  RETURN_NOT_OK(config_->get("rest.payer_namespace", &c_str));
  if (c_str != nullptr) {
    extra_headers_["X-Payer"] = std::string(c_str);
  }

  return Status::Ok();
}

Status RestClient::set_header(
    const std::string& name, const std::string& value) {
  extra_headers_[name] = value;
  return Status::Ok();
}

bool RestClient::use_refactored_query(const Config& config) {
  bool found = false, use_refactored_query = false;
  auto status = config.get<bool>(
      "rest.use_refactored_array_open_and_query_submit",
      &use_refactored_query,
      &found);
  if (!status.ok() || !found) {
    throw std::runtime_error(
        "Cannot get rest.use_refactored_array_open_and_query_submit "
        "configuration option from config");
  }

  return use_refactored_query;
}

tuple<Status, std::optional<bool>> RestClient::check_array_exists_from_rest(
    const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK_TUPLE(uri.get_rest_components(&array_ns, &array_uri), nullopt);
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK_TUPLE(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_),
      nullopt);
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri);

  // Make the request, the return data is ignored
  Buffer returned_data;
  auto curl_st = curlc.get_data(
      stats_, url, serialization_type_, &returned_data, cache_key);

  auto&& [status_st, http_status_code] = curlc.last_http_status_code();
  RETURN_NOT_OK_TUPLE(status_st, std::nullopt);
  // First check for 404's which indicate does not exist
  if (http_status_code == 404) {
    return {Status::Ok(), false};
  }

  // Next handle any errors. This is second because a 404 produces a status
  RETURN_NOT_OK_TUPLE(curl_st, std::nullopt);

  // 200 http responses yield the array exists and user has permissions
  if (http_status_code == 200) {
    return {Status::Ok(), true};
  }

  // Default fall back, indicate it does not exist
  return {Status::Ok(), false};
}

tuple<Status, std::optional<bool>> RestClient::check_group_exists_from_rest(
    const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK_TUPLE(uri.get_rest_components(&group_ns, &group_uri), nullopt);
  const std::string cache_key = group_ns + ":" + group_uri;
  RETURN_NOT_OK_TUPLE(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_),
      nullopt);
  const std::string url = redirect_uri(cache_key) + "/v2/groups/" + group_ns +
                          "/" + curlc.url_escape(group_uri);

  // Make the request, the returned data is ignored for now.
  Buffer returned_data;
  auto curl_st = curlc.options(
      stats_, url, serialization_type_, &returned_data, cache_key);

  auto&& [status_st, http_status_code] = curlc.last_http_status_code();
  RETURN_NOT_OK_TUPLE(status_st, std::nullopt);
  // First check for 404's which indicate does not exist
  if (http_status_code == 404) {
    return {Status::Ok(), false};
  }

  // Next handle any errors. This is second because a 404 produces a status
  RETURN_NOT_OK_TUPLE(curl_st, std::nullopt);

  // 200 http responses yield the group exists and user has permissions
  if (http_status_code == 200) {
    return {Status::Ok(), true};
  }

  // Default fall back, indicate it does not exist
  return {Status::Ok(), false};
}

tuple<Status, optional<shared_ptr<ArraySchema>>>
RestClient::get_array_schema_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK_TUPLE(uri.get_rest_components(&array_ns, &array_uri), nullopt);
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK_TUPLE(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_),
      nullopt);
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri);

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK_TUPLE(
      curlc.get_data(
          stats_, url, serialization_type_, &returned_data, cache_key),
      nullopt);
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return {
        LOG_STATUS(Status_RestError(
            "Error getting array schema from REST; server returned no data.")),
        nullopt};

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK_TUPLE(
      ensure_json_null_delimited_string(&returned_data), nullopt);
  return {
      Status::Ok(),
      serialization::array_schema_deserialize(
          serialization_type_, returned_data, memory_tracker_)};
}

shared_ptr<ArraySchema> RestClient::post_array_schema_from_rest(
    const Config& config,
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    bool include_enumerations) {
  serialization::LoadArraySchemaRequest req(include_enumerations);

  Buffer buf;
  serialization::serialize_load_array_schema_request(
      config, req, serialization_type_, buf);

  // Wrap in a list
  BufferList serialized;
  throw_if_not_ok(serialized.add_buffer(std::move(buf)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) + "/schema?" +
                          "start_timestamp=" + std::to_string(timestamp_start) +
                          "&end_timestamp=" + std::to_string(timestamp_end);

  // Get the data
  Buffer returned_data;
  throw_if_not_ok(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0) {
    throw RestClientException(
        "Error getting array schema from REST; server returned no data.");
  }

  // Ensure data has a null delimiter for cap'n proto if using JSON
  throw_if_not_ok(ensure_json_null_delimited_string(&returned_data));
  return serialization::deserialize_load_array_schema_response(
      serialization_type_, returned_data, memory_tracker_);
}

Status RestClient::post_array_schema_to_rest(
    const URI& uri, const ArraySchema& array_schema) {
  Buffer buff;
  RETURN_NOT_OK(serialization::array_schema_serialize(
      array_schema, serialization_type_, &buff, false));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  bool found = false;
  const std::string creation_access_credentials_name =
      config_->get("rest.creation_access_credentials_name", &found);
  if (found)
    RETURN_NOT_OK(set_header(
        "X-TILEDB-CLOUD-ACCESS-CREDENTIALS-NAME",
        creation_access_credentials_name));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  auto deduced_url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns + "/" +
                     curlc.url_escape(array_uri);
  Buffer returned_data;
  const Status sc = curlc.post_data(
      stats_,
      deduced_url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key);
  return sc;
}

void RestClient::post_array_from_rest(
    const URI& uri, ContextResources& resources, Array* array) {
  if (array == nullptr) {
    throw RestClientException("Error getting remote array; array is null.");
  }

  Buffer buff;
  throw_if_not_ok(
      serialization::array_open_serialize(*array, serialization_type_, &buff));
  // Wrap in a list
  BufferList serialized;
  throw_if_not_ok(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  std::string url = redirect_uri(cache_key) + "/v2/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) + "/?";

  // Remote array operations should provide start and end timestamps
  url += "start_timestamp=" + std::to_string(array->timestamp_start()) +
         "&end_timestamp=" + std::to_string(array->timestamp_end());

  // Get the data
  Buffer returned_data;
  throw_if_not_ok(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0) {
    throw RestClientException(
        "Error getting array from REST; server returned no data.");
  }

  // Ensure data has a null delimiter for cap'n proto if using JSON
  throw_if_not_ok(ensure_json_null_delimited_string(&returned_data));
  serialization::array_deserialize(
      array, serialization_type_, returned_data, resources, memory_tracker_);
}

void RestClient::delete_array_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri);

  Buffer returned_data;
  throw_if_not_ok(curlc.delete_data(
      stats_, url, serialization_type_, &returned_data, cache_key));
}

void RestClient::post_delete_fragments_to_rest(
    const URI& uri,
    Array* array,
    uint64_t timestamp_start,
    uint64_t timestamp_end) {
  Buffer buff;
  serialization::serialize_delete_fragments_timestamps_request(
      array->config(),
      timestamp_start,
      timestamp_end,
      serialization_type_,
      &buff);
  // Wrap in a list
  BufferList serialized;
  throw_if_not_ok(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) +
                          "/delete_fragments";

  Buffer returned_data;
  throw_if_not_ok(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
}

void RestClient::post_delete_fragments_list_to_rest(
    const URI& uri, Array* array, const std::vector<URI>& fragment_uris) {
  Buffer buff;
  serialization::serialize_delete_fragments_list_request(
      array->config(), fragment_uris, serialization_type_, &buff);
  // Wrap in a list
  BufferList serialized;
  throw_if_not_ok(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) +
                          "/delete_fragments_list";

  // Post query to rest
  Buffer returned_data;
  throw_if_not_ok(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
}

Status RestClient::deregister_array_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) + "/deregister";

  Buffer returned_data;
  return curlc.delete_data(
      stats_, url, serialization_type_, &returned_data, cache_key);
}

Status RestClient::get_array_non_empty_domain(
    Array* array, uint64_t timestamp_start, uint64_t timestamp_end) {
  if (array == nullptr)
    return LOG_STATUS(
        Status_RestError("Cannot get array non-empty domain; array is null"));
  if (array->array_uri().to_string().empty())
    return LOG_STATUS(Status_RestError(
        "Cannot get array non-empty domain; array URI is empty"));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(array->array_uri().get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v2/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) +
                          "/non_empty_domain?" +
                          "start_timestamp=" + std::to_string(timestamp_start) +
                          "&end_timestamp=" + std::to_string(timestamp_end);

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.get_data(
      stats_, url, serialization_type_, &returned_data, cache_key));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        Status_RestError("Error getting array non-empty domain "
                         "from REST; server returned no data."));

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK(ensure_json_null_delimited_string(&returned_data));

  // Deserialize data returned
  return serialization::nonempty_domain_deserialize(
      array, returned_data, serialization_type_);
}

Status RestClient::get_array_max_buffer_sizes(
    const URI& uri,
    const ArraySchema& schema,
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Convert subarray to string for query parameter
  std::string subarray_str;
  RETURN_NOT_OK(subarray_to_str(schema, subarray, &subarray_str));
  std::string subarray_query_param =
      subarray_str.empty() ? "" : ("?subarray=" + subarray_str);

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) +
                          "/max_buffer_sizes" + subarray_query_param;

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.get_data(
      stats_, url, serialization_type_, &returned_data, cache_key));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        Status_RestError("Error getting array max buffer sizes "
                         "from REST; server returned no data."));

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK(ensure_json_null_delimited_string(&returned_data));

  // Deserialize data returned
  return serialization::max_buffer_sizes_deserialize(
      schema, returned_data, serialization_type_, buffer_sizes);
}

Status RestClient::get_array_metadata_from_rest(
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    Array* array) {
  if (array == nullptr)
    return LOG_STATUS(Status_RestError(
        "Error getting array metadata from REST; array is null."));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) +
                          "/array_metadata?" +
                          "start_timestamp=" + std::to_string(timestamp_start) +
                          "&end_timestamp=" + std::to_string(timestamp_end);

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.get_data(
      stats_, url, serialization_type_, &returned_data, cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status_RestError(
        "Error getting array metadata from REST; server returned no data."));

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK(ensure_json_null_delimited_string(&returned_data));
  return serialization::metadata_deserialize(
      array->unsafe_metadata(),
      array->config(),
      serialization_type_,
      returned_data);
}

Status RestClient::post_array_metadata_to_rest(
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    Array* array) {
  if (array == nullptr)
    return LOG_STATUS(Status_RestError(
        "Error posting array metadata to REST; array is null."));

  Buffer buff;
  RETURN_NOT_OK(serialization::metadata_serialize(
      array->unsafe_metadata(), serialization_type_, &buff));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) +
                          "/array_metadata?" +
                          "start_timestamp=" + std::to_string(timestamp_start) +
                          "&end_timestamp=" + std::to_string(timestamp_end);

  // Put the data
  Buffer returned_data;
  return curlc.post_data(
      stats_, url, serialization_type_, &serialized, &returned_data, cache_key);
}

std::vector<shared_ptr<const Enumeration>>
RestClient::post_enumerations_from_rest(
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    Array* array,
    const std::vector<std::string>& enumeration_names,
    shared_ptr<MemoryTracker> memory_tracker) {
  if (array == nullptr) {
    throw RestClientException(
        "Error getting enumerations from REST; array is null.");
  }

  if (!memory_tracker) {
    memory_tracker = memory_tracker_;
  }

  // This should never be called with an empty list of enumeration names, but
  // there's no reason to not check an early return case here given that code
  // changes.
  if (enumeration_names.size() == 0) {
    return {};
  }

  Buffer buf;
  serialization::serialize_load_enumerations_request(
      array->config(), enumeration_names, serialization_type_, buf);

  // Wrap in a list
  BufferList serialized;
  throw_if_not_ok(serialized.add_buffer(std::move(buf)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) + "/enumerations?" +
                          "start_timestamp=" + std::to_string(timestamp_start) +
                          "&end_timestamp=" + std::to_string(timestamp_end);

  // Get the data
  Buffer returned_data;
  throw_if_not_ok(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0) {
    throw RestClientException(
        "Error getting enumerations from REST; server returned no data.");
  }

  // Ensure data has a null delimiter for cap'n proto if using JSON
  throw_if_not_ok(ensure_json_null_delimited_string(&returned_data));
  return serialization::deserialize_load_enumerations_response(
      serialization_type_, returned_data, memory_tracker);
}

void RestClient::post_query_plan_from_rest(
    const URI& uri, Query& query, QueryPlan& query_plan) {
  // Get array
  const Array* array = query.array();
  if (array == nullptr) {
    throw RestClientException(
        "Error submitting query plan to REST; null array.");
  }

  Buffer buff;
  serialization::serialize_query_plan_request(
      query.config(), query, serialization_type_, buff);

  // Wrap in a list
  BufferList serialized;
  throw_if_not_ok(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  std::string url;
  if (use_refactored_query(query.config())) {
    url = redirect_uri(cache_key) + "/v3/arrays/" + array_ns + "/" +
          curlc.url_escape(array_uri) +
          "/query/plan?type=" + query_type_str(query.type());
  } else {
    url = redirect_uri(cache_key) + "/v2/arrays/" + array_ns + "/" +
          curlc.url_escape(array_uri) +
          "/query/plan?type=" + query_type_str(query.type());
  }

  // Remote array reads always supply the timestamp.
  url += "&start_timestamp=" + std::to_string(array->timestamp_start());
  url += "&end_timestamp=" + std::to_string(array->timestamp_end());

  // Get the data
  Buffer returned_data;
  throw_if_not_ok(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0) {
    throw RestClientException(
        "Error getting query plan from REST; server returned no data.");
  }

  // Ensure data has a null delimiter for cap'n proto if using JSON
  throw_if_not_ok(ensure_json_null_delimited_string(&returned_data));
  query_plan = serialization::deserialize_query_plan_response(
      query, serialization_type_, returned_data);
}

Status RestClient::submit_query_to_rest(const URI& uri, Query* query) {
  // Local state tracking for the current offsets into the user's query buffers.
  // This allows resubmission of incomplete queries while appending to the
  // same user buffers.
  serialization::CopyState copy_state;

  RETURN_NOT_OK(post_query_submit(uri, query, &copy_state));

  // Now need to update the buffer sizes to the actual copied data size so that
  // the user can check the result size on reads.
  RETURN_NOT_OK(update_attribute_buffer_sizes(copy_state, query));

  return Status::Ok();
}

Status RestClient::post_query_submit(
    const URI& uri, Query* query, serialization::CopyState* copy_state) {
  // Get array
  const Array* array = query->array();
  if (array == nullptr) {
    return LOG_STATUS(
        Status_RestError("Error submitting query to REST; null array."));
  }

  // For remote global order writes only.
  auto& cache = query->get_remote_buffer_cache();
  if (cache.has_value()) {
    if (cache.value().should_cache_write()) {
      // If the entire write was less than a tile, cache all buffers and return.
      // We will prepend this data to the next write submission.
      cache.value().cache_write();
      return Status::Ok();
    }
    // If the write is not tile-aligned adjust query buffer sizes to hold tile
    // overflow bytes from this submission, aligning the write.
    cache.value().make_buffers_tile_aligned();
  }

  auto rest_scratch = query->rest_scratch();

  // When a read query overflows the user buffer we may already have the next
  // part loaded in the scratch buffer.
  if (rest_scratch->size() > 0) {
    bool skip;
    query_post_call_back(
        false, nullptr, 0, &skip, rest_scratch, query, copy_state);
  }

  // Serialize query to send
  BufferList serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  std::string url;
  if (use_refactored_query(query->config())) {
    url = redirect_uri(cache_key) + "/v3/arrays/" + array_ns + "/" +
          curlc.url_escape(array_uri) +
          "/query/submit?type=" + query_type_str(query->type()) +
          "&read_all=" + (resubmit_incomplete_ ? "true" : "false");
  } else {
    url = redirect_uri(cache_key) + "/v2/arrays/" + array_ns + "/" +
          curlc.url_escape(array_uri) +
          "/query/submit?type=" + query_type_str(query->type()) +
          "&read_all=" + (resubmit_incomplete_ ? "true" : "false");
  }

  // Remote array reads always supply the timestamp.
  url += "&start_timestamp=" + std::to_string(array->timestamp_start());
  url += "&end_timestamp=" + std::to_string(array->timestamp_end());

  // Create the callback that will process the response buffers as they
  // are received.
  auto write_cb = std::bind(
      &RestClient::query_post_call_back,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      std::placeholders::_4,
      rest_scratch,
      query,
      copy_state);

  const Status st = curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      rest_scratch.get(),
      std::move(write_cb),
      cache_key);

  if (!st.ok() && copy_state->empty()) {
    return LOG_STATUS(Status_RestError(
        "Error submitting query to REST; "
        "server returned no data. "
        "Curl error: " +
        st.message()));
  }

  // For remote global order writes only.
  if (cache.has_value()) {
    // Update cache with any tile-overflow bytes held from this submission.
    cache.value().cache_non_tile_aligned_data();
  }
  return st;
}

size_t RestClient::query_post_call_back(
    const bool reset,
    void* const contents,
    const size_t content_nbytes,
    bool* const skip_retries,
    shared_ptr<Buffer> scratch,
    Query* query,
    serialization::CopyState* copy_state) {
  // All return statements in this function must pass through this wrapper.
  // This is responsible for two things:
  // 1. The 'bytes_processed' may be negative in error scenarios. The negative
  //    number is only used for convenience during processing. We must restrict
  //    the return value to >= 0 before returning.
  // 2. When our return value ('bytes_processed') does not match the number of
  //    input bytes ('content_nbytes'), The CURL layer that invoked this
  //    callback will interpret this as an error and may attempt to retry. We
  //    specifically want to prevent CURL from retrying the request if we have
  //    reached this callback. If we encounter an error within this callback,
  //    the issue is with the response data itself and not an issue with
  //    transporting the response data (for example, the common issue will be
  //    deserialization failure). In this scenario, we will waste time retrying
  //    on response data that we know we cannot handle without error.
  auto return_wrapper = [content_nbytes, skip_retries](long bytes_processed) {
    bytes_processed = std::max(bytes_processed, 0L);
    if (static_cast<size_t>(bytes_processed) != content_nbytes) {
      *skip_retries = true;
    }
    return bytes_processed;
  };

  // This is the return value that represents the amount of bytes processed
  // in 'contents'. This will act as the return value and will always be
  // less-than-or-equal-to 'content_nbytes'.
  long bytes_processed = 0;

  // When 'reset' is true, we must discard the in-progress memory state.
  // The most likely scenario is that the request failed and was retried
  // from within the Curl object.
  if (reset) {
    scratch->set_size(0);
    scratch->reset_offset();
    copy_state->clear();
  }

  // If the current scratch size is non-empty, we must subtract its size
  // from 'bytes_processed' so that we do not count bytes processed from
  // a previous callback.
  bytes_processed -= scratch->size();

  // Copy 'contents' to the end of 'scratch'. As a future optimization, if
  // 'scratch' is empty, we could attempt to process 'contents' in-place and
  // only copy the remaining, unprocessed bytes into 'scratch'.
  scratch->set_offset(scratch->size());
  Status st = scratch->write(contents, content_nbytes);
  if (!st.ok()) {
    LOG_ERROR(
        "Cannot copy libcurl response data; buffer write failed: " +
        st.to_string());
    return return_wrapper(bytes_processed);
  }

  // Process all of the serialized queries contained within 'scratch'.
  scratch->reset_offset();
  while (scratch->offset() < scratch->size()) {
    // We need at least 8 bytes to determine the size of the next
    // serialized query.
    if (scratch->offset() + 8 > scratch->size()) {
      break;
    }

    // Decode the query size. We could cache this from the previous
    // callback to prevent decoding the same prefix multiple times.
    const uint64_t query_size =
        utils::endianness::decode_le<uint64_t>(scratch->cur_data());

    // We must have the full serialized query before attempting to
    // deserialize it.
    if (scratch->offset() + 8 + query_size > scratch->size()) {
      break;
    }

    // At this point of execution, we know that we the next serialized
    // query is entirely in 'scratch'. For convenience, we will advance
    // the offset to point to the start of the serialized query.
    scratch->advance_offset(8);

    // We can only deserialize the query if it is 8-byte aligned. If the
    // offset is 8-byte aligned, we can deserialize the query in-place.
    // Otherwise, we must make a copy to an auxiliary buffer.
    if (scratch->offset() % 8 != 0) {
      // Copy the entire serialized buffer to a newly allocated, 8-byte
      // aligned auxiliary buffer.
      Buffer aux;
      st = aux.write(scratch->cur_data(), query_size);
      if (!st.ok()) {
        scratch->set_offset(scratch->offset() - 8);
        return return_wrapper(bytes_processed);
      }

      // Deserialize the buffer and store it in 'copy_state'. If
      // the user buffers are too small to accommodate the attribute
      // data when deserializing read queries, this will return an
      // error status.
      aux.reset_offset();
      st = serialization::query_deserialize(
          aux, serialization_type_, true, copy_state, query, compute_tp_);
      if (!st.ok()) {
        scratch->set_offset(scratch->offset() - 8);
        return return_wrapper(bytes_processed);
      }
    } else {
      // Deserialize the buffer and store it in 'copy_state'. If
      // the user buffers are too small to accommodate the attribute
      // data when deserializing read queries, this will return an
      // error status.
      st = serialization::query_deserialize(
          *scratch, serialization_type_, true, copy_state, query, compute_tp_);
      if (!st.ok()) {
        scratch->set_offset(scratch->offset() - 8);
        return return_wrapper(bytes_processed);
      }
    }

    scratch->advance_offset(query_size);
    bytes_processed += (query_size + 8);
  }

  // Remove any processed queries from our scratch buffer. We track length
  // here because from the point of view of libcurl we have processed any
  // remaining bytes in our scratch buffer even though we won't get to
  // deserializing them on the next invocation of this callback.
  const uint64_t length = scratch->size() - scratch->offset();

  if (scratch->offset() != 0) {
    // Save any unprocessed query data in scratch by copying it to an
    // auxillary buffer before we truncate scratch. Then copy any unprocessed
    // bytes back into scratch.
    Buffer aux;
    if (length > 0) {
      throw_if_not_ok(aux.write(scratch->data(scratch->offset()), length));
    }

    scratch->reset_size();
    scratch->reset_offset();

    if (length > 0) {
      throw_if_not_ok(scratch->write(aux.data(), aux.size()));
    }

    assert(scratch->size() == length);
  }

  bytes_processed += length;

  assert(static_cast<size_t>(bytes_processed) == content_nbytes);
  return return_wrapper(bytes_processed);
}

Status RestClient::finalize_query_to_rest(const URI& uri, Query* query) {
  // Serialize data to send
  BufferList serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  std::string url;
  if (use_refactored_query(query->config())) {
    url = redirect_uri(cache_key) + "/v3/arrays/" + array_ns + "/" +
          curlc.url_escape(array_uri) +
          "/query/finalize?type=" + query_type_str(query->type());
  } else {
    url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns + "/" +
          curlc.url_escape(array_uri) +
          "/query/finalize?type=" + query_type_str(query->type());
  }
  Buffer returned_data;
  RETURN_NOT_OK(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));

  if (returned_data.data() == nullptr || returned_data.size() == 0) {
    return LOG_STATUS(
        Status_RestError("Error finalizing query; server returned no data."));
  }

  // Deserialize data returned
  returned_data.reset_offset();
  return serialization::query_deserialize(
      returned_data, serialization_type_, true, nullptr, query, compute_tp_);
}

Status RestClient::submit_and_finalize_query_to_rest(
    const URI& uri, Query* query) {
  serialization::CopyState copy_state;

  // Get array
  const Array* array = query->array();
  if (array == nullptr) {
    return LOG_STATUS(Status_RestError(
        "Error while submit_and_finalize query to REST; null array."));
  }

  auto rest_scratch = query->rest_scratch();

  // Serialize query to send
  BufferList serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  std::string url;
  if (use_refactored_query(query->config())) {
    url = redirect_uri(cache_key) + "/v3/arrays/" + array_ns + "/" +
          curlc.url_escape(array_uri) +
          "/query/submit_and_finalize?type=" + query_type_str(query->type());
  } else {
    url = redirect_uri(cache_key) + "/v2/arrays/" + array_ns + "/" +
          curlc.url_escape(array_uri) +
          "/query/submit_and_finalize?type=" + query_type_str(query->type());
  }

  auto write_cb = std::bind(
      &RestClient::query_post_call_back,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      std::placeholders::_4,
      rest_scratch,
      query,
      &copy_state);

  const Status st = curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      rest_scratch.get(),
      std::move(write_cb),
      cache_key);

  if (!st.ok() && copy_state.empty()) {
    return LOG_STATUS(Status_RestError(
        "Error while submit_and_finalize query to REST; "
        "server returned no data. "
        "Curl error: " +
        st.message()));
  }

  return st;
}

Status RestClient::subarray_to_str(
    const ArraySchema& schema,
    const void* subarray,
    std::string* subarray_str) {
  const auto coords_type{schema.dimension_ptr(0)->type()};
  const auto dim_num = schema.dim_num();
  const auto subarray_nelts = 2 * dim_num;

  if (subarray == nullptr) {
    *subarray_str = "";
    return Status::Ok();
  }

  std::stringstream ss;
  for (unsigned i = 0; i < subarray_nelts; i++) {
    auto g = [&](auto T) {
      if constexpr (tiledb::type::TileDBNumeric<decltype(T)>) {
        ss << reinterpret_cast<const decltype(T)*>(subarray)[i];
      } else {
        throw StatusException(Status_RestError(
            "Error converting subarray to string; unhandled datatype."));
      }
    };
    apply_with_type(g, coords_type);

    if (i < subarray_nelts - 1)
      ss << ",";
  }

  *subarray_str = ss.str();

  return Status::Ok();
}

Status RestClient::update_attribute_buffer_sizes(
    const serialization::CopyState& copy_state, Query* query) const {
  // Applicable only to reads
  if (query->type() != QueryType::READ) {
    return Status::Ok();
  }

  for (const auto& cit : copy_state) {
    const auto& name = cit.first;
    auto state = cit.second;
    auto query_buffer = query->buffer(name);
    if (query_buffer.buffer_var_size_ != nullptr) {
      *query_buffer.buffer_var_size_ = state.data_size;
      *query_buffer.buffer_size_ = state.offset_size;
    } else if (query_buffer.buffer_size_ != nullptr)
      *query_buffer.buffer_size_ = state.data_size;

    bool nullable = query->array_schema().is_nullable(name);
    if (nullable && query_buffer.validity_vector_.buffer_size()) {
      *query_buffer.validity_vector_.buffer_size() = state.validity_size;
    }
  }

  return Status::Ok();
}

Status RestClient::get_query_est_result_sizes(const URI& uri, Query* query) {
  if (query == nullptr) {
    return LOG_STATUS(Status_RestError(
        "Error getting query estimated result size from REST; Query is null."));
  }

  // Get array
  const Array* array = query->array();
  if (array == nullptr) {
    return LOG_STATUS(Status_RestError(
        "Error festing query estimated result size from REST; null array."));
  }

  // Serialize query to send
  BufferList serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  std::string url =
      redirect_uri(cache_key) + "/v1/arrays/" + array_ns + "/" +
      curlc.url_escape(array_uri) +
      "/query/est_result_sizes?type=" + query_type_str(query->type());

  // Remote array reads always supply the timestamp.
  if (query->type() == QueryType::READ) {
    url += "&start_timestamp=" + std::to_string(array->timestamp_start());
    url += "&end_timestamp=" + std::to_string(array->timestamp_end());
  }

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0) {
    return LOG_STATUS(Status_RestError(
        "Error getting array metadata from REST; server returned no data."));
  }

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK(ensure_json_null_delimited_string(&returned_data));
  return serialization::query_est_result_size_deserialize(
      query, serialization_type_, true, returned_data);
}

std::string RestClient::redirect_uri(const std::string& cache_key) {
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);
  std::unordered_map<std::string, std::string>::const_iterator cache_it =
      redirect_meta_.find(cache_key);

  return (cache_it == redirect_meta_.end()) ? rest_server_ : cache_it->second;
}

Status RestClient::post_array_schema_evolution_to_rest(
    const URI& uri, ArraySchemaEvolution* array_schema_evolution) {
  Buffer buff;
  RETURN_NOT_OK(serialization::array_schema_evolution_serialize(
      array_schema_evolution, serialization_type_, &buff, false));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  auto deduced_url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns + "/" +
                     curlc.url_escape(array_uri) + "/evolve";
  Buffer returned_data;
  const Status sc = curlc.post_data(
      stats_,
      deduced_url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key);
  return sc;
}

Status RestClient::post_fragment_info_from_rest(
    const URI& uri, FragmentInfo* fragment_info) {
  if (fragment_info == nullptr)
    return LOG_STATUS(Status_RestError(
        "Error getting fragment info from REST; fragment info is null."));

  Buffer buff;
  RETURN_NOT_OK(serialization::fragment_info_request_serialize(
      *fragment_info, serialization_type_, &buff));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) + "/fragment_info";

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status_RestError(
        "Error getting fragment info from REST; server returned no data."));

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK(ensure_json_null_delimited_string(&returned_data));
  return serialization::fragment_info_deserialize(
      fragment_info, serialization_type_, uri, returned_data, memory_tracker_);
}

Status RestClient::post_group_metadata_from_rest(const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(Status_RestError(
        "Error posting group metadata from REST; group is null."));
  }

  Buffer buff;
  RETURN_NOT_OK(serialization::group_metadata_serialize(
      group, serialization_type_, &buff, false));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(&group_ns, &group_uri));
  const std::string cache_key = group_ns + ":" + group_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v2/groups/" + group_ns +
                          "/" + curlc.url_escape(group_uri) + "/metadata";

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0) {
    return LOG_STATUS(Status_RestError(
        "Error getting group metadata from REST; server returned no data."));
  }

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK(ensure_json_null_delimited_string(&returned_data));
  return serialization::metadata_deserialize(
      group->unsafe_metadata(),
      group->config(),
      serialization_type_,
      returned_data);
}

Status RestClient::put_group_metadata_to_rest(const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(Status_RestError(
        "Error posting group metadata to REST; group is null."));
  }

  Buffer buff;
  RETURN_NOT_OK(serialization::group_metadata_serialize(
      group, serialization_type_, &buff, true));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(&group_ns, &group_uri));
  const std::string cache_key = group_ns + ":" + group_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v2/groups/" + group_ns +
                          "/" + curlc.url_escape(group_uri) + "/metadata";

  // Put the data
  Buffer returned_data;
  return curlc.put_data(
      stats_, url, serialization_type_, &serialized, &returned_data, cache_key);
}

Status RestClient::post_group_create_to_rest(const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(
        Status_RestError("Error posting group to REST; group is null."));
  }

  Buffer buff;
  RETURN_NOT_OK(
      serialization::group_create_serialize(group, serialization_type_, &buff));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(&group_ns, &group_uri));
  const std::string cache_key = group_ns + ":" + group_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v2/groups/" + group_ns;

  // Create the group and check for error
  Buffer returned_data;
  return curlc.post_data(
      stats_, url, serialization_type_, &serialized, &returned_data, cache_key);
}

Status RestClient::post_group_from_rest(const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(
        Status_RestError("Error posting group to REST; group is null."));
  }

  Buffer buff;
  RETURN_NOT_OK(
      serialization::group_serialize(group, serialization_type_, &buff));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(&group_ns, &group_uri));
  const std::string cache_key = group_ns + ":" + group_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v2/groups/" + group_ns +
                          "/" + curlc.url_escape(group_uri);

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status_RestError(
        "Error getting group from REST; server returned no data."));

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK(ensure_json_null_delimited_string(&returned_data));
  return serialization::group_details_deserialize(
      group, serialization_type_, returned_data);
}

Status RestClient::patch_group_to_rest(const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(
        Status_RestError("Error patching group to REST; group is null."));
  }

  Buffer buff;
  RETURN_NOT_OK(
      serialization::group_update_serialize(group, serialization_type_, &buff));

  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(&group_ns, &group_uri));
  const std::string cache_key = group_ns + ":" + group_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v2/groups/" + group_ns +
                          "/" + curlc.url_escape(group_uri);

  // Put the data
  Buffer returned_data;
  return curlc.patch_data(
      stats_, url, serialization_type_, &serialized, &returned_data, cache_key);
}

void RestClient::delete_group_from_rest(const URI& uri, bool recursive) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  throw_if_not_ok(uri.get_rest_components(&group_ns, &group_uri));
  const std::string cache_key = group_ns + ":" + group_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string recursive_str = recursive ? "true" : "false";
  const std::string url = redirect_uri(cache_key) + "/v2/groups/" + group_ns +
                          "/" + curlc.url_escape(group_uri) +
                          "/delete?recursive=" + recursive_str;

  Buffer returned_data;
  throw_if_not_ok(curlc.delete_data(
      stats_, url, serialization_type_, &returned_data, cache_key));
}

Status RestClient::ensure_json_null_delimited_string(Buffer* buffer) {
  if (serialization_type_ == SerializationType::JSON &&
      buffer->value<char>(buffer->size() - 1) != '\0') {
    RETURN_NOT_OK(buffer->write("\0", sizeof(char)));
  }
  return Status::Ok();
}

Status RestClient::post_consolidation_to_rest(
    const URI& uri, const Config& config) {
  Buffer buff;
  RETURN_NOT_OK(serialization::array_consolidation_request_serialize(
      config, serialization_type_, &buff));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) + "/consolidate";

  // Get the data
  Buffer returned_data;
  return curlc.post_data(
      stats_, url, serialization_type_, &serialized, &returned_data, cache_key);
}

Status RestClient::post_vacuum_to_rest(const URI& uri, const Config& config) {
  Buffer buff;
  RETURN_NOT_OK(serialization::array_vacuum_request_serialize(
      config, serialization_type_, &buff));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) + "/vacuum";

  // Get the data
  Buffer returned_data;
  return curlc.post_data(
      stats_, url, serialization_type_, &serialized, &returned_data, cache_key);
}

std::vector<std::vector<std::string>>
RestClient::post_consolidation_plan_from_rest(
    const URI& uri, const Config& config, uint64_t fragment_size) {
  Buffer buff;
  serialization::serialize_consolidation_plan_request(
      fragment_size, config, serialization_type_, buff);

  // Wrap in a list
  BufferList serialized;
  throw_if_not_ok(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(&array_ns, &array_uri));
  const std::string cache_key = array_ns + ":" + array_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) +
                          "/consolidate/plan";

  // Get the data
  Buffer returned_data;
  throw_if_not_ok(curlc.post_data(
      stats_,
      url,
      serialization_type_,
      &serialized,
      &returned_data,
      cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0) {
    throw RestClientException(
        "Error getting query plan from REST; server returned no data.");
  }

  // Ensure data has a null delimiter for cap'n proto if using JSON
  throw_if_not_ok(ensure_json_null_delimited_string(&returned_data));
  return serialization::deserialize_consolidation_plan_response(
      serialization_type_, returned_data);
}

void RestClient::load_headers(const Config& cfg) {
  for (auto iter = ConfigIter(cfg, constants::rest_header_prefix); !iter.end();
       iter.next()) {
    auto key = iter.param();
    if (key.size() == 0) {
      continue;
    }
    extra_headers_[key] = iter.value();
  }
}

#else

RestClient::RestClient() {
  (void)config_;
  (void)rest_server_;
  (void)serialization_type_;
}

Status RestClient::init(
    stats::Stats*,
    const Config*,
    ThreadPool*,
    const std::shared_ptr<Logger>&,
    ContextResources&) {
  throw RestClientDisabledException();
}

Status RestClient::set_header(const std::string&, const std::string&) {
  throw RestClientDisabledException();
}

tuple<Status, optional<shared_ptr<ArraySchema>>>
RestClient::get_array_schema_from_rest(const URI&) {
  return {
      LOG_STATUS(Status_RestError(
          "Cannot use rest client; serialization not enabled.")),
      nullopt};
}

Status RestClient::post_array_schema_to_rest(const URI&, const ArraySchema&) {
  throw RestClientDisabledException();
}

void RestClient::post_array_from_rest(const URI&, ContextResources&, Array*) {
  throw RestClientDisabledException();
}

void RestClient::delete_array_from_rest(const URI&) {
  throw RestClientDisabledException();
}

void RestClient::post_delete_fragments_to_rest(
    const URI&, Array*, uint64_t, uint64_t) {
  throw RestClientDisabledException();
}

void RestClient::post_delete_fragments_list_to_rest(
    const URI&, Array*, const std::vector<URI>&) {
  throw RestClientDisabledException();
}

Status RestClient::deregister_array_from_rest(const URI&) {
  throw RestClientDisabledException();
}

Status RestClient::get_array_non_empty_domain(Array*, uint64_t, uint64_t) {
  throw RestClientDisabledException();
}

Status RestClient::get_array_max_buffer_sizes(
    const URI&,
    const ArraySchema&,
    const void*,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*) {
  throw RestClientDisabledException();
}

Status RestClient::get_array_metadata_from_rest(
    const URI&, uint64_t, uint64_t, Array*) {
  throw RestClientDisabledException();
}

Status RestClient::post_array_metadata_to_rest(
    const URI&, uint64_t, uint64_t, Array*) {
  throw RestClientDisabledException();
}

std::vector<shared_ptr<const Enumeration>>
RestClient::post_enumerations_from_rest(
    const URI&,
    uint64_t,
    uint64_t,
    Array*,
    const std::vector<std::string>&,
    shared_ptr<MemoryTracker>) {
  throw RestClientDisabledException();
}

void RestClient::post_query_plan_from_rest(const URI&, Query&, QueryPlan&) {
  throw RestClientDisabledException();
}

Status RestClient::submit_query_to_rest(const URI&, Query*) {
  throw RestClientDisabledException();
}

Status RestClient::finalize_query_to_rest(const URI&, Query*) {
  throw RestClientDisabledException();
}

Status RestClient::submit_and_finalize_query_to_rest(const URI&, Query*) {
  throw RestClientDisabledException();
}

Status RestClient::get_query_est_result_sizes(const URI&, Query*) {
  throw RestClientDisabledException();
}

Status RestClient::post_array_schema_evolution_to_rest(
    const URI&, ArraySchemaEvolution*) {
  throw RestClientDisabledException();
}

tuple<Status, std::optional<bool>> RestClient::check_array_exists_from_rest(
    const URI&) {
  return {
      LOG_STATUS(Status_RestError(
          "Cannot use rest client; serialization not enabled.")),
      std::nullopt};
}

tuple<Status, std::optional<bool>> RestClient::check_group_exists_from_rest(
    const URI&) {
  return {
      LOG_STATUS(Status_RestError(
          "Cannot use rest client; serialization not enabled.")),
      std::nullopt};
}

Status RestClient::post_group_metadata_from_rest(const URI&, Group*) {
  throw RestClientDisabledException();
}

Status RestClient::post_fragment_info_from_rest(const URI&, FragmentInfo*) {
  throw RestClientDisabledException();
}

Status RestClient::put_group_metadata_to_rest(const URI&, Group*) {
  throw RestClientDisabledException();
}

Status RestClient::post_group_create_to_rest(const URI&, Group*) {
  throw RestClientDisabledException();
}

Status RestClient::post_group_from_rest(const URI&, Group*) {
  throw RestClientDisabledException();
}

Status RestClient::patch_group_to_rest(const URI&, Group*) {
  throw RestClientDisabledException();
}

void RestClient::delete_group_from_rest(const URI&, bool) {
  throw RestClientDisabledException();
}

Status RestClient::post_consolidation_to_rest(const URI&, const Config&) {
  throw RestClientDisabledException();
}

Status RestClient::post_vacuum_to_rest(const URI&, const Config&) {
  throw RestClientDisabledException();
}

std::vector<std::vector<std::string>>
RestClient::post_consolidation_plan_from_rest(
    const URI&, const Config&, uint64_t) {
  throw RestClientDisabledException();
}

void RestClient::load_headers(const Config&) {
  throw RestClientDisabledException();
}

#endif  // TILEDB_SERIALIZATION

}  // namespace tiledb::sm
