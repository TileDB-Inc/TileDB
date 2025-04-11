/**
 * @file   rest_client_remote.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file implements the server-enabled REST client class, used for remote
 * operation.
 */

#include "rest_client_remote.h"

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * Factory class for `RestClientRemote`.
 *
 * This class is defined in order that `RestClientFactoryAssistant` can declare
 * a friend class, instead of the rather more brittle way of declaring
 * particular friend functions.
 */
class RestClientRemoteFactory {
 public:
  using factory_type = RestClientFactoryAssistant::factory_type;

  /**
   * Factory function for `RestClientRemote`.
   *
   * Note that the return value contains `RestClient`, the base class of
   * `RestClientRemote`.
   */
  static std::shared_ptr<RestClient> make(
      stats::Stats& parent_stats,
      const Config& config,
      ThreadPool& compute_tp,
      Logger& logger,
      shared_ptr<MemoryTracker>&& tracker) {
    return tdb::make_shared<RestClientRemote>(
        HERE(), parent_stats, config, compute_tp, logger, std::move(tracker));
  }

  static factory_type* override_factory() {
    return RestClientFactoryAssistant::override_factory(&make);
  }
};

/*
 * Non-local variable to initialize the factory override during dynamic
 * initialization. See the definition of `RestClientFactory::factory_override_`
 * for more information.
 */
RestClientRemoteFactory::factory_type* original_factory{
    RestClientRemoteFactory::override_factory()};

RestClientRemote::RestClientRemote(
    stats::Stats& parent_stats,
    const Config& config,
    ThreadPool& compute_tp,
    Logger& logger,
    shared_ptr<MemoryTracker>&& tracker)
    : RestClient(config)
    , stats_(parent_stats.create_child("RestClient"))
    , config_(&config)
    , compute_tp_(&compute_tp)
    , resubmit_incomplete_(true)
    , logger_(logger.clone("curl ", ++logger_id_))
    , memory_tracker_(tracker) {
  // Setting the type of the memory tracker as MemoryTrackerType::REST_CLIENT
  // for now. This is because the class is used in many places not directly tied
  // to an array.
  memory_tracker_->set_type(MemoryTrackerType::REST_CLIENT);

  auto ssf = config.get<std::string>(
      "rest.server_serialization_format", Config::must_find);
  throw_if_not_ok(serialization_type_enum(ssf, &serialization_type_));

  auto ri = config.get<bool>("rest.resubmit_incomplete");
  resubmit_incomplete_ = ri.value_or(false);
}

bool RestClientRemote::use_refactored_query(const Config& config) {
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

tuple<Status, std::optional<bool>>
RestClientRemote::check_array_exists_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK_TUPLE(
      uri.get_rest_components(
          &array_ns, &array_uri, get_capabilities_from_rest().legacy_),
      nullopt);
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

tuple<Status, std::optional<bool>>
RestClientRemote::check_group_exists_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK_TUPLE(
      uri.get_rest_components(
          &group_ns, &group_uri, get_capabilities_from_rest().legacy_),
      nullopt);
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
RestClientRemote::get_array_schema_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK_TUPLE(
      uri.get_rest_components(
          &array_ns, &array_uri, get_capabilities_from_rest().legacy_),
      nullopt);
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

  auto array_schema = serialization::array_schema_deserialize(
      serialization_type_, returned_data, memory_tracker_);

  array_schema->set_array_uri(uri);

  return {Status::Ok(), array_schema};
}

std::tuple<
    shared_ptr<ArraySchema>,
    std::unordered_map<std::string, shared_ptr<ArraySchema>>>
RestClientRemote::post_array_schema_from_rest(
    const Config& config,
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    bool include_enumerations) {
  serialization::LoadArraySchemaRequest req(config);

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  serialization::serialize_load_array_schema_request(
      config, req, serialization_type_, buff);

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

  return serialization::deserialize_load_array_schema_response(
      uri, config, serialization_type_, returned_data, memory_tracker_);
}

Status RestClientRemote::post_array_schema_to_rest(
    const URI& uri, const ArraySchema& array_schema) {
  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(serialization::array_schema_serialize(
      array_schema, serialization_type_, buff, false));

  const auto creation_access_credentials_name{
      config_->get<std::string>("rest.creation_access_credentials_name")};
  if (creation_access_credentials_name.has_value()) {
    add_header(
        "X-TILEDB-CLOUD-ACCESS-CREDENTIALS-NAME",
        creation_access_credentials_name.value());
  }

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
  const std::string cache_key = array_ns + ":" + array_uri;
  // We don't want to cache the URI used for array creation as it will
  // always be hardcoded to the default server. After creation the REST
  // server knows the right region to direct the request to, so client
  // side caching should start from then on.
  RETURN_NOT_OK(curlc.init(
      config_, extra_headers_, &redirect_meta_, &redirect_mtx_, false));
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

void RestClientRemote::post_array_from_rest(
    const URI& uri, ContextResources& resources, Array* array) {
  if (array == nullptr) {
    throw RestClientException("Error getting remote array; array is null.");
  }

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  throw_if_not_ok(
      serialization::array_open_serialize(*array, serialization_type_, buff));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

  serialization::array_deserialize(
      array, serialization_type_, returned_data, resources, memory_tracker_);
}

void RestClientRemote::delete_array_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
  const std::string cache_key = array_ns + ":" + array_uri;
  throw_if_not_ok(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri);

  Buffer returned_data;
  throw_if_not_ok(curlc.delete_data(
      stats_, url, serialization_type_, &returned_data, cache_key));
}

void RestClientRemote::post_delete_fragments_to_rest(
    const URI& uri,
    Array* array,
    uint64_t timestamp_start,
    uint64_t timestamp_end) {
  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  serialization::serialize_delete_fragments_timestamps_request(
      array->config(),
      timestamp_start,
      timestamp_end,
      serialization_type_,
      buff);

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

void RestClientRemote::post_delete_fragments_list_to_rest(
    const URI& uri, Array* array, const std::vector<URI>& fragment_uris) {
  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  serialization::serialize_delete_fragments_list_request(
      array->config(), fragment_uris, serialization_type_, buff);

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

Status RestClientRemote::deregister_array_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
  const std::string cache_key = array_ns + ":" + array_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v1/arrays/" + array_ns +
                          "/" + curlc.url_escape(array_uri) + "/deregister";

  Buffer returned_data;
  return curlc.delete_data(
      stats_, url, serialization_type_, &returned_data, cache_key);
}

Status RestClientRemote::get_array_non_empty_domain(
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
  RETURN_NOT_OK(array->array_uri().get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

  // Deserialize data returned
  return serialization::nonempty_domain_deserialize(
      array, returned_data, serialization_type_);
}

Status RestClientRemote::get_array_metadata_from_rest(
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
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

  return serialization::metadata_deserialize(
      array->unsafe_metadata(),
      array->config(),
      serialization_type_,
      returned_data);
}

Status RestClientRemote::post_array_metadata_to_rest(
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    Array* array) {
  if (array == nullptr)
    return LOG_STATUS(Status_RestError(
        "Error posting array metadata to REST; array is null."));

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(serialization::metadata_serialize(
      array->unsafe_metadata(), serialization_type_, buff));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

std::unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>
RestClientRemote::post_enumerations_from_rest(
    const URI& uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    const Config& config,
    const ArraySchema& array_schema,
    const std::vector<std::string>& enumeration_names,
    shared_ptr<MemoryTracker> memory_tracker) {
  if (!memory_tracker) {
    memory_tracker = memory_tracker_;
  }

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  serialization::serialize_load_enumerations_request(
      config, enumeration_names, serialization_type_, buff);

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

  return serialization::deserialize_load_enumerations_response(
      array_schema, config, serialization_type_, returned_data, memory_tracker);
}

void RestClientRemote::post_query_plan_from_rest(
    const URI& uri, Query& query, QueryPlan& query_plan) {
  // Get array
  const Array* array = query.array();
  if (array == nullptr) {
    throw RestClientException(
        "Error submitting query plan to REST; null array.");
  }

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  serialization::serialize_query_plan_request(
      query.config(), query, serialization_type_, buff);

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

  query_plan = serialization::deserialize_query_plan_response(
      query, serialization_type_, returned_data);
}

Status RestClientRemote::submit_query_to_rest(const URI& uri, Query* query) {
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

Status RestClientRemote::post_query_submit(
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
  BufferList serialized{memory_tracker_};
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, serialized));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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
      &RestClientRemote::query_post_call_back,
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

size_t RestClientRemote::query_post_call_back(
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
      st = serialization::query_deserialize(
          aux,
          serialization_type_,
          true,
          copy_state,
          query,
          compute_tp_,
          memory_tracker_);
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
          // Pass only the part of the buffer after the offset. The offset is
          // important as we've been advancing it in the code.
          scratch->cur_span(),
          serialization_type_,
          true,
          copy_state,
          query,
          compute_tp_,
          memory_tracker_);
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

    if (scratch->size() != length) {
      throw std::logic_error("");
    };
  }

  bytes_processed += length;

  if (static_cast<size_t>(bytes_processed) != content_nbytes) {
    throw std::logic_error("");
  };
  return return_wrapper(bytes_processed);
}

Status RestClientRemote::finalize_query_to_rest(const URI& uri, Query* query) {
  // Serialize data to send
  BufferList serialized{memory_tracker_};
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, serialized));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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
  return serialization::query_deserialize(
      returned_data,
      serialization_type_,
      true,
      nullptr,
      query,
      compute_tp_,
      memory_tracker_);
}

Status RestClientRemote::submit_and_finalize_query_to_rest(
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
  BufferList serialized{memory_tracker_};
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, serialized));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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
      &RestClientRemote::query_post_call_back,
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

Status RestClientRemote::subarray_to_str(
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

Status RestClientRemote::update_attribute_buffer_sizes(
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

Status RestClientRemote::get_query_est_result_sizes(
    const URI& uri, Query* query) {
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
  BufferList serialized{memory_tracker_};
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, serialized));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

  return serialization::query_est_result_size_deserialize(
      query, serialization_type_, true, returned_data);
}

std::string RestClientRemote::redirect_uri(const std::string& cache_key) {
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);
  std::unordered_map<std::string, std::string>::const_iterator cache_it =
      redirect_meta_.find(cache_key);

  return (cache_it == redirect_meta_.end()) ? rest_server_ : cache_it->second;
}

Status RestClientRemote::post_array_schema_evolution_to_rest(
    const URI& uri, ArraySchemaEvolution* array_schema_evolution) {
  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(serialization::array_schema_evolution_serialize(
      array_schema_evolution, serialization_type_, buff, false));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

Status RestClientRemote::post_fragment_info_from_rest(
    const URI& uri, FragmentInfo* fragment_info) {
  if (fragment_info == nullptr)
    return LOG_STATUS(Status_RestError(
        "Error getting fragment info from REST; fragment info is null."));

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(serialization::fragment_info_request_serialize(
      *fragment_info, serialization_type_, buff));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

  return serialization::fragment_info_deserialize(
      fragment_info, serialization_type_, uri, returned_data, memory_tracker_);
}

Status RestClientRemote::post_group_metadata_from_rest(
    const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(Status_RestError(
        "Error posting group metadata from REST; group is null."));
  }

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(serialization::group_metadata_serialize(
      group, serialization_type_, buff, false));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &group_ns, &group_uri, get_capabilities_from_rest().legacy_));
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

  return serialization::metadata_deserialize(
      group->unsafe_metadata(),
      group->config(),
      serialization_type_,
      returned_data);
}

Status RestClientRemote::put_group_metadata_to_rest(
    const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(Status_RestError(
        "Error posting group metadata to REST; group is null."));
  }

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(serialization::group_metadata_serialize(
      group, serialization_type_, buff, true));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &group_ns, &group_uri, get_capabilities_from_rest().legacy_));
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

Status RestClientRemote::post_group_create_to_rest(
    const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(
        Status_RestError("Error posting group to REST; group is null."));
  }

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(serialization::group_create_serialize(
      group, serialization_type_, buff, get_capabilities_from_rest().legacy_));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &group_ns, &group_uri, get_capabilities_from_rest().legacy_));
  const std::string cache_key = group_ns + ":" + group_uri;
  RETURN_NOT_OK(
      curlc.init(config_, extra_headers_, &redirect_meta_, &redirect_mtx_));
  const std::string url = redirect_uri(cache_key) + "/v2/groups/" + group_ns;

  // Create the group and check for error
  Buffer returned_data;
  return curlc.post_data(
      stats_, url, serialization_type_, &serialized, &returned_data, cache_key);
}

Status RestClientRemote::post_group_from_rest(const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(
        Status_RestError("Error posting group to REST; group is null."));
  }

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(
      serialization::group_serialize(group, serialization_type_, buff));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &group_ns, &group_uri, get_capabilities_from_rest().legacy_));
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

  return serialization::group_details_deserialize(
      group, serialization_type_, returned_data);
}

Status RestClientRemote::patch_group_to_rest(const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(
        Status_RestError("Error patching group to REST; group is null."));
  }

  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(
      serialization::group_update_serialize(group, serialization_type_, buff));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &group_ns, &group_uri, get_capabilities_from_rest().legacy_));
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

void RestClientRemote::delete_group_from_rest(const URI& uri, bool recursive) {
  // Init curl and form the URL
  Curl curlc(logger_);
  std::string group_ns, group_uri;
  throw_if_not_ok(uri.get_rest_components(
      &group_ns, &group_uri, get_capabilities_from_rest().legacy_));
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

Status RestClientRemote::post_consolidation_to_rest(
    const URI& uri, const Config& config) {
  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(serialization::array_consolidation_request_serialize(
      config, serialization_type_, buff));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

Status RestClientRemote::post_vacuum_to_rest(
    const URI& uri, const Config& config) {
  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  RETURN_NOT_OK(serialization::array_vacuum_request_serialize(
      config, serialization_type_, buff));

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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
RestClientRemote::post_consolidation_plan_from_rest(
    const URI& uri, const Config& config, uint64_t fragment_size) {
  BufferList serialized{memory_tracker_};
  auto& buff = serialized.emplace_buffer();
  serialization::serialize_consolidation_plan_request(
      fragment_size, config, serialization_type_, buff);

  // Init curl and form the URL
  Curl curlc(logger_);
  std::string array_ns, array_uri;
  throw_if_not_ok(uri.get_rest_components(
      &array_ns, &array_uri, get_capabilities_from_rest().legacy_));
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

  return serialization::deserialize_consolidation_plan_response(
      serialization_type_, returned_data);
}

const RestCapabilities& RestClientRemote::get_capabilities_from_rest() const {
  // Return early if already detected REST capabilities for this REST client.
  if (rest_capabilities_.detected_) {
    return rest_capabilities_;
  }

  // Init curl and form the URL
  Curl curlc(logger_);
  std::unordered_map<std::string, std::string> redirect_meta;
  throw_if_not_ok(curlc.init(
      config_, extra_headers_, &redirect_meta, &redirect_mtx_, false));
  const std::string url = rest_server_ + "/v4/capabilities";

  Buffer data;
  try {
    throw_if_not_ok(
        curlc.get_data(stats_, url, serialization_type_, &data, {}));
  } catch (const std::exception& e) {
    std::string msg = e.what();
    if (msg.find("HTTP code 404") != std::string::npos) {
      // If the error was a 404, this indicates a legacy REST server.
      // Legacy REST supports clients <= 2.28.0.
      rest_capabilities_ = RestCapabilities({2, 28, 0}, {2, 0, 0}, true);
    } else {
      // Failed to determine REST capabilities due to an unexpected error.
      throw RestClientException(e.what());
    }
  }

  // Deserialize the response if the request completed against 3.0 REST server.
  if (!rest_capabilities_.legacy_) {
    rest_capabilities_ =
        serialization::rest_capabilities_deserialize(serialization_type_, data);
  }

  return rest_capabilities_;
}

}  // namespace tiledb::sm
