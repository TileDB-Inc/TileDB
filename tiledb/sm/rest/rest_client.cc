/**
 * @file   rest_client.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
#include "tiledb/sm/serialization/fragment_info.h"
#include "tiledb/sm/serialization/group.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/serialization/tiledb-rest.h"
#include "tiledb/sm/rest/curl.h" // must be included last to avoid Windows.h
#endif
// clang-format on

// Something, somewhere seems to be defining TIME_MS as a macro
#if defined(TIME_MS)
#undef TIME_MS
#endif

#include <cassert>

#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/endian.h"
#include "tiledb/sm/misc/parse_argument.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/serialization/array_schema.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

#ifdef TILEDB_SERIALIZATION

RestClient::RestClient()
    : stats_(nullptr)
    , config_(nullptr)
    , compute_tp_(nullptr)
    , resubmit_incomplete_(true) {
  auto st = utils::parse::convert(
      Config::REST_SERIALIZATION_DEFAULT_FORMAT, &serialization_type_);
  assert(st.ok());
  (void)st;
}

Status RestClient::init(
    stats::Stats* const parent_stats,
    const Config* config,
    ThreadPool* compute_tp,
    const std::shared_ptr<Logger>& logger) {
  if (config == nullptr)
    return LOG_STATUS(
        Status_RestError("Error initializing rest client; config is null."));

  stats_ = parent_stats->create_child("RestClient");

  logger_ = logger->clone("curl ", ++logger_id_);

  config_ = config;
  compute_tp_ = compute_tp;

  const char* c_str;
  RETURN_NOT_OK(config_->get("rest.server_address", &c_str));
  if (c_str != nullptr)
    rest_server_ = std::string(c_str);
  if (rest_server_.empty())
    return LOG_STATUS(Status_RestError(
        "Error initializing rest client; server address is empty."));

  RETURN_NOT_OK(config_->get("rest.server_serialization_format", &c_str));
  if (c_str != nullptr)
    RETURN_NOT_OK(serialization_type_enum(c_str, &serialization_type_));

  bool found;
  RETURN_NOT_OK(config_->get<bool>(
      "rest.resubmit_incomplete", &resubmit_incomplete_, &found));

  return Status::Ok();
}

Status RestClient::set_header(
    const std::string& name, const std::string& value) {
  extra_headers_[name] = value;
  return Status::Ok();
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
  return {Status::Ok(),
          make_shared<ArraySchema>(
              HERE(),
              serialization::array_schema_deserialize(
                  serialization_type_, returned_data))};
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

Status RestClient::post_array_from_rest(const URI& uri, Array* array) {
  if (array == nullptr) {
    return LOG_STATUS(Status_SerializationError(
        "Error getting remote array; array is null."));
  }

  Buffer buff;
  RETURN_NOT_OK(
      serialization::array_open_serialize(*array, serialization_type_, &buff));
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
  std::string url = redirect_uri(cache_key) + "/v2/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) + "/?";

  // Remote array operations should provide start and end timestamps
  url += "start_timestamp=" + std::to_string(array->timestamp_start()) +
         "&end_timestamp=" + std::to_string(array->timestamp_end());

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
        "Error getting array from REST; server returned no data."));

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK(ensure_json_null_delimited_string(&returned_data));
  return serialization::array_deserialize(
      array, serialization_type_, returned_data);
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
      array->unsafe_metadata(), serialization_type_, returned_data);
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

  auto rest_scratch = query->rest_scratch();

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
  std::string url = redirect_uri(cache_key) + "/v2/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) +
                    "/query/submit?type=" + query_type_str(query->type()) +
                    "&read_all=" + (resubmit_incomplete_ ? "true" : "false");

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
      // the user buffers are too small to accomodate the attribute
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
      // the user buffers are too small to accomodate the attribute
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

  // If there are unprocessed bytes left in the scratch space, copy them
  // to the beginning of 'scratch'. The intent is to reduce memory
  // consumption by overwriting the serialized query objects that we
  // have already processed.
  const uint64_t length = scratch->size() - scratch->offset();
  if (scratch->offset() != 0 && length != 0) {
    const uint64_t offset = scratch->offset();
    scratch->reset_offset();

    // When the length of the remaining bytes is less than offset,
    // we can safely read the remaining bytes from 'scratch' and
    // write them to the beginning of 'scratch' because there will
    // not be an overlap in accessed memory between the source
    // and destination. Otherwise, we must use an auxilary buffer
    // to temporarily store the remaining bytes because the behavior
    // of the 'memcpy' used 'Buffer::write' will be undefined because
    // there will be an overlap in the memory of the source and
    // destination.
    if (length <= offset) {
      scratch->reset_size();
      st = scratch->write(scratch->data(offset), length);
    } else {
      Buffer aux;
      st = aux.write(scratch->data(offset), length);
      if (st.ok()) {
        scratch->reset_size();
        st = scratch->write(aux.data(), aux.size());
      }
    }

    assert(st.ok());
    if (!st.ok()) {
      LOG_STATUS(st);
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
  const std::string url =
      redirect_uri(cache_key) + "/v1/arrays/" + array_ns + "/" +
      curlc.url_escape(array_uri) +
      "/query/finalize?type=" + query_type_str(query->type());
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
  std::string url =
      redirect_uri(cache_key) + "/v2/arrays/" + array_ns + "/" +
      curlc.url_escape(array_uri) +
      "/query/submit_and_finalize?type=" + query_type_str(query->type());

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
    switch (coords_type) {
      case Datatype::INT8:
        ss << ((const int8_t*)subarray)[i];
        break;
      case Datatype::UINT8:
        ss << ((const uint8_t*)subarray)[i];
        break;
      case Datatype::INT16:
        ss << ((const int16_t*)subarray)[i];
        break;
      case Datatype::UINT16:
        ss << ((const uint16_t*)subarray)[i];
        break;
      case Datatype::INT32:
        ss << ((const int32_t*)subarray)[i];
        break;
      case Datatype::UINT32:
        ss << ((const uint32_t*)subarray)[i];
        break;
      case Datatype::DATETIME_YEAR:
      case Datatype::DATETIME_MONTH:
      case Datatype::DATETIME_WEEK:
      case Datatype::DATETIME_DAY:
      case Datatype::DATETIME_HR:
      case Datatype::DATETIME_MIN:
      case Datatype::DATETIME_SEC:
      case Datatype::DATETIME_MS:
      case Datatype::DATETIME_US:
      case Datatype::DATETIME_NS:
      case Datatype::DATETIME_PS:
      case Datatype::DATETIME_FS:
      case Datatype::DATETIME_AS:
      case Datatype::TIME_HR:
      case Datatype::TIME_MIN:
      case Datatype::TIME_SEC:
      case Datatype::TIME_MS:
      case Datatype::TIME_US:
      case Datatype::TIME_NS:
      case Datatype::TIME_PS:
      case Datatype::TIME_FS:
      case Datatype::TIME_AS:
      case Datatype::INT64:
        ss << ((const int64_t*)subarray)[i];
        break;
      case Datatype::UINT64:
        ss << ((const uint64_t*)subarray)[i];
        break;
      case Datatype::FLOAT32:
        ss << ((const float*)subarray)[i];
        break;
      case Datatype::FLOAT64:
        ss << ((const double*)subarray)[i];
        break;
      default:
        return LOG_STATUS(Status_RestError(
            "Error converting subarray to string; unhandled datatype."));
    }

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

Status RestClient::get_fragment_info(
    const URI& uri, FragmentInfo* fragment_info) {
  if (fragment_info == nullptr)
    return LOG_STATUS(Status_RestError(
        "Error getting fragment info from REST; fragment info is null."));

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
  RETURN_NOT_OK(curlc.get_data(
      stats_, url, serialization_type_, &returned_data, cache_key));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status_RestError(
        "Error getting fragment info from REST; server returned no data."));

  // Ensure data has a null delimiter for cap'n proto if using JSON
  RETURN_NOT_OK(ensure_json_null_delimited_string(&returned_data));
  return serialization::fragment_info_deserialize(
      fragment_info, serialization_type_, returned_data);
}

Status RestClient::post_group_metadata_from_rest(const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(Status_RestError(
        "Error posting group metadata from REST; group is null."));
  }

  Buffer buff;
  RETURN_NOT_OK(serialization::group_metadata_serialize(
      group, serialization_type_, &buff));
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
      group->unsafe_metadata(), serialization_type_, returned_data);
}

Status RestClient::put_group_metadata_to_rest(const URI& uri, Group* group) {
  if (group == nullptr) {
    return LOG_STATUS(Status_RestError(
        "Error posting group metadata to REST; group is null."));
  }

  Buffer buff;
  RETURN_NOT_OK(serialization::group_metadata_serialize(
      group, serialization_type_, &buff));
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

Status RestClient::ensure_json_null_delimited_string(Buffer* buffer) {
  if (serialization_type_ == SerializationType::JSON &&
      buffer->value<char>(buffer->size() - 1) != '\0') {
    RETURN_NOT_OK(buffer->write("\0", sizeof(char)));
  }
  return Status::Ok();
}

#else

RestClient::RestClient() {
  (void)config_;
  (void)rest_server_;
  (void)serialization_type_;
}

Status RestClient::init(
    stats::Stats*, const Config*, ThreadPool*, const std::shared_ptr<Logger>&) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::set_header(const std::string&, const std::string&) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

tuple<Status, optional<shared_ptr<ArraySchema>>>
RestClient::get_array_schema_from_rest(const URI&) {
  return {LOG_STATUS(Status_RestError(
              "Cannot use rest client; serialization not enabled.")),
          nullopt};
}

Status RestClient::post_array_schema_to_rest(const URI&, const ArraySchema&) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::post_array_from_rest(const URI&, Array*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::deregister_array_from_rest(const URI&) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::get_array_non_empty_domain(Array*, uint64_t, uint64_t) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::get_array_max_buffer_sizes(
    const URI&,
    const ArraySchema&,
    const void*,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::get_array_metadata_from_rest(
    const URI&, uint64_t, uint64_t, Array*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::post_array_metadata_to_rest(
    const URI&, uint64_t, uint64_t, Array*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::submit_query_to_rest(const URI&, Query*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::finalize_query_to_rest(const URI&, Query*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::submit_and_finalize_query_to_rest(const URI&, Query*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::get_query_est_result_sizes(const URI&, Query*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::post_array_schema_evolution_to_rest(
    const URI&, ArraySchemaEvolution*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

tuple<Status, std::optional<bool>> RestClient::check_array_exists_from_rest(
    const URI&) {
  return {LOG_STATUS(Status_RestError(
              "Cannot use rest client; serialization not enabled.")),
          std::nullopt};
}

tuple<Status, std::optional<bool>> RestClient::check_group_exists_from_rest(
    const URI&) {
  return {LOG_STATUS(Status_RestError(
              "Cannot use rest client; serialization not enabled.")),
          std::nullopt};
}

Status RestClient::post_group_metadata_from_rest(const URI&, Group*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::get_fragment_info(const URI&, FragmentInfo*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::put_group_metadata_to_rest(const URI&, Group*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::post_group_create_to_rest(const URI&, Group*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::post_group_from_rest(const URI&, Group*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::patch_group_to_rest(const URI&, Group*) {
  return LOG_STATUS(
      Status_RestError("Cannot use rest client; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace sm
}  // namespace tiledb
