/**
 * @file   rest_client.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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

#include <cassert>

#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/capnp_utils.h"
#include "tiledb/sm/serialization/query.h"

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/rest/curl.h"
#include "tiledb/sm/serialization/tiledb-rest.capnp.h"
#endif

using namespace tiledb::common;

namespace tiledb {
namespace sm {

#ifdef TILEDB_SERIALIZATION

RestClient::RestClient()
    : config_(nullptr)
    , compute_tp_(nullptr)
    , resubmit_incomplete_(true) {
  auto st = utils::parse::convert(
      Config::REST_SERIALIZATION_DEFAULT_FORMAT, &serialization_type_);
  assert(st.ok());
  (void)st;
}

Status RestClient::init(const Config* config, ThreadPool* compute_tp) {
  if (config == nullptr)
    return LOG_STATUS(
        Status::RestError("Error initializing rest client; config is null."));

  config_ = config;
  compute_tp_ = compute_tp;

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

  RETURN_NOT_OK(config_->get("rest.resubmit_incomplete", &c_str));
  if (c_str != nullptr)
    RETURN_NOT_OK(utils::parse::convert(c_str, &resubmit_incomplete_));

  return Status::Ok();
}

Status RestClient::set_header(
    const std::string& name, const std::string& value) {
  extra_headers_[name] = value;
  return Status::Ok();
}

Status RestClient::get_array_schema_from_rest(
    const URI& uri, ArraySchema** array_schema) {
  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = (redirect_uri().empty() ? rest_server_ : redirect_uri()) +
                    "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri);

  // Get the data
  Buffer returned_data;
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);

  RETURN_NOT_OK(curlc.get_data(url, serialization_type_, &returned_data));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status::RestError(
        "Error getting array schema from REST; server returned no data."));

  return serialization::array_schema_deserialize(
      array_schema, serialization_type_, returned_data);
}

Status RestClient::post_array_schema_to_rest(
    const URI& uri, ArraySchema* array_schema) {
  Buffer buff;
  RETURN_NOT_OK(serialization::array_schema_serialize(
      array_schema, serialization_type_, &buff, false));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  redirect_meta_.first =
      (redirect_uri().empty() ? rest_server_ : redirect_uri()) + "/v1/arrays/" +
      array_ns + "/" + curlc.url_escape(array_uri);

  Buffer returned_data;
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);

  Status sc = curlc.post_data(
      redirect_meta_.first, serialization_type_, &serialized, &returned_data);
  return sc;
}

Status RestClient::deregister_array_from_rest(const URI& uri) {
  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = (redirect_uri().empty() ? rest_server_ : redirect_uri()) +
                    "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) + "/deregister";

  Buffer returned_data;
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);

  return curlc.delete_data(url, serialization_type_, &returned_data);
}

Status RestClient::get_array_non_empty_domain(
    Array* array, uint64_t timestamp) {
  if (array == nullptr)
    return LOG_STATUS(
        Status::RestError("Cannot get array non-empty domain; array is null"));
  if (array->array_uri().to_string().empty())
    return LOG_STATUS(Status::RestError(
        "Cannot get array non-empty domain; array URI is empty"));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(array->array_uri().get_rest_components(&array_ns, &array_uri));
  std::string url = (redirect_uri().empty() ? rest_server_ : redirect_uri()) +
                    "/v2/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) +
                    "/non_empty_domain?timestamp=" + std::to_string(timestamp);

  // Get the data
  Buffer returned_data;
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);

  RETURN_NOT_OK(curlc.get_data(url, serialization_type_, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        Status::RestError("Error getting array non-empty domain "
                          "from REST; server returned no data."));

  // Deserialize data returned
  return serialization::nonempty_domain_deserialize(
      array, returned_data, serialization_type_);
}

Status RestClient::get_array_max_buffer_sizes(
    const URI& uri,
    const ArraySchema* schema,
    const void* subarray,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes) {
  // Convert subarray to string for query parameter
  std::string subarray_str;
  RETURN_NOT_OK(subarray_to_str(schema, subarray, &subarray_str));
  std::string subarray_query_param =
      subarray_str.empty() ? "" : ("?subarray=" + subarray_str);

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = (redirect_uri().empty() ? rest_server_ : redirect_uri()) +
                    "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) + "/max_buffer_sizes" +
                    subarray_query_param;

  // Get the data
  Buffer returned_data;
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);

  RETURN_NOT_OK(curlc.get_data(url, serialization_type_, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        Status::RestError("Error getting array max buffer sizes "
                          "from REST; server returned no data."));

  // Deserialize data returned
  return serialization::max_buffer_sizes_deserialize(
      schema, returned_data, serialization_type_, buffer_sizes);
}

Status RestClient::get_array_metadata_from_rest(
    const URI& uri, uint64_t timestamp, Array* array) {
  if (array == nullptr)
    return LOG_STATUS(Status::RestError(
        "Error getting array metadata from REST; array is null."));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = (redirect_uri().empty() ? rest_server_ : redirect_uri()) +
                    "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) +
                    "/array_metadata?timestamp=" + std::to_string(timestamp);

  // Get the data
  Buffer returned_data;
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);

  RETURN_NOT_OK(curlc.get_data(url, serialization_type_, &returned_data));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status::RestError(
        "Error getting array metadata from REST; server returned no data."));

  return serialization::array_metadata_deserialize(
      array, serialization_type_, returned_data);
}

Status RestClient::post_array_metadata_to_rest(const URI& uri, Array* array) {
  if (array == nullptr)
    return LOG_STATUS(Status::RestError(
        "Error posting array metadata to REST; array is null."));

  Buffer buff;
  RETURN_NOT_OK(serialization::array_metadata_serialize(
      array, serialization_type_, &buff));
  // Wrap in a list
  BufferList serialized;
  RETURN_NOT_OK(serialized.add_buffer(std::move(buff)));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = (redirect_uri().empty() ? rest_server_ : redirect_uri()) +
                    "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) + "/array_metadata";

  // Put the data
  Buffer returned_data;
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);

  return curlc.post_data(url, serialization_type_, &serialized, &returned_data);
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
        Status::RestError("Error submitting query to REST; null array."));
  }

  // Serialize query to send
  BufferList serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = (redirect_uri().empty() ? rest_server_ : redirect_uri()) +
                    "/v2/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) +
                    "/query/submit?type=" + query_type_str(query->type()) +
                    "&read_all=" + (resubmit_incomplete_ ? "true" : "false");

  // Remote array reads always supply the timestamp.
  if (query->type() == QueryType::READ)
    url += "&open_at=" + std::to_string(array->timestamp());

  // Create the callback that will process the response buffers as they
  // are received.
  Buffer scratch;
  auto write_cb = std::bind(
      &RestClient::post_data_write_cb,
      this,
      std::placeholders::_1,
      std::placeholders::_2,
      std::placeholders::_3,
      std::placeholders::_4,
      &scratch,
      query,
      copy_state);

  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);
  const Status st = curlc.post_data(
      url, serialization_type_, &serialized, std::move(write_cb));

  if (!st.ok() && copy_state->empty()) {
    return LOG_STATUS(Status::RestError(
        "Error submitting query to REST; "
        "server returned no data. "
        "Curl error: " +
        st.message()));
  }

  return Status::Ok();
}

size_t RestClient::post_data_write_cb(
    const bool reset,
    void* const contents,
    const size_t content_nbytes,
    bool* const skip_retries,
    Buffer* const scratch,
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
    scratch->set_offset(0);
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
        scratch->set_size(scratch->offset());
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
        scratch->set_size(scratch->offset());
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
        scratch->set_size(scratch->offset());
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
  if (scratch->offset() != 0) {
    const uint64_t offset = scratch->offset();
    scratch->set_offset(0);

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
      st = scratch->write(scratch->data(offset), length);
    } else {
      Buffer aux;
      st = aux.write(scratch->data(offset), length);
      if (st.ok()) {
        st = scratch->write(aux.data(), aux.size());
      }
    }

    assert(st.ok());
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
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url = (redirect_uri().empty() ? rest_server_ : redirect_uri()) +
                    "/v1/arrays/" + array_ns + "/" +
                    curlc.url_escape(array_uri) +
                    "/query/finalize?type=" + query_type_str(query->type());
  Buffer returned_data;
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);

  RETURN_NOT_OK(
      curlc.post_data(url, serialization_type_, &serialized, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        Status::RestError("Error finalizing query; server returned no data."));

  // Deserialize data returned
  returned_data.set_offset(0);
  return serialization::query_deserialize(
      returned_data, serialization_type_, true, nullptr, query, compute_tp_);
}

Status RestClient::subarray_to_str(
    const ArraySchema* schema,
    const void* subarray,
    std::string* subarray_str) {
  const auto coords_type = schema->dimension(0)->type();
  const auto dim_num = schema->dim_num();
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
        return LOG_STATUS(Status::RestError(
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
  if (query->type() != QueryType::READ)
    return Status::Ok();

  for (const auto& cit : copy_state) {
    const auto& name = cit.first;
    auto state = cit.second;
    auto query_buffer = query->buffer(name);
    if (query_buffer.buffer_var_size_ != nullptr) {
      *query_buffer.buffer_var_size_ = state.data_size;
      *query_buffer.buffer_size_ = state.offset_size;
    } else if (query_buffer.buffer_size_ != nullptr)
      *query_buffer.buffer_size_ = state.data_size;
  }

  return Status::Ok();
}

Status RestClient::get_query_est_result_sizes(const URI& uri, Query* query) {
  if (query == nullptr)
    return LOG_STATUS(Status::RestError(
        "Error getting query estimated result size from REST; Query is null."));

  // Get array
  const Array* array = query->array();
  if (array == nullptr) {
    return LOG_STATUS(Status::RestError(
        "Error festing query estimated result size from REST; null array."));
  }

  // Serialize query to send
  BufferList serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_, extra_headers_, &redirect_meta_));
  std::string array_ns, array_uri;
  RETURN_NOT_OK(uri.get_rest_components(&array_ns, &array_uri));
  std::string url =
      (redirect_uri().empty() ? rest_server_ : redirect_uri()) + "/v1/arrays/" +
      array_ns + "/" + curlc.url_escape(array_uri) +
      "/query/est_result_sizes?type=" + query_type_str(query->type());

  // Remote array reads always supply the timestamp.
  if (query->type() == QueryType::READ)
    url += "&open_at=" + std::to_string(array->timestamp());

  // Get the data
  Buffer returned_data;
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);

  RETURN_NOT_OK(
      curlc.post_data(url, serialization_type_, &serialized, &returned_data));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status::RestError(
        "Error getting array metadata from REST; server returned no data."));

  return serialization::query_est_result_size_deserialize(
      query, serialization_type_, true, returned_data);
}
std::string RestClient::redirect_uri() {
  std::unique_lock<std::mutex> rd_lck(redirect_mtx_);
  std::unordered_map<std::string, std::string>::const_iterator got =
      redirect_meta_.second.find(constants::redirection_header_key);
  return (got == redirect_meta_.second.end()) ? std::string() : got->second;
}

#else

RestClient::RestClient() {
  (void)config_;
  (void)rest_server_;
  (void)serialization_type_;
}

Status RestClient::init(const Config*, ThreadPool*) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::set_header(const std::string&, const std::string&) {
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

Status RestClient::get_array_non_empty_domain(Array*, uint64_t) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::get_array_max_buffer_sizes(
    const URI&,
    const ArraySchema*,
    const void*,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::get_array_metadata_from_rest(const URI&, uint64_t, Array*) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

Status RestClient::post_array_metadata_to_rest(const URI&, Array*) {
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

Status RestClient::get_query_est_result_sizes(const URI&, Query*) {
  return LOG_STATUS(
      Status::RestError("Cannot use rest client; serialization not enabled."));
}

#endif  // TILEDB_SERIALIZATION

}  // namespace sm
}  // namespace tiledb
