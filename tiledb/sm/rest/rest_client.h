/**
 * @file   rest_client.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file defines a REST client class.
 */

#ifndef TILEDB_REST_CLIENT_H
#define TILEDB_REST_CLIENT_H

#include <string>
#include <unordered_map>

#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/stats/stats.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ArraySchema;
class Config;
class Query;

enum class SerializationType : uint8_t;

class RestClient {
 public:
  /** Constructor. */
  RestClient();

  /** Initialize the REST client with the given config. */
  Status init(
      stats::Stats* parent_stats, const Config* config, ThreadPool* compute_tp);

  /** Sets a header that will be attached to all requests. */
  Status set_header(const std::string& name, const std::string& value);

  /**
   * Get a data encoded array schema from rest server
   *
   * @param uri of array being loaded
   * @param array_schema array schema to send to server
   * @return Status Ok() on success Error() on failures
   */
  Status get_array_schema_from_rest(const URI& uri, ArraySchema** array_schema);

  /**
   * Post a data array schema to rest server
   *
   * @param uri of array being created
   * @param array_schema array schema to load into
   * @return Status Ok() on success Error() on failures
   */
  Status post_array_schema_to_rest(const URI& uri, ArraySchema* array_schema);

  /**
   * Deregisters an array at the given URI from the REST server.
   *
   * @param uri Array URI to deregister
   * @return Status
   */
  Status deregister_array_from_rest(const URI& uri);

  /**
   * Get array's non_empty domain from rest server
   *
   * @param array Array model to fetch and set non empty domain on
   * @param timestamp_start Inclusive starting timestamp at which to open array
   * @param timestamp_end Inclusive ending timestamp at which to open array
   * @return Status Ok() on success Error() on failures
   */
  Status get_array_non_empty_domain(
      Array* array, uint64_t timestamp_start, uint64_t timestamp_end);

  /**
   * Get array's max buffer sizes from rest server.
   *
   * @param uri URI of array
   * @param schema Array schema of array
   * @param subarray Subrray to get max buffer sizes for
   * @param buffer_sizes Will be populated with max buffer sizes
   * @return Status
   */
  Status get_array_max_buffer_sizes(
      const URI& uri,
      const ArraySchema* schema,
      const void* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          buffer_sizes);

  /**
   * Gets the array's metadata from the REST server (and updates the in-memory
   * Metadata of the array to match the returned values).
   *
   * @param uri Array URI
   * @param timestamp_start Inclusive starting timestamp at which to open array
   * @param timestamp_end Inclusive ending timestamp at which to open array
   * @param array Array to fetch metadata for
   * @return Status
   */
  Status get_array_metadata_from_rest(
      const URI& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      Array* array);

  /**
   * Posts the array's metadata to the REST server.
   *
   * @param uri Array URI
   * @param timestamp_start Inclusive starting timestamp at which to open array
   * @param timestamp_end Inclusive ending timestamp at which to open array
   * @param array Array to update/post metadata for.
   * @return Status
   */
  Status post_array_metadata_to_rest(
      const URI& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      Array* array);

  /**
   * Post a data query to rest server
   *
   * @param uri of array being queried
   * @param query to send to server and store results in, this qill be modified
   * @return Status Ok() on success Error() on failures
   */
  Status submit_query_to_rest(const URI& uri, Query* query);

  /**
   * Post a data query to rest server
   *
   * @param uri of array being queried
   * @param query to send to server and store results in, this will be modified
   * @return Status Ok() on success Error() on failures
   */
  Status finalize_query_to_rest(const URI& uri, Query* query);

  /**
   * Get array's non_empty domain from rest server
   *
   * @param array Array model to fetch and set non empty domain on
   * @return Status Ok() on success Error() on failures
   */
  Status get_query_est_result_sizes(const URI& uri, Query* query);

  /**
   * Post array schema evolution to rest server
   *
   * @param uri of array being queried
   * @param array_schema_evolution to send to server
   * @return Status Ok() on success Error() on failures
   */
  Status post_array_schema_evolution_to_rest(
      const URI& uri, ArraySchemaEvolution* array_schema_evolution);

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The class stats. */
  stats::Stats* stats_;

  /** The TileDB config options (contains server and auth info). */
  const Config* config_;

  /** The thread pool for compute-bound tasks. */
  ThreadPool* compute_tp_;

  /** Rest server config param. */
  std::string rest_server_;

  /** Serialization type. */
  SerializationType serialization_type_;

  /**
   * If true (the default), automatically resubmit incomplete queries on the
   * server-side. This guarantees that the user only receive a complete query
   * result from the server.
   *
   * When this is turned on, it is currently an error if the user buffers on the
   * client are too small to receive all data received from the server
   * (regardless of how many times the query is resubmitted).
   */
  bool resubmit_incomplete_;

  /** Collection of extra headers that are attached to REST requests. */
  std::unordered_map<std::string, std::string> extra_headers_;

  /** Array URI to redirected server mapping. */
  std::unordered_map<std::string, std::string> redirect_meta_;

  /** Mutex for thread-safety. */
  mutable std::mutex redirect_mtx_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * POSTs a query submit request to the REST server and deserializes the
   * response into the same query object.
   *
   * For read queries, this also updates the given copy state with the number of
   * bytes copied for each attribute, which allows for automatic resubmission of
   * incomplete queries while concatenating to the user buffers.
   *
   * @param uri URI of array being queried
   * @param query Query to send to server and store results in, this will be
   *    modified.
   * @param copy_state Map of copy state per attribute. As attribute data is
   *    copied into user buffers on reads, the state of each attribute in this
   *    map is updated accordingly.
   * @return
   */
  Status post_query_submit(
      const URI& uri, Query* query, serialization::CopyState* copy_state);

  /**
   * Callback to invoke as partial, buffered response data is received from
   * posting a query.
   *
   * This is not thread-safe. It expects the response data to be ordered. The
   * response must contain serialized query objects, prefixed by an 8-byte
   * unsigned integer that contains the byte-size of the serialized query object
   * it is prefixing. The scratch space must be empty before the first
   * invocation, and must not change until the last invocation has completed.
   *
   * @param reset True if the callback must wipe the in-memory state
   * @param contents the partial response data
   * @param content_nbytes the size of the response data in 'contents'
   * @param skip_retries Output argument that can be set to true to
   *    prevent the curl layer from retrying this request.
   * @scratch scratch space to use between invocations of this callback
   * @query the query object used for deserializing the serialized query
   *    objects in the response data.
   * @param copy_state Map of copy state per attribute. As attribute data is
   *    copied into user buffers on reads, the state of each attribute in this
   *    map is updated accordingly.
   * @return Number of acknowledged bytes
   */
  size_t post_data_write_cb(
      bool reset,
      void* contents,
      size_t content_nbytes,
      bool* skip_retries,
      Buffer* scratch,
      Query* query,
      serialization::CopyState* copy_state);

  /**
   * Returns a string representation of the given subarray. The format is:
   *
   *   "dim0min,dim0max,dim1min,dim1max,..."
   *
   * @param schema Array schema to use for domain information
   * @param subarray Subarray to convert to string
   * @param subarray_str Will be set to the CSV string
   * @return Status
   */
  static Status subarray_to_str(
      const ArraySchema* schema,
      const void* subarray,
      std::string* subarray_str);

  /**
   * Sets the buffer sizes on the given query using the given state mapping (per
   * attribute). Applicable only when deserializing read queries on the client.
   *
   * @param copy_state State map of attribute to buffer sizes.
   * @param query Query to update buffers for
   * @return Status
   */
  Status update_attribute_buffer_sizes(
      const serialization::CopyState& copy_state, Query* query) const;

  /**
   * Helper function encapsulating the functionality of looking up for cached
   * redirected rest server addresses to avoid the redirection overhead
   *
   * @param array_ns Array namespace
   * @param array_uri Array URI
   * @return Returns the redirection URI if exists and empty string otherwise
   */
  std::string redirect_uri(const std::string& cache_key);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_REST_CLIENT_H
