/**
 * @file   rest_client.h
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
 * This file defines a REST client class.
 */

#ifndef TILEDB_REST_CLIENT_H
#define TILEDB_REST_CLIENT_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/query/query.h"

namespace tiledb {
namespace sm {

class RestClient {
 public:
  /** Constructor. */
  RestClient();

  /** Initialize the REST client with the given config. */
  Status init(const Config* config);

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
   * @param uri of array being loaded
   * @param domain The domain to be retrieved.
   * @param is_empty The function sets it to `1` if the non-empty domain is
   *     empty (i.e., the array does not contain any data yet), and `0`
   * otherwise.
   * @return Status Ok() on success Error() on failures
   */
  Status get_array_non_empty_domain(Array* array, void* domain, bool* is_empty);

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
   * @param query to send to server and store results in, this qill be modified
   * @return Status Ok() on success Error() on failures
   */
  Status finalize_query_to_rest(const URI& uri, Query* query);

 private:
  /** The TileDB config options (contains server and auth info). */
  const Config* config_;

  /** Rest server config param. */
  std::string rest_server_;

  /** Serialization type. */
  SerializationType serialization_type_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_REST_CLIENT_H
