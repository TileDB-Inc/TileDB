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
 * This file defines curl client helper functions.
 */
#ifndef TILEDB_CLIENT_H
#define TILEDB_CLIENT_H

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/query/query.h"

namespace tiledb {
namespace rest {
/**
 * Get a data encoded array schema from rest server
 *
 * @param rest_server url
 * @param uri of array being loaded
 * @param serialization_type format to serialize in
 * @param array_schema array schema to send to server
 * @return Status Ok() on success Error() on failures
 */
tiledb::sm::Status get_array_schema_from_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::ArraySchema** array_schema);

/**
 * Post a data array schema to rest server
 *
 * @param rest_server url
 * @param uri of array being created
 * @param serialization_type format to serialize in
 * @param array_schema array schema to load into
 * @return Status Ok() on success Error() on failures
 */
tiledb::sm::Status post_array_schema_to_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::ArraySchema* array_schema);

/**
 * Get a data encoded array schema from rest server
 *
 * @param rest_server url
 * @param uri of array being loaded
 * @param serialization_type format to serialize in
 * @param array_schema array schema to send to server
 * @return Status Ok() on success Error() on failures
 */
tiledb::sm::Status delete_array_schema_from_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type);

/**
 * Post a data query to rest server
 *
 * @param rest_server url
 * @param uri of array being queried
 * @param serialization_type format to serialize in
 * @param query to send to server and store results in, this qill be modified
 * @return Status Ok() on success Error() on failures
 */
tiledb::sm::Status submit_query_data_to_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Query* query);

/**
 * Post a data query to rest server
 *
 * @param rest_server url
 * @param uri of array being queried
 * @param serialization_type format to serialize in
 * @param query to send to server and store results in, this qill be modified
 * @return Status Ok() on success Error() on failures
 */
tiledb::sm::Status finalize_query_data_to_rest(
    std::string rest_server,
    std::string uri,
    tiledb::sm::SerializationType serialization_type,
    tiledb::sm::Query* query);
}  // namespace rest
}  // namespace tiledb
#endif  // TILEDB_CLIENT_H
