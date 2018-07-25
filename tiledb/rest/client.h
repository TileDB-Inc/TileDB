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

#include <tiledb/sm/misc/status.h>

/**
 * Get a json encoded array schema from rest server
 *
 * @param rest_server url
 * @param uri of array being loaded
 * @param json_returned string where json response is stored
 * @return Status Ok() on success Error() on failures
 */
tiledb::sm::Status get_array_schema_json_from_rest(
    std::string rest_server, std::string uri, char** json_returned);

/**
 * Post a json array schema to rest server
 *
 * @param rest_server url
 * @param uri of array being created
 * @param json string of json serialized array
 * @return Status Ok() on success Error() on failures
 */
tiledb::sm::Status post_array_schema_json_to_rest(
    std::string rest_server, std::string uri, char* json);

/**
 * Post a json query to rest server
 *
 * @param rest_server url
 * @param uri of array being queried
 * @param json string of json serialized query
 * @param json_returned query json returned from api
 * @return Status Ok() on success Error() on failures
 */
tiledb::sm::Status submit_query_json_to_rest(
    std::string rest_server, std::string uri, char* json, char** json_returned);
#endif  // TILEDB_CLIENT_H
