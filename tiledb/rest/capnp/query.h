/**
 * @file   query.h
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
 * This file declares serialization for
 * tiledb::sm::Query
 */

#ifndef TILEDB_REST_CAPNP_QUERY_H
#define TILEDB_REST_CAPNP_QUERY_H

#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/query/query.h"

namespace tiledb {
namespace rest {
/**
 * Serialize a query
 *
 * @param query Query to serialize
 * @param serialize_type format to serialize to
 * @param serialized_string where serialziation will be stored, note this might
 * bytes
 * @param serialized_string_length
 */
tiledb::sm::Status query_serialize(
    tiledb::sm::Query* query,
    tiledb::sm::SerializationType serialize_type,
    char** serialized_string,
    uint64_t* serialized_string_length);

/**
 * Deserialize a query
 *
 * @param query Query to deserialize into
 * @param serialize_type format to deserialize from
 * @param serialized_string where the serialziation is stored
 * @param serialized_string_length
 */
tiledb::sm::Status query_deserialize(
    tiledb::sm::Query* query,
    tiledb::sm::SerializationType serialize_type,
    const char* serialized_string,
    const uint64_t serialized_string_length);
}  // namespace rest
}  // namespace tiledb
#endif  // TILEDB_REST_CAPNP_QUERY_H
