/**
 * @file   query.h
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
 * This file declares serialization for the Query class
 */

#ifndef TILEDB_SERIALIZATION_QUERY_H
#define TILEDB_SERIALIZATION_QUERY_H

#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/query/query.h"

namespace tiledb {
namespace sm {
namespace serialization {

/**
 * Serialize a query
 *
 * @param query Query to serialize
 * @param serialize_type format to serialize to
 * @param serialized_buffer Buffer to store serialized query
 */
Status query_serialize(
    Query* query,
    SerializationType serialize_type,
    bool clientside,
    Buffer* serialized_buffer);

/**
 * Deserialize a query
 *
 * @param query Query to deserialize into
 * @param serialize_type format to deserialize from
 * @param serialized_buffer Buffer storing serialized query
 */
Status query_deserialize(
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    bool clientside,
    Query* query);

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_QUERY_H
