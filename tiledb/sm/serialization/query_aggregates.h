/**
 * @file   query_aggregates.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares serialization functions for Query aggregates.
 */

#ifndef TILEDB_SERIALIZATION_QUERY_AGGREGATES_H
#define TILEDB_SERIALIZATION_QUERY_AGGREGATES_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/serialization/capnp_utils.h"

#endif

namespace tiledb::sm::serialization {

#ifdef TILEDB_SERIALIZATION

/**
 * Convert query channels to Cap'n Proto message
 *
 * @param query A TileDB query
 * @param query_builder cap'n proto class
 */
void query_channels_to_capnp(
    Query& query, capnp::Query::Builder* query_builder);

/**
 * Deserialize query channels from Cap'n Proto message
 *
 * @param query_reader capnp reader
 * @param query deserialized TileDB query
 */
void query_channels_from_capnp(
    const capnp::Query::Reader& query_reader, Query* const query);

#endif

}  // namespace tiledb::sm::serialization

#endif  // TILEDB_SERIALIZATION_QUERY_AGGREGATES_H
