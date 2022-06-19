/**
 * @file tiledb/sm/query/delete_condition/serialization.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file contains functions for parsing URIs for storage of an array.
 */

#ifndef TILEDB_PARSE_URI_H
#define TILEDB_PARSE_URI_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/query/query_condition.h"

namespace tiledb::sm::delete_condition::serialize {

/**
 * Serializes the delete condition.
 *
 * @param query_condition Query condition to serialize.
 * @return Serialized query condition.
 */
std::vector<uint8_t> serialize_delete_condition(
    const QueryCondition& query_condition);

/**
 * Serializes the delete condition.
 *
 * @param buff Serialized query condition.
 * @return Deserialized query condition.
 */
QueryCondition deserialize_delete_condition(const std::vector<uint8_t>& buff);

}  // namespace tiledb::sm::delete_condition::serialize

#endif  // TILEDB_PARSE_URI_H
