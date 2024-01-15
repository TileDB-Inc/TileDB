/**
 * @file query_plan.h
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
 * This file declares serialization functions for Query Plan.
 */

#ifndef TILEDB_SERIALIZATION_QUERY_PLAN_H
#define TILEDB_SERIALIZATION_QUERY_PLAN_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/query_plan/query_plan.h"

using namespace tiledb::common;

namespace tiledb::sm {

enum class SerializationType : uint8_t;

namespace serialization {

/**
 * Serialize a Query Plan request to cap'n proto object
 *
 * @param config Config to serialize.
 * @param query The query for which the plan is requested.
 * @param serialization_type Format to serialize into: Cap'n Proto or JSON.
 * @param request Buffer to store serialized bytes in.
 */
void serialize_query_plan_request(
    const Config& config,
    Query& query,
    const SerializationType serialization_type,
    Buffer& request);

/**
 * Deserialize a Query Plan request to cap'n proto object
 *
 * @param serialization_type Format to serialize from: Cap'n Proto or JSON.
 * @param request Buffer to read serialized bytes from.
 * @param compute_tp The thread pool for compute-bound tasks.
 * @param query The query for which the plan is requested.
 */
void deserialize_query_plan_request(
    const SerializationType serialization_type,
    const Buffer& request,
    ThreadPool& compute_tp,
    Query& query);

/**
 * Serialize a Query Plan response to cap'n proto object
 *
 * @param query_plan The query plan to serialize.
 * @param serialization_type Format to serialize into: Cap'n Proto or JSON.
 * @param response Buffer to store serialized bytes in.
 */
void serialize_query_plan_response(
    const QueryPlan& query_plan,
    const SerializationType serialization_type,
    Buffer& response);

/**
 * Deserialize a Query Plan response from cap'n proto object
 *
 * @param query The query the plan is requested for.
 * @param serialization_type Format to serialize from: Cap'n Proto or JSON.
 * @param response Buffer to read serialized bytes from.
 * @return The requested query plan.
 */
QueryPlan deserialize_query_plan_response(
    Query& query,
    const SerializationType serialization_type,
    const Buffer& response);

}  // namespace serialization
}  // namespace tiledb::sm

#endif  // TILEDB_SERIALIZATION_QUERY_PLAN_H
