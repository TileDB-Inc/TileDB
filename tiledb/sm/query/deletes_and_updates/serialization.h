/**
 * @file tiledb/sm/query/deletes_and_updates/serialization.h
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
 * This file contains functions for serializing/deserializing query conditions.
 */

#ifndef TILEDB_CONDITION_SERIALIZATION_H
#define TILEDB_CONDITION_SERIALIZATION_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/update_value.h"

namespace tiledb::sm::deletes_and_updates::serialization {

enum class NodeType : uint8_t { EXPRESSION = 0, VALUE };

/**
 * Serializes the condition.
 *
 * @param query_condition Query condition to serialize.
 * @return Serialized query condition tile.
 */
shared_ptr<WriterTile> serialize_condition(
    const QueryCondition& query_condition,
    shared_ptr<MemoryTracker> memory_tracker);

/**
 * Deserializes the condition.
 *
 * @param condition_index Index for this condition.
 * @param condition_marker Marker used to know which file the condition came
 * from.
 * @param buff Pointer to the serialized data.
 * @param size Size of the serialized data.
 * @return Deserialized query condition.
 */
QueryCondition deserialize_condition(
    const uint64_t condition_index,
    const std::string& condition_marker,
    const void* buff,
    const storage_size_t size);

/**
 * Serializes an update condition and values.
 *
 * @param query_condition Query condition to serialize.
 * @param update_values Update values to serialize.
 * @return Serialized condition and update values tile.
 */
shared_ptr<WriterTile> serialize_update_condition_and_values(
    const QueryCondition& query_condition,
    const std::vector<UpdateValue>& update_values,
    shared_ptr<MemoryTracker> memory_tracker);

/**
 * Deserializes a condition and update values.
 *
 * @param condition_index Index for this condition.
 * @param condition_marker Marker used to know which file the condition came
 * from.
 * @param buff Pointer to the serialized data.
 * @param size Size of the serialized data.
 * @return Deserialized query condition and update values.
 */
tuple<QueryCondition, std::vector<UpdateValue>>
deserialize_update_condition_and_values(
    const uint64_t condition_index,
    const std::string& condition_marker,
    const void* buff,
    const storage_size_t size);

}  // namespace tiledb::sm::deletes_and_updates::serialization

#endif  // TILEDB_CONDITION_SERIALIZATION_H
