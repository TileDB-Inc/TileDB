/**
 * @file   array_schema_evolution.h
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
 * This file declares serialization functions for ArraySchema.
 */

#ifndef TILEDB_SERIALIZATION_ARRAY_SCHEMA_EVOLUTION_H
#define TILEDB_SERIALIZATION_ARRAY_SCHEMA_EVOLUTION_H

#include <unordered_map>

#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class Buffer;
class ArraySchema;
class ArraySchemaEvolution;
class Dimension;
class MemoryTracker;
enum class SerializationType : uint8_t;

namespace serialization {

/**
 * Serialize an array schema evolution via Cap'n Proto
 * @param array_schema_evolution evolution object to serialize
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer to store serialized bytes in
 * @param client_side indicate if client or server side. If server side we won't
 * serialize the array URI
 * @return
 */
Status array_schema_evolution_serialize(
    ArraySchemaEvolution* array_schema_evolution,
    SerializationType serialize_type,
    Buffer* serialized_buffer,
    const bool client_side);

/**
 * Deserialize an array schema evolution via Cap'n Proto
 * @param array_schema_evolution pointer to store evolution object in
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer where serialized bytes are stored
 * @param memory_tracker memory tracker associated with the evolution object
 * @return
 */
Status array_schema_evolution_deserialize(
    ArraySchemaEvolution** array_schema_evolution,
    SerializationType serialize_type,
    const Buffer& serialized_buffer,
    shared_ptr<MemoryTracker> memory_tracker);

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
#endif  // TILEDB_SERIALIZATION_ARRAY_SCHEMA_Evolution_H
