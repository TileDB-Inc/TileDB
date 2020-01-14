/**
 * @file   array_schema.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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

#ifndef TILEDB_SERIALIZATION_ARRAY_SCHEMA_H
#define TILEDB_SERIALIZATION_ARRAY_SCHEMA_H

#include <unordered_map>

#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

class Array;
class Buffer;
class ArraySchema;
enum class SerializationType : uint8_t;

namespace serialization {

Status array_schema_serialize(
    ArraySchema* array_schema,
    SerializationType serialize_type,
    Buffer* serialized_buffer);

Status array_schema_deserialize(
    ArraySchema** array_schema,
    SerializationType serialize_type,
    const Buffer& serialized_buffer);

Status nonempty_domain_serialize(
    const Array* array,
    const void* nonempty_domain,
    bool is_empty,
    SerializationType serialize_type,
    Buffer* serialized_buffer);

Status nonempty_domain_deserialize(
    const Array* array,
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    void* nonempty_domain,
    bool* is_empty);

Status max_buffer_sizes_serialize(
    Array* array,
    const void* subarray,
    SerializationType serialize_type,
    Buffer* serialized_buffer);

Status max_buffer_sizes_deserialize(
    const ArraySchema* schema,
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);

Status array_metadata_serialize(
    Array* array, SerializationType serialize_type, Buffer* serialized_buffer);

Status array_metadata_deserialize(
    Array* array,
    SerializationType serialize_type,
    const Buffer& serialized_buffer);

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_ARRAY_SCHEMA_H
