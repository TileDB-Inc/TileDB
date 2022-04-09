/**
 * @file   array.h
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
 * This file declares serialization functions for Array.
 */

#ifndef TILEDB_SERIALIZATION_ARRAY_H
#define TILEDB_SERIALIZATION_ARRAY_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/array/array.h"
#include "tiledb/sm/buffer/buffer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class Buffer;
class ArraySchema;
class Dimension;
enum class SerializationType : uint8_t;

namespace serialization {

#ifdef TILEDB_SERIALIZATION
/**
 * Serialize an Array to Cap'n proto
 * @param array to serialize
 * @param array_builder cap'n proto class
 * @param cleint_side is serialization client or server side
 * @return Status
 */
Status array_to_capnp(
    Array* array, capnp::Array::Builder* array_builder, const bool client_side);

/**
 * DeSerialize an Array from Cap'n proto
 * @param array_reader cap'n proto class
 * @param array Array to deserialize into
 * @return Status
 */
Status array_from_capnp(const capnp::Array::Reader& array_reader, Array* array);

/**
 * Convert Cap'n Proto message to Array Metadata
 *
 * @param array_metadata_reader cap'n proto class
 * @param metadata metadata deserialize
 * @return Status
 */
Status metadata_from_capnp(
    const capnp::ArrayMetadata::Reader& array_metadata_reader,
    Metadata* metadata);

/**
 * Convert Array Metadata to Cap'n Proto message
 *
 * @param array_metadata_reader cap'n proto class
 * @param metadata metadata to serialize
 * @return Status
 */
Status metadata_to_capnp(
    const Metadata* metadata,
    capnp::ArrayMetadata::Builder* array_metadata_builder);
#endif

Status array_serialize(
    Array* array,
    SerializationType serialize_type,
    Buffer* serialized_buffer,
    const bool client_side);

Status array_deserialize(
    Array* array,
    SerializationType serialize_type,
    const Buffer& serialized_buffer);

Status metadata_serialize(
    Metadata* metadata,
    SerializationType serialize_type,
    Buffer* serialized_buffer);

Status metadata_deserialize(
    Metadata* metadata,
    SerializationType serialize_type,
    const Buffer& serialized_buffer);

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_ARRAY_H
