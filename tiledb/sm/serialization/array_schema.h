/**
 * @file   array_schema.h
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

#ifndef TILEDB_SERIALIZATION_ARRAY_SCHEMA_H
#define TILEDB_SERIALIZATION_ARRAY_SCHEMA_H

#include <unordered_map>

#include "tiledb/common/status.h"

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

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
 * Serialize a filter to cap'n proto object
 *
 * @param filter Filter to serialize.
 * @param filter_builder Cap'n proto class.
 * @return Status
 */
Status filter_to_capnp(
    const Filter* filter, capnp::Filter::Builder* filter_builder);

/**
 * Deserialize a filter from a cap'n proto object
 *
 * @param filter_reader Cap'n proto object
 * @return Status
 */
tuple<Status, optional<shared_ptr<Filter>>> filter_from_capnp(
    const capnp::Filter::Reader& filter_reader);

/**
 * Serialize an array schema to cap'n proto object
 *
 * @param array_schema Array schema to serialize
 * @param array_schema_builder Cap'n proto class
 * @param client_side indicate if client or server side. If server side we won't
 * serialize the array URI
 * @return Status
 */
Status array_schema_to_capnp(
    const ArraySchema& array_schema,
    capnp::ArraySchema::Builder* array_schema_builder,
    const bool client_side);

/**
 * Deserialize an array schema from a cap'n proto object
 *
 * @param schema_reader Cap'n proto object
 * @param uri A URI object
 * @return a new ArraySchema
 */
ArraySchema array_schema_from_capnp(
    const capnp::ArraySchema::Reader& schema_reader, const URI& uri);

#endif  // TILEDB_SERIALIZATION

/**
 * Serialize an array schema via Cap'n Prto
 * @param array_schema schema object to serialize
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer to store serialized bytes in
 * @param client_side indicate if client or server side. If server side we won't
 * serialize the array URI
 * @return
 */
Status array_schema_serialize(
    const ArraySchema& array_schema,
    SerializationType serialize_type,
    Buffer* serialized_buffer,
    const bool client_side);

ArraySchema array_schema_deserialize(
    SerializationType serialize_type, const Buffer& serialized_buffer);

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

Status nonempty_domain_serialize(
    Array* array, SerializationType serialize_type, Buffer* serialized_buffer);

Status nonempty_domain_deserialize(
    Array* array,
    const Buffer& serialized_buffer,
    SerializationType serialize_type);

Status max_buffer_sizes_serialize(
    Array* array,
    const void* subarray,
    SerializationType serialize_type,
    Buffer* serialized_buffer);

Status max_buffer_sizes_deserialize(
    const ArraySchema& schema,
    const Buffer& serialized_buffer,
    SerializationType serialize_type,
    std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
        buffer_sizes);

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_ARRAY_SCHEMA_H
