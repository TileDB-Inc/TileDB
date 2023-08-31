/**
 * @file   fragments.h
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
 * This file declares serialization functions for fragments.
 */

#ifndef TILEDB_SERIALIZATION_FRAGMENTS_H
#define TILEDB_SERIALIZATION_FRAGMENTS_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filesystem/uri.h"

using namespace tiledb::common;
namespace tiledb::sm {

class Buffer;
enum class SerializationType : uint8_t;

namespace serialization {

#ifdef TILEDB_SERIALIZATION
/**
 * Convert ArrayDeleteFragmentsTimestampsRequest to Cap'n Proto message
 *
 * @param uri the URI of the fragments' parent array
 * @param start_timestamp the start timestamp to serialize
 * @param end_timestamp the end timestamp to serialize
 * @param builder cap'n proto class
 */
void fragments_timestamps_to_capnp(
    std::string uri,
    uint64_t start_timestamp,
    uint64_t end_timestamp,
    capnp::ArrayDeleteFragmentsTimestampsRequest::Builder* builder);

/**
 * Convert Cap'n Proto message to ArrayDeleteFragmentsTimestampsRequest
 *
 * @param reader cap'n proto class
 * @return a tuple of uri, start_timestamp, end_timestamp
 */
std::tuple<const char*, uint64_t, uint64_t> fragments_timestamps_from_capnp(
    const capnp::ArrayDeleteFragmentsTimestampsRequest::Reader& reader);

/**
 * Convert ArrayDeleteFragmentsListRequest to Cap'n Proto message
 *
 * @param fragments fragments to serialize
 * @param builder cap'n proto class
 */
void fragments_list_to_capnp(
    const std::vector<URI>& fragments,
    capnp::ArrayDeleteFragmentsListRequest::Builder* builder);

/**
 * Convert Cap'n Proto message to ArrayDeleteFragmentsListRequest
 *
 * @param reader cap'n proto class
 * @param array_uri uri of the array that the fragments belong to
 * @return vector of deserialized fragments
 */
std::vector<URI> fragments_list_from_capnp(
    const capnp::ArrayDeleteFragmentsListRequest::Reader& reader,
    const URI& array_uri);
#endif

void fragments_timestamps_serialize(
    std::string uri,
    uint64_t start_timestamp,
    uint64_t end_timestamp,
    SerializationType serialize_type,
    Buffer* serialized_buffer);

std::tuple<const char*, uint64_t, uint64_t> fragments_timestamps_deserialize(
    SerializationType serialize_type, const Buffer& serialized_buffer);

void fragments_list_serialize(
    const std::vector<URI>& fragments,
    SerializationType serialize_type,
    Buffer* serialized_buffer);

std::vector<URI> fragments_list_deserialize(
    const URI& array_uri,
    SerializationType serialize_type,
    const Buffer& serialized_buffer);

}  // namespace serialization
}  // namespace tiledb::sm

#endif  // TILEDB_SERIALIZATION_FRAGMENTS_H
