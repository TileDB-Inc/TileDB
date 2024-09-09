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

class Config;
enum class SerializationType : uint8_t;

namespace serialization {

void serialize_delete_fragments_timestamps_request(
    const Config& config,
    uint64_t start_timestamp,
    uint64_t end_timestamp,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer);

std::tuple<uint64_t, uint64_t> deserialize_delete_fragments_timestamps_request(
    SerializationType serialize_type, span<const char> serialized_buffer);

void serialize_delete_fragments_list_request(
    const Config& config,
    const std::vector<URI>& fragments,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer);

std::vector<URI> deserialize_delete_fragments_list_request(
    const URI& array_uri,
    SerializationType serialize_type,
    span<const char> serialized_buffer);

}  // namespace serialization
}  // namespace tiledb::sm

#endif  // TILEDB_SERIALIZATION_FRAGMENTS_H
