/**
 * @file   vacuum.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2023 TileDB, Inc.
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
 * This file declares serialization for the Vacuum class
 */

#ifndef TILEDB_SERIALIZATION_VACUUM_H
#define TILEDB_SERIALIZATION_VACUUM_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Config;
class SerializationBuffer;
enum class SerializationType : uint8_t;

namespace serialization {

#ifdef TILEDB_SERIALIZATION

/**
 * Convert Cap'n Proto message to Vacuum request
 *
 * @param vacuum_req_reader cap'n proto class.
 * @param config config object to deserialize into.
 * @return Status
 */
Status array_vacuum_request_from_capnp(
    const capnp::ArrayVacuumRequest::Reader& vacuum_req_reader, Config* config);

/**
 * Convert Vacuum Request to Cap'n Proto message.
 *
 * @param config config to serialize info from
 * @param vacuum_req_builder cap'n proto class.
 * @return Status
 */
Status array_vacuum_request_to_capnp(
    const Config& config,
    capnp::ArrayVacuumRequest::Builder* vacuum_req_builder);
#endif

/**
 * Serialize a vacuum request via Cap'n Proto.
 *
 * @param config config object to get info to serialize.
 * @param serialize_type format to serialize into Cap'n Proto or JSON.
 * @param serialized_buffer buffer to store serialized bytes in.
 * @return Status
 */
Status array_vacuum_request_serialize(
    const Config& config,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer);

/**
 * Deserialize vacuum request via Cap'n Proto
 *
 * @param config config object to store the deserialized info in.
 * @param serialize_type format the data is serialized in: Cap'n Proto of JSON.
 * @param serialized_buffer buffer to read serialized bytes from.
 * @return Status
 */
Status array_vacuum_request_deserialize(
    Config** config,
    SerializationType serialize_type,
    span<const char> serialized_buffer);

}  // namespace serialization
}  // namespace sm
}  // namespace tiledb
#endif  // TILEDB_SERIALIZATION_VACUUM_H
