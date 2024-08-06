/**
 * @file   config.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file declares serialization functions for Config.
 */

#ifndef TILEDB_SERIALIZATION_CONFIG_H
#define TILEDB_SERIALIZATION_CONFIG_H

#include <unordered_map>

#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Config;
class SerializationBuffer;
enum class SerializationType : uint8_t;

namespace serialization {

/**
 * Serialize a config via Cap'n Prto
 *
 * @param Config config object to serialize
 * @param serialize_type format to serialize into Cap'n Proto or JSON
 * @param serialized_buffer buffer to store serialized bytes in
 * @param client_side indicate if client or server side. If server side we won't
 * serialize the array URI
 * @return
 */
Status config_serialize(
    const Config& config,
    SerializationType serialize_type,
    SerializationBuffer& serialized_buffer,
    const bool client_side);

/**
 * Deserialize a config via Cap'n proto
 *
 * @param config to deserialize into
 * @param serialize_type format the data is serialized in Cap'n Proto of JSON
 * @param serialized_buffer buffer to read serialized bytes from
 * @return Status
 */
Status config_deserialize(
    Config** config,
    SerializationType serialize_type,
    span<const char> serialized_buffer);
}  // namespace serialization
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_CONFIG_H
