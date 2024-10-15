/**
 * @file enumeration.h
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
 * This file declares serialization functions for Enumeration.
 */

#ifndef TILEDB_SERIALIZATION_ENUMERATION_H
#define TILEDB_SERIALIZATION_ENUMERATION_H

#ifdef TILEDB_SERIALIZATION
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/array_schema/enumeration.h"
#include "tiledb/sm/buffer/buffer.h"

using namespace tiledb::common;

namespace tiledb::sm {

enum class SerializationType : uint8_t;

namespace serialization {

#ifdef TILEDB_SERIALIZATION

/**
 * Serialize an Enumeration to cap'n proto object
 *
 * @param enumeration Enumeration to serialize.
 * @param enmr_builder Cap'n proto class.
 */
void enumeration_to_capnp(
    shared_ptr<const Enumeration> enumeration,
    capnp::Enumeration::Builder& enmr_builder);

/**
 * Deserialize an enumeration from a cap'n proto object
 *
 * @param reader Cap'n proto reader object
 * @param memory_tracker The memory tracker associated with the Enumeration
 * object.
 * @return A new Enumeration
 */
shared_ptr<const Enumeration> enumeration_from_capnp(
    const capnp::Enumeration::Reader& reader,
    shared_ptr<MemoryTracker> memory_tracker);

#endif

void serialize_load_enumerations_request(
    const Config& config,
    const std::vector<std::string>& enumeration_names,
    SerializationType serialization_type,
    SerializationBuffer& request);

std::vector<std::string> deserialize_load_enumerations_request(
    SerializationType serialization_type, span<const char> request);

void serialize_load_enumerations_response(
    const std::unordered_map<
        std::string,
        std::vector<shared_ptr<const Enumeration>>>& enumerations,
    SerializationType serialization_type,
    SerializationBuffer& response);

std::unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>
deserialize_load_enumerations_response(
    const Array& array,
    SerializationType serialization_type,
    span<const char> response,
    shared_ptr<MemoryTracker> memory_tracker);

}  // namespace serialization
}  // namespace tiledb::sm

#endif  // TILEDB_SERIALIZATION_ENUMERATION_H
